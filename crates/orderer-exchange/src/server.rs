//! A small framed TCP service next to a matching-group replica, for the
//! ledger's relays.
//!
//! Frame: `len u32 | crc32c u32 | kind u8 | body` (the envelope of the replica
//! transport; `len` counts kind and body, plus a MAC with a key). Connections
//! start with the handshake of `secure.rs` (pre-shared key and configuration
//! checksum); with no key configured the port is unauthenticated, so run it on a
//! private network.
//!
//! Requests (all integers little-endian):
//! - `FORWARD (1)`: `shard u16 | first_idx u64 | n u32 | n x message` (the body of a
//!   tag-5 entry from its shard on). The leader proposes it and waits until it is
//!   applied. Reply `CURSOR`.
//! - `CURSOR (2)`: `shard u16 [| lin u8]`. A linearizable read (committed through the
//!   log first) unless the optional byte is 0, which reads this replica's applied
//!   state at once. Reply `CURSOR`.
//! - `READ (3)`: `source u32 | from u64 | max u32`. Served by any replica from its
//!   applied state, no log round trip. Reply `EVENTS` or `TRUNCATED`.
//!
//! Replies: `CURSOR (1)`: `cursor u64 | gap u8` (the next outbox index expected;
//! `gap` is 1 when the request skipped ahead of it and was refused);
//! `EVENTS (2)`: `end u64 | n u32 | n x (idx u64 | event)` (`end` is the last
//! index of the stream, so `idx` below it means more to read);
//! `TRUNCATED (3)`: `base u64`; `NOT_LEADER (4)`: `hint i64` (-1 unknown);
//! `BUSY (5)`; `UNKNOWN (6)` (the proposal may have committed: retry, the intake
//! makes that safe); `ERR (7)`: text.

use crate::archive::{TierView, Truncated};
use crate::secure::{Auth, Link, Peer};
use crate::wire::{encode_forward, get_event, put_event, Forward, Rd, MAX_BATCH, MSG_BYTES};
use orderer_cluster::{ProposeError, ReplicaHandle};
use orderer_ledger::{Event, OutMsg};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Separates this port's sessions from the gateway's.
const LABEL: &[u8] = b"xchg";
const Q_FORWARD: u8 = 1;
const Q_CURSOR: u8 = 2;
const Q_READ: u8 = 3;
const R_CURSOR: u8 = 1;
const R_EVENTS: u8 = 2;
const R_TRUNCATED: u8 = 3;
const R_NOT_LEADER: u8 = 4;
const R_BUSY: u8 = 5;
const R_UNKNOWN: u8 = 6;
const R_ERR: u8 = 7;

pub struct ExchangeServer {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    pub addr: SocketAddr,
}

impl ExchangeServer {
    pub fn start(
        addr: SocketAddr,
        replica: ReplicaHandle,
        view: TierView,
    ) -> io::Result<ExchangeServer> {
        Self::start_with(addr, replica, view, Auth::none())
    }

    /// With `auth`, clients must hold the same key and config checksum.
    pub fn start_with(
        addr: SocketAddr,
        replica: ReplicaHandle,
        view: TierView,
        auth: Auth,
    ) -> io::Result<ExchangeServer> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let thread = thread::spawn(move || {
            while !s2.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((conn, _)) => {
                        let (h, v, stop) = (replica.clone(), view.clone(), s2.clone());
                        let auth = auth.clone();
                        thread::spawn(move || {
                            let _ = serve(conn, h, v, stop, auth);
                        });
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(_) => thread::sleep(Duration::from_millis(20)),
                }
            }
        });
        Ok(ExchangeServer {
            stop,
            thread: Some(thread),
            addr,
        })
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn serve(
    conn: TcpStream,
    h: ReplicaHandle,
    v: TierView,
    stop: Arc<AtomicBool>,
    auth: Auth,
) -> io::Result<()> {
    conn.set_nonblocking(false)?;
    conn.set_nodelay(true)?;
    conn.set_read_timeout(Some(Duration::from_millis(200)))?;
    let mut conn = Peer::accept(conn, &auth, LABEL)?;
    while !stop.load(Ordering::Relaxed) {
        let (kind, body) = match conn.recv() {
            Ok(x) => x,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue
            }
            Err(e) => return Err(e),
        };
        let (rk, rb) = handle(kind, &body, &h, &v);
        conn.send(rk, &rb)?;
    }
    Ok(())
}

fn refusal(e: ProposeError) -> (u8, Vec<u8>) {
    match e {
        ProposeError::NotLeader(hint) => (
            R_NOT_LEADER,
            hint.map(|n| n as i64).unwrap_or(-1).to_le_bytes().to_vec(),
        ),
        ProposeError::Busy | ProposeError::Stopped => (R_BUSY, vec![]),
        ProposeError::Timeout | ProposeError::Unknown | ProposeError::Superseded => {
            (R_UNKNOWN, vec![])
        }
        #[allow(unreachable_patterns)]
        _ => (R_ERR, b"propose failed".to_vec()),
    }
}

fn wait_applied(h: &ReplicaHandle, index: u64) -> bool {
    let end = Instant::now() + Duration::from_secs(30);
    while h.status().applied < index {
        if Instant::now() > end {
            return false;
        }
        thread::sleep(Duration::from_micros(200));
    }
    true
}

fn cursor_reply(cursor: u64, gap: bool) -> (u8, Vec<u8>) {
    let mut b = cursor.to_le_bytes().to_vec();
    b.push(gap as u8);
    (R_CURSOR, b)
}

fn handle(kind: u8, body: &[u8], h: &ReplicaHandle, v: &TierView) -> (u8, Vec<u8>) {
    let err = |m: &str| (R_ERR, m.as_bytes().to_vec());
    let mut r = Rd::new(body);
    match kind {
        Q_FORWARD => {
            // Re-frame as a tag-5 entry; decoding it here validates the request.
            let (Ok(shard), Ok(first), Ok(n)) = (r.u16(), r.u64(), r.u32()) else {
                return err("short forward");
            };
            let n = n as usize;
            if n > MAX_BATCH || r.left() != n * MSG_BYTES {
                return err("forward length");
            }
            let mut entry = Vec::with_capacity(16 + r.left());
            entry.extend_from_slice(&[crate::wire::TAG_FORWARD, 1]);
            entry.extend_from_slice(&shard.to_le_bytes());
            entry.extend_from_slice(&first.to_le_bytes());
            entry.extend_from_slice(&(n as u32).to_le_bytes());
            entry.extend_from_slice(body.get(14..).unwrap_or(&[]));
            if crate::wire::decode_forward(&entry).is_err() {
                return err("bad forward");
            }
            match h.propose(entry) {
                Ok(index) => {
                    if !wait_applied(h, index) {
                        return (R_UNKNOWN, vec![]);
                    }
                    let cursor = v.cursor(shard);
                    cursor_reply(cursor, first > cursor)
                }
                Err(e) => refusal(e),
            }
        }
        Q_CURSOR => {
            let Ok(shard) = r.u16() else {
                return err("short cursor");
            };
            // An optional third byte `0` asks for the applied state of this replica
            // with no round trip through the log (a cheap, possibly slightly stale read,
            // for observers); anything else, or no byte, is the linearizable read.
            if body.get(2) == Some(&0) {
                return cursor_reply(v.cursor(shard), false);
            }
            match h.barrier() {
                Ok(_) => cursor_reply(v.cursor(shard), false),
                Err(e) => refusal(e),
            }
        }
        Q_READ => {
            let (Ok(source), Ok(from), Ok(max)) = (r.u32(), r.u64(), r.u32()) else {
                return err("short read");
            };
            match v.read(source, from, (max as usize).min(65_536)) {
                None => err("unknown source"),
                Some(Err(Truncated { base })) => (R_TRUNCATED, base.to_le_bytes().to_vec()),
                Some(Ok(evs)) => {
                    let mut b = Vec::new();
                    b.extend_from_slice(&v.end(source).unwrap_or(0).to_le_bytes());
                    b.extend_from_slice(&(evs.len() as u32).to_le_bytes());
                    for (idx, e) in &evs {
                        b.extend_from_slice(&idx.to_le_bytes());
                        put_event(&mut b, e);
                    }
                    (R_EVENTS, b)
                }
            }
        }
        _ => err("unknown request"),
    }
}

/// Why a request was not served.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    NotLeader(Option<u64>),
    Busy,
    /// May have committed; retrying is safe.
    Unknown,
    Truncated {
        base: u64,
    },
    Error(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForwardAck {
    pub cursor: u64,
    pub gap: bool,
}

pub struct ExchangeClient {
    conn: Link,
}

impl ExchangeClient {
    pub fn connect(addr: SocketAddr) -> io::Result<ExchangeClient> {
        Self::connect_with(addr, &Auth::none())
    }

    /// Connect and complete the handshake: fails if the server does not hold the
    /// same key and configuration.
    pub fn connect_with(addr: SocketAddr, auth: &Auth) -> io::Result<ExchangeClient> {
        let conn = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
        let conn = Link::open(conn, auth, LABEL, Duration::from_secs(60))?;
        Ok(ExchangeClient { conn })
    }

    fn call(&mut self, kind: u8, body: &[u8]) -> io::Result<(u8, Vec<u8>)> {
        self.conn.call(kind, body)
    }

    fn refused(kind: u8, b: &[u8]) -> Refusal {
        match kind {
            R_NOT_LEADER => {
                let h = i64::from_le_bytes(b.try_into().unwrap_or([0xff; 8]));
                Refusal::NotLeader((h >= 0).then_some(h as u64))
            }
            R_BUSY => Refusal::Busy,
            R_UNKNOWN => Refusal::Unknown,
            R_TRUNCATED => Refusal::Truncated {
                base: u64::from_le_bytes(b.try_into().unwrap_or([0; 8])),
            },
            _ => Refusal::Error(String::from_utf8_lossy(b).into_owned()),
        }
    }

    fn cursor_of(kind: u8, b: &[u8]) -> Result<ForwardAck, Refusal> {
        if kind == R_CURSOR && b.len() == 9 {
            Ok(ForwardAck {
                cursor: u64::from_le_bytes(b[..8].try_into().unwrap()),
                gap: b[8] == 1,
            })
        } else {
            Err(Self::refused(kind, b))
        }
    }

    /// Deliver `msgs` (outbox indices `first_idx..`) of ledger `shard`.
    pub fn forward(
        &mut self,
        shard: u16,
        first_idx: u64,
        msgs: &[OutMsg],
    ) -> io::Result<Result<ForwardAck, Refusal>> {
        let entry = encode_forward(&Forward {
            shard,
            first_idx,
            msgs: msgs.to_vec(),
        });
        // The request body is the entry without its tag and version bytes.
        let (k, b) = self.call(Q_FORWARD, &entry[2..])?;
        Ok(Self::cursor_of(k, &b))
    }

    /// The next outbox index matching expects from `shard` (linearizable).
    pub fn cursor(&mut self, shard: u16) -> io::Result<Result<u64, Refusal>> {
        let (k, b) = self.call(Q_CURSOR, &shard.to_le_bytes())?;
        Ok(Self::cursor_of(k, &b).map(|a| a.cursor))
    }

    /// The same from the applied state of the replica that answers, without a
    /// round trip through the log: cheap and possibly a little stale, for observers.
    pub fn cursor_local(&mut self, shard: u16) -> io::Result<Result<u64, Refusal>> {
        let mut body = shard.to_le_bytes().to_vec();
        body.push(0);
        let (k, b) = self.call(Q_CURSOR, &body)?;
        Ok(Self::cursor_of(k, &b).map(|a| a.cursor))
    }

    /// Events of stream `source` from `from`: `(events, end)` where `end` is the
    /// stream's last index.
    #[allow(clippy::type_complexity)]
    pub fn read(
        &mut self,
        source: u32,
        from: u64,
        max: u32,
    ) -> io::Result<Result<(Vec<(u64, Event)>, u64), Refusal>> {
        let mut body = source.to_le_bytes().to_vec();
        body.extend_from_slice(&from.to_le_bytes());
        body.extend_from_slice(&max.to_le_bytes());
        let (k, b) = self.call(Q_READ, &body)?;
        if k != R_EVENTS {
            return Ok(Err(Self::refused(k, &b)));
        }
        let bad = || io::Error::new(io::ErrorKind::InvalidData, "events reply");
        let mut r = Rd::new(&b);
        let end = r.u64().map_err(|_| bad())?;
        let n = r.u32().map_err(|_| bad())? as usize;
        let mut out = Vec::with_capacity(n.min(65_536));
        for _ in 0..n {
            let idx = r.u64().map_err(|_| bad())?;
            out.push((idx, get_event(&mut r).map_err(|_| bad())?));
        }
        r.done().map_err(|_| bad())?;
        Ok(Ok((out, end)))
    }
}
