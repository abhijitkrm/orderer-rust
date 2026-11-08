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
//! - `SPAN (7)`: `shard u16 | first u64 | end u64 | n u32 | n x (offset u32 | message)`
//!   (the body of a tag-5 version-2 entry, `wire::Span`): the thinned forward. Reply
//!   `CURSOR`; `gap` is 1 when `first` is above the cursor.
//! - `SLICE (4)`: a credit-slice request (the whole tag-6 entry, `wire::SliceReq`), for
//!   a group in credit mode. The leader proposes it, waits until it is applied and
//!   answers with the result recorded for its `(client, seq)`. The retry key is the
//!   pair: a client repeats the same request until it gets a definite answer; the
//!   group applies only a sequence number above the last it saw for that client, so
//!   a retry never enters an order twice. Reply `SLICE` (or `STALE` for a sequence
//!   number the group has already moved past).
//! - `SLICE_STATE (5)`: linearizable read of the slice: reply `SLICE_STATE`.
//! - `TICK (6)`: propose an epoch tick (tag 7), wait for it, reply `OK`.
//!
//! Replies: `CURSOR (1)`: `cursor u64 | gap u8` (the next outbox index expected;
//! `gap` is 1 when the request skipped ahead of it and was refused);
//! `EVENTS (2)`: `end u64 | n u32 | n x (idx u64 | event)` (`end` is the last
//! index of the stream, so `idx` below it means more to read);
//! `TRUNCATED (3)`: `base u64`; `NOT_LEADER (4)`: `hint i64` (-1 unknown);
//! `BUSY (5)`; `UNKNOWN (6)` (the proposal may have committed: retry, the intake
//! makes that safe); `ERR (7)`: text; `SLICE (8)`: `kind u8 (0 entered, 1 refused) |
//! value u64` (the order id, or the `credit::reject_code`); `STALE (9)`: `last u64`;
//! `SLICE_STATE (10)`: `end u64 | behind u8 | nd u32 | nd x (account u64 | asset u32) | slice
//! snapshot`; `OK (11)`.

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
const Q_SLICE: u8 = 4;
const Q_SLICE_STATE: u8 = 5;
const Q_TICK: u8 = 6;
const Q_SPAN: u8 = 7;
const R_CURSOR: u8 = 1;
const R_EVENTS: u8 = 2;
const R_TRUNCATED: u8 = 3;
const R_NOT_LEADER: u8 = 4;
const R_BUSY: u8 = 5;
const R_UNKNOWN: u8 = 6;
const R_ERR: u8 = 7;
const R_SLICE: u8 = 8;
const R_STALE: u8 = 9;
const R_SLICE_STATE: u8 = 10;
const R_OK: u8 = 11;

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
        Q_SPAN => {
            let mut entry = vec![crate::wire::TAG_FORWARD, 2];
            entry.extend_from_slice(body);
            let Ok(sp) = crate::wire::decode_span(&entry) else {
                return err("bad span");
            };
            match h.propose(entry) {
                Ok(index) => {
                    if !wait_applied(h, index) {
                        return (R_UNKNOWN, vec![]);
                    }
                    let cursor = v.cursor(sp.shard);
                    cursor_reply(cursor, sp.first > cursor)
                }
                Err(e) => refusal(e),
            }
        }
        Q_SLICE => {
            let Ok(q) = crate::wire::decode_slice_req(body) else {
                return err("bad slice request");
            };
            match h.propose(body.to_vec()) {
                Ok(index) => {
                    if !wait_applied(h, index) {
                        return (R_UNKNOWN, vec![]);
                    }
                    match v.slice_result(q.client) {
                        Some((seq, res)) if seq == q.seq => {
                            let (kind, val) = match res {
                                Ok(order) => (0u8, order),
                                Err(rej) => (1u8, crate::tier::credit::reject_code(rej) as u64),
                            };
                            let mut b = vec![kind];
                            b.extend_from_slice(&val.to_le_bytes());
                            (R_SLICE, b)
                        }
                        Some((seq, _)) if seq > q.seq => (R_STALE, seq.to_le_bytes().to_vec()),
                        _ => err("slice request not applied (is the group in credit mode?)"),
                    }
                }
                Err(e) => refusal(e),
            }
        }
        Q_SLICE_STATE => match h.barrier() {
            Ok(_) => match v.slice_state() {
                Some((snap, end, demand, behind)) => {
                    let mut b = end.to_le_bytes().to_vec();
                    b.push(behind as u8);
                    b.extend_from_slice(&(demand.len() as u32).to_le_bytes());
                    for (a, asset) in demand {
                        b.extend_from_slice(&a.to_le_bytes());
                        b.extend_from_slice(&asset.to_le_bytes());
                    }
                    b.extend_from_slice(&snap);
                    (R_SLICE_STATE, b)
                }
                None => err("the group is not in credit mode"),
            },
            Err(e) => refusal(e),
        },
        Q_TICK => match h.propose(crate::wire::encode_epoch_tick()) {
            Ok(index) => {
                if !wait_applied(h, index) {
                    return (R_UNKNOWN, vec![]);
                }
                (R_OK, vec![])
            }
            Err(e) => refusal(e),
        },
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

/// The group's answer to a slice request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SliceAnswer {
    /// The order id (for a cancel, the id that was cancelled).
    Entered(u64),
    Refused(orderer_ledger::Reject),
    /// The group has already seen a later request from this client: the answer to
    /// this one was overwritten (a client that sends one at a time never gets this).
    Stale(u64),
}

/// A read of the group's credit slice.
#[derive(Clone, Debug)]
pub struct SliceState {
    pub snapshot: Vec<u8>,
    pub group_end: u64,
    /// The matcher has emitted events the slice has not settled yet.
    pub behind: bool,
    pub demand: Vec<(u64, u32)>,
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

    /// Deliver a thinned span of ledger `shard`'s outbox: `msgs` are the messages
    /// for this group among the indices `first..end`.
    pub fn forward_span(
        &mut self,
        shard: u16,
        first: u64,
        end: u64,
        msgs: &[(u64, OutMsg)],
    ) -> io::Result<Result<ForwardAck, Refusal>> {
        let entry = crate::wire::encode_span(&crate::wire::Span {
            shard,
            first,
            end,
            msgs: msgs.to_vec(),
        });
        let (k, b) = self.call(Q_SPAN, &entry[2..])?;
        Ok(Self::cursor_of(k, &b))
    }

    /// Enter or cancel an order at a credit-mode group. The answer is definite
    /// (`Entered`/`Refused`/`Stale`) or the request may or may not have been applied
    /// (`Refusal::Unknown`: repeat the same `(client, seq)`).
    pub fn slice_request(
        &mut self,
        q: &crate::wire::SliceReq,
    ) -> io::Result<Result<SliceAnswer, Refusal>> {
        let (k, b) = self.call(Q_SLICE, &crate::wire::encode_slice_req(q))?;
        Ok(match k {
            R_SLICE if b.len() == 9 => {
                let val = u64::from_le_bytes(b[1..9].try_into().unwrap());
                match b[0] {
                    0 => Ok(SliceAnswer::Entered(val)),
                    1 => match crate::tier::credit::reject_from_code(val as u8) {
                        Some(r) => Ok(SliceAnswer::Refused(r)),
                        None => Err(Refusal::Error("unknown reject code".into())),
                    },
                    _ => Err(Refusal::Error("bad slice reply".into())),
                }
            }
            R_STALE if b.len() == 8 => Ok(SliceAnswer::Stale(u64::from_le_bytes(
                b[..8].try_into().unwrap(),
            ))),
            _ => Err(Self::refused(k, &b)),
        })
    }

    /// The credit slice, linearizable: its snapshot (see `Slice::restore`), the last
    /// index of the group stream, and the demand list.
    #[allow(clippy::type_complexity)]
    pub fn slice_state(&mut self) -> io::Result<Result<SliceState, Refusal>> {
        let (k, b) = self.call(Q_SLICE_STATE, &[])?;
        if k != R_SLICE_STATE {
            return Ok(Err(Self::refused(k, &b)));
        }
        let bad = || io::Error::new(io::ErrorKind::InvalidData, "slice state reply");
        let mut r = Rd::new(&b);
        let end = r.u64().map_err(|_| bad())?;
        let behind = r.u8().map_err(|_| bad())? == 1;
        let nd = r.u32().map_err(|_| bad())? as usize;
        if nd > r.left() / 12 {
            return Err(bad());
        }
        let mut demand = Vec::with_capacity(nd);
        for _ in 0..nd {
            demand.push((r.u64().map_err(|_| bad())?, r.u32().map_err(|_| bad())?));
        }
        let rest = r.take(r.left()).map_err(|_| bad())?.to_vec();
        Ok(Ok(SliceState {
            snapshot: rest,
            group_end: end,
            behind,
            demand,
        }))
    }

    /// Close the credit epoch now (a quiet group has no entries to count).
    pub fn tick(&mut self) -> io::Result<Result<(), Refusal>> {
        let (k, b) = self.call(Q_TICK, &[])?;
        Ok(if k == R_OK {
            Ok(())
        } else {
            Err(Self::refused(k, &b))
        })
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
