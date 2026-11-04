//! Client protocol: a framed request/response TCP protocol for submitting
//! command batches to a replica and for inspecting it, plus a leader-following
//! client.
//!
//! Frame: `len u32 | crc32c u32 | kind u8 | body` (same envelope as the
//! replica transport). Requests: `PROPOSE` (body = an encoded command batch),
//! `STATUS`, `HASH`. Responses name their outcome explicitly; in particular
//! `UNKNOWN` means "may or may not have committed" and must not be retried
//! blindly.

use crate::replica::{ProposeError, ReplicaHandle};
use orderer::journal::crc32c;
use orderer_consensus::{Index, NodeId};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_FRAME: usize = 64 << 20;

const Q_PROPOSE: u8 = 1;
const Q_STATUS: u8 = 2;
const Q_HASH: u8 = 3;

const R_OK: u8 = 1;
const R_NOT_LEADER: u8 = 2;
const R_BUSY: u8 = 3;
const R_UNKNOWN: u8 = 4;
const R_ERR: u8 = 5;
const R_TEXT: u8 = 6;
const R_HASH: u8 = 7;

fn write_frame(w: &mut impl Write, kind: u8, body: &[u8]) -> io::Result<()> {
    let mut b = Vec::with_capacity(1 + body.len());
    b.push(kind);
    b.extend_from_slice(body);
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(&crc32c(&b).to_le_bytes())?;
    w.write_all(&b)?;
    w.flush()
}

/// Read exactly `buf.len()` bytes, tolerating read timeouts without losing
/// bytes already consumed. `Ok(false)` = asked to stop, or clean EOF at a
/// frame boundary (`at_boundary`).
fn read_full(
    r: &mut impl Read,
    buf: &mut [u8],
    stop: &AtomicBool,
    at_boundary: bool,
) -> io::Result<bool> {
    let mut got = 0;
    while got < buf.len() {
        if stop.load(Ordering::Relaxed) {
            return Ok(false);
        }
        match r.read(&mut buf[got..]) {
            Ok(0) => {
                return if got == 0 && at_boundary {
                    Ok(false)
                } else {
                    Err(io::ErrorKind::UnexpectedEof.into())
                }
            }
            Ok(n) => got += n,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

/// Server-side frame read: survives idle timeouts without desynchronising.
fn read_frame_serving(r: &mut impl Read, stop: &AtomicBool) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut head = [0u8; 8];
    if !read_full(r, &mut head, stop, true)? {
        return Ok(None);
    }
    let len = u32::from_le_bytes(head[..4].try_into().expect("4")) as usize;
    let crc = u32::from_le_bytes(head[4..].try_into().expect("4"));
    if len == 0 || len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame length"));
    }
    let mut body = vec![0u8; len];
    if !read_full(r, &mut body, stop, false)? {
        return Ok(None);
    }
    if crc32c(&body) != crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame checksum"));
    }
    let kind = body.remove(0);
    Ok(Some((kind, body)))
}

fn read_frame(r: &mut impl Read) -> io::Result<(u8, Vec<u8>)> {
    let mut head = [0u8; 8];
    r.read_exact(&mut head)?;
    let len = u32::from_le_bytes(head[..4].try_into().expect("4")) as usize;
    let crc = u32::from_le_bytes(head[4..].try_into().expect("4"));
    if len == 0 || len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame length"));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    if crc32c(&body) != crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame checksum"));
    }
    let kind = body.remove(0);
    Ok((kind, body))
}

/// Serves one replica's client port.
pub struct ClientServer {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ClientServer {
    pub fn start(addr: SocketAddr, replica: ReplicaHandle) -> io::Result<ClientServer> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let thread = thread::spawn(move || {
            while !s2.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((conn, _)) => {
                        let (h, stop) = (replica.clone(), s2.clone());
                        thread::spawn(move || {
                            if let Err(e) = serve(conn, h, stop) {
                                if e.kind() != io::ErrorKind::UnexpectedEof {
                                    eprintln!("orderer-cluster: client connection closed: {e}");
                                }
                            }
                        });
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => thread::sleep(Duration::from_millis(20)),
                }
            }
        });
        Ok(ClientServer {
            stop,
            thread: Some(thread),
        })
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn serve(mut conn: TcpStream, h: ReplicaHandle, stop: Arc<AtomicBool>) -> io::Result<()> {
    // A socket accepted from a non-blocking listener is non-blocking on some
    // platforms (macOS). Make it blocking, with a timeout so shutdown is seen.
    conn.set_nonblocking(false)?;
    conn.set_nodelay(true)?;
    conn.set_read_timeout(Some(Duration::from_millis(200)))?;
    while !stop.load(Ordering::Relaxed) {
        let Some((kind, body)) = read_frame_serving(&mut conn, &stop)? else {
            return Ok(());
        };
        match kind {
            Q_PROPOSE => match h.propose(body) {
                Ok(i) => write_frame(&mut conn, R_OK, &i.to_le_bytes())?,
                Err(ProposeError::NotLeader(hint)) => {
                    write_frame(&mut conn, R_NOT_LEADER, &[hint.unwrap_or(NodeId::MAX)])?
                }
                Err(ProposeError::Busy) => write_frame(&mut conn, R_BUSY, &[])?,
                Err(ProposeError::Stopped) => write_frame(&mut conn, R_NOT_LEADER, &[NodeId::MAX])?,
                Err(e) => write_frame(&mut conn, R_UNKNOWN, format!("{e:?}").as_bytes())?,
            },
            Q_STATUS => {
                let s = h.status();
                let text = format!(
                    "id={} role={:?} term={} leader={} commit={} applied={} base={} last={} pending={} halted={} digests_verified={}\n",
                    s.id,
                    s.role,
                    s.term,
                    s.leader.map_or(-1, i32::from),
                    s.commit,
                    s.applied,
                    s.base,
                    s.last_index,
                    s.pending,
                    s.halted.as_deref().unwrap_or("-"),
                    s.digests_verified
                );
                write_frame(&mut conn, R_TEXT, text.as_bytes())?;
            }
            Q_HASH => match h.state_hash() {
                Some((i, d)) => {
                    let mut b = i.to_le_bytes().to_vec();
                    b.extend_from_slice(&d);
                    write_frame(&mut conn, R_HASH, &b)?;
                }
                None => write_frame(&mut conn, R_ERR, b"state hash unsupported")?,
            },
            _ => write_frame(&mut conn, R_ERR, b"unknown request")?,
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientError {
    /// Could not reach a leader within the retry budget. Nothing was sent
    /// that could have committed.
    NoLeader,
    /// The request may or may not have committed. Do not retry blindly.
    Unknown(String),
    Protocol(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ClientError {}

/// A connection to one replica.
pub struct Conn {
    s: TcpStream,
}

impl Conn {
    pub fn connect(addr: SocketAddr) -> io::Result<Conn> {
        let s = TcpStream::connect_timeout(&addr, Duration::from_millis(500))?;
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(Duration::from_secs(30)))?;
        Ok(Conn { s })
    }

    fn call(&mut self, kind: u8, body: &[u8]) -> io::Result<(u8, Vec<u8>)> {
        write_frame(&mut self.s, kind, body)?;
        read_frame(&mut self.s)
    }

    pub fn status(&mut self) -> io::Result<String> {
        let (k, b) = self.call(Q_STATUS, &[])?;
        if k != R_TEXT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad status reply",
            ));
        }
        Ok(String::from_utf8_lossy(&b).into_owned())
    }

    pub fn hash(&mut self) -> io::Result<Option<(Index, [u8; 32])>> {
        let (k, b) = self.call(Q_HASH, &[])?;
        if k != R_HASH || b.len() != 40 {
            return Ok(None);
        }
        Ok(Some((
            u64::from_le_bytes(b[..8].try_into().expect("8")),
            b[8..].try_into().expect("32"),
        )))
    }
}

/// Submits batches to a cluster, following leader hints and surviving
/// restarts.
pub struct Client {
    addrs: Vec<(NodeId, SocketAddr)>,
    conns: Vec<Option<Conn>>,
    current: usize,
}

impl Client {
    /// `addrs` pairs each replica's node id with its client address; redirect
    /// hints name a node id, so the order of the list does not matter.
    pub fn new(addrs: Vec<(NodeId, SocketAddr)>) -> Client {
        let n = addrs.len();
        Client {
            addrs,
            conns: (0..n).map(|_| None).collect(),
            current: 0,
        }
    }

    /// Submit an encoded command batch. `Ok(index)` means it is committed on
    /// a quorum. `Err(Unknown)` means it may still commit.
    pub fn propose(&mut self, payload: &[u8]) -> Result<Index, ClientError> {
        let mut attempts = 0;
        while attempts < 600 {
            attempts += 1;
            let i = self.current % self.addrs.len();
            if self.conns[i].is_none() {
                self.conns[i] = Conn::connect(self.addrs[i].1).ok();
            }
            let Some(conn) = self.conns[i].as_mut() else {
                self.current += 1;
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            match conn.call(Q_PROPOSE, payload) {
                Ok((R_OK, b)) if b.len() == 8 => {
                    return Ok(u64::from_le_bytes(b.try_into().expect("8")))
                }
                Ok((R_NOT_LEADER, b)) => {
                    match b
                        .first()
                        .and_then(|&h| self.addrs.iter().position(|a| a.0 == h))
                    {
                        Some(pos) => self.current = pos,
                        None => self.current += 1,
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Ok((R_BUSY, _)) => thread::sleep(Duration::from_millis(2)),
                Ok((R_UNKNOWN, b)) => {
                    self.conns[i] = None;
                    return Err(ClientError::Unknown(
                        String::from_utf8_lossy(&b).into_owned(),
                    ));
                }
                Ok((k, b)) => {
                    return Err(ClientError::Protocol(format!(
                        "reply {k}: {}",
                        String::from_utf8_lossy(&b)
                    )))
                }
                // The connection died mid-request: the proposal may have been
                // accepted. That is an unknown outcome, never a retry.
                Err(e) => {
                    self.conns[i] = None;
                    self.current += 1;
                    return Err(ClientError::Unknown(format!("connection lost: {e}")));
                }
            }
        }
        Err(ClientError::NoLeader)
    }
}
