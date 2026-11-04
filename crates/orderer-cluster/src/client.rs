//! Client protocol: a framed request/response TCP protocol for submitting
//! command batches to a replica and for inspecting it, plus a leader-following
//! client.
//!
//! Frame: `len u32 | crc32c u32 | kind u8 | body` (same envelope as the
//! replica transport). Requests: `PROPOSE` (body = an encoded command batch),
//! `STATUS`, `HASH`. Responses name their outcome explicitly; in particular
//! `UNKNOWN` means "may or may not have committed" and must not be retried
//! blindly.

use crate::auth::{ct_eq, handshake, nonce, Session, MAC_LEN, NONCE_LEN};
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
const Q_BARRIER: u8 = 4;
const Q_HELLO: u8 = 0;

const R_OK: u8 = 1;
const R_NOT_LEADER: u8 = 2;
const R_BUSY: u8 = 3;
const R_UNKNOWN: u8 = 4;
const R_ERR: u8 = 5;
const R_TEXT: u8 = 6;
const R_HASH: u8 = 7;
const R_CHALLENGE: u8 = 8;

fn write_frame(
    w: &mut impl Write,
    kind: u8,
    body: &[u8],
    sess: Option<&mut Session>,
) -> io::Result<()> {
    let mac = sess.map(|s| s.seal(kind, body));
    let mut b = Vec::with_capacity(1 + body.len() + MAC_LEN);
    b.push(kind);
    b.extend_from_slice(body);
    if let Some(m) = mac {
        b.extend_from_slice(&m);
    }
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(&crc32c(&b).to_le_bytes())?;
    w.write_all(&b)?;
    w.flush()
}

/// Strip and verify the MAC trailer when the connection is authenticated.
fn unseal(kind: u8, mut body: Vec<u8>, sess: Option<&mut Session>) -> io::Result<Vec<u8>> {
    let Some(sess) = sess else { return Ok(body) };
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "frame authentication");
    if body.len() < MAC_LEN {
        return Err(bad());
    }
    let mac = body.split_off(body.len() - MAC_LEN);
    if sess.open(kind, &body, &mac) {
        Ok(body)
    } else {
        Err(bad())
    }
}

/// A connection end that seals and checks frames once authenticated.
struct Wire {
    s: TcpStream,
    sess: Option<Session>,
}

impl Wire {
    fn send(&mut self, kind: u8, body: &[u8]) -> io::Result<()> {
        write_frame(&mut self.s, kind, body, self.sess.as_mut())
    }
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
fn read_frame_serving(
    r: &mut impl Read,
    stop: &AtomicBool,
    sess: Option<&mut Session>,
) -> io::Result<Option<(u8, Vec<u8>)>> {
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
    Ok(Some((kind, unseal(kind, body, sess)?)))
}

fn read_frame(r: &mut impl Read, sess: Option<&mut Session>) -> io::Result<(u8, Vec<u8>)> {
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
    Ok((kind, unseal(kind, body, sess)?))
}

/// Serves one replica's client port.
pub struct ClientServer {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ClientServer {
    pub fn start(addr: SocketAddr, replica: ReplicaHandle) -> io::Result<ClientServer> {
        ClientServer::start_with_key(addr, replica, None)
    }

    /// With `key`, clients must complete the pre-shared-key handshake and
    /// every frame is authenticated (see `auth.rs`).
    pub fn start_with_key(
        addr: SocketAddr,
        replica: ReplicaHandle,
        key: Option<Vec<u8>>,
    ) -> io::Result<ClientServer> {
        let key = key.map(Arc::new);
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let thread = thread::spawn(move || {
            while !s2.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((conn, _)) => {
                        let (h, stop, key) = (replica.clone(), s2.clone(), key.clone());
                        thread::spawn(move || {
                            if let Err(e) = serve(conn, h, stop, key) {
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

fn serve(
    mut conn: TcpStream,
    h: ReplicaHandle,
    stop: Arc<AtomicBool>,
    key: Option<Arc<Vec<u8>>>,
) -> io::Result<()> {
    // A socket accepted from a non-blocking listener is non-blocking on some
    // platforms (macOS). Make it blocking, with a timeout so shutdown is seen.
    conn.set_nonblocking(false)?;
    conn.set_nodelay(true)?;
    conn.set_read_timeout(Some(Duration::from_millis(200)))?;
    let mut sess = None;
    if let Some(k) = &key {
        // The first frame must be a hello carrying the client's nonce.
        let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
        let Some((Q_HELLO, nc)) = read_frame_serving(&mut conn, &stop, None)? else {
            return Err(bad("expected hello"));
        };
        if nc.len() != NONCE_LEN {
            return Err(bad("bad hello"));
        }
        let ns = nonce();
        let mac = handshake(k, b"cchal", &[&nc, &ns]);
        let mut reply = ns.to_vec();
        reply.extend_from_slice(&mac[..MAC_LEN]);
        write_frame(&mut conn, R_CHALLENGE, &reply, None)?;
        sess = Some(Session::new(handshake(k, b"csess", &[&nc, &ns]), 1));
    }
    let mut w = Wire { s: conn, sess };
    while !stop.load(Ordering::Relaxed) {
        let Some((kind, body)) = read_frame_serving(&mut w.s, &stop, w.sess.as_mut())? else {
            return Ok(());
        };
        if kind == Q_HELLO {
            w.send(R_ERR, b"this server does not use authentication")?;
            return Ok(());
        }
        match kind {
            Q_PROPOSE => match h.propose(body) {
                Ok(i) => w.send(R_OK, &i.to_le_bytes())?,
                Err(ProposeError::NotLeader(hint)) => {
                    w.send(R_NOT_LEADER, &[hint.unwrap_or(NodeId::MAX)])?
                }
                Err(ProposeError::Busy) => w.send(R_BUSY, &[])?,
                Err(ProposeError::Stopped) => w.send(R_NOT_LEADER, &[NodeId::MAX])?,
                Err(e) => w.send(R_UNKNOWN, format!("{e:?}").as_bytes())?,
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
                w.send(R_TEXT, text.as_bytes())?;
            }
            Q_HASH => match h.state_hash() {
                Some((i, d)) => {
                    let mut b = i.to_le_bytes().to_vec();
                    b.extend_from_slice(&d);
                    w.send(R_HASH, &b)?;
                }
                None => w.send(R_ERR, b"state hash unsupported")?,
            },
            Q_BARRIER => match h.barrier() {
                Ok(i) => w.send(R_OK, &i.to_le_bytes())?,
                Err(ProposeError::NotLeader(hint)) => {
                    w.send(R_NOT_LEADER, &[hint.unwrap_or(NodeId::MAX)])?
                }
                Err(ProposeError::Busy) => w.send(R_BUSY, &[])?,
                Err(e) => w.send(R_UNKNOWN, format!("{e:?}").as_bytes())?,
            },
            _ => w.send(R_ERR, b"unknown request")?,
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
    sess: Option<Session>,
}

impl Conn {
    pub fn connect(addr: SocketAddr) -> io::Result<Conn> {
        Conn::connect_with_key(addr, None)
    }

    /// Connect, authenticating with the cluster key when one is given. The
    /// server must hold the same key (or the handshake fails).
    pub fn connect_with_key(addr: SocketAddr, key: Option<&[u8]>) -> io::Result<Conn> {
        let mut s = TcpStream::connect_timeout(&addr, Duration::from_millis(500))?;
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(Duration::from_secs(30)))?;
        let mut sess = None;
        if let Some(k) = key {
            let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, m.to_string());
            let nc = nonce();
            write_frame(&mut s, Q_HELLO, &nc, None)?;
            let (kind, body) = read_frame(&mut s, None)?;
            if kind != R_CHALLENGE || body.len() != NONCE_LEN + MAC_LEN {
                return Err(bad("expected a challenge"));
            }
            let (ns, mac) = body.split_at(NONCE_LEN);
            let want = handshake(k, b"cchal", &[&nc, ns]);
            if !ct_eq(&want[..MAC_LEN], mac) {
                return Err(bad("the server does not know the cluster key"));
            }
            sess = Some(Session::new(handshake(k, b"csess", &[&nc, ns]), 0));
        }
        Ok(Conn { s, sess })
    }

    fn call(&mut self, kind: u8, body: &[u8]) -> io::Result<(u8, Vec<u8>)> {
        write_frame(&mut self.s, kind, body, self.sess.as_mut())?;
        read_frame(&mut self.s, self.sess.as_mut())
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

    /// A linearizable read barrier on this node (it must be the leader):
    /// `Ok(Some(index))` once the node has applied everything acknowledged
    /// before the call; `Ok(None)` if it is not the leader or could not
    /// commit.
    pub fn barrier(&mut self) -> io::Result<Option<Index>> {
        let (k, b) = self.call(Q_BARRIER, &[])?;
        Ok((k == R_OK && b.len() == 8).then(|| u64::from_le_bytes(b.try_into().expect("8"))))
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
    /// Session id and the last sequence number used, when sessions are on.
    session: Option<(u64, u64)>,
    /// A session batch whose outcome is still unknown: it is retried (same
    /// sequence number) before anything newer is sent.
    pending: Option<(u64, Vec<u8>)>,
    /// How long `propose_session` keeps retrying one batch.
    pub retry_budget: Duration,
    key: Option<Vec<u8>>,
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
            session: None,
            pending: None,
            retry_budget: Duration::from_secs(30),
            key: None,
        }
    }

    /// Turn on exactly-once submission under session id `id`. Use one id per
    /// sequential stream of requests, and never reuse it concurrently.
    /// Authenticate to the servers with the pre-shared cluster key.
    pub fn with_key(mut self, key: Vec<u8>) -> Client {
        self.key = Some(key);
        self
    }

    pub fn with_session(mut self, id: u64) -> Client {
        self.session = Some((id, 0));
        self
    }

    /// Submit a batch with exactly-once effect. A lost connection or a
    /// leader change is not an error here: the same `(session, seq)` is
    /// retried until it is definitely committed, and replicas apply it only
    /// once. `Err(Unknown)` only after the retry budget is spent; the batch
    /// is then re-sent first on the next call.
    pub fn propose_session(
        &mut self,
        cmds: &[(orderer_core::Symbol, orderer_core::Command)],
    ) -> Result<Index, ClientError> {
        let Some((id, last)) = self.session else {
            return Err(ClientError::Protocol(
                "no session: call with_session first".into(),
            ));
        };
        if self.pending.is_none() {
            let seq = last + 1;
            let mut payload = Vec::new();
            crate::codec::encode_session_batch(id, seq, cmds, &mut payload);
            self.session = Some((id, seq));
            self.pending = Some((seq, payload));
        }
        let (_, payload) = self.pending.clone().expect("just set");
        let deadline = std::time::Instant::now() + self.retry_budget;
        loop {
            match self.propose(&payload) {
                Ok(i) => {
                    self.pending = None;
                    return Ok(i);
                }
                Err(ClientError::Protocol(e)) => return Err(ClientError::Protocol(e)),
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(e);
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
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
                self.conns[i] = Conn::connect_with_key(self.addrs[i].1, self.key.as_deref()).ok();
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
