//! TCP transport between replicas.
//!
//! Every frame is `len u32 | crc32c u32 | kind u8 | body` (len and crc cover
//! kind and body). Outbound sends never block the protocol: each peer has a
//! bounded queue and a thread that reconnects with backoff; when the queue is
//! full or the peer is down, messages are dropped, which the protocol
//! tolerates by design. Snapshots stream in chunks and are verified by length
//! and checksum before they are accepted.

use crate::codec::{decode_msg, encode_msg, Reader};
use crate::snapstore::SnapStore;
use orderer::journal::crc32c;
use orderer_consensus::{Index, Msg, NodeId, Term};
use std::collections::HashMap;
use std::io::{self, BufWriter, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_FRAME: usize = 64 << 20;
const CHUNK: usize = 1 << 20;
const QUEUE: usize = 8192;

const K_MSG: u8 = 1;
const K_SNAP_BEGIN: u8 = 2;
const K_SNAP_CHUNK: u8 = 3;
const K_SNAP_END: u8 = 4;
const K_DIGEST: u8 = 5;
const K_HELLO: u8 = 6;

/// What the transport hands to the node.
#[derive(Debug)]
pub enum Inbound {
    Msg {
        from: NodeId,
        msg: Msg,
    },
    /// A snapshot from `from` is complete and verified in the store.
    Snapshot {
        from: NodeId,
        term: Term,
        index: Index,
        last_term: Term,
    },
    Digest {
        from: NodeId,
        index: Index,
        digest: [u8; 32],
    },
}

pub type Sink = Arc<dyn Fn(Inbound) + Send + Sync>;

/// Test hook that cuts links: `allows(from, to)` is consulted for every send
/// and every receive.
#[derive(Default)]
pub struct Filter {
    blocked: Mutex<Vec<(NodeId, NodeId)>>,
}

impl Filter {
    pub fn block(&self, a: NodeId, b: NodeId) {
        let mut v = self.blocked.lock().expect("filter lock");
        v.push((a, b));
        v.push((b, a));
    }
    pub fn isolate(&self, n: NodeId, all: &[NodeId]) {
        for &o in all {
            if o != n {
                self.block(n, o);
            }
        }
    }
    pub fn heal(&self) {
        self.blocked.lock().expect("filter lock").clear();
    }
    pub fn allows(&self, from: NodeId, to: NodeId) -> bool {
        !self
            .blocked
            .lock()
            .expect("filter lock")
            .contains(&(from, to))
    }
}

#[derive(Default)]
pub struct NetStats {
    pub sent: AtomicU64,
    pub dropped: AtomicU64,
    pub received: AtomicU64,
    pub bad_frames: AtomicU64,
}

enum Job {
    Msg(Msg),
    Snapshot {
        term: Term,
        index: Index,
        last_term: Term,
    },
    Digest {
        index: Index,
        digest: [u8; 32],
    },
}

pub struct Transport {
    me: NodeId,
    stop: Arc<AtomicBool>,
    queues: HashMap<NodeId, SyncSender<Job>>,
    filter: Arc<Filter>,
    pub stats: Arc<NetStats>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

fn write_frame(w: &mut impl Write, kind: u8, body: &[u8]) -> io::Result<()> {
    let mut crc_in = Vec::with_capacity(1 + body.len());
    crc_in.push(kind);
    crc_in.extend_from_slice(body);
    w.write_all(&(crc_in.len() as u32).to_le_bytes())?;
    w.write_all(&crc32c(&crc_in).to_le_bytes())?;
    w.write_all(&crc_in)
}

/// Fill `buf` unless asked to stop. `Ok(false)` = stopped or clean EOF at a
/// frame boundary.
fn read_full(
    s: &mut TcpStream,
    buf: &mut [u8],
    stop: &AtomicBool,
    at_boundary: bool,
) -> io::Result<bool> {
    let mut got = 0;
    while got < buf.len() {
        if stop.load(Ordering::Relaxed) {
            return Ok(false);
        }
        match s.read(&mut buf[got..]) {
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
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(true)
}

fn read_frame(s: &mut TcpStream, stop: &AtomicBool) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut head = [0u8; 8];
    if !read_full(s, &mut head, stop, true)? {
        return Ok(None);
    }
    let len = u32::from_le_bytes(head[..4].try_into().expect("4")) as usize;
    let crc = u32::from_le_bytes(head[4..].try_into().expect("4"));
    if len == 0 || len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame length"));
    }
    let mut body = vec![0u8; len];
    if !read_full(s, &mut body, stop, false)? {
        return Ok(None);
    }
    if crc32c(&body) != crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame checksum"));
    }
    let kind = body[0];
    body.remove(0);
    Ok(Some((kind, body)))
}

impl Transport {
    /// Listen on this node's address and start a sender per peer.
    pub fn start(
        me: NodeId,
        members: &[(NodeId, SocketAddr)],
        cluster_id: u64,
        sink: Sink,
        snaps: Arc<SnapStore>,
        filter: Arc<Filter>,
    ) -> io::Result<Transport> {
        let my_addr = members
            .iter()
            .find(|m| m.0 == me)
            .expect("self in members")
            .1;
        let listener = TcpListener::bind(my_addr)?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(NetStats::default());
        let ids: Vec<NodeId> = members.iter().map(|m| m.0).collect();
        let mut threads = Vec::new();

        // acceptor
        {
            let (stop, sink, snaps, filter, stats, ids) = (
                stop.clone(),
                sink,
                snaps.clone(),
                filter.clone(),
                stats.clone(),
                ids,
            );
            threads.push(thread::spawn(move || {
                let readers: Mutex<Vec<JoinHandle<()>>> = Mutex::new(Vec::new());
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((conn, _)) => {
                            let (stop, sink, snaps, filter, stats, ids) = (
                                stop.clone(),
                                sink.clone(),
                                snaps.clone(),
                                filter.clone(),
                                stats.clone(),
                                ids.clone(),
                            );
                            let h = thread::spawn(move || {
                                let _ = reader(
                                    conn, me, cluster_id, &ids, sink, snaps, filter, stats, stop,
                                );
                            });
                            readers.lock().expect("readers lock").push(h);
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5))
                        }
                        Err(_) => thread::sleep(Duration::from_millis(20)),
                    }
                }
                for h in readers.into_inner().expect("readers lock") {
                    let _ = h.join();
                }
            }));
        }

        // one sender per peer
        let mut queues = HashMap::new();
        for &(id, addr) in members {
            if id == me {
                continue;
            }
            let (tx, rx) = sync_channel::<Job>(QUEUE);
            queues.insert(id, tx);
            let (stop, snaps, filter, stats) =
                (stop.clone(), snaps.clone(), filter.clone(), stats.clone());
            threads.push(thread::spawn(move || {
                sender(me, id, addr, cluster_id, rx, snaps, filter, stats, stop)
            }));
        }
        Ok(Transport {
            me,
            stop,
            queues,
            filter,
            stats,
            threads: Mutex::new(threads),
        })
    }

    fn enqueue(&self, to: NodeId, job: Job) {
        if !self.filter.allows(self.me, to) {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if let Some(q) = self.queues.get(&to) {
            match q.try_send(job) {
                Ok(()) => {}
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                    self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    pub fn send(&self, to: NodeId, msg: Msg) {
        self.enqueue(to, Job::Msg(msg));
    }

    /// Stream the stored snapshot at `index` to `to`.
    pub fn send_snapshot(&self, to: NodeId, term: Term, index: Index, last_term: Term) {
        self.enqueue(
            to,
            Job::Snapshot {
                term,
                index,
                last_term,
            },
        );
    }

    pub fn send_digest(&self, to: NodeId, index: Index, digest: [u8; 32]) {
        self.enqueue(to, Job::Digest { index, digest });
    }

    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.threads.lock().expect("threads lock").drain(..) {
            let _ = h.join();
        }
    }
}

impl Drop for Transport {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn hello(me: NodeId, cluster_id: u64) -> Vec<u8> {
    let mut b = vec![me];
    b.extend_from_slice(&cluster_id.to_le_bytes());
    b
}

#[allow(clippy::too_many_arguments)]
fn sender(
    me: NodeId,
    peer: NodeId,
    addr: SocketAddr,
    cluster_id: u64,
    rx: Receiver<Job>,
    snaps: Arc<SnapStore>,
    filter: Arc<Filter>,
    stats: Arc<NetStats>,
    stop: Arc<AtomicBool>,
) {
    let mut backoff = Duration::from_millis(20);
    while !stop.load(Ordering::Relaxed) {
        let conn = TcpStream::connect_timeout(&addr, Duration::from_millis(300)).and_then(|c| {
            c.set_nodelay(true)?;
            Ok(c)
        });
        let Ok(conn) = conn else {
            // Peer down: shed what queued meanwhile, then back off.
            while rx.try_recv().is_ok() {
                stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
            thread::sleep(backoff);
            backoff = (backoff * 2).min(Duration::from_millis(500));
            continue;
        };
        backoff = Duration::from_millis(20);
        let mut w = BufWriter::with_capacity(256 << 10, conn);
        let mut scratch = Vec::new();
        let result: io::Result<()> = (|| {
            write_frame(&mut w, K_HELLO, &hello(me, cluster_id))?;
            loop {
                if stop.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let job = match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(j) => j,
                    Err(RecvTimeoutError::Timeout) => {
                        w.flush()?;
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => return Ok(()),
                };
                // Write this job and everything already queued, then flush once
                // so bursts coalesce into few TCP segments.
                let mut next = Some(job);
                while let Some(job) = next.take() {
                    if filter.allows(me, peer) {
                        stats.sent.fetch_add(1, Ordering::Relaxed);
                        match job {
                            Job::Msg(m) => {
                                scratch.clear();
                                encode_msg(&m, &mut scratch);
                                write_frame(&mut w, K_MSG, &scratch)?;
                            }
                            Job::Digest { index, digest } => {
                                let mut b = index.to_le_bytes().to_vec();
                                b.extend_from_slice(&digest);
                                write_frame(&mut w, K_DIGEST, &b)?;
                            }
                            Job::Snapshot {
                                term,
                                index,
                                last_term,
                            } => {
                                stream_snapshot(&mut w, &snaps, term, index, last_term)?;
                            }
                        }
                    }
                    next = rx.try_recv().ok();
                }
                w.flush()?;
            }
        })();
        let _ = result;
    }
}

fn stream_snapshot(
    w: &mut impl Write,
    snaps: &SnapStore,
    term: Term,
    index: Index,
    last_term: Term,
) -> io::Result<()> {
    let (meta, mut file) = snaps.open_stream(index)?;
    let mut b = Vec::new();
    b.extend_from_slice(&term.to_le_bytes());
    b.extend_from_slice(&index.to_le_bytes());
    b.extend_from_slice(&last_term.to_le_bytes());
    b.extend_from_slice(&meta.len.to_le_bytes());
    b.extend_from_slice(&meta.crc.to_le_bytes());
    write_frame(w, K_SNAP_BEGIN, &b)?;
    let mut buf = vec![0u8; CHUNK];
    let mut left = meta.len;
    while left > 0 {
        let n = file.read(&mut buf[..CHUNK.min(left as usize)])?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        write_frame(w, K_SNAP_CHUNK, &buf[..n])?;
        left -= n as u64;
    }
    write_frame(w, K_SNAP_END, &[])?;
    w.flush()
}

#[allow(clippy::too_many_arguments)]
fn reader(
    mut conn: TcpStream,
    me: NodeId,
    cluster_id: u64,
    ids: &[NodeId],
    sink: Sink,
    snaps: Arc<SnapStore>,
    filter: Arc<Filter>,
    stats: Arc<NetStats>,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    // Accepted from a non-blocking listener: blocking mode is not inherited
    // everywhere (macOS), and a non-blocking reader would spin.
    conn.set_nonblocking(false)?;
    conn.set_nodelay(true)?;
    conn.set_read_timeout(Some(Duration::from_millis(100)))?;
    let bad = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_string());
    let Some((kind, body)) = read_frame(&mut conn, &stop)? else {
        return Ok(());
    };
    if kind != K_HELLO || body.len() != 9 {
        stats.bad_frames.fetch_add(1, Ordering::Relaxed);
        return Err(bad("expected hello"));
    }
    let from = body[0];
    if u64::from_le_bytes(body[1..9].try_into().expect("8")) != cluster_id
        || !ids.contains(&from)
        || from == me
    {
        stats.bad_frames.fetch_add(1, Ordering::Relaxed);
        return Err(bad("hello from a stranger"));
    }
    let mut incoming: Option<(crate::snapstore::Incoming, Term, Index, Term)> = None;
    loop {
        let r = read_frame(&mut conn, &stop);
        let (kind, body) = match r {
            Ok(Some(f)) => f,
            Ok(None) => return Ok(()),
            Err(e) => {
                if e.kind() == io::ErrorKind::InvalidData {
                    stats.bad_frames.fetch_add(1, Ordering::Relaxed);
                }
                return Err(e);
            }
        };
        if !filter.allows(from, me) {
            continue;
        }
        stats.received.fetch_add(1, Ordering::Relaxed);
        let mut rd = Reader::new(&body);
        let fail = |stats: &NetStats| {
            stats.bad_frames.fetch_add(1, Ordering::Relaxed);
            bad("malformed frame body")
        };
        match kind {
            K_MSG => {
                let msg = decode_msg(&mut rd).map_err(|_| fail(&stats))?;
                rd.finish().map_err(|_| fail(&stats))?;
                sink(Inbound::Msg { from, msg });
            }
            K_DIGEST => {
                let index = rd.u64().map_err(|_| fail(&stats))?;
                let digest: [u8; 32] = rd
                    .bytes(32)
                    .map_err(|_| fail(&stats))?
                    .try_into()
                    .expect("32");
                sink(Inbound::Digest {
                    from,
                    index,
                    digest,
                });
            }
            K_SNAP_BEGIN => {
                let (term, index, last_term) = (
                    rd.u32().map_err(|_| fail(&stats))?,
                    rd.u64().map_err(|_| fail(&stats))?,
                    rd.u32().map_err(|_| fail(&stats))?,
                );
                let (len, crc) = (
                    rd.u64().map_err(|_| fail(&stats))?,
                    rd.u32().map_err(|_| fail(&stats))?,
                );
                if let Some((old, ..)) = incoming.take() {
                    old.abort();
                }
                incoming = Some((
                    snaps.receive(index, last_term, len, crc)?,
                    term,
                    index,
                    last_term,
                ));
            }
            K_SNAP_CHUNK => match incoming.as_mut() {
                Some((inc, ..)) => inc.write(&body)?,
                None => return Err(bad("snapshot chunk without begin")),
            },
            K_SNAP_END => match incoming.take() {
                Some((inc, term, index, last_term)) => {
                    inc.finish()?;
                    sink(Inbound::Snapshot {
                        from,
                        term,
                        index,
                        last_term,
                    });
                }
                None => return Err(bad("snapshot end without begin")),
            },
            _ => return Err(fail(&stats)),
        }
    }
}
