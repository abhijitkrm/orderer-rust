//! The replica runtime: one driver thread owns the protocol [`Node`] and
//! executes its effects (WAL writes on an I/O thread, network sends, state
//! machine applies, snapshots), as `spec/REPLICATION.md` describes.
//!
//! Threads: *driver* (node + state machine, the only thread that touches
//! either), *wal* (group-commit appends and fsync), and the transport's
//! acceptor, readers and per-peer senders.

use crate::codec::encode_batch;
use crate::sha256::hex;
use crate::snapstore::SnapStore;
use crate::transport::{Filter, Inbound, Transport};
use crate::wal::{Wal, WalError};
use orderer_consensus::{Config, Effect, Entry, Index, Node, NodeId, Persist, Role, Term};
use orderer_core::{Command, Symbol};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// The deterministic state machine the log drives. Called only from the
/// driver thread, in log order.
pub trait StateMachine: Send + 'static {
    /// Apply committed entry `index`. No-op entries arrive with empty data.
    fn apply(&mut self, index: Index, entry: &Entry);
    /// Serialize the state after applying `index` (the latest applied entry).
    fn snapshot(&mut self, index: Index) -> Vec<u8>;
    /// Replace the state with a snapshot taken at `index`.
    fn restore(&mut self, index: Index, data: &[u8]) -> Result<(), String>;
    /// A digest of the state after `index`, for divergence detection.
    /// `None` = unsupported.
    fn digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        None
    }
}

#[derive(Clone, Debug)]
pub struct ReplicaConfig {
    pub id: NodeId,
    pub members: Vec<(NodeId, SocketAddr)>,
    pub cluster_id: u64,
    pub dir: PathBuf,
    pub tick: Duration,
    pub consensus: Config,
    /// Snapshot and compact the log every this many applied entries (0 = never).
    pub compact_every: u64,
    /// Exchange state digests at every multiple of this index (0 = off).
    pub digest_every: u64,
    /// `false` skips fsync (tests and benchmarks only; loses durability).
    pub fsync: bool,
    pub propose_timeout: Duration,
    /// Backpressure: refuse proposals beyond this many in flight.
    pub max_pending: usize,
}

impl ReplicaConfig {
    pub fn new(id: NodeId, members: Vec<(NodeId, SocketAddr)>, dir: impl Into<PathBuf>) -> Self {
        ReplicaConfig {
            id,
            members,
            cluster_id: 1,
            dir: dir.into(),
            tick: Duration::from_millis(10),
            consensus: Config {
                election_ticks: 30,
                heartbeat_ticks: 3,
                max_batch: 256,
                ..Config::default()
            },
            compact_every: 0,
            digest_every: 0,
            fsync: true,
            propose_timeout: Duration::from_secs(5),
            max_pending: 4096,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeError {
    /// Not the leader; the hint is the leader this node last heard from.
    NotLeader(Option<NodeId>),
    /// Too many proposals in flight.
    Busy,
    /// No outcome within the timeout. The command may or may not commit.
    Timeout,
    /// The entry at that index belongs to another leader's term: lost.
    Superseded,
    /// The replica stopped while the proposal was in flight. It may already
    /// be replicated and later commit: the outcome is unknown.
    Unknown,
    /// The replica had stopped before the proposal was submitted. Nothing
    /// was sent; retrying elsewhere is safe.
    Stopped,
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ProposeError {}

#[derive(Clone, Debug)]
pub struct Status {
    pub id: NodeId,
    pub role: Role,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit: Index,
    pub applied: Index,
    pub base: Index,
    pub last_index: Index,
    pub pending: usize,
    /// Set once the replica has stopped itself (state divergence, fatal I/O).
    pub halted: Option<String>,
    pub digests_verified: u64,
}

enum Event {
    Inbound(Inbound),
    PersistDone(u64),
    Propose {
        payload: Vec<u8>,
        reply: SyncSender<Result<Index, ProposeError>>,
    },
    Hash(SyncSender<Option<(Index, [u8; 32])>>),
    Stop,
}

/// Cloneable access to a running replica.
#[derive(Clone)]
pub struct ReplicaHandle {
    tx: Sender<Event>,
    status: Arc<Mutex<Status>>,
}

impl ReplicaHandle {
    /// Submit a pre-encoded log entry payload; blocks until it is committed
    /// and applied on this node.
    pub fn propose(&self, payload: Vec<u8>) -> Result<Index, ProposeError> {
        self.propose_async(payload)
            .recv()
            .unwrap_or(Err(ProposeError::Stopped))
    }

    /// Submit without waiting; receive the outcome on the returned channel.
    pub fn propose_async(&self, payload: Vec<u8>) -> Receiver<Result<Index, ProposeError>> {
        let (reply, rx) = sync_channel(1);
        if self.tx.send(Event::Propose { payload, reply }).is_err() {
            let (r2, rx2) = sync_channel(1);
            let _ = r2.send(Err(ProposeError::Stopped));
            return rx2;
        }
        rx
    }

    /// Submit a batch of commands as one log entry.
    pub fn propose_commands(&self, cmds: &[(Symbol, Command)]) -> Result<Index, ProposeError> {
        let mut payload = Vec::new();
        encode_batch(cmds, &mut payload);
        self.propose(payload)
    }

    pub fn status(&self) -> Status {
        self.status.lock().expect("status lock").clone()
    }

    /// `(applied index, state digest)`, if the state machine supports it.
    pub fn state_hash(&self) -> Option<(Index, [u8; 32])> {
        let (reply, rx) = sync_channel(1);
        self.tx.send(Event::Hash(reply)).ok()?;
        rx.recv().ok().flatten()
    }
}

pub struct Replica {
    handle: ReplicaHandle,
    thread: Option<JoinHandle<()>>,
    abort: Arc<AtomicBool>,
    pub filter: Arc<Filter>,
}

#[derive(Debug)]
pub enum StartError {
    Wal(WalError),
    Io(std::io::Error),
    State(String),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartError::Wal(e) => write!(f, "{e}"),
            StartError::Io(e) => write!(f, "{e}"),
            StartError::State(e) => write!(f, "state machine: {e}"),
        }
    }
}

impl std::error::Error for StartError {}

impl From<std::io::Error> for StartError {
    fn from(e: std::io::Error) -> Self {
        StartError::Io(e)
    }
}

impl From<WalError> for StartError {
    fn from(e: WalError) -> Self {
        StartError::Wal(e)
    }
}

impl Replica {
    /// Recover from `cfg.dir` and start. `sm` is restored from the newest
    /// snapshot the log's base requires, then catches up from the log.
    pub fn start<S: StateMachine>(cfg: ReplicaConfig, sm: S) -> Result<Replica, StartError> {
        Self::start_with_filter(cfg, sm, Arc::new(Filter::default()))
    }

    pub fn start_with_filter<S: StateMachine>(
        cfg: ReplicaConfig,
        mut sm: S,
        filter: Arc<Filter>,
    ) -> Result<Replica, StartError> {
        let (wal, disk, _repair) = Wal::open(cfg.dir.join("wal"))?;
        let snaps = Arc::new(SnapStore::open(cfg.dir.join("snap"))?);
        if disk.base > 0 {
            let (_, data) = snaps.get(disk.base).map_err(|e| {
                StartError::State(format!(
                    "log base {} needs a snapshot that cannot be read: {e}",
                    disk.base
                ))
            })?;
            sm.restore(disk.base, &data).map_err(StartError::State)?;
        }
        let peers: Vec<NodeId> = cfg.members.iter().map(|m| m.0).collect();
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64)
            ^ cfg.cluster_id;
        let node = Node::restart(cfg.id, peers, cfg.consensus.clone(), seed, disk);

        let (tx, rx) = channel::<Event>();
        let status = Arc::new(Mutex::new(Status {
            id: cfg.id,
            role: Role::Follower,
            term: node.term(),
            leader: None,
            commit: node.commit_index(),
            applied: node.applied_index(),
            base: node.base(),
            last_index: node.last_index(),
            pending: 0,
            halted: None,
            digests_verified: 0,
        }));

        let sink_tx = Mutex::new(tx.clone());
        let transport = Transport::start(
            cfg.id,
            &cfg.members,
            cfg.cluster_id,
            Arc::new(move |m| {
                let _ = sink_tx.lock().expect("sink lock").send(Event::Inbound(m));
            }),
            snaps.clone(),
            filter.clone(),
        )?;

        let abort = Arc::new(AtomicBool::new(false));
        let (io_tx, io_rx) = channel::<Persist>();
        let io_thread = {
            let (tx, abort, fsync) = (tx.clone(), abort.clone(), cfg.fsync);
            thread::spawn(move || wal_thread(wal, io_rx, tx, abort, fsync))
        };

        let handle = ReplicaHandle {
            tx,
            status: status.clone(),
        };
        let driver = Driver {
            cfg,
            node,
            sm,
            snaps,
            transport,
            io_tx: Some(io_tx),
            io_thread: Some(io_thread),
            status,
            pending: HashMap::new(),
            compact_tokens: HashMap::new(),
            digests: BTreeMap::new(),
            digests_verified: 0,
            halted: None,
        };
        let thread = thread::spawn(move || driver.run(rx));
        Ok(Replica {
            handle,
            thread: Some(thread),
            abort,
            filter,
        })
    }

    pub fn handle(&self) -> ReplicaHandle {
        self.handle.clone()
    }

    /// Graceful stop: queued WAL writes are flushed first.
    pub fn stop(mut self) {
        let _ = self.handle.tx.send(Event::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }

    /// Simulated power cut: threads stop at once and writes not yet fsynced
    /// are discarded. What is on disk stays.
    pub fn crash(mut self) {
        self.abort.store(true, Ordering::SeqCst);
        let _ = self.handle.tx.send(Event::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        if self.thread.is_some() {
            self.abort.store(true, Ordering::SeqCst);
            let _ = self.handle.tx.send(Event::Stop);
            if let Some(t) = self.thread.take() {
                let _ = t.join();
            }
        }
    }
}

fn wal_thread(
    mut wal: Wal,
    rx: Receiver<Persist>,
    done: Sender<Event>,
    abort: Arc<AtomicBool>,
    fsync: bool,
) {
    while let Ok(first) = rx.recv() {
        if abort.load(Ordering::SeqCst) {
            return;
        }
        // Group commit: everything already queued shares one fsync.
        let mut batch = vec![first];
        while let Ok(p) = rx.try_recv() {
            batch.push(p);
        }
        let mut compacted = false;
        for p in &batch {
            wal.append(p);
            compacted |= p.compact.is_some();
        }
        let r = if fsync {
            wal.sync()
        } else {
            // Benchmark mode: hand to the OS but do not wait for the disk.
            wal.sync_no_fsync()
        };
        if let Err(e) = r {
            eprintln!("orderer-cluster: wal write failed: {e}; stopping");
            let _ = done.send(Event::Stop);
            return;
        }
        if abort.load(Ordering::SeqCst) {
            return;
        }
        if compacted {
            if let Err(e) = wal.rewrite() {
                eprintln!("orderer-cluster: wal rewrite failed: {e}; stopping");
                let _ = done.send(Event::Stop);
                return;
            }
        }
        let last = batch.last().expect("non-empty").token;
        if done.send(Event::PersistDone(last)).is_err() {
            return;
        }
    }
}

struct Pending {
    term: Term,
    reply: SyncSender<Result<Index, ProposeError>>,
    deadline: Instant,
}

struct Driver<S: StateMachine> {
    cfg: ReplicaConfig,
    node: Node,
    sm: S,
    snaps: Arc<SnapStore>,
    transport: Transport,
    io_tx: Option<Sender<Persist>>,
    io_thread: Option<JoinHandle<()>>,
    status: Arc<Mutex<Status>>,
    pending: HashMap<Index, Pending>,
    /// write token -> snapshot index, for old-snapshot cleanup once durable
    compact_tokens: HashMap<u64, Index>,
    digests: BTreeMap<Index, HashMap<NodeId, [u8; 32]>>,
    digests_verified: u64,
    halted: Option<String>,
}

impl<S: StateMachine> Driver<S> {
    fn run(mut self, rx: Receiver<Event>) {
        let mut next_tick = Instant::now() + self.cfg.tick;
        'outer: loop {
            let wait = next_tick.saturating_duration_since(Instant::now());
            let mut replicate = false;
            match rx.recv_timeout(wait) {
                Ok(ev) => {
                    if !self.handle(ev, &mut replicate) {
                        break 'outer;
                    }
                    for _ in 0..4096 {
                        match rx.try_recv() {
                            Ok(ev) => {
                                if !self.handle(ev, &mut replicate) {
                                    break 'outer;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            let now = Instant::now();
            if now >= next_tick {
                self.node.tick();
                next_tick = now + self.cfg.tick;
                self.expire(now);
            }
            if replicate {
                self.node.replicate();
            }
            self.pump();
            self.maybe_compact();
            self.pump();
            self.publish_status();
            if self.halted.is_some() {
                break;
            }
        }
        self.publish_status();
        for (_, p) in self.pending.drain() {
            let _ = p.reply.send(Err(ProposeError::Unknown));
        }
        self.transport.shutdown();
        drop(self.io_tx.take());
        if let Some(t) = self.io_thread.take() {
            let _ = t.join();
        }
    }

    /// Returns false to stop.
    fn handle(&mut self, ev: Event, replicate: &mut bool) -> bool {
        match ev {
            Event::Stop => return false,
            Event::PersistDone(token) => {
                self.node.persisted(token);
                let done: Vec<u64> = self
                    .compact_tokens
                    .keys()
                    .copied()
                    .filter(|&t| t <= token)
                    .collect();
                for t in done {
                    if let Some(idx) = self.compact_tokens.remove(&t) {
                        // Keep the snapshot the log now starts from.
                        let _ = self.snaps.gc(idx);
                    }
                }
            }
            Event::Inbound(Inbound::Msg { from, msg }) => self.node.step(from, msg),
            Event::Inbound(Inbound::Snapshot {
                from,
                term,
                index,
                last_term,
            }) => self.node.install_snapshot(from, term, index, last_term),
            Event::Inbound(Inbound::Digest {
                from,
                index,
                digest,
            }) => {
                self.digests.entry(index).or_default().insert(from, digest);
                self.check_digests(index);
            }
            Event::Propose { payload, reply } => {
                if self.pending.len() >= self.cfg.max_pending {
                    let _ = reply.send(Err(ProposeError::Busy));
                } else {
                    match self.node.propose(payload) {
                        Ok(index) => {
                            self.pending.insert(
                                index,
                                Pending {
                                    term: self.node.term(),
                                    reply,
                                    deadline: Instant::now() + self.cfg.propose_timeout,
                                },
                            );
                            *replicate = true;
                        }
                        Err(hint) => {
                            let _ = reply.send(Err(ProposeError::NotLeader(hint)));
                        }
                    }
                }
            }
            Event::Hash(reply) => {
                let applied = self.node.applied_index();
                let _ = reply.send(self.sm.digest(applied).map(|d| (applied, d)));
            }
        }
        true
    }

    fn expire(&mut self, now: Instant) {
        let dead: Vec<Index> = self
            .pending
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(&i, _)| i)
            .collect();
        for i in dead {
            if let Some(p) = self.pending.remove(&i) {
                let _ = p.reply.send(Err(ProposeError::Timeout));
            }
        }
    }

    /// Execute the node's pending effects.
    fn pump(&mut self) {
        loop {
            let effects = self.node.take_effects();
            if effects.is_empty() {
                return;
            }
            for e in effects {
                match e {
                    Effect::Send { to, msg } => self.transport.send(to, msg),
                    Effect::Persist(p) => {
                        if let Some((idx, _)) = p.compact {
                            self.compact_tokens.insert(p.token, idx);
                        }
                        if let Some(tx) = &self.io_tx {
                            let _ = tx.send(p);
                        }
                    }
                    Effect::Apply { index, entry } => self.apply(index, &entry),
                    Effect::NeedSnapshot { to, index, term } => {
                        self.transport
                            .send_snapshot(to, self.node.term(), index, term);
                    }
                    Effect::Restore { index, .. } => match self.snaps.get(index) {
                        Ok((_, data)) => {
                            if let Err(e) = self.sm.restore(index, &data) {
                                self.halt(format!("restoring snapshot {index}: {e}"));
                            }
                        }
                        Err(e) => self.halt(format!("reading snapshot {index}: {e}")),
                    },
                }
            }
        }
    }

    fn apply(&mut self, index: Index, entry: &Entry) {
        self.sm.apply(index, entry);
        if let Some(p) = self.pending.remove(&index) {
            let r = if p.term == entry.term {
                Ok(index)
            } else {
                Err(ProposeError::Superseded)
            };
            let _ = p.reply.send(r);
        }
        let every = self.cfg.digest_every;
        if every > 0 && index % every == 0 {
            if let Some(d) = self.sm.digest(index) {
                self.digests
                    .entry(index)
                    .or_default()
                    .insert(self.cfg.id, d);
                for &(id, _) in &self.cfg.members {
                    if id != self.cfg.id {
                        self.transport.send_digest(id, index, d);
                    }
                }
                self.check_digests(index);
            }
        }
    }

    /// Fail-stop if this node's state differs from the majority's.
    fn check_digests(&mut self, index: Index) {
        let Some(seen) = self.digests.get(&index) else {
            return;
        };
        let Some(own) = seen.get(&self.cfg.id).copied() else {
            return;
        };
        let n = self.cfg.members.len();
        let quorum = n / 2 + 1;
        let mut groups: HashMap<[u8; 32], usize> = HashMap::new();
        for d in seen.values() {
            *groups.entry(*d).or_default() += 1;
        }
        if groups[&own] >= quorum {
            if seen.len() == n {
                self.digests_verified += 1;
                self.digests.remove(&index);
            }
        } else if groups.values().any(|&c| c >= quorum) {
            self.halt(format!(
                "state diverged from the majority at index {index} (own digest {})",
                hex(&own)
            ));
        } else if seen.len() == n {
            self.halt(format!("state digests at index {index} have no majority"));
        }
        // Bound memory.
        let keep_from = index.saturating_sub(self.cfg.digest_every * 64);
        self.digests = self.digests.split_off(&keep_from);
    }

    fn halt(&mut self, why: String) {
        eprintln!("orderer-cluster node {}: HALT: {why}", self.cfg.id);
        self.halted = Some(why);
    }

    fn maybe_compact(&mut self) {
        let every = self.cfg.compact_every;
        if every == 0 || self.halted.is_some() {
            return;
        }
        let applied = self.node.applied_index();
        if applied.saturating_sub(self.node.base()) < every {
            return;
        }
        let Some(term) = self.node.entry_at(applied).map(|e| e.term) else {
            return;
        };
        let data = self.sm.snapshot(applied);
        if let Err(e) = self.snaps.put(applied, term, &data) {
            self.halt(format!("writing snapshot {applied}: {e}"));
            return;
        }
        self.node.compact(applied);
    }

    fn publish_status(&self) {
        let mut s = self.status.lock().expect("status lock");
        s.role = self.node.role();
        s.term = self.node.term();
        s.leader = self.node.leader();
        s.commit = self.node.commit_index();
        s.applied = self.node.applied_index();
        s.base = self.node.base();
        s.last_index = self.node.last_index();
        s.pending = self.pending.len();
        s.halted = self.halted.clone();
        s.digests_verified = self.digests_verified;
    }
}
