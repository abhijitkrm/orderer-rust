//! Pipeline assembly: rings, threads and control (spec/PIPELINE.md).
//!
//! ```text
//! Handle::publish ─▶ ingress (multi-producer) ─▶ router ─┬─▶ inbox[p] ─▶ journal[p] ─▶ engine[p] ─▶ outbox[p] ─▶ egress[p]
//!                                                        └─▶ …            (fsync: syncer thread)
//! ```
//!
//! - **router** (1 thread): sole ingress consumer. Stamps `iseq`, routes by
//!   symbol, broadcasts controls, commits every inbox once per batch.
//! - **journal[p]**: first consumer of `inbox[p]`; appends each command record.
//!   **engine[p]** depends on it — journal-before-apply is a ring
//!   dependency, not a call.
//! - **engine[p]**: owns the partition's `MatchingCore`; stages each event into
//!   `outbox[p]`, commits once per inbox batch.
//! - **egress[p]**: runs the egress plugs in order, marks drain epochs.
//! - **syncer** (1 thread): group-commits fsyncs of every partition's command
//!   journal off the journal threads, then advances `durable_iseq`.

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use orderer_core::{snapshot, BookConfig, Command, MatchingCore, Symbol};
use orderer_disruptor::{
    Consumer, MultiProducer, PublishError, RingBuilder, SingleProducer, WaitStrategy,
};

use crate::egress::{Egress, EgressCtx, EgressFactory};
use crate::journal::{
    open_journal, push_cmd, push_evt, JournalConfig, JournalFormat, Kind, MAX_RECORD,
};
use crate::msg::{Body, CmdMsg, Control, EvtBody, EvtMsg};
use crate::routing::{PartitionMap, RoutingError};
use crate::writer::{ChunkWriter, Marks};

// ---- errors -------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The pipeline is shut down (or shutting down).
    Closed,
    /// `try_publish`: the ingress ring is full. Nothing was sequenced.
    Full,
    /// A pipeline thread failed; the pipeline stopped.
    Failed(String),
    /// Invalid configuration (exit code 2 territory).
    Config(String),
    Io(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Closed => f.write_str("pipeline closed"),
            Error::Full => f.write_str("ingress full"),
            Error::Failed(m) => write!(f, "pipeline failed: {m}"),
            Error::Config(m) => write!(f, "configuration error: {m}"),
            Error::Io(m) => write!(f, "I/O error: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<RoutingError> for Error {
    fn from(e: RoutingError) -> Self {
        Error::Config(e.0)
    }
}

// ---- configuration ------------------------------------------------------------------

/// Wait strategy per stage (spec: not observable; a CPU/latency trade).
#[derive(Clone, Copy, Debug)]
pub struct Waits {
    pub router: WaitStrategy,
    pub journal: WaitStrategy,
    pub engine: WaitStrategy,
    pub egress: WaitStrategy,
}

impl Waits {
    /// Every stage backs off when idle — the friendly default.
    pub const fn relaxed() -> Waits {
        Waits {
            router: WaitStrategy::backoff(),
            journal: WaitStrategy::backoff(),
            engine: WaitStrategy::backoff(),
            egress: WaitStrategy::backoff(),
        }
    }

    /// Router and engines busy-spin (one core each); journal and egress back
    /// off. The benchmark configuration.
    pub const fn low_latency() -> Waits {
        Waits {
            router: WaitStrategy::BusySpin,
            journal: WaitStrategy::backoff(),
            engine: WaitStrategy::BusySpin,
            egress: WaitStrategy::backoff(),
        }
    }
}

impl Default for Waits {
    fn default() -> Self {
        Waits::relaxed()
    }
}

/// Starting state for a pipeline built after recovery.
pub struct Initial<C> {
    /// One core per partition, already restored/replayed.
    pub cores: Vec<C>,
    /// `iseq` the first new command will get.
    pub next_iseq: u64,
}

pub struct PipelineBuilder<C> {
    book: BookConfig,
    partitions: u32,
    map: Option<PartitionMap>,
    ingress: usize,
    inbox: usize,
    outbox: usize,
    waits: Waits,
    journal: Option<JournalConfig>,
    egress: Vec<Box<dyn EgressFactory>>,
    timestamps: bool,
    check_invariants: bool,
    initial: Option<Initial<C>>,
    journal_threads: Option<usize>,
    egress_threads: Option<usize>,
}

impl<C: MatchingCore> Default for PipelineBuilder<C> {
    fn default() -> Self {
        PipelineBuilder {
            book: BookConfig::default(),
            partitions: 1,
            map: None,
            ingress: 1 << 16,
            inbox: 1 << 14,
            outbox: 1 << 15,
            waits: Waits::default(),
            journal: None,
            egress: Vec::new(),
            timestamps: false,
            check_invariants: false,
            initial: None,
            journal_threads: None,
            egress_threads: None,
        }
    }
}

impl<C: MatchingCore> PipelineBuilder<C> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Default config of every book (spec matcher SPEC §2).
    pub fn book_config(mut self, cfg: BookConfig) -> Self {
        self.book = cfg;
        self
    }

    /// Partition count `P` with hash routing.
    pub fn partitions(mut self, p: u32) -> Self {
        self.partitions = p;
        self
    }

    /// Explicit routing (its `partitions()` overrides [`partitions`](Self::partitions)).
    pub fn partition_map(mut self, map: PartitionMap) -> Self {
        self.partitions = map.partitions();
        self.map = Some(map);
        self
    }

    /// Ring sizes (powers of two).
    pub fn ring_sizes(mut self, ingress: usize, inbox: usize, outbox: usize) -> Self {
        self.ingress = ingress;
        self.inbox = inbox;
        self.outbox = outbox;
        self
    }

    pub fn waits(mut self, waits: Waits) -> Self {
        self.waits = waits;
        self
    }

    /// Per-partition command (and optionally event) journals.
    pub fn journal(mut self, cfg: JournalConfig) -> Self {
        self.journal = Some(cfg);
        self
    }

    /// Attach an egress plug (instantiated once per partition).
    pub fn egress(mut self, f: impl EgressFactory + 'static) -> Self {
        self.egress.push(Box::new(f));
        self
    }

    /// Stamp publish times for end-to-end latency (costs a clock read per
    /// publish).
    pub fn timestamps(mut self, on: bool) -> Self {
        self.timestamps = on;
        self
    }

    /// Check book invariants after every command (fuzzing; slow).
    pub fn check_invariants(mut self, on: bool) -> Self {
        self.check_invariants = on;
        self
    }

    /// How many threads run the partitions' journal and egress stages
    /// (partition `p` → thread `p % n`). Default: one of each for the whole
    /// pipeline — engine threads keep the cores; journal and egress work is
    /// light and batches well. Not observable (spec/PIPELINE.md §8).
    pub fn stage_threads(mut self, journal: usize, egress: usize) -> Self {
        self.journal_threads = Some(journal.max(1));
        self.egress_threads = Some(egress.max(1));
        self
    }

    /// Start from recovered cores instead of empty ones.
    pub fn initial(mut self, initial: Initial<C>) -> Self {
        self.initial = Some(initial);
        self
    }

    pub fn build(self) -> Result<Pipeline<C>, Error> {
        Pipeline::start(self)
    }
}

// ---- shared state -------------------------------------------------------------

#[repr(align(128))]
#[derive(Default)]
struct Padded(AtomicU64);

#[repr(align(128))]
#[derive(Default)]
struct InFlight(AtomicBool);

struct SnapState {
    blocks: Vec<(Symbol, String)>,
    remaining: u32,
    cut: u64,
}

struct Shared {
    partitions: u32,
    book: BookConfig,
    epoch: Instant,
    timestamps: AtomicBool,
    closed: AtomicBool,
    failed: AtomicBool,
    failure: Mutex<Option<String>>,
    handles: Mutex<Vec<Arc<InFlight>>>,
    next_epoch: AtomicU64,
    egress_epoch: Box<[Padded]>,
    next_op: AtomicU64,
    snaps: Mutex<HashMap<u64, SnapState>>,
    snap_cv: Condvar,
    flushed: Box<[Arc<AtomicU64>]>,
    durable: Box<[Arc<AtomicU64>]>,
    alerts: Mutex<Vec<Box<dyn Fn() + Send + Sync>>>,
}

impl Shared {
    fn fail(&self, msg: String) {
        {
            let mut f = self.failure.lock().unwrap_or_else(|e| e.into_inner());
            f.get_or_insert(msg);
        }
        self.failed.store(true, Ordering::SeqCst);
        if let Ok(alerts) = self.alerts.lock() {
            for a in alerts.iter() {
                a();
            }
        }
        self.snap_cv.notify_all();
    }

    fn check(&self) -> Result<(), Error> {
        if self.failed.load(Ordering::Acquire) {
            let m = self.failure.lock().unwrap_or_else(|e| e.into_inner());
            return Err(Error::Failed(m.clone().unwrap_or_default()));
        }
        Ok(())
    }

    #[inline]
    fn now_ns(&self) -> u64 {
        if self.timestamps.load(Ordering::Relaxed) {
            (self.epoch.elapsed().as_nanos() as u64).max(1)
        } else {
            0
        }
    }
}

/// Marks the pipeline failed if its thread unwinds (a panic in a core, a
/// plug, an invariant check) so waiters return instead of hanging.
struct FailOnPanic(Arc<Shared>, &'static str);

impl Drop for FailOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.fail(format!("{} thread panicked", self.1));
        }
    }
}

/// Wait with spin → yield → short sleeps until `done()`, failing fast.
fn wait_until(shared: &Shared, mut done: impl FnMut() -> bool) -> Result<(), Error> {
    let mut step = 0u32;
    while !done() {
        shared.check()?;
        step = step.saturating_add(1);
        if step < 64 {
            std::hint::spin_loop();
        } else if step < 256 {
            std::thread::yield_now();
        } else {
            std::thread::sleep(Duration::from_micros(50));
        }
    }
    Ok(())
}

// ---- the handle ----------------------------------------------------------------

/// Publishes commands. Cheap to clone; `Send + Sync`. Each clone registers
/// an in-flight flag so `shutdown` can wait out publishes that began before
/// it closed the pipeline — a publish that returns `Ok` is always applied.
pub struct Handle {
    ingress: MultiProducer<CmdMsg>,
    shared: Arc<Shared>,
    flag: Arc<InFlight>,
}

impl Clone for Handle {
    fn clone(&self) -> Self {
        Handle::register(self.ingress.clone(), self.shared.clone())
    }
}

impl Handle {
    fn register(ingress: MultiProducer<CmdMsg>, shared: Arc<Shared>) -> Handle {
        let flag = Arc::new(InFlight::default());
        shared.handles.lock().unwrap().push(flag.clone());
        Handle {
            ingress,
            shared,
            flag,
        }
    }

    #[inline]
    fn enter(&self) -> Result<(), Error> {
        self.flag.0.store(true, Ordering::SeqCst);
        if self.shared.closed.load(Ordering::SeqCst) {
            self.flag.0.store(false, Ordering::Release);
            return Err(Error::Closed);
        }
        Ok(())
    }

    #[inline]
    fn exit(&self) {
        self.flag.0.store(false, Ordering::Release);
    }

    #[inline]
    fn fill(&self, m: &mut CmdMsg, sym: Symbol, cmd: Command, t: u64) {
        m.symbol = sym;
        m.t_pub = t;
        m.body = Body::Cmd(cmd);
    }

    /// Sequence one command (blocks while ingress is full). `Ok` means
    /// sequenced and will be applied — not durable (spec/PIPELINE.md §5).
    #[inline]
    pub fn publish(&self, sym: Symbol, cmd: Command) -> Result<(), Error> {
        self.enter()?;
        let t = self.shared.now_ns();
        let r = self.ingress.publish(|m| self.fill(m, sym, cmd, t));
        self.exit();
        r.map(|_| ()).map_err(|_| Error::Closed)
    }

    /// Sequence one command, or `Err(Full)` without waiting.
    #[inline]
    pub fn try_publish(&self, sym: Symbol, cmd: Command) -> Result<(), Error> {
        self.enter()?;
        let t = self.shared.now_ns();
        let r = self.ingress.try_publish(|m| self.fill(m, sym, cmd, t));
        self.exit();
        match r {
            Ok(_) => Ok(()),
            Err(PublishError::Full) => Err(Error::Full),
            Err(PublishError::Alerted) => Err(Error::Closed),
        }
    }

    /// Sequence many commands with one claim per ring-sized chunk; they get
    /// consecutive `iseq`s (no other producer interleaves within a chunk).
    pub fn publish_batch(&self, cmds: &[(Symbol, Command)]) -> Result<(), Error> {
        self.enter()?;
        let chunk = self.ingress.size().min(256);
        let mut r = Ok(0);
        for part in cmds.chunks(chunk) {
            let t = self.shared.now_ns();
            r = self
                .ingress
                .publish_batch(part.len(), |i, m| self.fill(m, part[i].0, part[i].1, t));
            if r.is_err() {
                break;
            }
        }
        self.exit();
        r.map(|_| ()).map_err(|_| Error::Closed)
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if let Ok(mut hs) = self.shared.handles.lock() {
            hs.retain(|h| !Arc::ptr_eq(h, &self.flag));
        }
    }
}

// ---- snapshots ----------------------------------------------------------------

/// A merged `matcher-snap/1` snapshot plus its cut (spec/JOURNAL.md §4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub body: String,
    /// The snapshot reflects exactly the commands with `iseq ≤ this`.
    pub iseq: u64,
    /// The writer's partition count (informational).
    pub partitions: u32,
}

impl Snapshot {
    /// The `.meta` sidecar line (with newline).
    pub fn meta(&self) -> String {
        format!(
            "{{\"format\":\"orderer-meta/1\",\"iseq\":{},\"partitions\":{}}}\n",
            self.iseq, self.partitions
        )
    }

    /// Write the body to `path` and the sidecar to `path.meta`.
    pub fn write(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        let path = path.as_ref();
        std::fs::write(path, &self.body)?;
        std::fs::write(meta_path(path), self.meta())
    }
}

pub fn meta_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".meta");
    s.into()
}

// ---- the pipeline -----------------------------------------------------------------

/// A running pipeline. Dropping it shuts it down.
pub struct Pipeline<C> {
    handle: Handle,
    shared: Arc<Shared>,
    map: PartitionMap,
    threads: Vec<(&'static str, JoinHandle<()>)>,
    shut: bool,
    _core: PhantomData<fn() -> C>,
}

impl<C: MatchingCore> Pipeline<C> {
    pub fn builder() -> PipelineBuilder<C> {
        PipelineBuilder::new()
    }

    fn start(b: PipelineBuilder<C>) -> Result<Pipeline<C>, Error> {
        let map = match b.map {
            Some(m) => m,
            None => PartitionMap::hash(b.partitions)?,
        };
        let p_count = map.partitions();
        for (name, n) in [
            ("ingress", b.ingress),
            ("inbox", b.inbox),
            ("outbox", b.outbox),
        ] {
            if !n.is_power_of_two() || n < 2 {
                return Err(Error::Config(format!(
                    "{name} ring size must be a power of two ≥ 2"
                )));
            }
        }
        let (cores, next_iseq) = match b.initial {
            Some(init) => {
                if init.cores.len() != p_count as usize {
                    return Err(Error::Config(format!(
                        "{} recovered cores for {p_count} partitions",
                        init.cores.len()
                    )));
                }
                (init.cores, init.next_iseq.max(1))
            }
            None => ((0..p_count).map(|_| C::new(b.book)).collect(), 1),
        };
        let journaled = b.journal.is_some();
        let start_wm = next_iseq - 1;
        let shared = Arc::new(Shared {
            partitions: p_count,
            book: b.book,
            epoch: Instant::now(),
            timestamps: AtomicBool::new(b.timestamps),
            closed: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            failure: Mutex::new(None),
            handles: Mutex::new(Vec::new()),
            next_epoch: AtomicU64::new(0),
            egress_epoch: (0..p_count).map(|_| Padded::default()).collect(),
            next_op: AtomicU64::new(0),
            snaps: Mutex::new(HashMap::new()),
            snap_cv: Condvar::new(),
            flushed: (0..p_count)
                .map(|_| Arc::new(AtomicU64::new(start_wm)))
                .collect(),
            durable: (0..p_count)
                .map(|_| Arc::new(AtomicU64::new(if journaled { start_wm } else { u64::MAX })))
                .collect(),
            alerts: Mutex::new(Vec::new()),
        });

        // Journals are opened (and their I/O threads started) before any
        // pipeline thread, so I/O errors surface from `build`.
        let mut cmd_writers: Vec<Option<ChunkWriter>> = Vec::new();
        let mut evt_writers: Vec<Option<ChunkWriter>> = Vec::new();
        if let Some(cfg) = &b.journal {
            let io = |e: std::io::Error| Error::Io(e.to_string());
            std::fs::create_dir_all(&cfg.dir).map_err(io)?;
            for p in 0..p_count {
                let f = open_journal(cfg, Kind::Cmd, p, p_count).map_err(io)?;
                let marks = Marks {
                    flushed: shared.flushed[p as usize].clone(),
                    durable: shared.durable[p as usize].clone(),
                };
                cmd_writers.push(Some(
                    ChunkWriter::start(f, format!("orderer-io-cmd-{p}"), Some(cfg.fsync), marks)
                        .map_err(io)?,
                ));
                evt_writers.push(if cfg.events {
                    let f = open_journal(cfg, Kind::Evt, p, p_count).map_err(io)?;
                    let marks = Marks {
                        flushed: Arc::new(AtomicU64::new(0)),
                        durable: Arc::new(AtomicU64::new(0)),
                    };
                    Some(
                        ChunkWriter::start(f, format!("orderer-io-evt-{p}"), None, marks)
                            .map_err(io)?,
                    )
                } else {
                    None
                });
            }
        }

        let mut threads = Vec::new();
        let mut alerts: Vec<Box<dyn Fn() + Send + Sync>> = Vec::new();
        let mut inbox_producers = Vec::new();
        let n_journal = b.journal_threads.unwrap_or(1).min(p_count as usize);
        let n_egress = b.egress_threads.unwrap_or(1).min(p_count as usize);
        let mut journal_groups: Vec<Vec<JournalPart>> =
            (0..n_journal).map(|_| Vec::new()).collect();
        let mut egress_groups: Vec<Vec<EgressPart>> = (0..n_egress).map(|_| Vec::new()).collect();
        let factories = b.egress;

        for (p, core) in (0..p_count).zip(cores) {
            let mut ib = RingBuilder::<CmdMsg>::new(b.inbox);
            let jid = journaled.then(|| ib.consumer_with(&[], b.waits.journal));
            let deps: Vec<_> = jid.into_iter().collect();
            let eid = ib.consumer_with(&deps, b.waits.engine);
            let (inbox, inbox_cons) = ib.build_single();
            let ctl = inbox.control();
            alerts.push(Box::new(move || ctl.alert()));
            inbox_producers.push(inbox);
            let mut inbox_cons: Vec<Option<Consumer<CmdMsg>>> =
                inbox_cons.into_iter().map(Some).collect();

            let mut ob = RingBuilder::<EvtMsg>::new(b.outbox);
            ob.consumer_with(&[], b.waits.egress);
            let (outbox, mut outbox_cons) = ob.build_single();
            let ctl = outbox.control();
            alerts.push(Box::new(move || ctl.alert()));

            if let Some(jid) = jid {
                journal_groups[p as usize % n_journal].push(JournalPart {
                    p,
                    inbox: inbox_cons[jid.index()].take().unwrap(),
                    w: cmd_writers[p as usize].take().unwrap(),
                    format: b.journal.as_ref().unwrap().format,
                    scratch: String::with_capacity(MAX_RECORD),
                    stopped: false,
                    last_handoff: Instant::now(),
                });
            }
            {
                let cons = inbox_cons[eid.index()].take().unwrap();
                let sh = shared.clone();
                let check = b.check_invariants;
                threads.push(spawn("engine", p, move || {
                    engine_thread(sh, cons, outbox, core, check)
                }));
            }
            let ctx = EgressCtx {
                partition: p,
                partitions: p_count,
                epoch: shared.epoch,
                durable: shared.durable[p as usize].clone(),
            };
            let mut plugs: Vec<Box<dyn Egress>> = Vec::with_capacity(factories.len() + 1);
            if let Some(w) = evt_writers.get_mut(p as usize).and_then(Option::take) {
                plugs.push(Box::new(EvtJournal {
                    w,
                    format: b.journal.as_ref().unwrap().format,
                    scratch: String::with_capacity(MAX_RECORD),
                    last_handoff: Instant::now(),
                }));
            }
            plugs.extend(factories.iter().map(|f| f.create(&ctx)));
            egress_groups[p as usize % n_egress].push(EgressPart {
                p,
                outbox: outbox_cons.pop().unwrap(),
                plugs,
                ctx,
                last_iseq: 0,
                stopped: false,
            });
        }
        if journaled {
            for (i, parts) in journal_groups.into_iter().enumerate() {
                let sh = shared.clone();
                threads.push(spawn("journal", i as u32, move || {
                    journal_thread(sh, parts)
                }));
            }
        }
        for (i, parts) in egress_groups.into_iter().enumerate() {
            let sh = shared.clone();
            threads.push(spawn("egress", i as u32, move || {
                egress_thread(sh, parts, journaled)
            }));
        }

        let mut rb = RingBuilder::<CmdMsg>::new(b.ingress);
        rb.consumer_with(&[], b.waits.router);
        let (ingress, mut router_cons) = rb.build_multi();
        let ctl = ingress.control();
        alerts.push(Box::new(move || ctl.alert()));
        {
            let cons = router_cons.pop().unwrap();
            let sh = shared.clone();
            let m = map.clone();
            threads.push(spawn("router", 0, move || {
                router_thread(sh, cons, inbox_producers, m, next_iseq)
            }));
        }
        *shared.alerts.lock().unwrap() = alerts;

        let handle = Handle::register(ingress, shared.clone());
        Ok(Pipeline {
            handle,
            shared,
            map,
            threads,
            shut: false,
            _core: PhantomData,
        })
    }
}

fn spawn(
    name: &'static str,
    p: u32,
    f: impl FnOnce() + Send + 'static,
) -> (&'static str, JoinHandle<()>) {
    let h = std::thread::Builder::new()
        .name(format!("orderer-{name}-{p}"))
        .spawn(f)
        .expect("spawn pipeline thread");
    (name, h)
}

impl<C> Pipeline<C> {
    /// A new publishing handle.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    #[inline]
    pub fn publish(&self, sym: Symbol, cmd: Command) -> Result<(), Error> {
        self.handle.publish(sym, cmd)
    }

    #[inline]
    pub fn try_publish(&self, sym: Symbol, cmd: Command) -> Result<(), Error> {
        self.handle.try_publish(sym, cmd)
    }

    pub fn publish_batch(&self, cmds: &[(Symbol, Command)]) -> Result<(), Error> {
        self.handle.publish_batch(cmds)
    }

    pub fn partitions(&self) -> u32 {
        self.shared.partitions
    }

    pub fn partition_of(&self, sym: Symbol) -> u32 {
        self.map.partition(sym)
    }

    pub fn book_config(&self) -> BookConfig {
        self.shared.book
    }

    /// Turn publish timestamps (latency probes) on or off at runtime.
    pub fn set_timestamps(&self, on: bool) {
        self.shared.timestamps.store(on, Ordering::Relaxed);
    }

    /// Highest durable `iseq` of partition `p` (`u64::MAX` without journals).
    pub fn durable_iseq(&self, p: u32) -> u64 {
        self.shared.durable[p as usize].load(Ordering::Acquire)
    }

    fn publish_ctl(&self, ctl: Control) -> Result<(), Error> {
        self.shared.check()?;
        if self.shared.closed.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        self.handle
            .ingress
            .publish(|m| {
                m.symbol = 0;
                m.t_pub = 0;
                m.body = Body::Ctl(ctl);
            })
            .map(|_| ())
            .map_err(|_| Error::Closed)
    }

    /// Barrier: returns once every command published before the call has
    /// been applied and its events delivered to every egress plug
    /// (spec/PIPELINE.md §6).
    pub fn drain(&self) -> Result<(), Error> {
        let epoch = self.shared.next_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.publish_ctl(Control::Barrier { epoch })?;
        wait_until(&self.shared, || {
            self.shared
                .egress_epoch
                .iter()
                .all(|e| e.0.load(Ordering::Acquire) >= epoch)
        })
    }

    /// A consistent snapshot of every book, cut at this point of the ingress
    /// order (spec/PIPELINE.md §6, spec/JOURNAL.md §4).
    pub fn snapshot(&self) -> Result<Snapshot, Error> {
        let op_id = self.shared.next_op.fetch_add(1, Ordering::SeqCst) + 1;
        self.shared.snaps.lock().unwrap().insert(
            op_id,
            SnapState {
                blocks: Vec::new(),
                remaining: self.shared.partitions,
                cut: 0,
            },
        );
        self.publish_ctl(Control::Snapshot { op_id })?;
        let mut g = self.shared.snaps.lock().unwrap();
        loop {
            self.shared.check()?;
            if g.get(&op_id).is_some_and(|s| s.remaining == 0) {
                break;
            }
            g = self
                .shared
                .snap_cv
                .wait_timeout(g, Duration::from_millis(10))
                .unwrap()
                .0;
        }
        let mut st = g.remove(&op_id).unwrap();
        drop(g);
        st.blocks.sort_by_key(|(s, _)| *s);
        let mut body = String::new();
        snapshot::write_header(self.shared.book, &mut body);
        for (_, b) in &st.blocks {
            body.push_str(b);
        }
        Ok(Snapshot {
            body,
            iseq: st.cut,
            partitions: self.shared.partitions,
        })
    }

    /// Stop accepting commands, drain everything already sequenced, stop
    /// every thread. Idempotent.
    pub fn shutdown(&mut self) -> Result<(), Error> {
        if self.shut {
            return self.shared.check();
        }
        self.shut = true;
        self.shared.closed.store(true, Ordering::SeqCst);
        // wait out publishes that saw the pipeline open
        let flags: Vec<_> = self.shared.handles.lock().unwrap().clone();
        let _ = wait_until(&self.shared, || {
            flags.iter().all(|f| !f.0.load(Ordering::SeqCst))
        });
        if self.shared.check().is_ok() {
            let r = self.handle.ingress.publish(|m| {
                m.symbol = 0;
                m.t_pub = 0;
                m.body = Body::Ctl(Control::Shutdown);
            });
            if r.is_err() {
                self.shared.fail("ingress alerted before shutdown".into());
            }
        }
        for (name, t) in self.threads.drain(..) {
            if t.join().is_err() {
                self.shared.fail(format!("{name} thread panicked"));
            }
        }
        for a in self.shared.alerts.lock().unwrap().iter() {
            a();
        }
        self.shared.check()
    }
}

impl<C> Drop for Pipeline<C> {
    fn drop(&mut self) {
        if !self.shut {
            let _ = self.shutdown();
        }
    }
}

// ---- threads --------------------------------------------------------------------

fn router_thread(
    shared: Arc<Shared>,
    mut ingress: Consumer<CmdMsg>,
    mut inboxes: Vec<SingleProducer<CmdMsg>>,
    map: PartitionMap,
    next_iseq: u64,
) {
    let _g = FailOnPanic(shared.clone(), "router");
    let mut iseq = next_iseq - 1; // last assigned
    let mut stop = false;
    while !stop {
        let r = ingress.wait_poll(|m, _, eob| {
            match m.body {
                Body::Cmd(_) => {
                    iseq += 1;
                    let p = map.partition(m.symbol) as usize;
                    let _ = inboxes[p].stage(|s| {
                        *s = *m;
                        s.iseq = iseq;
                    });
                }
                Body::Ctl(ctl) => {
                    for ib in inboxes.iter_mut() {
                        let _ = ib.stage(|s| {
                            *s = *m;
                            s.iseq = iseq; // the cut: commands before this control
                        });
                    }
                    if ctl == Control::Shutdown {
                        stop = true;
                    }
                }
            }
            if eob {
                for ib in inboxes.iter_mut() {
                    ib.commit();
                }
            }
        });
        if r.is_err() {
            break;
        }
    }
    for ib in inboxes.iter_mut() {
        ib.commit();
    }
}

/// Hand a partial journal chunk to its I/O thread once the stage has been
/// idle this long (full chunks go at once). Bounds journaling latency at
/// low load without a handoff per tiny batch.
const HANDOFF_IDLE: Duration = Duration::from_micros(50);

struct JournalPart {
    p: u32,
    inbox: Consumer<CmdMsg>,
    w: ChunkWriter,
    format: JournalFormat,
    scratch: String,
    stopped: bool,
    last_handoff: Instant,
}

/// Encodes command records for a group of partitions into their chunk
/// writers. Each partition's engine depends on this consumer: a record is
/// handed to the journal before its command is applied (spec/PIPELINE.md
/// §4). No syscalls here — the I/O threads write and fsync.
fn journal_thread(shared: Arc<Shared>, mut parts: Vec<JournalPart>) {
    let _g = FailOnPanic(shared.clone(), "journal");
    crate::writer::prewarm_thread();
    let mut idle_on = 0;
    loop {
        let mut total = 0;
        let mut live = 0;
        for (i, jp) in parts.iter_mut().enumerate() {
            if jp.stopped {
                continue;
            }
            live += 1;
            idle_on = i;
            let (mut force, mut stop) = (false, false);
            let (w, format, scratch) = (&mut jp.w, jp.format, &mut jp.scratch);
            let n = jp.inbox.poll(|m, _, _| match m.body {
                Body::Cmd(cmd) => {
                    w.reserve(MAX_RECORD);
                    push_cmd(w.buf(), format, m.iseq, m.symbol, &cmd, scratch);
                    w.record(m.iseq);
                }
                Body::Ctl(Control::Shutdown) => stop = true,
                Body::Ctl(Control::Barrier { .. } | Control::Snapshot { .. }) => force = true,
                Body::Ctl(Control::Nop) => {}
            });
            total += n;
            if stop {
                if let Err(e) = jp.w.finish() {
                    shared.fail(format!("journal {}: {e}", jp.p));
                    return;
                }
                jp.stopped = true;
            } else if jp.w.pending() > 0
                && (force || (n == 0 && jp.last_handoff.elapsed() >= HANDOFF_IDLE))
            {
                jp.w.hand_off();
                jp.last_handoff = Instant::now();
            }
        }
        if live == 0 || parts.iter().any(|jp| jp.inbox.is_alerted()) {
            return;
        }
        let c = &mut parts[idle_on].inbox;
        if total == 0 {
            c.idle();
        } else {
            c.reset_idle();
        }
    }
}

fn engine_thread<C: MatchingCore>(
    shared: Arc<Shared>,
    mut inbox: Consumer<CmdMsg>,
    mut out: SingleProducer<EvtMsg>,
    mut core: C,
    check_invariants: bool,
) {
    let _g = FailOnPanic(shared.clone(), "engine");
    let mut stop = false;
    while !stop {
        let r = inbox.wait_poll(|m, _, eob| {
            match m.body {
                Body::Cmd(cmd) => {
                    let (iseq, t_pub) = (m.iseq, m.t_pub);
                    core.apply(m.symbol, cmd, &mut |sym, seq, ev| {
                        let _ = out.stage(|e| {
                            e.iseq = iseq;
                            e.seq = seq;
                            e.t_pub = t_pub;
                            e.symbol = sym;
                            e.body = EvtBody::Event(*ev);
                        });
                    });
                    if check_invariants {
                        core.check_invariants();
                    }
                }
                Body::Ctl(ctl) => {
                    if let Control::Snapshot { op_id } = ctl {
                        let mut blocks = Vec::new();
                        core.snapshot_blocks(&mut blocks);
                        let mut g = shared.snaps.lock().unwrap();
                        if let Some(st) = g.get_mut(&op_id) {
                            st.blocks.append(&mut blocks);
                            st.cut = m.iseq;
                            st.remaining -= 1;
                        }
                        drop(g);
                        shared.snap_cv.notify_all();
                    }
                    let _ = out.stage(|e| {
                        e.iseq = m.iseq;
                        e.seq = 0;
                        e.t_pub = 0;
                        e.symbol = 0;
                        e.body = EvtBody::Ctl(ctl);
                    });
                    if ctl == Control::Shutdown {
                        stop = true;
                    }
                }
            }
            if eob {
                out.commit();
            }
        });
        if r.is_err() {
            break;
        }
    }
    out.commit();
}

/// The event journal, as the first egress plug of its partition.
struct EvtJournal {
    w: ChunkWriter,
    format: JournalFormat,
    scratch: String,
    last_handoff: Instant,
}

impl Egress for EvtJournal {
    #[inline]
    fn on_event(&mut self, m: &EvtMsg) {
        if let EvtBody::Event(ev) = &m.body {
            self.w.reserve(MAX_RECORD);
            push_evt(
                self.w.buf(),
                self.format,
                m.seq,
                m.symbol,
                ev,
                &mut self.scratch,
            );
            self.w.record(m.seq);
        }
    }
    fn on_idle(&mut self) {
        if self.w.pending() > 0 && self.last_handoff.elapsed() >= HANDOFF_IDLE {
            self.w.hand_off();
            self.last_handoff = Instant::now();
        }
    }
    fn on_shutdown(&mut self) {
        self.w.finish().expect("event journal write");
    }
}

struct EgressPart {
    p: u32,
    outbox: Consumer<EvtMsg>,
    plugs: Vec<Box<dyn Egress>>,
    ctx: EgressCtx,
    last_iseq: u64,
    stopped: bool,
}

/// Runs the egress plugs of a group of partitions.
fn egress_thread(shared: Arc<Shared>, mut parts: Vec<EgressPart>, journaled: bool) {
    let _g = FailOnPanic(shared.clone(), "egress");
    crate::writer::prewarm_thread();
    let mut idle_on = 0;
    loop {
        let mut total = 0;
        let mut live = 0;
        for (i, ep) in parts.iter_mut().enumerate() {
            if ep.stopped {
                continue;
            }
            live += 1;
            idle_on = i;
            let mut stop = false;
            let (plugs, last_iseq, p) = (&mut ep.plugs, &mut ep.last_iseq, ep.p);
            let n = ep.outbox.poll(|m, _, eob| {
                match m.body {
                    EvtBody::Event(_) => {
                        *last_iseq = m.iseq;
                        for pl in plugs.iter_mut() {
                            pl.on_event(m);
                        }
                    }
                    EvtBody::Ctl(Control::Barrier { epoch }) => {
                        for pl in plugs.iter_mut() {
                            pl.on_batch_end();
                            pl.on_idle();
                        }
                        shared.egress_epoch[p as usize]
                            .0
                            .store(epoch, Ordering::Release);
                    }
                    EvtBody::Ctl(Control::Shutdown) => stop = true,
                    EvtBody::Ctl(_) => {}
                }
                if eob {
                    for pl in plugs.iter_mut() {
                        pl.on_batch_end();
                    }
                }
            });
            total += n;
            if stop {
                if journaled {
                    // acks may only go out once the syncer has covered them
                    let (durable, last) = (ep.ctx.durable.clone(), ep.last_iseq);
                    let _ = wait_until(&shared, || durable.load(Ordering::Acquire) >= last);
                }
                for pl in ep.plugs.iter_mut() {
                    pl.on_idle();
                    pl.on_shutdown();
                }
                ep.stopped = true;
            } else if n == 0 {
                for pl in ep.plugs.iter_mut() {
                    pl.on_idle();
                }
            }
        }
        if live == 0 || parts.iter().any(|ep| ep.outbox.is_alerted()) {
            return;
        }
        let c = &mut parts[idle_on].outbox;
        if total == 0 {
            c.idle();
        } else {
            c.reset_idle();
        }
    }
}
