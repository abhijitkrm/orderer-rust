//! Egress: pluggable consumers of each partition's event stream
//! (spec/PIPELINE.md §7).
//!
//! An [`EgressFactory`] is attached to the pipeline once and asked for one
//! [`Egress`] instance per partition. All of a partition's instances run on
//! that partition's egress thread, in attachment order, and see every event
//! of the partition in order. Cross-partition interleaving is unspecified.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use orderer_core::{Event, Symbol};

use crate::msg::{EvtBody, EvtMsg};

/// What a partition's egress instances know about their partition.
#[derive(Clone)]
pub struct EgressCtx {
    pub partition: u32,
    pub partitions: u32,
    /// Monotonic clock origin for `EvtMsg::t_pub`.
    pub epoch: Instant,
    pub(crate) durable: Arc<AtomicU64>,
}

impl EgressCtx {
    /// Highest `iseq` of this partition covered by a completed fsync
    /// (spec/PIPELINE.md §4). `u64::MAX` when journaling is off.
    #[inline]
    pub fn durable_iseq(&self) -> u64 {
        self.durable.load(Ordering::Acquire)
    }

    /// Nanoseconds since `epoch`, comparable with `EvtMsg::t_pub`.
    #[inline]
    pub fn now_ns(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }
}

/// One partition's consumer of events.
pub trait Egress: Send {
    /// Every event, in partition order. `m.body` is always `EvtBody::Event`.
    fn on_event(&mut self, m: &EvtMsg);
    /// After the last event of a ring batch, and before a drain completes.
    fn on_batch_end(&mut self) {}
    /// Called while the partition is idle — release time- or
    /// durability-gated work here.
    fn on_idle(&mut self) {}
    /// Once, when the pipeline shuts down (after every event).
    fn on_shutdown(&mut self) {}
    /// A checkpoint with cut `cut` passed this partition (after every event
    /// of commands up to `cut`). The event journal starts a new segment here.
    fn on_checkpoint(&mut self, _cut: u64) {}
}

/// Creates one [`Egress`] per partition.
pub trait EgressFactory: Send + Sync {
    fn create(&self, ctx: &EgressCtx) -> Box<dyn Egress>;
}

impl<F> EgressFactory for F
where
    F: Fn(&EgressCtx) -> Box<dyn Egress> + Send + Sync,
{
    fn create(&self, ctx: &EgressCtx) -> Box<dyn Egress> {
        self(ctx)
    }
}

#[inline]
fn event(m: &EvtMsg) -> &Event {
    match &m.body {
        EvtBody::Event(ev) => ev,
        EvtBody::Ctl(_) => unreachable!("egress plugs only see events"),
    }
}

// ---- Collect: canonical lines per partition (harnesses, tests) -----------

/// Collects each partition's canonical event lines (spec/HARNESS.md §3),
/// symbol-tagged or not. Read them with [`CollectHandle`] after a drain.
pub struct Collect {
    tagged: bool,
    bufs: Arc<Mutex<Vec<Vec<u8>>>>,
}

/// Shared view of what a [`Collect`] gathered.
#[derive(Clone)]
pub struct CollectHandle {
    bufs: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Collect {
    pub fn new(tagged: bool) -> (Collect, CollectHandle) {
        let bufs = Arc::new(Mutex::new(Vec::new()));
        (
            Collect {
                tagged,
                bufs: bufs.clone(),
            },
            CollectHandle { bufs },
        )
    }
}

impl CollectHandle {
    /// Partition `p`'s lines so far (each `\n`-terminated).
    pub fn partition(&self, p: u32) -> Vec<u8> {
        self.bufs
            .lock()
            .unwrap()
            .get(p as usize)
            .cloned()
            .unwrap_or_default()
    }

    /// The spec/HARNESS.md §3 listing: partition 0's lines, then 1's, …
    pub fn listing(&self) -> Vec<u8> {
        self.bufs.lock().unwrap().concat()
    }

    /// Drain everything collected so far, per partition.
    pub fn take(&self) -> Vec<Vec<u8>> {
        let mut g = self.bufs.lock().unwrap();
        g.iter_mut().map(std::mem::take).collect()
    }
}

struct CollectEgress {
    partition: usize,
    tagged: bool,
    local: Vec<u8>,
    scratch: String,
    bufs: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl EgressFactory for Collect {
    fn create(&self, ctx: &EgressCtx) -> Box<dyn Egress> {
        {
            let mut g = self.bufs.lock().unwrap();
            if g.len() < ctx.partitions as usize {
                g.resize(ctx.partitions as usize, Vec::new());
            }
        }
        Box::new(CollectEgress {
            partition: ctx.partition as usize,
            tagged: self.tagged,
            local: Vec::with_capacity(1 << 16),
            scratch: String::with_capacity(128),
            bufs: self.bufs.clone(),
        })
    }
}

impl Egress for CollectEgress {
    fn on_event(&mut self, m: &EvtMsg) {
        self.scratch.clear();
        if self.tagged {
            Event::write_canonical_sym(m.seq, m.symbol, event(m), &mut self.scratch);
        } else {
            Event::write_canonical(m.seq, event(m), &mut self.scratch);
        }
        self.local.extend_from_slice(self.scratch.as_bytes());
        self.local.push(b'\n');
    }

    fn on_batch_end(&mut self) {
        if !self.local.is_empty() {
            self.bufs.lock().unwrap()[self.partition].append(&mut self.local);
        }
    }

    fn on_shutdown(&mut self) {
        self.on_batch_end();
    }
}

// ---- Callback --------------------------------------------------------------

/// Calls `f(partition, msg)` for every event. `f` is cloned per partition.
pub struct Callback<F>(pub F);

struct CallbackEgress<F> {
    partition: u32,
    f: F,
}

impl<F> EgressFactory for Callback<F>
where
    F: FnMut(u32, &EvtMsg) + Clone + Send + Sync + 'static,
{
    fn create(&self, ctx: &EgressCtx) -> Box<dyn Egress> {
        Box::new(CallbackEgress {
            partition: ctx.partition,
            f: self.0.clone(),
        })
    }
}

impl<F: FnMut(u32, &EvtMsg) + Send> Egress for CallbackEgress<F> {
    #[inline]
    fn on_event(&mut self, m: &EvtMsg) {
        (self.f)(self.partition, m)
    }
}

// ---- Acks: durability-gated delivery ---------------------------------------

/// Delivers each event to `f(partition, msg)` only once the command that
/// caused it is **durable** — `durable_iseq ≥ iseq` for its partition
/// (spec/PIPELINE.md §5). Events wait in a queue preallocated to
/// `capacity`; it grows only if more than `capacity` events are pending.
pub struct Acks<F> {
    f: F,
    capacity: usize,
}

impl<F> Acks<F> {
    pub fn new(f: F, capacity: usize) -> Acks<F> {
        Acks { f, capacity }
    }
}

struct AckEgress<F> {
    ctx: EgressCtx,
    pending: VecDeque<EvtMsg>,
    f: F,
}

impl<F> EgressFactory for Acks<F>
where
    F: FnMut(u32, &EvtMsg) + Clone + Send + Sync + 'static,
{
    fn create(&self, ctx: &EgressCtx) -> Box<dyn Egress> {
        Box::new(AckEgress {
            ctx: ctx.clone(),
            pending: VecDeque::with_capacity(self.capacity),
            f: self.f.clone(),
        })
    }
}

impl<F: FnMut(u32, &EvtMsg) + Send> AckEgress<F> {
    fn release(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let durable = self.ctx.durable_iseq();
        while let Some(m) = self.pending.front() {
            if m.iseq > durable {
                break;
            }
            let m = self.pending.pop_front().unwrap();
            (self.f)(self.ctx.partition, &m);
        }
    }
}

impl<F: FnMut(u32, &EvtMsg) + Send> Egress for AckEgress<F> {
    fn on_event(&mut self, m: &EvtMsg) {
        self.pending.push_back(*m);
    }
    fn on_batch_end(&mut self) {
        self.release();
    }
    fn on_idle(&mut self) {
        self.release();
    }
    fn on_shutdown(&mut self) {
        // the journal thread synced everything before shutdown completed
        self.release();
    }
}

// ---- Metrics: counts + end-to-end latency ----------------------------------

/// Per-partition counters and end-to-end latency samples (spec/BENCH.md
/// §2.2 step 5): one sample per command, taken at its first event,
/// `now - t_pub`. Requires pipeline timestamps.
pub struct Metrics {
    sample_capacity: usize,
    out: Arc<Mutex<Vec<PartitionMetrics>>>,
}

#[derive(Clone, Debug, Default)]
pub struct PartitionMetrics {
    pub partition: u32,
    pub events: u64,
    pub commands: u64,
    pub trades: u64,
    /// Nanoseconds, in arrival order (sort before taking percentiles).
    pub latencies: Vec<u64>,
}

#[derive(Clone)]
pub struct MetricsHandle {
    out: Arc<Mutex<Vec<PartitionMetrics>>>,
}

impl MetricsHandle {
    /// Per-partition results; complete once the pipeline is shut down.
    pub fn results(&self) -> Vec<PartitionMetrics> {
        let mut v = self.out.lock().unwrap().clone();
        v.sort_by_key(|m| m.partition);
        v
    }
}

impl Metrics {
    /// `sample_capacity`: latency samples preallocated per partition (no
    /// allocation while under it).
    pub fn new(sample_capacity: usize) -> (Metrics, MetricsHandle) {
        let out = Arc::new(Mutex::new(Vec::new()));
        (
            Metrics {
                sample_capacity,
                out: out.clone(),
            },
            MetricsHandle { out },
        )
    }
}

struct MetricsEgress {
    ctx: EgressCtx,
    m: PartitionMetrics,
    last_iseq: u64,
    out: Arc<Mutex<Vec<PartitionMetrics>>>,
}

impl EgressFactory for Metrics {
    fn create(&self, ctx: &EgressCtx) -> Box<dyn Egress> {
        Box::new(MetricsEgress {
            ctx: ctx.clone(),
            m: PartitionMetrics {
                partition: ctx.partition,
                latencies: Vec::with_capacity(self.sample_capacity),
                ..Default::default()
            },
            last_iseq: 0,
            out: self.out.clone(),
        })
    }
}

impl Egress for MetricsEgress {
    #[inline]
    fn on_event(&mut self, m: &EvtMsg) {
        self.m.events += 1;
        if matches!(m.body, EvtBody::Event(Event::Trade { .. })) {
            self.m.trades += 1;
        }
        if m.iseq != self.last_iseq {
            self.last_iseq = m.iseq;
            self.m.commands += 1;
            if m.t_pub != 0 {
                self.m
                    .latencies
                    .push(self.ctx.now_ns().saturating_sub(m.t_pub));
            }
        }
    }

    fn on_shutdown(&mut self) {
        self.out.lock().unwrap().push(std::mem::take(&mut self.m));
    }
}

/// Symbol and per-book sequence of an event — convenience for consumers
/// that merge partition streams.
pub fn event_key(m: &EvtMsg) -> (Symbol, u64) {
    (m.symbol, m.seq)
}
