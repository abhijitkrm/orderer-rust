//! Operational statistics (not part of the spec contract): ring depths,
//! per-partition counters, journal watermarks and fsync timings, and a
//! Prometheus text-format rendering.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

/// fsync timings one journal I/O thread records.
#[derive(Default, Debug)]
pub struct IoStats {
    pub fsyncs: AtomicU64,
    pub fsync_ns_total: AtomicU64,
    pub fsync_ns_max: AtomicU64,
}

impl IoStats {
    pub(crate) fn record(&self, ns: u64) {
        self.fsyncs.fetch_add(1, Ordering::Relaxed);
        self.fsync_ns_total.fetch_add(ns, Ordering::Relaxed);
        self.fsync_ns_max.fetch_max(ns, Ordering::Relaxed);
    }
}

/// Counters an engine thread publishes once per batch.
#[derive(Default, Debug)]
pub struct EngineCounters {
    pub commands: AtomicU64,
    pub events: AtomicU64,
}

/// One partition's view.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionStats {
    pub partition: u32,
    /// Commands routed to the partition but not yet taken by its engine.
    pub inbox_depth: u64,
    /// Events staged by the engine but not yet consumed by egress.
    pub outbox_depth: u64,
    pub commands: u64,
    pub events: u64,
    /// Highest iseq written to the OS / covered by a completed fsync
    /// (both `u64::MAX` without journals).
    pub flushed_iseq: u64,
    pub durable_iseq: u64,
    pub fsyncs: u64,
    pub fsync_ns_total: u64,
    pub fsync_ns_max: u64,
}

/// A point-in-time view of the whole pipeline (fields are read without a
/// global lock, so they are individually exact, not mutually consistent).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PipelineStats {
    /// Commands published but not yet taken by the router.
    pub ingress_depth: u64,
    pub partitions: Vec<PartitionStats>,
}

impl PipelineStats {
    /// Prometheus text exposition format (version 0.0.4).
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        let mut gauge =
            |name: &str,
             help: &str,
             kind: &str,
             rows: &mut dyn Iterator<Item = (Option<u32>, u64)>| {
                let _ = writeln!(
                    out,
                    "# HELP orderer_{name} {help}\n# TYPE orderer_{name} {kind}"
                );
                for (p, v) in rows {
                    match p {
                        Some(p) => {
                            let _ = writeln!(out, "orderer_{name}{{partition=\"{p}\"}} {v}");
                        }
                        None => {
                            let _ = writeln!(out, "orderer_{name} {v}");
                        }
                    }
                }
            };
        let ps = &self.partitions;
        gauge(
            "ingress_depth",
            "Commands published, not yet routed.",
            "gauge",
            &mut std::iter::once((None, self.ingress_depth)),
        );
        gauge(
            "inbox_depth",
            "Commands routed, not yet applied.",
            "gauge",
            &mut ps.iter().map(|p| (Some(p.partition), p.inbox_depth)),
        );
        gauge(
            "outbox_depth",
            "Events staged, not yet consumed by egress.",
            "gauge",
            &mut ps.iter().map(|p| (Some(p.partition), p.outbox_depth)),
        );
        gauge(
            "commands_total",
            "Commands applied.",
            "counter",
            &mut ps.iter().map(|p| (Some(p.partition), p.commands)),
        );
        gauge(
            "events_total",
            "Events emitted.",
            "counter",
            &mut ps.iter().map(|p| (Some(p.partition), p.events)),
        );
        gauge(
            "durable_iseq",
            "Highest iseq covered by a completed fsync.",
            "gauge",
            &mut ps.iter().map(|p| (Some(p.partition), p.durable_iseq)),
        );
        gauge(
            "fsyncs_total",
            "Journal fsyncs.",
            "counter",
            &mut ps.iter().map(|p| (Some(p.partition), p.fsyncs)),
        );
        gauge(
            "fsync_ns_total",
            "Time spent in journal fsync, in nanoseconds.",
            "counter",
            &mut ps.iter().map(|p| (Some(p.partition), p.fsync_ns_total)),
        );
        gauge(
            "fsync_max_ns",
            "Longest journal fsync, in nanoseconds.",
            "gauge",
            &mut ps.iter().map(|p| (Some(p.partition), p.fsync_ns_max)),
        );
        out
    }
}
