//! Ring slot types. In-memory and implementation-private (spec/PIPELINE.md
//! §8): their layout is a performance choice, not contract. Both are plain
//! `Copy` data so slots are overwritten in place — no allocation, no drop.

use orderer_core::{Command, Event, Symbol};

/// Control operations ride the same rings as commands, so they cut every
/// partition at the same point of the ingress order (spec/PIPELINE.md §6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Control {
    /// Empty slot (ring initialisation only).
    #[default]
    Nop,
    /// Drain token: done once every partition's egress has passed it.
    Barrier { epoch: u64 },
    /// Capture books; the payload lives in the `ControlTable`, keyed by
    /// `op_id`, so slots stay small and `Copy`.
    Snapshot { op_id: u64 },
    /// Drain, then stop every thread.
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Body {
    Cmd(Command),
    Ctl(Control),
}

impl Default for Body {
    fn default() -> Self {
        Body::Ctl(Control::Nop)
    }
}

/// Ingress and partition-inbox slot.
#[derive(Clone, Copy, Debug, Default)]
pub struct CmdMsg {
    /// Commands: the ingress sequence, stamped by the router (spec
    /// PIPELINE.md §2). Controls: the number of commands before the
    /// control in ingress order — the cut.
    pub iseq: u64,
    /// Publish time (ns since the pipeline epoch) when timestamps are on.
    pub t_pub: u64,
    pub symbol: Symbol,
    pub body: Body,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvtBody {
    Event(Event),
    Ctl(Control),
}

impl Default for EvtBody {
    fn default() -> Self {
        EvtBody::Ctl(Control::Nop)
    }
}

/// Partition-outbox slot: one event (or a control passing through to
/// egress) with its causing command's `iseq`.
#[derive(Clone, Copy, Debug, Default)]
pub struct EvtMsg {
    /// The command that caused this event.
    pub iseq: u64,
    /// Per-book event sequence.
    pub seq: u64,
    /// The causing command's publish time (latency probes).
    pub t_pub: u64,
    pub symbol: Symbol,
    pub body: EvtBody,
}

const _: () = assert!(std::mem::size_of::<CmdMsg>() <= 64);
const _: () = assert!(std::mem::size_of::<EvtMsg>() <= 80);
