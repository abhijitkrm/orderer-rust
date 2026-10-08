//! The event-sink seam — the engine's only I/O. Journaling, market-data
//! feeds, replay, and metrics all hang off this interface without touching
//! the core.

use crate::types::Event;

/// Receives every event emitted by a book, in order, with its per-book `seq`.
pub trait Sink {
    fn on_event(&mut self, seq: u64, ev: &Event);
}

/// No-op sink for benchmarks. Folds `seq` into `acc` so the call can't be
/// optimized away; read `acc` after the run to keep it observable.
pub struct NullSink {
    pub acc: u64,
}

impl NullSink {
    pub fn new() -> NullSink {
        NullSink { acc: 0 }
    }
}

impl Default for NullSink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for NullSink {
    #[inline(always)]
    fn on_event(&mut self, seq: u64, ev: &Event) {
        self.acc = self.acc.wrapping_add(seq ^ ev.fold());
    }
}

/// Records `(seq, Event)` pairs — tests and replay.
#[derive(Default)]
pub struct VecSink {
    pub events: Vec<(u64, Event)>,
}

impl VecSink {
    pub fn new() -> VecSink {
        VecSink { events: Vec::new() }
    }
}

impl Sink for VecSink {
    #[inline]
    fn on_event(&mut self, seq: u64, ev: &Event) {
        self.events.push((seq, *ev));
    }
}

/// Serializes each event to its canonical JSON line (SCHEMA.md) — the golden
/// vector harness and any text journal.
#[derive(Default)]
pub struct LinesSink {
    pub lines: Vec<String>,
    scratch: String,
}

impl LinesSink {
    pub fn new() -> LinesSink {
        LinesSink::default()
    }
}

impl Sink for LinesSink {
    #[inline]
    fn on_event(&mut self, seq: u64, ev: &Event) {
        self.scratch.clear();
        Event::write_canonical(seq, ev, &mut self.scratch);
        self.lines.push(self.scratch.clone());
    }
}
