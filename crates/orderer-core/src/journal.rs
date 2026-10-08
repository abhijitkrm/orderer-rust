//! Journaling on the sink seam (spec/JOURNAL.md). The command journal is
//! written *before* apply; the event journal wraps the downstream sink so
//! every emitted line is appended in order. Both use canonical JSONL — the
//! same bytes the golden vectors and fuzz corpora carry.

use std::io::Write;

use crate::sink::Sink;
use crate::types::{Command, Event, Symbol};

/// Ingress tap: appends each command's canonical line before it is applied.
/// The caller decides fsync policy; `flush()` after each line is the safe
/// default for a recovery journal.
pub struct CmdJournal<W: Write> {
    out: W,
    scratch: String,
}

impl<W: Write> CmdJournal<W> {
    pub fn new(out: W) -> CmdJournal<W> {
        CmdJournal {
            out,
            scratch: String::with_capacity(128),
        }
    }

    /// Book-level (no symbol) record.
    pub fn record(&mut self, cmd: &Command) {
        cmd.write_canonical(&mut self.scratch);
        self.scratch.push('\n');
        self.out
            .write_all(self.scratch.as_bytes())
            .expect("journal write");
        self.scratch.clear();
    }

    /// Engine-level record: symbol-tagged command line.
    pub fn record_sym(&mut self, sym: Symbol, cmd: &Command) {
        cmd.write_canonical_sym(sym, &mut self.scratch);
        self.scratch.push('\n');
        self.out
            .write_all(self.scratch.as_bytes())
            .expect("journal write");
        self.scratch.clear();
    }

    pub fn flush(&mut self) {
        self.out.flush().expect("journal flush");
    }
}

/// Egress decorator: appends the canonical event line, then forwards to the
/// inner sink. Events stay visible to downstream consumers in real time.
pub struct JournalSink<S: Sink, W: Write> {
    inner: S,
    out: W,
    scratch: String,
}

impl<S: Sink, W: Write> JournalSink<S, W> {
    pub fn new(inner: S, out: W) -> JournalSink<S, W> {
        JournalSink {
            inner,
            out,
            scratch: String::with_capacity(128),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S: Sink, W: Write> Sink for JournalSink<S, W> {
    fn on_event(&mut self, seq: u64, ev: &Event) {
        Event::write_canonical(seq, ev, &mut self.scratch);
        self.scratch.push('\n');
        self.out
            .write_all(self.scratch.as_bytes())
            .expect("journal write");
        self.scratch.clear();
        self.inner.on_event(seq, ev);
    }
}

/// One engine-mode journal event line (symbol-tagged) — for use inside
/// `submit_tagged` closures:
/// `eng.submit_tagged(sym, cmd, &mut |s, q, e| { journal_event(s, q, e, &mut w, &mut scratch); f(s, q, e); })`
pub fn journal_event<W: Write>(
    sym: Symbol,
    seq: u64,
    ev: &Event,
    out: &mut W,
    scratch: &mut String,
) {
    Event::write_canonical_sym(seq, sym, ev, scratch);
    scratch.push('\n');
    out.write_all(scratch.as_bytes()).expect("journal write");
    scratch.clear();
}
