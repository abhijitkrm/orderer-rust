//! The plug seam between the orderer pipeline and a matching core.
//!
//! A partition's engine thread owns exactly one `MatchingCore` and is its
//! sole writer. The pipeline is generic over the core, so `apply`'s emitter
//! is monomorphized — no dynamic dispatch per event.

use std::collections::HashMap;
use std::fmt;

use crate::engine::Engine;
use crate::snapshot;
use crate::types::*;

/// A snapshot that cannot be restored (spec/JOURNAL.md §4) — corrupt or
/// inconsistent with the book config. Never a panic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreError(pub String);

impl fmt::Display for RestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RestoreError {}

/// What a partition's engine thread needs from a matching core.
pub trait MatchingCore: Send + Sized + 'static {
    /// An empty core whose books are created lazily with `cfg`.
    fn new(cfg: BookConfig) -> Self;

    /// Apply one command to `sym`'s book; `emit(sym, seq, event)` is called
    /// for every event in match order.
    fn apply<F: FnMut(Symbol, u64, &Event)>(&mut self, sym: Symbol, cmd: Command, emit: &mut F);

    /// Append this core's `matcher-snap/1` book blocks (no header) to `out`,
    /// one `(symbol, block)` per book, in any order — the pipeline merges
    /// partitions by symbol. Off the hot path (allocates).
    fn snapshot_blocks(&self, out: &mut Vec<(Symbol, String)>);

    /// Install one book from a snapshot block: `seq` resumes after the
    /// block's sequence, `orders` rest in file order.
    fn restore_book(
        &mut self,
        sym: Symbol,
        seq: u64,
        orders: &[RestingOrder],
    ) -> Result<(), RestoreError>;

    /// Debug-build invariant check after every command (ordererfuzz).
    fn check_invariants(&self) {}
}

/// The spec-proven FIFO core: matcher's `Engine`, unchanged.
pub struct FifoCore {
    engine: Engine,
}

impl FifoCore {
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}

impl MatchingCore for FifoCore {
    fn new(cfg: BookConfig) -> Self {
        FifoCore {
            engine: Engine::new(cfg),
        }
    }

    #[inline]
    fn apply<F: FnMut(Symbol, u64, &Event)>(&mut self, sym: Symbol, cmd: Command, emit: &mut F) {
        self.engine.submit_tagged(sym, cmd, emit);
    }

    fn snapshot_blocks(&self, out: &mut Vec<(Symbol, String)>) {
        for (sym, book) in self.engine.books_iter() {
            let mut block = String::new();
            snapshot::write_book(book, sym, &mut block);
            out.push((sym, block));
        }
    }

    fn restore_book(
        &mut self,
        sym: Symbol,
        seq: u64,
        orders: &[RestingOrder],
    ) -> Result<(), RestoreError> {
        let cfg = self.engine.default_cfg();
        snapshot::validate_book(cfg, sym, orders)?;
        self.engine.add_symbol(sym, cfg);
        *self.engine.book_mut(sym).expect("just added") =
            crate::book::OrderBook::restore(cfg, seq, orders);
        Ok(())
    }

    fn check_invariants(&self) {
        for (_, book) in self.engine.books_iter() {
            book.check_invariants();
        }
    }
}

/// Test core: no book at all. Echoes each command as one event —
/// `new` → `accepted{qty}`, `cancel` → `closed{cancelled}`, `replace` →
/// `replaced` — with a dense per-symbol `seq`. Proves the seam carries
/// every command through the pipeline untouched.
pub struct NoopCore {
    seqs: HashMap<Symbol, u64>,
}

impl MatchingCore for NoopCore {
    fn new(_cfg: BookConfig) -> Self {
        NoopCore {
            seqs: HashMap::new(),
        }
    }

    #[inline]
    fn apply<F: FnMut(Symbol, u64, &Event)>(&mut self, sym: Symbol, cmd: Command, emit: &mut F) {
        let seq = self.seqs.entry(sym).or_insert(0);
        *seq += 1;
        let ev = match cmd {
            Command::New { order_id, qty, .. } => Event::Accepted {
                order_id,
                leaves_qty: qty,
            },
            Command::Cancel { order_id } => Event::Closed {
                order_id,
                reason: CloseReason::Cancelled,
            },
            Command::Replace {
                order_id,
                price,
                qty,
            } => Event::Replaced {
                order_id,
                price,
                qty,
            },
        };
        emit(sym, *seq, &ev);
    }

    fn snapshot_blocks(&self, out: &mut Vec<(Symbol, String)>) {
        for (&sym, &seq) in &self.seqs {
            out.push((
                sym,
                format!("{{\"rec\":\"book\",\"symbol\":{sym},\"seq\":{seq}}}\n"),
            ));
        }
    }

    fn restore_book(
        &mut self,
        sym: Symbol,
        seq: u64,
        _orders: &[RestingOrder],
    ) -> Result<(), RestoreError> {
        self.seqs.insert(sym, seq);
        Ok(())
    }
}
