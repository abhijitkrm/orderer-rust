//! Consumers and their `SequenceBarrier`: what a consumer may read is
//! bounded by the producer cursor (published slots) **and** by every
//! upstream consumer it depends on — the LMAX diamond. A journal consumer
//! upstream of an engine consumer is how journal-before-apply becomes a
//! structural property instead of a synchronous call.

use crate::sequence::{min_of, Sequence};
use crate::sequencer::Shared;
use crate::sync::Arc;
use crate::wait::{WaitStrategy, Waiter};

/// The ring was alerted (shut down) while waiting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Alerted;

impl std::fmt::Display for Alerted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ring alerted")
    }
}

impl std::error::Error for Alerted {}

/// Default cap on events handed to one batch callback: bounded batches
/// release slots back to the producer regularly.
pub const DEFAULT_MAX_BATCH: usize = 1024;

/// One consumer of a ring. Owns a [`Sequence`] (its "processed up to"
/// watermark) that downstream consumers and the producer gate on.
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    deps: Box<[Arc<Sequence>]>,
    seq: Arc<Sequence>,
    next: i64,
    waiter: Waiter,
    max_batch: i64,
}

impl<T> Consumer<T> {
    pub(crate) fn new(
        shared: Arc<Shared<T>>,
        deps: Box<[Arc<Sequence>]>,
        seq: Arc<Sequence>,
        wait: WaitStrategy,
    ) -> Consumer<T> {
        let next = seq.get() + 1;
        Consumer {
            shared,
            deps,
            seq,
            next,
            waiter: Waiter::new(wait),
            max_batch: DEFAULT_MAX_BATCH as i64,
        }
    }

    /// This consumer's watermark — pass it as a dependency to downstream
    /// stages, or watch it for drains.
    pub fn sequence(&self) -> Arc<Sequence> {
        self.seq.clone()
    }

    pub fn set_wait(&mut self, wait: WaitStrategy) {
        self.waiter.set(wait);
    }

    pub fn wait_strategy(&self) -> WaitStrategy {
        self.waiter.strategy()
    }

    pub fn set_max_batch(&mut self, n: usize) {
        assert!(n >= 1);
        self.max_batch = n as i64;
    }

    /// Next seq this consumer will process.
    pub fn next_seq(&self) -> i64 {
        self.next
    }

    /// Highest seq readable now (`next_seq() - 1` if none), capped at
    /// `max_batch` past `next_seq()`.
    #[inline]
    pub fn available(&self) -> i64 {
        let limit = self.next + self.max_batch - 1;
        if self.deps.is_empty() {
            self.shared.published_upto(self.next, limit)
        } else {
            // Upstream consumers only pass published slots, so their
            // minimum is already a published bound.
            min_of(&self.deps, limit)
        }
    }

    /// Process every available event (up to the batch cap) without waiting:
    /// `f(event, seq, end_of_batch)`. Then publish the watermark. Returns the
    /// number processed.
    #[inline]
    pub fn poll(&mut self, mut f: impl FnMut(&T, i64, bool)) -> usize {
        let avail = self.available();
        if avail < self.next {
            return 0;
        }
        let ring = &self.shared.ring;
        for s in self.next..=avail {
            ring.read(s, |t| f(t, s, s == avail));
        }
        let n = (avail - self.next + 1) as usize;
        self.next = avail + 1;
        self.seq.set(avail);
        self.shared.notifier.signal();
        n
    }

    /// Wait (per this consumer's strategy) until at least one event is
    /// available, then `poll`. `Err(Alerted)` once the ring is alerted and
    /// nothing is left to read.
    #[inline]
    pub fn wait_poll(&mut self, mut f: impl FnMut(&T, i64, bool)) -> Result<usize, Alerted> {
        loop {
            let n = self.poll(&mut f);
            if n > 0 {
                self.waiter.reset();
                return Ok(n);
            }
            if self.shared.is_alerted() {
                return Err(Alerted);
            }
            self.waiter.idle(&self.shared.notifier);
        }
    }

    /// One idle step of this consumer's wait strategy — for loops that
    /// poll several sources and found nothing.
    #[inline]
    pub fn idle(&mut self) {
        self.waiter.idle(&self.shared.notifier);
    }

    #[inline]
    pub fn reset_idle(&mut self) {
        self.waiter.reset();
    }

    pub fn is_alerted(&self) -> bool {
        self.shared.is_alerted()
    }
}
