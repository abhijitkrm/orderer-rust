//! Producers: claim slots, write payloads, publish.
//!
//! Two overflow policies, chosen per call:
//! - **Block** (`publish*`): wait for the slowest gating consumer to free
//!   space. Never loses a message.
//! - **Try** (`try_publish*`): fail with [`PublishError::Full`] instead of
//!   waiting — nothing is claimed, nothing is consumed.

use crate::sequencer::{ProducerKind, Shared};
use crate::sync::{spin_hint, yield_now, Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishError {
    /// `try_*` only: no space without lapping a consumer.
    Full,
    /// The ring was alerted (shut down) — nothing published.
    Alerted,
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            PublishError::Full => "ring full",
            PublishError::Alerted => "ring alerted",
        })
    }
}

impl std::error::Error for PublishError {}

/// Spin briefly, then yield — producers wait only on slow consumers, which
/// should be rare in a sized pipeline.
#[inline]
fn backoff(step: &mut u32) {
    if *step < 64 {
        *step += 1;
        spin_hint();
    } else {
        yield_now();
    }
}

/// The only producer of a [`ProducerKind::Single`] ring. Claims are a local
/// counter; `commit` is one Release store.
///
/// Beyond plain `publish`, it can **stage** writes and **commit** them in
/// one go — the engine and router stage every output of an input batch,
/// then make them visible with a single store.
pub struct SingleProducer<T> {
    shared: Arc<Shared<T>>,
    /// Highest seq claimed (written, maybe not yet published).
    next: i64,
    /// Highest seq published (mirror of the cursor — this is its only writer).
    published: i64,
    /// Last observed minimum gating seq.
    cached_gate: i64,
}

impl<T> SingleProducer<T> {
    pub(crate) fn new(shared: Arc<Shared<T>>) -> SingleProducer<T> {
        debug_assert_eq!(shared.kind, ProducerKind::Single);
        let next = shared.cursor.get();
        SingleProducer {
            shared,
            next,
            published: next,
            cached_gate: next,
        }
    }

    /// Ring capacity.
    pub fn size(&self) -> usize {
        self.shared.ring.size()
    }

    /// Number of staged, uncommitted slots.
    #[inline(always)]
    pub fn staged(&self) -> usize {
        (self.next - self.published) as usize
    }

    /// Is there room for `n` more claims without waiting?
    #[inline]
    fn has_room(&mut self, n: i64) -> bool {
        let wrap = self.next + n - self.shared.size();
        if wrap <= self.cached_gate {
            return true;
        }
        self.cached_gate = self.shared.min_gating();
        wrap <= self.cached_gate
    }

    #[inline]
    fn wait_room(&mut self, n: i64) -> Result<(), PublishError> {
        if self.has_room(n) {
            return Ok(());
        }
        // Consumers can't free space they can't see: publish staged work
        // before waiting on them.
        self.commit();
        let mut step = 0;
        while !self.has_room(n) {
            if self.shared.is_alerted() {
                return Err(PublishError::Alerted);
            }
            backoff(&mut step);
        }
        Ok(())
    }

    /// Claim the next slot and fill it via `f`; not visible to consumers
    /// until [`commit`](Self::commit). Blocks while the ring is full.
    #[inline]
    pub fn stage(&mut self, f: impl FnOnce(&mut T)) -> Result<i64, PublishError> {
        self.wait_room(1)?;
        self.next += 1;
        self.shared.ring.write(self.next, f);
        Ok(self.next)
    }

    /// Publish every staged slot with one Release store.
    #[inline]
    pub fn commit(&mut self) {
        if self.next != self.published {
            self.published = self.next;
            self.shared.cursor.set(self.next);
            self.shared.notifier.signal();
        }
    }

    /// Stage + commit one slot. Blocks while full.
    #[inline]
    pub fn publish(&mut self, f: impl FnOnce(&mut T)) -> Result<i64, PublishError> {
        let seq = self.stage(f)?;
        self.commit();
        Ok(seq)
    }

    /// Publish one slot, or fail with `Full` without waiting.
    #[inline]
    pub fn try_publish(&mut self, f: impl FnOnce(&mut T)) -> Result<i64, PublishError> {
        if !self.has_room(1) {
            return Err(PublishError::Full);
        }
        self.publish(f)
    }

    /// Claim `n` contiguous slots, fill each with `f(i, slot)`, publish all
    /// with one store. Returns the last seq. Blocks while full.
    pub fn publish_batch(
        &mut self,
        n: usize,
        mut f: impl FnMut(usize, &mut T),
    ) -> Result<i64, PublishError> {
        assert!(n <= self.size(), "batch larger than the ring");
        self.wait_room(n as i64)?;
        for i in 0..n {
            self.next += 1;
            self.shared.ring.write(self.next, |t| f(i, t));
        }
        self.commit();
        Ok(self.next)
    }

    /// `publish_batch`, or `Full` without waiting.
    pub fn try_publish_batch(
        &mut self,
        n: usize,
        f: impl FnMut(usize, &mut T),
    ) -> Result<i64, PublishError> {
        if !self.has_room(n as i64) {
            return Err(PublishError::Full);
        }
        self.publish_batch(n, f)
    }

    /// Shared control handle (alert, cursor) for this ring.
    pub fn control(&self) -> RingControl<T> {
        RingControl {
            shared: self.shared.clone(),
        }
    }
}

impl<T> Drop for SingleProducer<T> {
    fn drop(&mut self) {
        self.commit();
    }
}

/// A producer of a [`ProducerKind::Multi`] ring. Cheap to clone; every
/// clone may publish concurrently.
pub struct MultiProducer<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for MultiProducer<T> {
    fn clone(&self) -> Self {
        MultiProducer {
            shared: self.shared.clone(),
        }
    }
}

impl<T> MultiProducer<T> {
    pub(crate) fn new(shared: Arc<Shared<T>>) -> MultiProducer<T> {
        debug_assert_eq!(shared.kind, ProducerKind::Multi);
        MultiProducer { shared }
    }

    pub fn size(&self) -> usize {
        self.shared.ring.size()
    }

    #[inline]
    fn room_for(&self, hi: i64) -> bool {
        let wrap = hi - self.shared.size();
        if wrap <= self.shared.gating_cache.get() {
            return true;
        }
        let g = self.shared.min_gating();
        self.shared.gating_cache.set(g);
        wrap <= g
    }

    #[inline]
    fn fill_and_publish(&self, lo: i64, hi: i64, mut f: impl FnMut(usize, &mut T)) -> i64 {
        for s in lo..=hi {
            self.shared.ring.write(s, |t| f((s - lo) as usize, t));
        }
        for s in lo..=hi {
            self.shared.set_available(s);
        }
        self.shared.notifier.signal();
        hi
    }

    /// Claim `n` slots (one `fetch_add`), fill each with `f(i, slot)`,
    /// publish. Blocks while full. Returns the last seq.
    pub fn publish_batch(
        &self,
        n: usize,
        f: impl FnMut(usize, &mut T),
    ) -> Result<i64, PublishError> {
        assert!(n >= 1 && n <= self.size(), "batch must be 1..=ring size");
        let hi = self.shared.cursor.fetch_add(n as i64) + n as i64;
        let mut step = 0;
        while !self.room_for(hi) {
            if self.shared.is_alerted() {
                return Err(PublishError::Alerted);
            }
            backoff(&mut step);
        }
        Ok(self.fill_and_publish(hi - n as i64 + 1, hi, f))
    }

    /// Publish one slot. Blocks while full.
    #[inline]
    pub fn publish(&self, f: impl FnOnce(&mut T)) -> Result<i64, PublishError> {
        let mut f = Some(f);
        self.publish_batch(1, |_, t| (f.take().unwrap())(t))
    }

    /// Claim `n` slots by CAS only if they fit — otherwise `Full`, with the
    /// cursor untouched (a `fetch_add` claim could not be backed out).
    pub fn try_publish_batch(
        &self,
        n: usize,
        f: impl FnMut(usize, &mut T),
    ) -> Result<i64, PublishError> {
        assert!(n >= 1 && n <= self.size(), "batch must be 1..=ring size");
        loop {
            if self.shared.is_alerted() {
                return Err(PublishError::Alerted);
            }
            let cur = self.shared.cursor.get();
            let hi = cur + n as i64;
            if !self.room_for(hi) {
                return Err(PublishError::Full);
            }
            if self.shared.cursor.compare_exchange(cur, hi).is_ok() {
                return Ok(self.fill_and_publish(cur + 1, hi, f));
            }
        }
    }

    /// Publish one slot, or `Full` without waiting.
    #[inline]
    pub fn try_publish(&self, f: impl FnOnce(&mut T)) -> Result<i64, PublishError> {
        let mut f = Some(f);
        self.try_publish_batch(1, |_, t| (f.take().unwrap())(t))
    }

    pub fn control(&self) -> RingControl<T> {
        RingControl {
            shared: self.shared.clone(),
        }
    }
}

/// Ring-wide controls, held by anyone (e.g. a supervisor thread).
pub struct RingControl<T> {
    pub(crate) shared: Arc<Shared<T>>,
}

impl<T> Clone for RingControl<T> {
    fn clone(&self) -> Self {
        RingControl {
            shared: self.shared.clone(),
        }
    }
}

impl<T> RingControl<T> {
    /// Abort: every waiting consumer returns `Alerted`, every blocked
    /// producer returns `PublishError::Alerted`.
    pub fn alert(&self) {
        self.shared.alert();
    }

    pub fn is_alerted(&self) -> bool {
        self.shared.is_alerted()
    }

    /// Highest seq published contiguously (every seq ≤ it is readable).
    pub fn published(&self) -> i64 {
        self.shared.published_high_water()
    }

    /// Minimum of the gating consumers' sequences (`i64::MAX` if none).
    pub fn consumed(&self) -> i64 {
        self.shared.min_gating()
    }
}
