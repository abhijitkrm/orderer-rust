//! `Sequence` — a cache-line-isolated counter: a producer cursor or a
//! consumer's "processed up to" watermark.

use crate::sync::{AtomicI64, Ordering};

/// Initial value of every sequence: nothing claimed, published or consumed.
pub const INITIAL: i64 = -1;

/// A padded atomic sequence. Aligned to 128 bytes — Apple Silicon's cache
/// line (`hw.cachelinesize`), and two x86 lines (adjacent-line prefetch) —
/// so no two hot counters ever share a line.
#[repr(align(128))]
#[derive(Debug)]
pub struct Sequence {
    value: AtomicI64,
}

impl Sequence {
    pub fn new(initial: i64) -> Sequence {
        Sequence {
            value: AtomicI64::new(initial),
        }
    }

    /// Acquire load: everything written before the matching `set` is visible.
    #[inline(always)]
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Acquire)
    }

    /// Release store: publishes everything written before it.
    #[inline(always)]
    pub fn set(&self, v: i64) {
        self.value.store(v, Ordering::Release)
    }

    #[inline(always)]
    pub(crate) fn fetch_add(&self, n: i64) -> i64 {
        self.value.fetch_add(n, Ordering::AcqRel)
    }

    #[inline(always)]
    pub(crate) fn compare_exchange(&self, current: i64, new: i64) -> Result<i64, i64> {
        self.value
            .compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
    }
}

impl Default for Sequence {
    fn default() -> Self {
        Sequence::new(INITIAL)
    }
}

/// Minimum of a set of sequences (`i64::MAX` when empty).
#[inline]
pub(crate) fn min_of(seqs: &[crate::sync::Arc<Sequence>], floor: i64) -> i64 {
    let mut m = floor;
    for s in seqs {
        m = m.min(s.get());
    }
    m
}
