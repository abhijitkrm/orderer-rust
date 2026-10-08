//! `RingBuffer<T>` — preallocated power-of-two slot array. The **only**
//! module in the workspace that contains `unsafe` code.
//!
//! # Protocol (why the unsafe is sound)
//!
//! Slots are plain `UnsafeCell<T>`; exclusion is provided by sequences, not
//! locks. The crate upholds two rules, enforced by the `pub(crate)` callers
//! in `producer.rs` and `consumer.rs`:
//!
//! 1. **Write** `seq` only while holding an unpublished claim on it. A
//!    claim on `seq` is only granted once every gating consumer's sequence
//!    is `≥ seq - size`, i.e. every reader has released the previous lap's
//!    occupant of that slot. Claims are exclusive (single producer: one
//!    thread; multi: `fetch_add`/CAS hands each seq to exactly one claimer).
//! 2. **Read** `seq` only after observing it published (an Acquire load of
//!    the cursor or of its available-flag that the writer set with Release
//!    *after* writing the payload), and only until the reader advances its
//!    own sequence past `seq` (a Release store after the read).
//!
//! Release/Acquire pairs therefore order every payload write before every
//! read of it, and every read before the next lap's write. Readers only get
//! `&T`, so concurrent readers of one slot never alias a `&mut`.
//!
//! Verified by `tests/loom.rs` (model-checked interleavings, `--cfg loom`)
//! and `tests/ring.rs` (multi-producer stress).

#![allow(unsafe_code)]

use crate::sync::UnsafeCell;

pub struct RingBuffer<T> {
    slots: Box<[UnsafeCell<T>]>,
    mask: i64,
}

// SAFETY: slots are only reached through the sequence protocol above, which
// gives each slot either one writer or any number of readers at a time,
// with Release/Acquire edges between them. `T: Send` lets values move
// between the producer and consumer threads.
unsafe impl<T: Send> Sync for RingBuffer<T> {}
unsafe impl<T: Send> Send for RingBuffer<T> {}

impl<T: Default> RingBuffer<T> {
    /// `size` must be a power of two ≥ 1. All slots are created up front —
    /// no allocation ever happens on the hot path.
    pub fn new(size: usize) -> RingBuffer<T> {
        assert!(size.is_power_of_two(), "ring size must be a power of two");
        RingBuffer {
            slots: (0..size).map(|_| UnsafeCell::new(T::default())).collect(),
            mask: size as i64 - 1,
        }
    }
}

impl<T> RingBuffer<T> {
    #[inline(always)]
    pub fn size(&self) -> usize {
        self.slots.len()
    }

    #[inline(always)]
    fn cell(&self, seq: i64) -> &UnsafeCell<T> {
        // `seq & mask` is always in bounds; indexing keeps the bounds check
        // honest at no measurable cost.
        &self.slots[(seq & self.mask) as usize]
    }

    /// Crate contract (rule 1): the caller holds an unpublished claim on
    /// `seq`.
    #[inline(always)]
    pub(crate) fn write<R>(&self, seq: i64, f: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: rule 1 — this thread is the slot's only accessor until it
        // publishes `seq`.
        self.cell(seq).with_mut(|p| f(unsafe { &mut *p }))
    }

    /// Crate contract (rule 2): `seq` is published and not yet released by
    /// the calling consumer.
    #[inline(always)]
    pub(crate) fn read<R>(&self, seq: i64, f: impl FnOnce(&T) -> R) -> R {
        // SAFETY: rule 2 — no writer can claim `seq`'s slot until this
        // consumer's sequence passes `seq`.
        self.cell(seq).with(|p| f(unsafe { &*p }))
    }
}
