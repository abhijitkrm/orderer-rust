//! The state shared by one ring's producer(s) and consumers: the payload
//! slots, the producer cursor, the multi-producer availability flags, and
//! the gating sequences the producer must not lap.

use crate::ring::RingBuffer;
use crate::sequence::{min_of, Sequence, INITIAL};
use crate::sync::{Arc, AtomicBool, AtomicI32, Ordering};
use crate::wait::Notifier;

/// How slots are claimed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProducerKind {
    /// One producer thread: claims are a plain local counter, publish is one
    /// Release store of the cursor.
    Single,
    /// Many producer threads: claims are `fetch_add` (or CAS for
    /// `try_publish`), publish sets a per-slot availability flag so a slow
    /// producer never blocks faster ones from publishing (LMAX's
    /// `MultiProducerSequencer`).
    Multi,
}

pub(crate) struct Shared<T> {
    pub(crate) ring: RingBuffer<T>,
    pub(crate) kind: ProducerKind,
    /// Single: highest *published* seq. Multi: highest *claimed* seq.
    pub(crate) cursor: Sequence,
    /// Multi only: `available[seq & mask] == lap(seq)` once `seq` is
    /// published. Lap numbers make stale flags from earlier laps harmless.
    available: Box<[AtomicI32]>,
    shift: u32,
    /// Multi only: last observed minimum gating seq, shared by all
    /// producer handles to avoid rescanning the gating set on every claim.
    pub(crate) gating_cache: Sequence,
    /// Terminal consumers — the producer may not lap the slowest of them.
    pub(crate) gating: Box<[Arc<Sequence>]>,
    pub(crate) notifier: Notifier,
    alerted: AtomicBool,
}

impl<T: Default> Shared<T> {
    pub(crate) fn new(size: usize, kind: ProducerKind, gating: Box<[Arc<Sequence>]>) -> Shared<T> {
        let available = match kind {
            ProducerKind::Single => Box::default(),
            ProducerKind::Multi => (0..size).map(|_| AtomicI32::new(-1)).collect(),
        };
        Shared {
            ring: RingBuffer::new(size),
            kind,
            cursor: Sequence::new(INITIAL),
            available,
            shift: size.trailing_zeros(),
            gating_cache: Sequence::new(INITIAL),
            gating,
            notifier: Notifier::default(),
            alerted: AtomicBool::new(false),
        }
    }
}

impl<T> Shared<T> {
    #[inline(always)]
    pub(crate) fn size(&self) -> i64 {
        self.ring.size() as i64
    }

    #[inline(always)]
    pub(crate) fn min_gating(&self) -> i64 {
        min_of(&self.gating, i64::MAX)
    }

    #[inline(always)]
    fn lap(&self, seq: i64) -> i32 {
        (seq >> self.shift) as i32
    }

    #[inline(always)]
    pub(crate) fn set_available(&self, seq: i64) {
        let idx = (seq & (self.size() - 1)) as usize;
        self.available[idx].store(self.lap(seq), Ordering::Release);
    }

    #[inline(always)]
    fn is_available(&self, seq: i64) -> bool {
        let idx = (seq & (self.size() - 1)) as usize;
        self.available[idx].load(Ordering::Acquire) == self.lap(seq)
    }

    /// Highest seq such that every seq in `lo..=result` is published, looking
    /// no further than `limit`. `lo - 1` when `lo` itself isn't published.
    #[inline]
    pub(crate) fn published_upto(&self, lo: i64, limit: i64) -> i64 {
        let hi = self.cursor.get().min(limit);
        match self.kind {
            ProducerKind::Single => hi,
            ProducerKind::Multi => {
                let mut s = lo;
                while s <= hi {
                    if !self.is_available(s) {
                        return s - 1;
                    }
                    s += 1;
                }
                hi
            }
        }
    }

    /// Highest seq published contiguously from the start — for drains.
    pub(crate) fn published_high_water(&self) -> i64 {
        match self.kind {
            ProducerKind::Single => self.cursor.get(),
            ProducerKind::Multi => {
                let floor = self.min_gating().min(self.cursor.get());
                let start = if floor == i64::MAX { INITIAL } else { floor };
                self.published_upto(start + 1, i64::MAX)
            }
        }
    }

    #[inline(always)]
    pub(crate) fn is_alerted(&self) -> bool {
        self.alerted.load(Ordering::Acquire)
    }

    pub(crate) fn alert(&self) {
        self.alerted.store(true, Ordering::Release);
        self.notifier.wake_all();
    }
}
