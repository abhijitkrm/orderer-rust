//! Incremental state hashing: a map whose contents are summarised by an
//! order-independent 128-bit accumulator that is brought up to date from the
//! entries that changed, so a digest costs O(changed), not O(state).
//!
//! Each entry hashes to a [`Lane`] by a pure function of its key and value; the
//! accumulator is the wrapping sum of the lanes of the entries present. Insert,
//! change and remove keep it equal to that sum, so two maps with the same
//! contents have the same accumulator however they got there, and a restored
//! snapshot rebuilds exactly the same value.
//!
//! This is for detecting divergence between replicas, not for security: the lanes
//! are fast mixers, not a cryptographic hash. The full SHA-256 of a snapshot
//! (`Ledger::deep_digest`) stays available.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};

pub type Lane = (u64, u64);

pub const ZERO: Lane = (0, 0);

pub fn lane_add(a: Lane, b: Lane) -> Lane {
    (a.0.wrapping_add(b.0), a.1.wrapping_add(b.1))
}

pub fn lane_sub(a: Lane, b: Lane) -> Lane {
    (a.0.wrapping_sub(b.0), a.1.wrapping_sub(b.1))
}

pub fn lane_bytes(l: Lane) -> [u8; 16] {
    let mut o = [0u8; 16];
    o[..8].copy_from_slice(&l.0.to_le_bytes());
    o[8..].copy_from_slice(&l.1.to_le_bytes());
    o
}

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A small sequential mixer producing a [`Lane`] from a stream of words.
pub struct Mix {
    a: u64,
    b: u64,
}

impl Mix {
    /// `tag` separates the cell kinds, so equal fields in different tables differ.
    pub fn new(tag: u64) -> Mix {
        Mix {
            a: splitmix(tag ^ 0x243F_6A88_85A3_08D3),
            b: splitmix(tag ^ 0x1319_8A2E_0370_7344),
        }
    }
    pub fn u64(&mut self, v: u64) -> &mut Mix {
        self.a = splitmix(self.a ^ v).wrapping_add(self.a.rotate_left(17));
        self.b = splitmix(self.b.wrapping_add(v).rotate_left(13) ^ self.a);
        self
    }
    pub fn u128(&mut self, v: u128) -> &mut Mix {
        self.u64(v as u64).u64((v >> 64) as u64)
    }
    pub fn finish(&self) -> Lane {
        (
            splitmix(self.a ^ self.b.rotate_left(7)),
            splitmix(self.b ^ self.a.rotate_left(41)),
        )
    }
}

struct Slot<V> {
    v: V,
    /// The lane currently counted in the accumulator, if any.
    h: Cell<Option<Lane>>,
}

/// An ordered map with an incrementally maintained accumulator over its entries.
pub struct Tracked<K: Ord + Copy, V> {
    map: BTreeMap<K, Slot<V>>,
    dirty: RefCell<BTreeSet<K>>,
    acc: Cell<Lane>,
}

impl<K: Ord + Copy, V> Default for Tracked<K, V> {
    fn default() -> Self {
        Tracked {
            map: BTreeMap::new(),
            dirty: RefCell::new(BTreeSet::new()),
            acc: Cell::new(ZERO),
        }
    }
}

impl<K: Ord + Copy, V> Tracked<K, V> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn contains_key(&self, k: &K) -> bool {
        self.map.contains_key(k)
    }
    pub fn get(&self, k: &K) -> Option<&V> {
        self.map.get(k).map(|s| &s.v)
    }
    /// A mutable reference; the entry is re-hashed at the next digest.
    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        let s = self.map.get_mut(k)?;
        self.dirty.get_mut().insert(*k);
        Some(&mut s.v)
    }
    pub fn get_or_insert_with(&mut self, k: K, f: impl FnOnce() -> V) -> &mut V {
        self.dirty.get_mut().insert(k);
        &mut self
            .map
            .entry(k)
            .or_insert_with(|| Slot {
                v: f(),
                h: Cell::new(None),
            })
            .v
    }
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.dirty.get_mut().insert(k);
        let old = self.map.insert(
            k,
            Slot {
                v,
                h: Cell::new(None),
            },
        )?;
        if let Some(h) = old.h.get() {
            self.acc.set(lane_sub(self.acc.get(), h));
        }
        Some(old.v)
    }
    pub fn remove(&mut self, k: &K) -> Option<V> {
        let old = self.map.remove(k)?;
        if let Some(h) = old.h.get() {
            self.acc.set(lane_sub(self.acc.get(), h));
        }
        self.dirty.get_mut().remove(k);
        Some(old.v)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.map.iter().map(|(k, s)| (k, &s.v))
    }
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.map.keys()
    }
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.map.values().map(|s| &s.v)
    }

    /// Bring the accumulator up to date from the entries changed since the last
    /// call (cost: their number) and return it.
    pub fn lane(&self, cell: impl Fn(&K, &V) -> Lane) -> Lane {
        let dirty = std::mem::take(&mut *self.dirty.borrow_mut());
        let mut acc = self.acc.get();
        for k in dirty {
            if let Some(s) = self.map.get(&k) {
                let new = cell(&k, &s.v);
                if let Some(old) = s.h.replace(Some(new)) {
                    acc = lane_sub(acc, old);
                }
                acc = lane_add(acc, new);
            }
        }
        self.acc.set(acc);
        acc
    }

    /// The number of entries waiting to be re-hashed (tests and benchmarks).
    pub fn pending_hashes(&self) -> usize {
        self.dirty.borrow().len()
    }

    /// The same accumulator computed from scratch, ignoring all caching (tests).
    pub fn lane_from_scratch(&self, cell: impl Fn(&K, &V) -> Lane) -> Lane {
        self.map
            .iter()
            .fold(ZERO, |a, (k, s)| lane_add(a, cell(k, &s.v)))
    }
}
