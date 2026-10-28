//! Open-addressed `order_id -> loc` map (docs/LAYOUT.md §3).
//! Linear probing, power-of-two capacity, tombstone-free deletion via
//! backward-shift (Knuth 6.4 algorithm R). Fibonacci hashing keeps sequential
//! order ids spread evenly.
//!
//! Storage is split: a dense 1-byte tag per slot (0 = empty, else
//! `0x80 | h >> 57`) and the `{key, val}` entries. Probes compare the tag
//! first and read an entry only on a tag match, so an absent-id check — every
//! New's duplicate test — touches only the tag array.

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Entry {
    key: u64,
    val: u32,
}

/// Map `u64 -> u32` optimized for dense sequential ids.
pub struct OrderMap {
    tags: Vec<u8>,
    entries: Vec<Entry>,
    mask: usize,
    len: usize,
    /// Bumped by every `remove`. A slot from `find` stays valid until the
    /// next removal (backward shift may relocate entries; inserting a new key
    /// never moves existing ones).
    pub removes: u64,
}

#[inline(always)]
fn mix(k: u64) -> u64 {
    // Fibonacci multiply then fold high entropy down — sequential order ids
    // have near-zero entropy in the low product bits, which would cluster.
    let h = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 32)
}

/// Slot from the low bits, tag from the top 7 (independent of the slot).
#[inline(always)]
fn tag_of(h: u64) -> u8 {
    (h >> 57) as u8 | 0x80
}

impl OrderMap {
    pub fn with_capacity(live_max: usize) -> OrderMap {
        let cap = (live_max.saturating_mul(2)).next_power_of_two().max(16);
        OrderMap {
            tags: vec![0; cap],
            entries: vec![Entry::default(); cap],
            mask: cap - 1,
            len: 0,
            removes: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Slot index holding `key`, if present.
    #[inline(always)]
    pub fn find(&self, key: u64) -> Option<usize> {
        let h = mix(key);
        let t = tag_of(h);
        let mut i = h as usize & self.mask;
        loop {
            let ti = self.tags[i];
            if ti == 0 {
                return None;
            }
            if ti == t && self.entries[i].key == key {
                return Some(i);
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn contains(&self, key: u64) -> bool {
        self.find(key).is_some()
    }

    #[inline(always)]
    pub fn get(&self, key: u64) -> Option<u32> {
        self.find(key).map(|i| self.entries[i].val)
    }

    /// Value at a slot returned by `find`.
    #[inline(always)]
    pub fn val_at(&self, slot: usize) -> u32 {
        self.entries[slot].val
    }

    /// Overwrite the value at a slot from `find` — valid only while
    /// `removes` is unchanged since that `find`.
    #[inline(always)]
    pub fn set_val_at(&mut self, slot: usize, val: u32) {
        self.entries[slot].val = val;
    }

    /// Prefetch hint for `key`'s home slot (tag + entry).
    #[inline(always)]
    pub fn prefetch(&self, key: u64) {
        let i = mix(key) as usize & self.mask;
        if let (Some(t), Some(e)) = (self.tags.get(i), self.entries.get(i)) {
            crate::hint::prefetch_read(t);
            crate::hint::prefetch_read(&e.key);
        }
    }

    /// Insert; returns old value if the key existed.
    #[inline(always)]
    pub fn insert(&mut self, key: u64, val: u32) -> Option<u32> {
        let h = mix(key);
        let t = tag_of(h);
        let mut i = h as usize & self.mask;
        loop {
            let ti = self.tags[i];
            if ti == 0 {
                self.tags[i] = t;
                self.entries[i] = Entry { key, val };
                self.len += 1;
                return None;
            }
            if ti == t && self.entries[i].key == key {
                return Some(core::mem::replace(&mut self.entries[i].val, val));
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Remove; returns the value if present.
    #[inline(always)]
    pub fn remove(&mut self, key: u64) -> Option<u32> {
        let i = self.find(key)?;
        let val = self.entries[i].val;
        self.tags[i] = 0;
        self.len -= 1;
        self.removes += 1;
        // Backward-shift deletion (Knuth 6.4, Algorithm R): scan forward from
        // the hole; move back any entry whose probe chain crosses it.
        //
        // An entry that cannot move must leave the hole where it is — only
        // the scan advances. (Moving the hole onto that live slot lets a
        // later shift overwrite it: a key silently vanishes.)
        let mut hole = i;
        let mut j = i;
        loop {
            j = (j + 1) & self.mask;
            let tj = self.tags[j];
            if tj == 0 {
                return Some(val);
            }
            let home = (mix(self.entries[j].key) as usize) & self.mask;
            // `home` in the cyclic interval (hole, j] means j cannot move back.
            let in_interval = if hole < j {
                home > hole && home <= j
            } else {
                home > hole || home <= j
            };
            if !in_interval {
                self.tags[hole] = tj;
                self.entries[hole] = self.entries[j];
                self.tags[j] = 0;
                hole = j;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Random insert/remove against a model, at high load factor so probe
    /// chains overlap and wrap — the regime that exposed the deletion bug.
    #[test]
    fn matches_a_model_under_heavy_churn() {
        let mut x: u64 = 0x1234_5678;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for live_max in [8usize, 32, 100, 512] {
            let mut m = OrderMap::with_capacity(live_max);
            let mut model: HashMap<u64, u32> = HashMap::new();
            let ids = (live_max * 3) as u64;
            for step in 0..200_000u32 {
                let k = next() % ids;
                if model.len() < live_max && next() % 2 == 0 {
                    assert_eq!(m.insert(k, step), model.insert(k, step));
                } else {
                    assert_eq!(m.remove(k), model.remove(&k), "remove {k} at step {step}");
                }
                assert_eq!(m.len(), model.len());
                if step % 997 == 0 {
                    for (&k, &v) in &model {
                        assert_eq!(m.get(k), Some(v), "lost key {k}");
                    }
                }
            }
        }
    }

    /// `replace`'s shortcut: a slot from `find` survives inserts of other
    /// keys (no removal in between), so writing through it is exact.
    #[test]
    fn find_slot_survives_inserts() {
        let mut m = OrderMap::with_capacity(64);
        for k in 0..60u64 {
            m.insert(k * 977, k as u32);
        }
        let r = m.removes;
        let slot = m.find(5 * 977).unwrap();
        for k in 1000..1060u64 {
            m.insert(k, 0);
        }
        assert_eq!(m.removes, r);
        m.set_val_at(slot, 4242);
        assert_eq!(m.get(5 * 977), Some(4242));
        m.remove(1000);
        assert_eq!(m.removes, r + 1);
    }
}
