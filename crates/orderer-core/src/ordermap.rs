//! Open-addressed `order_id -> pool index` map.
//! Linear probing, power-of-two capacity, tombstone-free deletion via
//! backward-shift (Knuth 6.4 algorithm R). Fibonacci hashing keeps sequential
//! order ids spread evenly.

/// Map `u64 -> u32` optimized for dense sequential ids.
pub struct OrderMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    used: Vec<u8>, // 0 = empty, 1 = occupied
    mask: usize,
    len: usize,
}

#[inline]
fn mix(k: u64) -> u64 {
    // Fibonacci multiply then fold high entropy down — sequential order ids
    // have near-zero entropy in the low product bits, which would cluster.
    let h = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 32)
}

impl OrderMap {
    pub fn with_capacity(live_max: usize) -> OrderMap {
        let cap = (live_max.saturating_mul(2)).next_power_of_two().max(16);
        OrderMap {
            keys: vec![0; cap],
            vals: vec![0; cap],
            used: vec![0; cap],
            mask: cap - 1,
            len: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn contains(&self, key: u64) -> bool {
        self.get(key).is_some()
    }

    #[inline]
    pub fn get(&self, key: u64) -> Option<u32> {
        let mut i = (mix(key) as usize) & self.mask;
        loop {
            if self.used[i] == 0 {
                return None;
            }
            if self.keys[i] == key {
                return Some(self.vals[i]);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Insert; returns old value if the key existed.
    pub fn insert(&mut self, key: u64, val: u32) -> Option<u32> {
        let mut i = (mix(key) as usize) & self.mask;
        loop {
            if self.used[i] == 0 {
                self.used[i] = 1;
                self.keys[i] = key;
                self.vals[i] = val;
                self.len += 1;
                return None;
            }
            if self.keys[i] == key {
                return Some(core::mem::replace(&mut self.vals[i], val));
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Remove; returns the value if present.
    pub fn remove(&mut self, key: u64) -> Option<u32> {
        let mut i = (mix(key) as usize) & self.mask;
        loop {
            if self.used[i] == 0 {
                return None;
            }
            if self.keys[i] == key {
                break;
            }
            i = (i + 1) & self.mask;
        }
        let val = self.vals[i];
        self.used[i] = 0;
        self.len -= 1;
        // Backward-shift deletion (Knuth 6.4, Algorithm R): scan forward from
        // the hole; move back any entry whose probe chain crosses it.
        //
        // orderer fix (matcher-rust 459a22a had `hole = j` in the can't-move
        // branch too): an entry that cannot move must leave the hole where
        // it is — only the scan advances. Moving the hole onto a live slot
        // let a later shift overwrite that entry, silently dropping a key
        // (map `len` then drifts from the pool, and level totals underflow).
        let mut hole = i;
        let mut j = i;
        loop {
            j = (j + 1) & self.mask;
            if self.used[j] == 0 {
                return Some(val);
            }
            let home = (mix(self.keys[j]) as usize) & self.mask;
            // `home` in the cyclic interval (hole, j] means j cannot move back.
            let in_interval = if hole < j {
                home > hole && home <= j
            } else {
                home > hole || home <= j
            };
            if !in_interval {
                self.keys[hole] = self.keys[j];
                self.vals[hole] = self.vals[j];
                self.used[hole] = 1;
                self.used[j] = 0;
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
}
