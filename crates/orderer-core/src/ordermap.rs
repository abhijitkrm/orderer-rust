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
        // Backward-shift deletion: pull forward any entry whose probe chain
        // crosses the cleared slot.
        let mut hole = i;
        loop {
            let j = (hole + 1) & self.mask;
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
            } else {
                hole = j;
            }
        }
    }
}
