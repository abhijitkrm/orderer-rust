//! Price -> Level indexes. Two interchangeable implementations behind one
//! interface:
//!
//! * `LadderIndex` — direct-indexed level array + occupancy bitmap. Best price
//!   is a word scan with clz/ctz. This is the hot path used for bounded tick
//!   domains (the default, matching how exchange engines lay out futures
//!   books).
//! * `TreeIndex` — `BTreeMap` fallback for unbounded/sparse domains.
//!
//! Semantics are identical; choice is purely performance/memory.

use std::collections::BTreeMap;

use crate::level::Level;
use crate::pool::NIL;
use crate::types::{Price, Qty, Side};

pub enum PriceIndex {
    Ladder(LadderIndex),
    Tree(TreeIndex),
}

impl PriceIndex {
    pub fn ladder(side: Side, price_min: Price, price_max: Price) -> PriceIndex {
        PriceIndex::Ladder(LadderIndex::new(side, price_min, price_max))
    }

    pub fn tree(side: Side) -> PriceIndex {
        PriceIndex::Tree(TreeIndex::new(side))
    }

    /// Best price on this side (max for bids, min for asks).
    #[inline]
    pub fn best_price(&self) -> Option<Price> {
        match self {
            PriceIndex::Ladder(l) => l.best_price(),
            PriceIndex::Tree(t) => t.best_price(),
        }
    }

    /// Existing level at `price` (mutable).
    #[inline]
    pub fn level_mut(&mut self, price: Price) -> Option<&mut Level> {
        match self {
            PriceIndex::Ladder(l) => l.level_mut(price),
            PriceIndex::Tree(t) => t.map.get_mut(&price),
        }
    }

    /// Existing level at `price` (read-only).
    #[inline]
    pub fn level(&self, price: Price) -> Option<&Level> {
        match self {
            PriceIndex::Ladder(l) => l.level(price),
            PriceIndex::Tree(t) => t.map.get(&price),
        }
    }

    /// Get-or-create the level at `price` for a resting insert.
    #[inline]
    pub fn level_insert(&mut self, price: Price) -> &mut Level {
        match self {
            PriceIndex::Ladder(l) => l.level_insert(price),
            PriceIndex::Tree(t) => t.map.entry(price).or_default(),
        }
    }

    /// Called after order removal: drop the level bookkeeping if it emptied.
    #[inline]
    pub fn unlink_level(&mut self, price: Price) {
        match self {
            PriceIndex::Ladder(l) => l.unlink_level(price),
            PriceIndex::Tree(t) => {
                if t.map.get(&price).is_some_and(|l| l.is_empty()) {
                    t.map.remove(&price);
                }
            }
        }
    }

    /// Sum of level totals in `[lo, hi]` (inclusive). For FOK fillability.
    pub fn sum_range(&self, lo: Price, hi: Price) -> Qty {
        match self {
            PriceIndex::Ladder(l) => l.sum_range(lo, hi),
            PriceIndex::Tree(t) => t.map.range(lo..=hi).fold(0u64, |acc, (_, l)| acc + l.total),
        }
    }

    /// Number of non-empty levels.
    pub fn len(&self) -> usize {
        match self {
            PriceIndex::Ladder(l) => l.count as usize,
            PriceIndex::Tree(t) => t.map.len(),
        }
    }

    /// Up to `n` (price, total_qty) pairs from best price inward.
    pub fn depth(&self, n: usize) -> Vec<(Price, Qty)> {
        match self {
            PriceIndex::Ladder(l) => l.depth(n),
            PriceIndex::Tree(t) => {
                let iter: Box<dyn Iterator<Item = (&Price, &Level)>> = match t.side {
                    Side::Bid => Box::new(t.map.iter().rev()),
                    Side::Ask => Box::new(t.map.iter()),
                };
                iter.take(n).map(|(p, l)| (*p, l.total)).collect()
            }
        }
    }
}

/// Direct-indexed ladder + occupancy bitmap for one book side.
/// Keeps a top-of-book cursor: `best` is always the best occupied index,
/// rescanned only when the best level empties. The rescan walks `summary`
/// (bit w set iff `bits[w] != 0`), so it skips 4096 empty ticks per word
/// and stays short even on a wide, nearly empty ladder.
pub struct LadderIndex {
    base: Price, // array index = price - base
    side: Side,
    levels: Vec<Level>,
    bits: Vec<u64>,
    summary: Vec<u64>,
    count: u32, // non-empty levels
    best: u32,  // index of best occupied level; NIL when empty
}

impl LadderIndex {
    pub fn new(side: Side, price_min: Price, price_max: Price) -> LadderIndex {
        let span = (price_max - price_min + 1).max(1) as usize;
        LadderIndex {
            base: price_min,
            side,
            levels: vec![Level::default(); span],
            bits: vec![0u64; span.div_ceil(64)],
            summary: vec![0u64; span.div_ceil(64).div_ceil(64)],
            count: 0,
            best: NIL,
        }
    }

    #[inline]
    fn idx(&self, price: Price) -> usize {
        (price - self.base) as usize
    }

    #[inline]
    pub fn level_mut(&mut self, price: Price) -> Option<&mut Level> {
        let i = self.idx(price);
        self.levels.get_mut(i).filter(|l| !l.is_empty())
    }

    #[inline]
    pub fn level(&self, price: Price) -> Option<&Level> {
        let i = self.idx(price);
        self.levels.get(i).filter(|l| !l.is_empty())
    }

    /// Level for insert: sets the occupancy bit (idempotent — caller is about
    /// to make the level non-empty) and improves the cursor if this price
    /// beats it.
    #[inline]
    pub fn level_insert(&mut self, price: Price) -> &mut Level {
        let i = self.idx(price);
        let lvl = &mut self.levels[i];
        if lvl.is_empty() {
            self.bits[i / 64] |= 1u64 << (i % 64);
            self.summary[i / 4096] |= 1u64 << ((i / 64) % 64);
            self.count += 1;
            let better = self.best == NIL
                || match self.side {
                    Side::Bid => i as u32 > self.best,
                    Side::Ask => (i as u32) < self.best,
                };
            if better {
                self.best = i as u32;
            }
        }
        lvl
    }

    /// Clear bookkeeping for an emptied level; rescan the cursor only if it
    /// pointed at this level.
    #[inline]
    pub fn unlink_level(&mut self, price: Price) {
        let i = self.idx(price);
        if !self.levels[i].is_empty() {
            return;
        }
        let w = i / 64;
        self.bits[w] &= !(1u64 << (i % 64));
        if self.bits[w] == 0 {
            self.summary[w / 64] &= !(1u64 << (w % 64));
        }
        self.count -= 1;
        if self.count == 0 {
            self.best = NIL;
        } else if i as u32 == self.best {
            self.best = self.rescan(i);
        }
    }

    /// Next occupied index moving inward from `from` (inclusive). For asks:
    /// higher prices; for bids: lower prices.
    fn rescan(&self, from: usize) -> u32 {
        let w = from / 64;
        match self.side {
            Side::Ask => {
                let word = self.bits[w] & (u64::MAX << (from % 64));
                if word != 0 {
                    return (w * 64 + word.trailing_zeros() as usize) as u32;
                }
                // next non-empty word above w, via the summary
                let mut s = w + 1;
                while s < self.bits.len() {
                    let sw = s / 64;
                    let sword = self.summary[sw] & (u64::MAX << (s % 64));
                    if sword != 0 {
                        let nw = sw * 64 + sword.trailing_zeros() as usize;
                        return (nw * 64 + self.bits[nw].trailing_zeros() as usize) as u32;
                    }
                    s = sw * 64 + 64;
                }
                NIL
            }
            Side::Bid => {
                let word = self.bits[w] & (u64::MAX >> (63 - (from % 64) as u32));
                if word != 0 {
                    return (w * 64 + (63 - word.leading_zeros()) as usize) as u32;
                }
                // next non-empty word below w, via the summary
                let mut s = w as i64 - 1;
                while s >= 0 {
                    let sw = (s as usize) / 64;
                    let sword = self.summary[sw] & (u64::MAX >> (63 - (s % 64) as u32));
                    if sword != 0 {
                        let nw = sw * 64 + (63 - sword.leading_zeros()) as usize;
                        return (nw * 64 + (63 - self.bits[nw].leading_zeros()) as usize) as u32;
                    }
                    s = (sw as i64) * 64 - 1;
                }
                NIL
            }
        }
    }

    /// Best occupied price (cursor read — O(1)).
    #[inline]
    pub fn best_price(&self) -> Option<Price> {
        if self.best == NIL {
            None
        } else {
            Some(self.base + self.best as Price)
        }
    }

    /// Sum totals over occupied levels in `[lo, hi]`.
    pub fn sum_range(&self, lo: Price, hi: Price) -> Qty {
        let lo_i = self.idx(lo);
        let hi_i = self.idx(hi).min(self.levels.len() - 1);
        if lo_i > hi_i {
            return 0;
        }
        let mut sum = 0u64;
        let mut i = lo_i;
        while i <= hi_i {
            let w = i / 64;
            let hi_bit = (w * 64 + 63).min(hi_i) % 64;
            // bits in this word restricted to [i % 64, hi_bit]
            let mut word = self.bits[w] & (u64::MAX << (i % 64)) & (u64::MAX >> (63 - hi_bit));
            while word != 0 {
                let b = word.trailing_zeros() as usize;
                sum += self.levels[w * 64 + b].total;
                word &= word - 1;
            }
            i = w * 64 + 64;
        }
        sum
    }

    /// Raw level access for invariant checks (debug builds / tests).
    pub fn levels(&self) -> &[Level] {
        &self.levels
    }

    /// Is `i` marked occupied in the bitmap?
    pub fn occupied(&self, i: usize) -> bool {
        self.bits[i / 64] & (1u64 << (i % 64)) != 0
    }

    pub fn depth(&self, n: usize) -> Vec<(Price, Qty)> {
        let mut out = Vec::with_capacity(n.min(64));
        match self.side {
            Side::Ask => {
                for w in 0..self.bits.len() {
                    let mut word = self.bits[w];
                    while word != 0 {
                        let b = word.trailing_zeros() as usize;
                        let i = w * 64 + b;
                        out.push((self.base + i as Price, self.levels[i].total));
                        if out.len() == n {
                            return out;
                        }
                        word &= word - 1;
                    }
                }
            }
            Side::Bid => {
                for w in (0..self.bits.len()).rev() {
                    let mut word = self.bits[w];
                    while word != 0 {
                        let b = 63 - word.leading_zeros() as usize;
                        let i = w * 64 + b;
                        out.push((self.base + i as Price, self.levels[i].total));
                        if out.len() == n {
                            return out;
                        }
                        word &= !(1u64 << b);
                    }
                }
            }
        }
        out
    }
}

/// Ordered-map index for unbounded price domains.
pub struct TreeIndex {
    side: Side,
    pub(crate) map: BTreeMap<Price, Level>,
}

impl TreeIndex {
    pub fn new(side: Side) -> TreeIndex {
        TreeIndex {
            side,
            map: BTreeMap::new(),
        }
    }

    #[inline]
    pub fn best_price(&self) -> Option<Price> {
        match self.side {
            Side::Bid => self.map.keys().next_back().copied(),
            Side::Ask => self.map.keys().next().copied(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The ladder's cursor agrees with a sorted set under random churn on a
    /// wide ladder, including whole summary words emptying and refilling.
    #[test]
    fn ladder_best_matches_reference() {
        const SPAN: i64 = 1 << 20;
        for side in [Side::Bid, Side::Ask] {
            let mut lad = LadderIndex::new(side, 0, SPAN - 1);
            let mut set = BTreeSet::new();
            let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
            for step in 0..200_000 {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                // mostly clustered prices, sometimes far out or at the edges
                let p = match x % 8 {
                    0 => 0,
                    1 => SPAN - 1,
                    2 => (x >> 8) as i64 % SPAN,
                    _ => SPAN / 2 - 6000 + (x >> 8) as i64 % 12_000,
                };
                let remove = step % 1000 < 520 && !set.is_empty();
                if remove {
                    // remove the best half the time, else the level at p's neighbour
                    let q = if x & 16 == 0 {
                        *match side {
                            Side::Bid => set.last(),
                            Side::Ask => set.first(),
                        }
                        .unwrap()
                    } else {
                        *set.range(p..).next().or(set.iter().next_back()).unwrap()
                    };
                    set.remove(&q);
                    lad.level_mut(q).unwrap().head = NIL;
                    lad.unlink_level(q);
                } else if set.insert(p) {
                    lad.level_insert(p).head = 0;
                }
                let want = match side {
                    Side::Bid => set.last(),
                    Side::Ask => set.first(),
                }
                .copied();
                assert_eq!(lad.best_price(), want, "{side:?} step {step}");
                assert_eq!(lad.count as usize, set.len());
            }
        }
    }
}
