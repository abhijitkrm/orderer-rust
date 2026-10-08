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
/// rescanned only when the best level empties (O(1) amortized).
pub struct LadderIndex {
    base: Price, // array index = price - base
    side: Side,
    levels: Vec<Level>,
    bits: Vec<u64>,
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
        self.bits[i / 64] &= !(1u64 << (i % 64));
        self.count -= 1;
        if i as u32 == self.best {
            self.best = self.rescan(i);
        }
    }

    /// Next occupied index moving inward from `from` (inclusive). For asks:
    /// higher prices; for bids: lower prices.
    fn rescan(&self, from: usize) -> u32 {
        match self.side {
            Side::Ask => {
                let mut i = from;
                while i < self.levels.len() {
                    let w = i / 64;
                    let word = self.bits[w] & (u64::MAX << (i % 64));
                    if word != 0 {
                        return (w * 64 + word.trailing_zeros() as usize) as u32;
                    }
                    i = w * 64 + 64;
                }
                NIL
            }
            Side::Bid => {
                let mut i = from as i64;
                while i >= 0 {
                    let w = (i as usize) / 64;
                    let word = self.bits[w] & (u64::MAX >> (63 - (i % 64) as u32));
                    if word != 0 {
                        return (w * 64 + (63 - word.leading_zeros()) as usize) as u32;
                    }
                    i = (w as i64) * 64 - 1;
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
