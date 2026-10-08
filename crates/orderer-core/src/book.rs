//! The order book: single-writer, deterministic, FIFO price-time matching.
//! All borrow-sensitive hot paths touch `self` fields directly (never via
//! `&mut self` methods) so the pool, map and price index stay disjoint
//! borrows.

use crate::index::PriceIndex;
use crate::ordermap::OrderMap;
use crate::pool::{Order, Pool, NIL};
use crate::sink::Sink;
use crate::types::*;

/// Read-only view of a live order (query API).
#[derive(Clone, Copy, Debug)]
pub struct OrderInfo {
    pub order_id: OrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
}

pub struct OrderBook {
    pool: Pool,
    map: OrderMap,
    bids: PriceIndex,
    asks: PriceIndex,
    seq: u64,
    cfg: BookConfig,
}

impl OrderBook {
    pub fn new(cfg: BookConfig) -> OrderBook {
        assert!(
            cfg.price_max > cfg.price_min || matches!(cfg.index, IndexKind::Tree),
            "price range must be non-empty"
        );
        let mk = |side| match cfg.index {
            IndexKind::Ladder => PriceIndex::ladder(side, cfg.price_min, cfg.price_max),
            IndexKind::Tree => PriceIndex::tree(side),
        };
        OrderBook {
            pool: Pool::new(cfg.max_orders),
            map: OrderMap::with_capacity(cfg.max_orders),
            bids: mk(Side::Bid),
            asks: mk(Side::Ask),
            seq: 0,
            cfg,
        }
    }

    /// Apply one command, emitting its event stream through `sink`.
    pub fn apply<S: Sink>(&mut self, cmd: Command, sink: &mut S) {
        match cmd {
            Command::New {
                order_id,
                side,
                otype,
                price,
                qty,
                tif,
            } => self.new_order(order_id, side, otype, price, qty, tif, sink),
            Command::Cancel { order_id } => self.cancel(order_id, sink),
            Command::Replace {
                order_id,
                price,
                qty,
            } => self.replace(order_id, price, qty, sink),
        }
    }

    // ---- commands --------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn new_order<S: Sink>(
        &mut self,
        order_id: OrderId,
        side: Side,
        otype: OType,
        price: Price,
        qty: Qty,
        tif: Tif,
        sink: &mut S,
    ) {
        // SPEC §4.3 validation precedence.
        if qty == 0 {
            return self.reject(sink, order_id, RejectReason::InvalidQty);
        }
        if otype == OType::Limit && !self.price_ok(price) {
            return self.reject(sink, order_id, RejectReason::InvalidPrice);
        }
        if self.map.contains(order_id) {
            return self.reject(sink, order_id, RejectReason::DuplicateOrderId);
        }
        if self.pool.live() >= self.cfg.max_orders {
            return self.reject(sink, order_id, RejectReason::BookFull);
        }
        if otype == OType::Limit {
            match tif {
                Tif::PostOnly => {
                    if self.would_cross(side, price) {
                        return self.reject(sink, order_id, RejectReason::PostOnlyWouldCross);
                    }
                }
                Tif::Fok if self.fillable(side, price) < qty => {
                    return self.reject(sink, order_id, RejectReason::FokCannotFill);
                }
                _ => {}
            }
        }

        let mut remaining = qty;
        let bound = (otype == OType::Limit).then_some(price);
        self.cross(side, bound, order_id, &mut remaining, sink);

        if remaining == 0 {
            self.emit(
                sink,
                Event::Closed {
                    order_id,
                    reason: CloseReason::Filled,
                },
            );
        } else if otype == OType::Limit && matches!(tif, Tif::Gtc | Tif::PostOnly) {
            self.rest(order_id, side, price, remaining, tif);
            self.emit(
                sink,
                Event::Accepted {
                    order_id,
                    leaves_qty: remaining,
                },
            );
        } else {
            self.emit(
                sink,
                Event::Closed {
                    order_id,
                    reason: CloseReason::Expired,
                },
            );
        }
    }

    fn cancel<S: Sink>(&mut self, order_id: OrderId, sink: &mut S) {
        let Some(idx) = self.map.get(order_id) else {
            return self.reject(sink, order_id, RejectReason::UnknownOrderId);
        };
        let (price, side) = {
            let o = self.pool.get(idx);
            (o.price, o.side)
        };
        let own = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if let Some(lvl) = own.level_mut(price) {
            self.pool.level_unlink(lvl, idx);
        }
        own.unlink_level(price);
        self.map.remove(order_id);
        self.pool.free(idx);
        self.emit(
            sink,
            Event::Closed {
                order_id,
                reason: CloseReason::Cancelled,
            },
        );
    }

    fn replace<S: Sink>(&mut self, order_id: OrderId, price: Price, qty: Qty, sink: &mut S) {
        // SPEC §4.3: unknown -> invalid_qty -> invalid_price.
        let Some(idx) = self.map.get(order_id) else {
            return self.reject(sink, order_id, RejectReason::UnknownOrderId);
        };
        if qty == 0 {
            return self.reject(sink, order_id, RejectReason::InvalidQty);
        }
        if !self.price_ok(price) {
            return self.reject(sink, order_id, RejectReason::InvalidPrice);
        }
        let (old_price, old_qty, side) = {
            let o = self.pool.get(idx);
            (o.price, o.qty, o.side)
        };

        if price == old_price && qty <= old_qty {
            // Quantity decrease (or no-op): keeps time priority.
            let own = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            if let Some(lvl) = own.level_mut(price) {
                lvl.total -= old_qty - qty;
            }
            self.pool.get_mut(idx).qty = qty;
            self.emit(
                sink,
                Event::Replaced {
                    order_id,
                    price,
                    qty,
                },
            );
            return;
        }

        // Priority loss: unlink and re-enter the aggressive GTC limit path.
        {
            let own = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            if let Some(lvl) = own.level_mut(old_price) {
                self.pool.level_unlink(lvl, idx);
            }
            own.unlink_level(old_price);
        }
        {
            let o = self.pool.get_mut(idx);
            o.price = price;
            o.qty = qty;
        }

        let mut remaining = qty;
        self.cross(side, Some(price), order_id, &mut remaining, sink);

        if remaining == 0 {
            self.map.remove(order_id);
            self.pool.free(idx);
            self.emit(
                sink,
                Event::Closed {
                    order_id,
                    reason: CloseReason::Filled,
                },
            );
        } else {
            self.pool.get_mut(idx).qty = remaining;
            let own = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            let lvl = own.level_insert(price);
            self.pool.level_push(lvl, idx);
            self.emit(
                sink,
                Event::Replaced {
                    order_id,
                    price,
                    qty: remaining,
                },
            );
        }
    }

    // ---- matching core ---------------------------------------------------

    /// Aggressive walk of the opposite side. `bound = None` means market
    /// (match all available depth). Mutates `*qty` down to what is left.
    fn cross<S: Sink>(
        &mut self,
        side: Side,
        bound: Option<Price>,
        taker: OrderId,
        qty: &mut Qty,
        sink: &mut S,
    ) {
        // Disjoint field borrows — see module docs.
        let pool = &mut self.pool;
        let map = &mut self.map;
        let seq = &mut self.seq;
        let opp = match side {
            Side::Bid => &mut self.asks,
            Side::Ask => &mut self.bids,
        };

        while let Some(bp) = opp.best_price() {
            if let Some(b) = bound {
                let crosses = match side {
                    Side::Bid => bp <= b,
                    Side::Ask => bp >= b,
                };
                if !crosses {
                    break;
                }
            }
            let emptied = {
                let Some(lvl) = opp.level_mut(bp) else {
                    break;
                };
                loop {
                    let mi = lvl.head;
                    if mi == NIL {
                        break;
                    }
                    let (mid, mqty) = {
                        let m = pool.get(mi);
                        (m.id, m.qty)
                    };
                    let q = (*qty).min(mqty);
                    *seq += 1;
                    sink.on_event(
                        *seq,
                        &Event::Trade {
                            maker: mid,
                            taker,
                            price: bp,
                            qty: q,
                        },
                    );
                    lvl.total -= q;
                    pool.get_mut(mi).qty = mqty - q;
                    *qty -= q;
                    if mqty == q {
                        pool.level_unlink(lvl, mi);
                        map.remove(mid);
                        pool.free(mi);
                        *seq += 1;
                        sink.on_event(
                            *seq,
                            &Event::Closed {
                                order_id: mid,
                                reason: CloseReason::Filled,
                            },
                        );
                    }
                    if *qty == 0 {
                        break;
                    }
                }
                lvl.is_empty()
            };
            if emptied {
                opp.unlink_level(bp);
            }
            if *qty == 0 {
                break;
            }
        }
    }

    /// Insert a resting order (pool slot guaranteed available by the
    /// `book_full` check at ingest).
    fn rest(&mut self, order_id: OrderId, side: Side, price: Price, qty: Qty, tif: Tif) {
        let idx = self.pool.alloc().expect("slot reserved by book_full check");
        *self.pool.get_mut(idx) = Order {
            id: order_id,
            side,
            price,
            qty,
            tif,
            prev: NIL,
            next: NIL,
        };
        let own = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let lvl = own.level_insert(price);
        self.pool.level_push(lvl, idx);
        self.map.insert(order_id, idx);
    }

    // ---- helpers ---------------------------------------------------------

    #[inline]
    fn price_ok(&self, price: Price) -> bool {
        match self.cfg.index {
            IndexKind::Ladder => price >= self.cfg.price_min && price <= self.cfg.price_max,
            IndexKind::Tree => price > 0,
        }
    }

    #[inline]
    fn would_cross(&self, side: Side, price: Price) -> bool {
        match side {
            Side::Bid => self.asks.best_price().is_some_and(|bp| price >= bp),
            Side::Ask => self.bids.best_price().is_some_and(|bp| price <= bp),
        }
    }

    /// Quantity available on the opposite side within `price` (FOK pre-check).
    #[inline]
    fn fillable(&self, side: Side, price: Price) -> Qty {
        let (lo, hi) = self.range_bounds();
        match side {
            Side::Bid => self.asks.sum_range(lo, price),
            Side::Ask => self.bids.sum_range(price, hi),
        }
    }

    #[inline]
    fn range_bounds(&self) -> (Price, Price) {
        match self.cfg.index {
            IndexKind::Ladder => (self.cfg.price_min, self.cfg.price_max),
            IndexKind::Tree => (Price::MIN, Price::MAX),
        }
    }

    #[inline]
    fn emit<S: Sink>(&mut self, sink: &mut S, ev: Event) {
        self.seq += 1;
        sink.on_event(self.seq, &ev);
    }

    #[inline]
    fn reject<S: Sink>(&mut self, sink: &mut S, order_id: OrderId, reason: RejectReason) {
        self.emit(sink, Event::Rejected { order_id, reason });
    }

    // ---- snapshot (JOURNAL.md §3) ------------------------------------------

    /// Resting orders in book order: bids best-price-first, asks
    /// best-price-first, FIFO within each level — insertion in this order
    /// reproduces exact FIFO position.
    pub fn resting_orders(&self) -> Vec<RestingOrder> {
        let mut out = Vec::with_capacity(self.map.len());
        for idx in [&self.bids, &self.asks] {
            for (price, _) in idx.depth(usize::MAX) {
                let lvl = idx.level(price).expect("depth level exists");
                let mut i = lvl.head;
                while i != NIL {
                    let o = self.pool.get(i);
                    out.push(RestingOrder {
                        order_id: o.id,
                        side: o.side,
                        tif: o.tif,
                        price: o.price,
                        qty: o.qty,
                    });
                    i = o.next;
                }
            }
        }
        out
    }

    /// Rebuild a book from snapshot state: orders inserted directly as
    /// resting, in file order (a correct book is never crossed).
    pub fn restore(cfg: BookConfig, seq: u64, orders: &[RestingOrder]) -> OrderBook {
        let mut b = OrderBook::new(cfg);
        b.seq = seq;
        for o in orders {
            debug_assert!(b.map.get(o.order_id).is_none());
            b.rest(o.order_id, o.side, o.price, o.qty, o.tif);
        }
        b
    }

    // ---- query API (SPEC §8) ---------------------------------------------

    pub fn best_bid(&self) -> Option<Price> {
        self.bids.best_price()
    }

    pub fn best_ask(&self) -> Option<Price> {
        self.asks.best_price()
    }

    pub fn order(&self, order_id: OrderId) -> Option<OrderInfo> {
        self.map.get(order_id).map(|i| {
            let o = self.pool.get(i);
            OrderInfo {
                order_id: o.id,
                side: o.side,
                price: o.price,
                qty: o.qty,
            }
        })
    }

    pub fn order_count(&self) -> usize {
        self.pool.live()
    }

    pub fn level_count(&self, side: Side) -> usize {
        match side {
            Side::Bid => self.bids.len(),
            Side::Ask => self.asks.len(),
        }
    }

    pub fn depth(&self, side: Side, n: usize) -> Vec<(Price, Qty)> {
        match side {
            Side::Bid => self.bids.depth(n),
            Side::Ask => self.asks.depth(n),
        }
    }

    /// Current event sequence number (events emitted so far).
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Debug invariant check — level totals match order sums, intrusive links
    /// are consistent, bitmap ⇔ non-empty levels, live pool count == map size.
    /// O(book size); for tests and debugging, never the hot path.
    #[cfg(debug_assertions)]
    pub fn check_invariants(&self) {
        assert_eq!(
            self.pool.live(),
            self.map.len(),
            "pool live count != map size"
        );
        assert!(self.pool.live() <= self.pool.cap(), "live exceeds capacity");
        for side in [Side::Bid, Side::Ask] {
            let index = match side {
                Side::Bid => &self.bids,
                Side::Ask => &self.asks,
            };
            if let PriceIndex::Ladder(lad) = index {
                let mut seen = 0usize;
                for (i, lvl) in lad.levels().iter().enumerate() {
                    assert_eq!(
                        lad.occupied(i),
                        !lvl.is_empty(),
                        "bit/level mismatch at index {i}"
                    );
                    if lvl.is_empty() {
                        continue;
                    }
                    seen += 1;
                    // walk chain: links consistent, qty sum == level total
                    let mut n = lvl.head;
                    let mut prev = NIL;
                    let mut sum = 0u64;
                    while n != NIL {
                        let o = self.pool.get(n);
                        assert_eq!(o.prev, prev, "broken prev link");
                        prev = n;
                        sum += o.qty;
                        n = o.next;
                    }
                    assert_eq!(prev, lvl.tail, "broken tail link");
                    assert_eq!(sum, lvl.total, "level total drift");
                }
                assert_eq!(seen, index.len(), "level count drift");
            }
        }
    }
}
