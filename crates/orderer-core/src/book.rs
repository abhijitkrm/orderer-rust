//! The order book: single-writer, deterministic, FIFO price-time matching.
//! Storage follows docs/LAYOUT.md: resting orders live inline in per-level
//! chunk queues (`store`), the order map holds `loc`s, and the price indexes
//! hold levels. All borrow-sensitive hot paths touch `self` fields directly
//! (never via `&mut self` methods) so store, map and price index stay
//! disjoint borrows.

use crate::hint::{prefetch_read, prefetch_write};
use crate::index::PriceIndex;
use crate::ordermap::OrderMap;
use crate::sink::Sink;
use crate::store::{loc_chunk, loc_slot, Store, NIL};
use crate::types::*;

/// Read-only view of a live order (query API).
#[derive(Clone, Copy, Debug)]
pub struct OrderInfo {
    pub order_id: OrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
}

/// `apply_batch` look-ahead distances (LAYOUT.md §4).
const D_MAP: usize = 16;
const D_CHUNK: usize = 8;
const D_LEVEL: usize = 4;
/// Below this many live orders the book is cache-resident: look-ahead probes
/// cost more than they save, so `apply_batch` is a plain loop.
const PREFETCH_MIN_LIVE: usize = 1 << 16;

pub struct OrderBook {
    store: Store,
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
        // loc = chunk << 3 | slot must fit a u32.
        assert!(cfg.max_orders <= 1 << 29, "max_orders exceeds 2^29");
        let mk = |side| match cfg.index {
            IndexKind::Ladder => PriceIndex::ladder(side, cfg.price_min, cfg.price_max),
            IndexKind::Tree => PriceIndex::tree(side),
        };
        OrderBook {
            store: Store::new(cfg.max_orders),
            map: OrderMap::with_capacity(cfg.max_orders),
            bids: mk(Side::Bid),
            asks: mk(Side::Ask),
            seq: 0,
            cfg,
        }
    }

    /// Commit all order storage now (it is otherwise committed lazily as the
    /// book grows), so no allocation or page fault lands on a later command.
    /// Costs 128 B of resident memory per `max_orders`.
    pub fn prefault(&mut self) {
        self.store.prefault();
    }

    /// Apply one command, emitting its event stream through `sink`.
    #[inline(always)]
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

    /// Apply a run of commands. The event stream and resulting state are
    /// identical to calling [`apply`](Self::apply) on each in order; the
    /// batch only lets the book look ahead and hint the memory upcoming
    /// commands will touch (map slot, then the order's chunk / level, then
    /// the level or tail chunk), hiding DRAM latency behind the current
    /// command. Hints read possibly-stale state and never write.
    pub fn apply_batch<S: Sink>(&mut self, cmds: &[Command], sink: &mut S) {
        if self.store.live < PREFETCH_MIN_LIVE {
            for &c in cmds {
                self.apply(c, sink);
            }
            return;
        }
        for (i, &c) in cmds.iter().enumerate() {
            if let Some(f) = cmds.get(i + D_MAP) {
                self.map.prefetch(cmd_id(f));
            }
            if let Some(f) = cmds.get(i + D_CHUNK) {
                self.prefetch_chunk(f);
            }
            if let Some(f) = cmds.get(i + D_LEVEL) {
                self.prefetch_level(f);
            }
            self.apply(c, sink);
        }
    }

    // ---- commands --------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
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
        if self.store.live >= self.cfg.max_orders {
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

    #[inline(always)]
    fn cancel<S: Sink>(&mut self, order_id: OrderId, sink: &mut S) {
        let Some(loc) = self.map.get(order_id) else {
            return self.reject(sink, order_id, RejectReason::UnknownOrderId);
        };
        let (price, side) = {
            let ch = self.store.chunk(loc);
            (ch.price, ch.side)
        };
        let own = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if let Some(lvl) = own.level_mut(price) {
            self.store.unlink(lvl, loc);
        }
        own.unlink_level(price);
        self.map.remove(order_id);
        self.emit(
            sink,
            Event::Closed {
                order_id,
                reason: CloseReason::Cancelled,
            },
        );
    }

    #[inline(always)]
    fn replace<S: Sink>(&mut self, order_id: OrderId, price: Price, qty: Qty, sink: &mut S) {
        // SPEC §4.3: unknown -> invalid_qty -> invalid_price.
        let Some(mslot) = self.map.find(order_id) else {
            return self.reject(sink, order_id, RejectReason::UnknownOrderId);
        };
        if qty == 0 {
            return self.reject(sink, order_id, RejectReason::InvalidQty);
        }
        if !self.price_ok(price) {
            return self.reject(sink, order_id, RejectReason::InvalidPrice);
        }
        let loc = self.map.val_at(mslot);
        let (ci, si) = (loc_chunk(loc), loc_slot(loc));
        let (old_price, old_qty, side, tif) = {
            let ch = &self.store.chunks[ci];
            (ch.price, ch.s[si].qty, ch.side, ch.tif[si])
        };
        let own = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };

        if price == old_price && qty <= old_qty {
            // Quantity decrease (or no-op): keeps time priority.
            if let Some(lvl) = own.level_mut(price) {
                lvl.total -= old_qty - qty;
            }
            self.store.chunks[ci].s[si].qty = qty;
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

        // Priority loss: leave the book, re-cross at the new price, rest any
        // remainder at the back of the new level. The map entry is kept
        // across `cross` so the id stays reserved; it is rewritten (or
        // removed) afterwards — through the earlier slot when `cross`
        // removed nothing (no entry can have moved), else by re-probing.
        if let Some(lvl) = own.level_mut(old_price) {
            self.store.unlink(lvl, loc);
        }
        own.unlink_level(old_price);

        let mut remaining = qty;
        let removes = self.map.removes;
        self.cross(side, Some(price), order_id, &mut remaining, sink);

        if remaining == 0 {
            self.map.remove(order_id);
            self.emit(
                sink,
                Event::Closed {
                    order_id,
                    reason: CloseReason::Filled,
                },
            );
            return;
        }
        let own = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let nloc = self.store.push(
            own.level_insert(price),
            side,
            price,
            order_id,
            remaining,
            tif,
        );
        if self.map.removes == removes {
            self.map.set_val_at(mslot, nloc);
        } else {
            self.map.insert(order_id, nloc);
        }
        self.emit(
            sink,
            Event::Replaced {
                order_id,
                price,
                qty: remaining,
            },
        );
    }

    // ---- matching core ---------------------------------------------------

    /// Aggressive walk of the opposite side. `bound = None` means market
    /// (match all available depth). Mutates `*qty` down to what is left.
    #[inline(always)]
    fn cross<S: Sink>(
        &mut self,
        side: Side,
        bound: Option<Price>,
        taker: OrderId,
        qty: &mut Qty,
        sink: &mut S,
    ) {
        // Disjoint field borrows — see module docs.
        let store = &mut self.store;
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
                // Walk makers in FIFO order: slots of the head chunk, then
                // the next chunk. Dead slots (qty == 0) are skipped.
                while lvl.head != NIL {
                    let c = lvl.head as usize;
                    let (mid, q, chunk_dead) = {
                        let ch = &mut store.chunks[c];
                        let mut h = ch.head as usize;
                        while ch.s[h].qty == 0 {
                            h += 1;
                        }
                        ch.head = h as u8;
                        let m = &mut ch.s[h];
                        let (mid, mqty) = (m.id, m.qty);
                        let q = (*qty).min(mqty);
                        m.qty = mqty - q;
                        let filled = mqty == q;
                        if filled {
                            ch.live -= 1;
                        }
                        (mid, q, filled.then_some(ch.live == 0))
                    };
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
                    *qty -= q;
                    if let Some(dead) = chunk_dead {
                        // Maker filled: its slot is already dead (qty 0).
                        store.live -= 1;
                        if dead {
                            store.drop_chunk(lvl, c);
                        }
                        map.remove(mid);
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

    /// Insert a resting order (capacity guaranteed by the `book_full` check
    /// at ingest).
    #[inline(always)]
    fn rest(&mut self, order_id: OrderId, side: Side, price: Price, qty: Qty, tif: Tif) {
        let own = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        let loc = self
            .store
            .push(own.level_insert(price), side, price, order_id, qty, tif);
        self.map.insert(order_id, loc);
    }

    // ---- look-ahead hints (apply_batch) ------------------------------------

    #[inline(always)]
    fn index(&self, side: Side) -> &PriceIndex {
        match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        }
    }

    /// i + D_CHUNK: a New's ladder level; a Cancel/Replace's chunk.
    #[inline(always)]
    fn prefetch_chunk(&self, c: &Command) {
        match *c {
            Command::New {
                side, otype, price, ..
            } => {
                if otype == OType::Limit {
                    if let Some(l) = self.index(side).peek(price) {
                        prefetch_write(&l.total);
                    }
                }
            }
            Command::Cancel { order_id } | Command::Replace { order_id, .. } => {
                if let Some(ch) = self
                    .map
                    .get(order_id)
                    .and_then(|l| self.store.chunks.get(loc_chunk(l)))
                {
                    prefetch_write(&ch.price);
                }
            }
        }
    }

    /// i + D_LEVEL: a New's tail chunk; a Cancel/Replace's own level (and
    /// the new price's level for a Replace that moves).
    #[inline(always)]
    fn prefetch_level(&self, c: &Command) {
        let (order_id, new_price) = match *c {
            Command::New {
                side, otype, price, ..
            } => {
                if otype == OType::Limit {
                    if let Some(ch) = self
                        .index(side)
                        .peek(price)
                        .filter(|l| l.tail != NIL)
                        .and_then(|l| self.store.chunks.get(l.tail as usize))
                    {
                        prefetch_write(&ch.price);
                    }
                }
                return;
            }
            Command::Cancel { order_id } => (order_id, None),
            Command::Replace {
                order_id, price, ..
            } => (order_id, Some(price)),
        };
        let Some(ch) = self
            .map
            .get(order_id)
            .and_then(|l| self.store.chunks.get(loc_chunk(l)))
        else {
            return;
        };
        let idx = self.index(ch.side);
        if let Some(l) = idx.peek(ch.price) {
            prefetch_write(&l.total);
        }
        if let Some(p) = new_price.filter(|&p| p != ch.price) {
            if let Some(l) = idx.peek(p) {
                prefetch_read(&l.total);
            }
        }
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

    #[inline(always)]
    fn emit<S: Sink>(&mut self, sink: &mut S, ev: Event) {
        self.seq += 1;
        sink.on_event(self.seq, &ev);
    }

    #[inline(always)]
    fn reject<S: Sink>(&mut self, sink: &mut S, order_id: OrderId, reason: RejectReason) {
        self.emit(sink, Event::Rejected { order_id, reason });
    }

    // ---- snapshot (JOURNAL.md §3) ------------------------------------------

    /// Resting orders in book order: bids best-price-first, asks
    /// best-price-first, FIFO within each level — insertion in this order
    /// reproduces exact FIFO position.
    pub fn resting_orders(&self) -> Vec<RestingOrder> {
        let mut out = Vec::with_capacity(self.store.live);
        for (side, idx) in [(Side::Bid, &self.bids), (Side::Ask, &self.asks)] {
            for (price, _) in idx.depth(usize::MAX) {
                let lvl = idx.level(price).expect("depth level exists");
                for loc in self.store.iter_level(lvl) {
                    let ch = self.store.chunk(loc);
                    let i = loc_slot(loc);
                    out.push(RestingOrder {
                        order_id: ch.s[i].id,
                        side,
                        tif: ch.tif[i],
                        price: ch.price,
                        qty: ch.s[i].qty,
                    });
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
            assert!(b.store.live < cfg.max_orders, "snapshot exceeds max_orders");
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

    /// Total resting quantity at exactly `price` on `side` (0 if no level).
    pub fn level_total(&self, side: Side, price: Price) -> Qty {
        let idx = match side {
            Side::Bid => &self.bids,
            Side::Ask => &self.asks,
        };
        idx.level(price).map_or(0, |l| l.total)
    }

    pub fn order(&self, order_id: OrderId) -> Option<OrderInfo> {
        self.map.get(order_id).map(|loc| {
            let ch = self.store.chunk(loc);
            let s = ch.s[loc_slot(loc)];
            OrderInfo {
                order_id: s.id,
                side: ch.side,
                price: ch.price,
                qty: s.qty,
            }
        })
    }

    pub fn order_count(&self) -> usize {
        self.store.live
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

    /// Debug invariant check — chunk links and headers consistent, slot
    /// liveness counts, level totals match order sums, every live slot is
    /// mapped to its loc, bitmap ⇔ non-empty levels, chunk accounting.
    /// O(book size); for tests and debugging, never the hot path.
    #[cfg(debug_assertions)]
    pub fn check_invariants(&self) {
        use crate::store::CHUNK_SLOTS;
        assert_eq!(
            self.store.live,
            self.map.len(),
            "store live count != map size"
        );
        assert!(
            self.store.live <= self.cfg.max_orders,
            "live exceeds capacity"
        );
        let mut chunks_seen = 0usize;
        let mut orders_seen = 0usize;
        for side in [Side::Bid, Side::Ask] {
            let index = self.index(side);
            if let PriceIndex::Ladder(lad) = index {
                let mut seen = 0usize;
                for (i, lvl) in lad.levels().iter().enumerate() {
                    assert_eq!(
                        lad.occupied(i),
                        !lvl.is_empty(),
                        "bit/level mismatch at {i}"
                    );
                }
                for lvl in lad.levels() {
                    seen += !lvl.is_empty() as usize;
                }
                assert_eq!(seen, index.len(), "level count drift");
            }
            for (price, total) in index.depth(usize::MAX) {
                let lvl = index.level(price).expect("depth level exists");
                assert_eq!(lvl.total, total);
                let mut c = lvl.head;
                let mut prev = NIL;
                let mut sum = 0u64;
                while c != NIL {
                    chunks_seen += 1;
                    let ch = &self.store.chunks[c as usize];
                    assert_eq!(ch.prev, prev, "broken prev link");
                    assert_eq!(ch.price, price, "chunk price != level price");
                    assert_eq!(ch.side, side, "chunk side mismatch");
                    assert!(ch.head <= ch.tail && ch.tail as usize <= CHUNK_SLOTS);
                    let mut live = 0u8;
                    for i in 0..CHUNK_SLOTS {
                        let s = ch.s[i];
                        let in_range = i >= ch.head as usize && i < ch.tail as usize;
                        if s.qty != 0 {
                            assert!(in_range, "live slot outside [head, tail)");
                            live += 1;
                            sum += s.qty;
                            let loc = c << 3 | i as u32;
                            assert_eq!(self.map.get(s.id), Some(loc), "map loc mismatch");
                        }
                    }
                    assert!(live > 0, "empty chunk left linked");
                    assert_eq!(live, ch.live, "chunk live count drift");
                    orders_seen += live as usize;
                    prev = c;
                    c = ch.next;
                }
                assert_eq!(prev, lvl.tail, "broken tail link");
                assert_eq!(sum, lvl.total, "level total drift");
            }
        }
        assert_eq!(orders_seen, self.store.live, "orders in levels != live");
        assert_eq!(chunks_seen, self.store.chunks_in_use(), "chunk leak");
        assert!(
            chunks_seen <= self.store.live,
            "more chunks than live orders"
        );
    }
}

#[inline(always)]
fn cmd_id(c: &Command) -> OrderId {
    match *c {
        Command::New { order_id, .. }
        | Command::Cancel { order_id }
        | Command::Replace { order_id, .. } => order_id,
    }
}
