//! Snapshot write/restore (spec/JOURNAL.md §3, `matcher-snap/1`).
//!
//! A snapshot captures *resting* state in book order — restoring by direct
//! insertion reproduces exact FIFO position and `seq`, so the continuation
//! emits byte-identical events. Verify: `snap → restore → snap` must equal.

use std::fmt::Write as _;

use crate::book::OrderBook;
use crate::core::RestoreError;
use crate::engine::Engine;
use crate::jsonflat::{get_i64, get_str, get_u64};
use crate::types::*;

/// Serialize one book's resting state as a `{"rec":"book"}` block + orders.
pub fn write_book(book: &OrderBook, sym: Symbol, out: &mut String) {
    let _ = writeln!(
        out,
        "{{\"rec\":\"book\",\"symbol\":{sym},\"seq\":{}}}",
        book.seq()
    );
    for o in book.resting_orders() {
        let _ = writeln!(
            out,
            "{{\"rec\":\"order\",\"order_id\":{},\"side\":\"{}\",\"otype\":\"limit\",\"tif\":\"{}\",\"price\":{},\"qty\":{}}}",
            o.order_id,
            o.side.as_str(),
            o.tif.as_str(),
            o.price,
            o.qty
        );
    }
}

/// The `matcher-snap/1` header line for an engine default config.
pub fn write_header(cfg: BookConfig, out: &mut String) {
    let _ = writeln!(
        out,
        "{{\"format\":\"matcher-snap/1\",\"pmin\":{},\"pmax\":{},\"max_orders\":{},\"index\":\"{}\"}}",
        cfg.price_min,
        cfg.price_max,
        cfg.max_orders,
        match cfg.index {
            IndexKind::Ladder => "ladder",
            IndexKind::Tree => "tree",
        }
    );
}

/// Full engine snapshot: header (default config) + every book, sorted by
/// symbol for deterministic output.
pub fn write_engine(engine: &Engine, out: &mut String) {
    write_header(engine.default_cfg(), out);
    let mut books: Vec<_> = engine.books_iter().collect();
    books.sort_by_key(|(s, _)| *s);
    for (sym, book) in books {
        write_book(book, sym, out);
    }
}

/// One parsed book block: symbol, last seq, resting orders in file order.
pub struct SnapshotBook {
    pub symbol: Symbol,
    pub seq: u64,
    pub orders: Vec<RestingOrder>,
}

/// Parsed snapshot: engine default config + book blocks in file order.
pub struct ParsedSnapshot {
    pub cfg: BookConfig,
    pub books: Vec<SnapshotBook>,
}

/// Parse a `matcher-snap/1` document. Panics on malformed input — use
/// [`try_parse`] where the snapshot is not a trusted local artifact.
pub fn parse(text: &str) -> ParsedSnapshot {
    try_parse(text).unwrap_or_else(|e| panic!("{e}"))
}

/// Parse a `matcher-snap/1` document, reporting malformed input as an error.
pub fn try_parse(text: &str) -> Result<ParsedSnapshot, RestoreError> {
    let err = |n: usize, msg: &str| RestoreError(format!("snapshot line {n}: {msg}"));
    let mut lines = text.lines();
    let hdr = lines.next().ok_or_else(|| err(1, "empty snapshot"))?;
    if get_str(hdr, "format") != Some("matcher-snap/1") {
        return Err(err(1, "not a matcher-snap/1 header"));
    }
    let cfg = BookConfig {
        price_min: get_i64(hdr, "pmin").unwrap_or(0),
        price_max: get_i64(hdr, "pmax").unwrap_or(1_000_000),
        max_orders: get_u64(hdr, "max_orders").unwrap_or(65_536) as usize,
        index: match get_str(hdr, "index") {
            Some("tree") => IndexKind::Tree,
            _ => IndexKind::Ladder,
        },
    };
    let mut books: Vec<SnapshotBook> = Vec::new();
    for (i, line) in lines.enumerate() {
        let n = i + 2;
        if line.is_empty() {
            continue;
        }
        match get_str(line, "rec") {
            Some("book") => books.push(SnapshotBook {
                symbol: get_u64(line, "symbol").unwrap_or(0) as Symbol,
                seq: get_u64(line, "seq").unwrap_or(0),
                orders: Vec::new(),
            }),
            Some("order") => {
                let o = RestingOrder {
                    order_id: get_u64(line, "order_id").ok_or_else(|| err(n, "bad order_id"))?,
                    side: match get_str(line, "side") {
                        Some("ask") => Side::Ask,
                        Some("bid") => Side::Bid,
                        _ => return Err(err(n, "bad side")),
                    },
                    tif: match get_str(line, "tif") {
                        Some("gtc") => Tif::Gtc,
                        Some("ioc") => Tif::Ioc,
                        Some("fok") => Tif::Fok,
                        Some("post_only") => Tif::PostOnly,
                        _ => return Err(err(n, "bad tif")),
                    },
                    price: get_i64(line, "price").ok_or_else(|| err(n, "bad price"))?,
                    qty: get_u64(line, "qty").ok_or_else(|| err(n, "bad qty"))?,
                };
                books
                    .last_mut()
                    .ok_or_else(|| err(n, "order line before book block"))?
                    .orders
                    .push(o);
            }
            _ => return Err(err(n, "bad rec")),
        }
    }
    Ok(ParsedSnapshot { cfg, books })
}

/// Check a book block can be restored under `cfg`: within capacity, unique
/// ids, positive quantities, valid prices, and not crossed.
pub fn validate_book(
    cfg: BookConfig,
    sym: Symbol,
    orders: &[RestingOrder],
) -> Result<(), RestoreError> {
    let err = |msg: String| Err(RestoreError(format!("snapshot book {sym}: {msg}")));
    if orders.len() > cfg.max_orders {
        return err(format!(
            "{} orders exceed max_orders {}",
            orders.len(),
            cfg.max_orders
        ));
    }
    let mut ids: Vec<OrderId> = orders.iter().map(|o| o.order_id).collect();
    ids.sort_unstable();
    if ids.windows(2).any(|w| w[0] == w[1]) {
        return err("duplicate order_id".into());
    }
    let (mut best_bid, mut best_ask) = (None::<Price>, None::<Price>);
    for o in orders {
        if o.qty == 0 {
            return err(format!("order {} has qty 0", o.order_id));
        }
        let valid = match cfg.index {
            IndexKind::Ladder => (cfg.price_min..=cfg.price_max).contains(&o.price),
            IndexKind::Tree => o.price > 0,
        };
        if !valid {
            return err(format!(
                "order {} price {} out of range",
                o.order_id, o.price
            ));
        }
        match o.side {
            Side::Bid => best_bid = Some(best_bid.map_or(o.price, |b| b.max(o.price))),
            Side::Ask => best_ask = Some(best_ask.map_or(o.price, |a| a.min(o.price))),
        }
    }
    if let (Some(b), Some(a)) = (best_bid, best_ask) {
        if b >= a {
            return err(format!("crossed book (bid {b} >= ask {a})"));
        }
    }
    Ok(())
}

/// Rebuild an engine from a parsed snapshot.
pub fn restore_engine(parsed: &ParsedSnapshot) -> Engine {
    let mut eng = Engine::new(parsed.cfg);
    for b in &parsed.books {
        eng.add_symbol(b.symbol, parsed.cfg);
        *eng.book_mut(b.symbol).unwrap() = OrderBook::restore(parsed.cfg, b.seq, &b.orders);
    }
    eng
}
