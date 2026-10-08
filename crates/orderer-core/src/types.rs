//! Core types shared by every matcher implementation. Field encodings and
//! canonical event serialization follow `spec/SPEC.md` + `spec/SCHEMA.md`.

use core::fmt;

pub type OrderId = u64;
pub type Symbol = u32;
pub type Price = i64;
pub type Qty = u64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    #[inline]
    pub fn opposite(self) -> Side {
        match self {
            Side::Bid => Side::Ask,
            Side::Ask => Side::Bid,
        }
    }
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Bid => "bid",
            Side::Ask => "ask",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum OType {
    Limit,
    Market,
}

impl OType {
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            OType::Limit => "limit",
            OType::Market => "market",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tif {
    Gtc,
    Ioc,
    Fok,
    PostOnly,
}

impl Tif {
    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Tif::Gtc => "gtc",
            Tif::Ioc => "ioc",
            Tif::Fok => "fok",
            Tif::PostOnly => "post_only",
        }
    }
}

/// A command submitted to a book.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Command {
    New {
        order_id: OrderId,
        side: Side,
        otype: OType,
        price: Price,
        qty: Qty,
        tif: Tif,
    },
    Cancel {
        order_id: OrderId,
    },
    Replace {
        order_id: OrderId,
        price: Price,
        qty: Qty,
    },
}

impl Command {
    /// New limit order.
    #[inline]
    pub fn new(order_id: OrderId, side: Side, price: Price, qty: Qty, tif: Tif) -> Command {
        Command::New {
            order_id,
            side,
            otype: OType::Limit,
            price,
            qty,
            tif,
        }
    }

    /// New market order (never rests; price/tif ignored).
    #[inline]
    pub fn market(order_id: OrderId, side: Side, qty: Qty) -> Command {
        Command::New {
            order_id,
            side,
            otype: OType::Market,
            price: 0,
            qty,
            tif: Tif::Ioc,
        }
    }

    #[inline]
    pub fn cancel(order_id: OrderId) -> Command {
        Command::Cancel { order_id }
    }

    #[inline]
    pub fn replace(order_id: OrderId, price: Price, qty: Qty) -> Command {
        Command::Replace {
            order_id,
            price,
            qty,
        }
    }

    /// Canonical command line (SCHEMA.md) appended to `out`, no newline.
    /// Inverse of the vector-file parser used by the golden harnesses.
    pub fn write_canonical(&self, out: &mut String) {
        self.write_inner(None, out);
    }

    /// Engine-journal variant: `"symbol":N` after `"cmd"`.
    pub fn write_canonical_sym(&self, sym: Symbol, out: &mut String) {
        self.write_inner(Some(sym), out);
    }

    fn write_inner(&self, sym: Option<Symbol>, out: &mut String) {
        let mut sf = String::new();
        if let Some(s) = sym {
            let _ = fmt::Write::write_fmt(&mut sf, format_args!(",\"symbol\":{s}"));
        }
        match *self {
            Command::New {
                order_id,
                side,
                otype,
                price,
                qty,
                tif,
            } => {
                let _ = fmt::Write::write_fmt(
                    out,
                    format_args!(
                        "{{\"cmd\":\"new\"{},\"order_id\":{},\"side\":\"{}\",\"otype\":\"{}\",\"price\":{},\"qty\":{},\"tif\":\"{}\"}}",
                        sf,
                        order_id,
                        side.as_str(),
                        otype.as_str(),
                        price,
                        qty,
                        tif.as_str()
                    ),
                );
            }
            Command::Cancel { order_id } => {
                let _ = fmt::Write::write_fmt(
                    out,
                    format_args!("{{\"cmd\":\"cancel\"{},\"order_id\":{}}}", sf, order_id),
                );
            }
            Command::Replace {
                order_id,
                price,
                qty,
            } => {
                let _ = fmt::Write::write_fmt(
                    out,
                    format_args!(
                        "{{\"cmd\":\"replace\"{},\"order_id\":{},\"price\":{},\"qty\":{}}}",
                        sf, order_id, price, qty
                    ),
                );
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RejectReason {
    InvalidQty,
    InvalidPrice,
    DuplicateOrderId,
    UnknownOrderId,
    PostOnlyWouldCross,
    FokCannotFill,
    BookFull,
}

impl RejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            RejectReason::InvalidQty => "invalid_qty",
            RejectReason::InvalidPrice => "invalid_price",
            RejectReason::DuplicateOrderId => "duplicate_order_id",
            RejectReason::UnknownOrderId => "unknown_order_id",
            RejectReason::PostOnlyWouldCross => "post_only_would_cross",
            RejectReason::FokCannotFill => "fok_cannot_fill",
            RejectReason::BookFull => "book_full",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CloseReason {
    Filled,
    Cancelled,
    Expired,
}

impl CloseReason {
    pub fn as_str(self) -> &'static str {
        match self {
            CloseReason::Filled => "filled",
            CloseReason::Cancelled => "cancelled",
            CloseReason::Expired => "expired",
        }
    }
}

/// An event emitted by a book. Paired with a per-book `seq` at emit time.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Event {
    Accepted {
        order_id: OrderId,
        leaves_qty: Qty,
    },
    Rejected {
        order_id: OrderId,
        reason: RejectReason,
    },
    Trade {
        maker: OrderId,
        taker: OrderId,
        price: Price,
        qty: Qty,
    },
    Closed {
        order_id: OrderId,
        reason: CloseReason,
    },
    Replaced {
        order_id: OrderId,
        price: Price,
        qty: Qty,
    },
}

impl Event {
    /// Append this event's canonical JSON line (SCHEMA.md) to `out`,
    /// without the trailing newline.
    pub fn write_canonical(seq: u64, ev: &Event, out: &mut String) {
        Event::write_inner(seq, "", ev, out);
    }

    /// Canonical line for `engine:true` vectors: `"symbol":N` after `ev`.
    pub fn write_canonical_sym(seq: u64, sym: Symbol, ev: &Event, out: &mut String) {
        let sym_field = format!(",\"symbol\":{sym}");
        Event::write_inner(seq, &sym_field, ev, out);
    }

    fn write_inner(seq: u64, sym_field: &str, ev: &Event, out: &mut String) {
        match *ev {
            Event::Accepted {
                order_id,
                leaves_qty,
            } => {
                let _ = fmt::Write::write_fmt(
                    out,
                    format_args!(
                        "{{\"seq\":{},\"ev\":\"accepted\"{},\"order_id\":{},\"leaves_qty\":{}}}",
                        seq, sym_field, order_id, leaves_qty
                    ),
                );
            }
            Event::Rejected { order_id, reason } => {
                let _ = fmt::Write::write_fmt(
                    out,
                    format_args!(
                        "{{\"seq\":{},\"ev\":\"rejected\"{},\"order_id\":{},\"reason\":\"{}\"}}",
                        seq,
                        sym_field,
                        order_id,
                        reason.as_str()
                    ),
                );
            }
            Event::Trade {
                maker,
                taker,
                price,
                qty,
            } => {
                let _ = fmt::Write::write_fmt(
                    out,
                    format_args!(
                        "{{\"seq\":{},\"ev\":\"trade\"{},\"maker\":{},\"taker\":{},\"price\":{},\"qty\":{}}}",
                        seq, sym_field, maker, taker, price, qty
                    ),
                );
            }
            Event::Closed { order_id, reason } => {
                let _ = fmt::Write::write_fmt(
                    out,
                    format_args!(
                        "{{\"seq\":{},\"ev\":\"closed\"{},\"order_id\":{},\"reason\":\"{}\"}}",
                        seq,
                        sym_field,
                        order_id,
                        reason.as_str()
                    ),
                );
            }
            Event::Replaced {
                order_id,
                price,
                qty,
            } => {
                let _ = fmt::Write::write_fmt(
                    out,
                    format_args!(
                        "{{\"seq\":{},\"ev\":\"replaced\"{},\"order_id\":{},\"price\":{},\"qty\":{}}}",
                        seq, sym_field, order_id, price, qty
                    ),
                );
            }
        }
    }

    pub fn canonical(seq: u64, ev: &Event) -> String {
        let mut s = String::with_capacity(96);
        Event::write_canonical(seq, ev, &mut s);
        s
    }

    pub fn canonical_sym(seq: u64, sym: Symbol, ev: &Event) -> String {
        let mut s = String::with_capacity(96);
        Event::write_canonical_sym(seq, sym, ev, &mut s);
        s
    }

    /// Cheap content hash — lets sinks observe every field without storing
    /// events (e.g. `NullSink` in benchmarks).
    #[inline]
    pub fn fold(&self) -> u64 {
        match *self {
            Event::Accepted {
                order_id,
                leaves_qty,
            } => order_id.wrapping_mul(0x9E37_79B1).wrapping_add(leaves_qty),
            Event::Rejected { order_id, reason } => order_id
                .wrapping_mul(0x9E37_79B2)
                .wrapping_add(reason as u64),
            Event::Trade {
                maker,
                taker,
                price,
                qty,
            } => maker
                .wrapping_mul(0x9E37_79B3)
                .wrapping_add(taker.rotate_left(21))
                .wrapping_add((price as u64).rotate_left(42))
                .wrapping_add(qty),
            Event::Closed { order_id, reason } => order_id
                .wrapping_mul(0x9E37_79B4)
                .wrapping_add(reason as u64),
            Event::Replaced {
                order_id,
                price,
                qty,
            } => order_id
                .wrapping_mul(0x9E37_79B5)
                .wrapping_add((price as u64).rotate_left(31))
                .wrapping_add(qty),
        }
    }
}

/// A resting order as persisted by a snapshot (JOURNAL.md §3). `qty` is the
/// leaves quantity; `otype` is always `Limit` (only limits can rest).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RestingOrder {
    pub order_id: OrderId,
    pub side: Side,
    pub tif: Tif,
    pub price: Price,
    pub qty: Qty,
}

/// Which price-index implementation backs a book side.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IndexKind {
    /// Bounded direct-indexed ladder + occupancy bitmap. O(1) best price.
    Ladder,
    /// Ordered-map fallback for unbounded/sparse price domains.
    Tree,
}

/// Construction parameters for an `OrderBook`.
#[derive(Clone, Copy, Debug)]
pub struct BookConfig {
    pub price_min: Price,
    pub price_max: Price,
    pub max_orders: usize,
    pub index: IndexKind,
}

impl Default for BookConfig {
    fn default() -> Self {
        BookConfig {
            price_min: 0,
            price_max: 1_000_000,
            max_orders: 65_536,
            index: IndexKind::Ladder,
        }
    }
}
