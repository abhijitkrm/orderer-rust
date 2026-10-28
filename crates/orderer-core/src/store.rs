//! Order store (docs/LAYOUT.md §1–2): resting orders live inline in their
//! price level's queue — an unrolled doubly-linked list of 128-byte chunks
//! (one M1 cache line), each holding up to 6 `{id, qty}` slots plus the
//! level's price and side. There is no per-order record.
//!
//! * A dead slot has `qty == 0` (a resting order always has `qty > 0`).
//! * `loc = chunk << 3 | slot` identifies a resting order; it is the order
//!   map's value and stays stable until the order leaves the book.
//! * A chunk is released as soon as its last live slot dies, so chunks in use
//!   never exceed live orders, and the head chunk of a non-empty level always
//!   holds a live slot.
//!
//! All storage is bounded by `max_orders` chunks; [`Store::prefault`] commits
//! it up front so steady-state matching never allocates or page-faults.

use crate::types::{OrderId, Price, Qty, Side, Tif};

/// Sentinel link value ("null index").
pub const NIL: u32 = u32::MAX;

/// Order slots per chunk.
pub const CHUNK_SLOTS: usize = 6;

#[derive(Clone, Copy, Debug)]
pub struct Slot {
    pub id: OrderId,
    pub qty: Qty,
}

#[derive(Clone, Copy, Debug)]
#[repr(C, align(128))]
pub struct Chunk {
    pub price: Price,
    pub prev: u32,
    pub next: u32,
    /// First slot that may be live; live slots are in `[head, tail)`.
    pub head: u8,
    /// Next free slot.
    pub tail: u8,
    /// Live slots in this chunk.
    pub live: u8,
    pub side: Side,
    _pad: u32,
    pub s: [Slot; CHUNK_SLOTS],
    /// Tif of slot `i` (6 used).
    pub tif: [Tif; 8],
}

const _: () = assert!(core::mem::size_of::<Chunk>() == 128);

impl Chunk {
    const EMPTY: Chunk = Chunk {
        price: 0,
        prev: NIL,
        next: NIL,
        head: 0,
        tail: 0,
        live: 0,
        side: Side::Bid,
        _pad: 0,
        s: [Slot { id: 0, qty: 0 }; CHUNK_SLOTS],
        tif: [Tif::Gtc; 8],
    };
}

#[inline(always)]
pub fn loc_chunk(loc: u32) -> usize {
    (loc >> 3) as usize
}

#[inline(always)]
pub fn loc_slot(loc: u32) -> usize {
    (loc & 7) as usize
}

/// A price level: head/tail chunk indices plus the running quantity total
/// (FOK pre-checks and depth queries). Empty ⇔ `head == NIL`.
#[derive(Clone, Copy, Debug)]
pub struct Level {
    pub head: u32,
    pub tail: u32,
    pub total: Qty,
}

impl Default for Level {
    fn default() -> Level {
        Level {
            head: NIL,
            tail: NIL,
            total: 0,
        }
    }
}

impl Level {
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.head == NIL
    }
}

pub struct Store {
    pub chunks: Vec<Chunk>,
    /// `chunks[..used]` have been handed out at least once.
    used: u32,
    free_chunk: u32,
    /// Live resting orders.
    pub live: usize,
    max_chunks: usize,
}

impl Store {
    pub fn new(max_orders: usize) -> Store {
        Store {
            // Commit lazily (bounded reservation); `prefault` commits it all.
            chunks: Vec::with_capacity(max_orders.min(1 << 20)),
            used: 0,
            free_chunk: NIL,
            live: 0,
            max_chunks: max_orders,
        }
    }

    /// Commit and touch every chunk now so no allocation or page fault lands
    /// on the hot path (BENCH.md: no growth inside timed sections).
    pub fn prefault(&mut self) {
        if self.chunks.len() < self.max_chunks {
            self.chunks
                .reserve_exact(self.max_chunks - self.chunks.len());
            self.chunks.resize(self.max_chunks, Chunk::EMPTY);
        }
    }

    #[inline(always)]
    pub fn chunk(&self, loc: u32) -> &Chunk {
        &self.chunks[loc_chunk(loc)]
    }

    /// Never fails while `live < max_orders` (chunks in use ≤ live orders).
    #[inline]
    fn alloc_chunk(&mut self, side: Side, price: Price, prev: u32) -> u32 {
        let c = if self.free_chunk != NIL {
            let c = self.free_chunk;
            self.free_chunk = self.chunks[c as usize].next;
            c
        } else {
            let c = self.used;
            self.used += 1;
            if c as usize == self.chunks.len() {
                self.chunks.push(Chunk::EMPTY);
            }
            c
        };
        let ch = &mut self.chunks[c as usize];
        ch.price = price;
        ch.prev = prev;
        ch.next = NIL;
        ch.head = 0;
        ch.tail = 0;
        ch.live = 0;
        ch.side = side;
        c
    }

    /// Unlink chunk `c` (its last live slot just died) from `lvl` and free it.
    #[inline]
    pub fn drop_chunk(&mut self, lvl: &mut Level, c: usize) {
        let (prev, next) = {
            let ch = &self.chunks[c];
            (ch.prev, ch.next)
        };
        if prev != NIL {
            self.chunks[prev as usize].next = next;
        } else {
            lvl.head = next;
        }
        if next != NIL {
            self.chunks[next as usize].prev = prev;
        } else {
            lvl.tail = prev;
        }
        self.chunks[c].next = self.free_chunk;
        self.free_chunk = c as u32;
    }

    /// Append a resting order at the back of `lvl`; returns its loc.
    #[inline(always)]
    pub fn push(
        &mut self,
        lvl: &mut Level,
        side: Side,
        price: Price,
        id: OrderId,
        qty: Qty,
        tif: Tif,
    ) -> u32 {
        let mut c = lvl.tail;
        if c == NIL || self.chunks[c as usize].tail as usize == CHUNK_SLOTS {
            let n = self.alloc_chunk(side, price, c);
            if c != NIL {
                self.chunks[c as usize].next = n;
            } else {
                lvl.head = n;
            }
            lvl.tail = n;
            c = n;
        }
        let ch = &mut self.chunks[c as usize];
        let slot = ch.tail as usize;
        ch.tail += 1;
        ch.live += 1;
        ch.s[slot] = Slot { id, qty };
        ch.tif[slot] = tif;
        lvl.total += qty;
        self.live += 1;
        c << 3 | slot as u32
    }

    /// Remove a live order from anywhere in `lvl`.
    #[inline(always)]
    pub fn unlink(&mut self, lvl: &mut Level, loc: u32) {
        let c = loc_chunk(loc);
        let ch = &mut self.chunks[c];
        let s = &mut ch.s[loc_slot(loc)];
        lvl.total -= s.qty;
        s.qty = 0;
        ch.live -= 1;
        let dead = ch.live == 0;
        self.live -= 1;
        if dead {
            self.drop_chunk(lvl, c);
        }
    }

    /// Live orders of `lvl` in FIFO order, as locs.
    pub fn iter_level<'a>(&'a self, lvl: &Level) -> impl Iterator<Item = u32> + 'a {
        let mut c = lvl.head;
        core::iter::from_fn(move || {
            if c == NIL {
                return None;
            }
            let ch = &self.chunks[c as usize];
            let cur = c;
            c = ch.next;
            Some(
                (ch.head..ch.tail).filter_map(move |i| {
                    (ch.s[i as usize].qty != 0).then_some(cur << 3 | i as u32)
                }),
            )
        })
        .flatten()
    }

    /// Chunks currently handed out (debug accounting).
    #[cfg(debug_assertions)]
    pub fn chunks_in_use(&self) -> usize {
        let mut free = 0usize;
        let mut c = self.free_chunk;
        while c != NIL {
            free += 1;
            c = self.chunks[c as usize].next;
        }
        self.used as usize - free
    }
}
