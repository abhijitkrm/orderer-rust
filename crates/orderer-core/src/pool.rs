//! Slab pool of order slots with an intrusive free-list. All storage is
//! preallocated up to `max_orders`; steady-state matching performs zero
//! heap allocation.

use crate::types::{OrderId, Price, Qty, Side, Tif};

/// Sentinel link value ("null index").
pub const NIL: u32 = u32::MAX;

#[derive(Clone, Copy, Debug)]
pub struct Order {
    pub id: OrderId,
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
    pub tif: Tif,
    pub prev: u32,
    pub next: u32,
}

pub struct Pool {
    slots: Vec<Order>,
    free_head: u32,
    live: usize,
    cap: usize,
}

impl Pool {
    pub fn new(cap: usize) -> Pool {
        Pool {
            slots: Vec::with_capacity(cap.min(1 << 20)),
            free_head: NIL,
            live: 0,
            cap,
        }
    }

    #[inline]
    pub fn live(&self) -> usize {
        self.live
    }

    #[inline]
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Acquire a slot index, or `None` when at capacity.
    #[inline]
    pub fn alloc(&mut self) -> Option<u32> {
        if self.free_head != NIL {
            let idx = self.free_head;
            self.free_head = self.slots[idx as usize].next;
            self.live += 1;
            Some(idx)
        } else if self.slots.len() < self.cap {
            let idx = self.slots.len() as u32;
            self.slots.push(Order {
                id: 0,
                side: Side::Bid,
                price: 0,
                qty: 0,
                prev: NIL,
                tif: Tif::Gtc,
                next: NIL,
            });
            self.live += 1;
            Some(idx)
        } else {
            None
        }
    }

    #[inline]
    pub fn free(&mut self, idx: u32) {
        debug_assert!(self.live > 0);
        self.slots[idx as usize].next = self.free_head;
        self.slots[idx as usize].prev = NIL;
        self.free_head = idx;
        self.live -= 1;
    }

    #[inline]
    pub fn get(&self, idx: u32) -> &Order {
        &self.slots[idx as usize]
    }

    #[inline]
    pub fn get_mut(&mut self, idx: u32) -> &mut Order {
        &mut self.slots[idx as usize]
    }
}
