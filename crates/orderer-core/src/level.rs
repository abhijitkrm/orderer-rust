//! A price level: an intrusive FIFO queue of pool orders plus a running
//! quantity total (used by FOK pre-checks and depth queries).

use crate::pool::{Pool, NIL};
use crate::types::Qty;

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
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head == NIL
    }
}

impl Pool {
    /// Append `idx` at the tail of `lvl`. The order's `qty` must already be set.
    #[inline]
    pub fn level_push(&mut self, lvl: &mut Level, idx: u32) {
        let qty = self.slots_get_qty(idx);
        self.get_mut(idx).prev = lvl.tail;
        self.get_mut(idx).next = NIL;
        if lvl.tail != NIL {
            self.get_mut(lvl.tail).next = idx;
        } else {
            lvl.head = idx;
        }
        lvl.tail = idx;
        lvl.total += qty;
    }

    /// Unlink `idx` from anywhere in `lvl` and adjust the level total.
    #[inline]
    pub fn level_unlink(&mut self, lvl: &mut Level, idx: u32) {
        let (prev, next, qty) = {
            let o = self.get(idx);
            (o.prev, o.next, o.qty)
        };
        if prev != NIL {
            self.get_mut(prev).next = next;
        } else {
            lvl.head = next;
        }
        if next != NIL {
            self.get_mut(next).prev = prev;
        } else {
            lvl.tail = prev;
        }
        let o = self.get_mut(idx);
        o.prev = NIL;
        o.next = NIL;
        lvl.total -= qty;
    }

    #[inline]
    fn slots_get_qty(&self, idx: u32) -> Qty {
        self.get(idx).qty
    }
}
