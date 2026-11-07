//! Wire formats of this crate: the forwarded-message log entry and the encoding
//! of a settlement event. Little-endian, fixed layouts. Decoders never panic.
//!
//! **Entry tag 5, forwarded batch** (a ledger shard's outbox messages):
//! ```text
//!   0   u8   5
//!   1   u8   version (1)
//!   2   u16  ledger shard
//!   4   u64  first outbox index
//!   12  u32  n
//!   16  n x message (30 bytes)
//! message: kind u8 (1 submit, 2 cancel) | order u64 | symbol u32 |
//!          side u8 (0 buy, 1 sell) | price u64 | qty u64
//!          (a cancel carries side 0, price 0, qty 0)
//! ```
//! Message `i` has outbox index `first + i`. Tags 1 and 2 (plain and session
//! batches) are unchanged and still go to the pipeline. Tags 3 and 4 of
//! `COMMAND-FORMAT.md` are not used here: the ledger enforces ownership before it
//! forwards, and nothing in the matching tier needs an account or a leader
//! timestamp yet.

use orderer_ledger::{Event, OutMsg, Side};

pub const TAG_FORWARD: u8 = 5;
pub const MSG_BYTES: usize = 30;
pub const MAX_BATCH: usize = 1 << 16;

#[derive(Debug, PartialEq, Eq)]
pub struct WireError(pub &'static str);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Forward {
    pub shard: u16,
    pub first_idx: u64,
    pub msgs: Vec<OutMsg>,
}

pub struct Rd<'a>(&'a [u8]);

impl<'a> Rd<'a> {
    pub fn new(b: &'a [u8]) -> Rd<'a> {
        Rd(b)
    }
    pub fn left(&self) -> usize {
        self.0.len()
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        if self.0.len() < n {
            return Err(WireError("truncated"));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    pub fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, WireError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn done(&self) -> Result<(), WireError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(WireError("trailing bytes"))
        }
    }
}

pub fn put_side(out: &mut Vec<u8>, s: Side) {
    out.push(if s == Side::Buy { 0 } else { 1 });
}

pub fn get_side(r: &mut Rd) -> Result<Side, WireError> {
    match r.u8()? {
        0 => Ok(Side::Buy),
        1 => Ok(Side::Sell),
        _ => Err(WireError("bad side")),
    }
}

fn put_msg(out: &mut Vec<u8>, m: &OutMsg) {
    match *m {
        OutMsg::Submit {
            order,
            symbol,
            side,
            price,
            qty,
        } => {
            out.push(1);
            out.extend_from_slice(&order.to_le_bytes());
            out.extend_from_slice(&symbol.to_le_bytes());
            put_side(out, side);
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
        }
        OutMsg::Cancel { order, symbol } => {
            out.push(2);
            out.extend_from_slice(&order.to_le_bytes());
            out.extend_from_slice(&symbol.to_le_bytes());
            out.push(0);
            out.extend_from_slice(&0u64.to_le_bytes());
            out.extend_from_slice(&0u64.to_le_bytes());
        }
    }
}

fn get_msg(r: &mut Rd) -> Result<OutMsg, WireError> {
    let kind = r.u8()?;
    let (order, symbol) = (r.u64()?, r.u32()?);
    let side = get_side(r)?;
    let (price, qty) = (r.u64()?, r.u64()?);
    match kind {
        1 => Ok(OutMsg::Submit {
            order,
            symbol,
            side,
            price,
            qty,
        }),
        2 if side == Side::Buy && price == 0 && qty == 0 => Ok(OutMsg::Cancel { order, symbol }),
        2 => Err(WireError("non-canonical cancel")),
        _ => Err(WireError("bad message kind")),
    }
}

pub fn encode_forward(f: &Forward) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + f.msgs.len() * MSG_BYTES);
    out.push(TAG_FORWARD);
    out.push(1);
    out.extend_from_slice(&f.shard.to_le_bytes());
    out.extend_from_slice(&f.first_idx.to_le_bytes());
    out.extend_from_slice(&(f.msgs.len() as u32).to_le_bytes());
    for m in &f.msgs {
        put_msg(&mut out, m);
    }
    out
}

pub fn decode_forward(b: &[u8]) -> Result<Forward, WireError> {
    let mut r = Rd::new(b);
    if r.u8()? != TAG_FORWARD {
        return Err(WireError("not a forwarded batch"));
    }
    if r.u8()? != 1 {
        return Err(WireError("unknown version"));
    }
    let shard = r.u16()?;
    let first_idx = r.u64()?;
    let n = r.u32()? as usize;
    if n > MAX_BATCH || n != r.left() / MSG_BYTES || r.left() % MSG_BYTES != 0 {
        return Err(WireError("batch length"));
    }
    let msgs = (0..n).map(|_| get_msg(&mut r)).collect::<Result<_, _>>()?;
    r.done()?;
    Ok(Forward {
        shard,
        first_idx,
        msgs,
    })
}

/// A settlement event: `1 trade (buy u64, sell u64, price u64, qty u64, taker_is_buy u8)` or
/// `2 closed (order u64)`.
pub fn put_event(out: &mut Vec<u8>, e: &Event) {
    match *e {
        Event::Trade {
            buy,
            sell,
            price,
            qty,
            taker_is_buy,
        } => {
            out.push(1);
            for v in [buy, sell, price, qty] {
                out.extend_from_slice(&v.to_le_bytes());
            }
            out.push(taker_is_buy as u8);
        }
        Event::Closed { order } => {
            out.push(2);
            out.extend_from_slice(&order.to_le_bytes());
        }
    }
}

pub fn get_event(r: &mut Rd) -> Result<Event, WireError> {
    match r.u8()? {
        1 => {
            let (buy, sell, price, qty) = (r.u64()?, r.u64()?, r.u64()?, r.u64()?);
            let taker_is_buy = match r.u8()? {
                0 => false,
                1 => true,
                _ => return Err(WireError("bad taker flag")),
            };
            Ok(Event::Trade {
                buy,
                sell,
                price,
                qty,
                taker_is_buy,
            })
        }
        2 => Ok(Event::Closed { order: r.u64()? }),
        _ => Err(WireError("bad event kind")),
    }
}
