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
//! message: kind u8 (1 submit, 5 submit IOC, 2 cancel) | order u64 | symbol u32 |
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
        OutMsg::SubmitIoc {
            order,
            symbol,
            side,
            price,
            qty,
        } => {
            out.push(5);
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
        // Phase 2 (CREDIT-SLICES.md): same 30 bytes, the account in the order field,
        // the asset in the symbol field and the 128-bit amount split over the
        // price (low half) and qty (high half) fields.
        OutMsg::Grant {
            account,
            asset,
            amount,
        }
        | OutMsg::Recall {
            account,
            asset,
            amount,
        } => {
            out.push(if matches!(m, OutMsg::Grant { .. }) {
                3
            } else {
                4
            });
            out.extend_from_slice(&account.to_le_bytes());
            out.extend_from_slice(&asset.to_le_bytes());
            out.push(0);
            out.extend_from_slice(&(amount as u64).to_le_bytes());
            out.extend_from_slice(&((amount >> 64) as u64).to_le_bytes());
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
        5 => Ok(OutMsg::SubmitIoc {
            order,
            symbol,
            side,
            price,
            qty,
        }),
        2 if side == Side::Buy && price == 0 && qty == 0 => Ok(OutMsg::Cancel { order, symbol }),
        2 => Err(WireError("non-canonical cancel")),
        3 | 4 if side == Side::Buy => {
            let (account, asset) = (order, symbol);
            let amount = price as u128 | (qty as u128) << 64;
            Ok(if kind == 3 {
                OutMsg::Grant {
                    account,
                    asset,
                    amount,
                }
            } else {
                OutMsg::Recall {
                    account,
                    asset,
                    amount,
                }
            })
        }
        3 | 4 => Err(WireError("non-canonical credit message")),
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

/// A settlement event: `1 trade (buy u64, sell u64, price u64, qty u64, taker_is_buy u8)`,
/// `2 closed (order u64)`, or the phase 2 kinds `3 slice-in`, `4 slice-out`, `6 returned`
/// (`account u64, asset u32, amount u128`) and `5 slice-fee (asset u32, amount u128)`.
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
        Event::SliceIn {
            account,
            asset,
            amount,
        } => put_account_event(out, 3, account, asset, amount),
        Event::SliceOut {
            account,
            asset,
            amount,
        } => put_account_event(out, 4, account, asset, amount),
        Event::SliceFee { asset, amount } => {
            out.push(5);
            out.extend_from_slice(&asset.to_le_bytes());
            out.extend_from_slice(&amount.to_le_bytes());
        }
        Event::Returned {
            account,
            asset,
            amount,
        } => put_account_event(out, 6, account, asset, amount),
    }
}

fn put_account_event(out: &mut Vec<u8>, kind: u8, account: u64, asset: u32, amount: u128) {
    out.push(kind);
    out.extend_from_slice(&account.to_le_bytes());
    out.extend_from_slice(&asset.to_le_bytes());
    out.extend_from_slice(&amount.to_le_bytes());
}

fn get_u128(r: &mut Rd) -> Result<u128, WireError> {
    Ok(u128::from_le_bytes(r.take(16)?.try_into().unwrap()))
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
        3 => Ok(Event::SliceIn {
            account: r.u64()?,
            asset: r.u32()?,
            amount: get_u128(r)?,
        }),
        4 => Ok(Event::SliceOut {
            account: r.u64()?,
            asset: r.u32()?,
            amount: get_u128(r)?,
        }),
        5 => Ok(Event::SliceFee {
            asset: r.u32()?,
            amount: get_u128(r)?,
        }),
        6 => Ok(Event::Returned {
            account: r.u64()?,
            asset: r.u32()?,
            amount: get_u128(r)?,
        }),
        _ => Err(WireError("bad event kind")),
    }
}

/// **Entry tag 6, credit-slice request** (a gateway talking to a matching group in
/// credit mode, `CREDIT-SLICES.md`; one request per entry):
/// ```text
///   0   u8   6
///   1   u8   version (1)
///   2   u64  client id        (a gateway's session)
///   10  u64  sequence         (stop-and-wait; a retry repeats it)
///   18  u8   kind: 1 enter, 2 cancel
///   19  u64  account
///   enter:  u32 symbol | u8 side (0 buy, 1 sell) | u64 price | u64 qty
///   cancel: u64 order
/// ```
/// An entered order is a GTC limit order; the group assigns its id and answers
/// through `TierView::slice_result`.
pub const TAG_SLICE: u8 = 6;

/// **Entry tag 7, epoch tick**: `7 | version (1)`. A no-op for the pipeline. In
/// credit mode it counts as an applied entry and *closes the credit epoch now*:
/// the matcher's new events are settled into the slice and reported, as at every
/// `epoch`-th entry. A quiet group has no entries to count, so its node proposes
/// one of these periodically; being in the log, it closes the epoch at the same
/// point on every replica. Outside credit mode it does nothing.
pub const TAG_EPOCH: u8 = 7;

pub fn encode_epoch_tick() -> Vec<u8> {
    vec![TAG_EPOCH, 1]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SliceBody {
    Enter {
        account: u64,
        symbol: u32,
        side: Side,
        price: u64,
        qty: u64,
    },
    /// Like `Enter`, immediate-or-cancel: what does not trade at once is closed.
    EnterIoc {
        account: u64,
        symbol: u32,
        side: Side,
        price: u64,
        qty: u64,
    },
    Cancel {
        account: u64,
        order: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SliceReq {
    pub client: u64,
    pub seq: u64,
    pub body: SliceBody,
}

pub fn encode_slice_req(q: &SliceReq) -> Vec<u8> {
    let mut out = vec![TAG_SLICE, 1];
    out.extend_from_slice(&q.client.to_le_bytes());
    out.extend_from_slice(&q.seq.to_le_bytes());
    match q.body {
        SliceBody::Enter {
            account,
            symbol,
            side,
            price,
            qty,
        } => {
            out.push(1);
            out.extend_from_slice(&account.to_le_bytes());
            out.extend_from_slice(&symbol.to_le_bytes());
            put_side(&mut out, side);
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
        }
        SliceBody::EnterIoc {
            account,
            symbol,
            side,
            price,
            qty,
        } => {
            out.push(3);
            out.extend_from_slice(&account.to_le_bytes());
            out.extend_from_slice(&symbol.to_le_bytes());
            put_side(&mut out, side);
            out.extend_from_slice(&price.to_le_bytes());
            out.extend_from_slice(&qty.to_le_bytes());
        }
        SliceBody::Cancel { account, order } => {
            out.push(2);
            out.extend_from_slice(&account.to_le_bytes());
            out.extend_from_slice(&order.to_le_bytes());
        }
    }
    out
}

pub fn decode_slice_req(b: &[u8]) -> Result<SliceReq, WireError> {
    let mut r = Rd::new(b);
    if r.u8()? != TAG_SLICE {
        return Err(WireError("not a slice request"));
    }
    if r.u8()? != 1 {
        return Err(WireError("unknown version"));
    }
    let (client, seq) = (r.u64()?, r.u64()?);
    let body = match r.u8()? {
        1 => SliceBody::Enter {
            account: r.u64()?,
            symbol: r.u32()?,
            side: get_side(&mut r)?,
            price: r.u64()?,
            qty: r.u64()?,
        },
        3 => SliceBody::EnterIoc {
            account: r.u64()?,
            symbol: r.u32()?,
            side: get_side(&mut r)?,
            price: r.u64()?,
            qty: r.u64()?,
        },
        2 => SliceBody::Cancel {
            account: r.u64()?,
            order: r.u64()?,
        },
        _ => return Err(WireError("bad request kind")),
    };
    r.done()?;
    Ok(SliceReq { client, seq, body })
}
