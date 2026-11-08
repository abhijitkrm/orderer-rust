//! Wire format of ledger commands and the snapshot format. Little-endian,
//! fixed layouts, no padding; amounts are 16 bytes.
//!
//! Command: `tag u8` then, by tag
//!   1 Deposit   account u64 | asset u32 | amount u128 | external_id u64
//!   2 Reserve   account u64 | symbol u32 | side u8 (0 buy, 1 sell) | price u64 | qty u64
//!   3 Cancel    account u64 | order u64
//!   4 Settle    source u32 | prev u64 | idx u64 | kind u8 (1 trade, 2 closed) | trade: buy u64 sell u64 price u64 qty u64; closed: order u64
//!   5 Withdraw  account u64 | asset u32 | amount u128 | external_id u64
//!   6 Finalize  external_id u64 | paid u8
//!   7 OutboxAck upto u64
//!   8 Clock     now_ns u64
//!   9 Sweep     max_age_ns u64 | retry_ns u64 | max u32
//!  10 Adjust    account u64 | asset u32 | credit u8 | amount u128 | reason u32 | external_id u64
//!  11 FeeWithdraw asset u32 | amount u128 | external_id u64
//!  12 Lend      group u16 | account u64 | asset u32 | amount u128   (phase 2)
//!  13 Recall    group u16 | account u64 | asset u32 | amount u128   (phase 2)
//!  14 ReserveIoc same layout as Reserve (immediate-or-cancel; Reserve, tag 2, is GTC)
//!  20 At        ts_ns u64 | one command (not an At): the leader's time sidecar
//! A trade event (kind 1) ends with `taker_is_buy u8` (changed in step 4; the
//! other layouts are unchanged). Phase 2 adds settle event kinds
//! 3 SliceIn, 4 SliceOut, 6 Returned (`group u16 | account u64 | asset u32 | amount u128`)
//! and 5 SliceFee (`asset u32 | amount u128`).

use crate::ledger::*;
use orderer_cluster::sha256::sha256;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Debug, PartialEq, Eq)]
pub struct DecodeError(pub &'static str);

struct W(Vec<u8>);
impl W {
    fn u8(&mut self, v: u8) {
        self.0.push(v)
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes())
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes())
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes())
    }
    fn u128(&mut self, v: u128) {
        self.0.extend_from_slice(&v.to_le_bytes())
    }
    fn side(&mut self, s: Side) {
        self.u8(if s == Side::Buy { 0 } else { 1 })
    }
}

struct R<'a>(&'a [u8]);
impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.0.len() < n {
            return Err(DecodeError("truncated"));
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, DecodeError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn side(&mut self) -> Result<Side, DecodeError> {
        match self.u8()? {
            0 => Ok(Side::Buy),
            1 => Ok(Side::Sell),
            _ => Err(DecodeError("bad side")),
        }
    }
    fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(DecodeError("bad bool")),
        }
    }
    fn done(&self) -> Result<(), DecodeError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(DecodeError("trailing bytes"))
        }
    }
}

pub fn encode_cmd(c: &Cmd) -> Vec<u8> {
    let mut w = W(Vec::with_capacity(48));
    match *c {
        Cmd::Deposit {
            account,
            asset,
            amount,
            external_id,
        } => {
            w.u8(1);
            w.u64(account);
            w.u32(asset);
            w.u128(amount);
            w.u64(external_id);
        }
        Cmd::Reserve {
            account,
            symbol,
            side,
            price,
            qty,
        } => {
            w.u8(2);
            w.u64(account);
            w.u32(symbol);
            w.side(side);
            w.u64(price);
            w.u64(qty);
        }
        Cmd::ReserveIoc {
            account,
            symbol,
            side,
            price,
            qty,
        } => {
            w.u8(14);
            w.u64(account);
            w.u32(symbol);
            w.side(side);
            w.u64(price);
            w.u64(qty);
        }
        Cmd::At { ts_ns, ref cmd } => {
            w.u8(20);
            w.u64(ts_ns);
            w.0.extend_from_slice(&encode_cmd(cmd));
        }
        Cmd::Cancel { account, order } => {
            w.u8(3);
            w.u64(account);
            w.u64(order);
        }
        Cmd::Settle {
            source,
            prev,
            idx,
            event,
        } => {
            w.u8(4);
            w.u32(source);
            w.u64(prev);
            w.u64(idx);
            match event {
                Event::Trade {
                    buy,
                    sell,
                    price,
                    qty,
                    taker_is_buy,
                } => {
                    w.u8(1);
                    w.u64(buy);
                    w.u64(sell);
                    w.u64(price);
                    w.u64(qty);
                    w.u8(taker_is_buy as u8);
                }
                Event::Closed { order } => {
                    w.u8(2);
                    w.u64(order);
                }
                Event::SliceIn {
                    group,
                    account,
                    asset,
                    amount,
                } => {
                    w.u8(3);
                    w.u16(group);
                    w.u64(account);
                    w.u32(asset);
                    w.u128(amount);
                }
                Event::SliceOut {
                    group,
                    account,
                    asset,
                    amount,
                } => {
                    w.u8(4);
                    w.u16(group);
                    w.u64(account);
                    w.u32(asset);
                    w.u128(amount);
                }
                Event::SliceFee { asset, amount } => {
                    w.u8(5);
                    w.u32(asset);
                    w.u128(amount);
                }
                Event::Returned {
                    group,
                    account,
                    asset,
                    amount,
                } => {
                    w.u8(6);
                    w.u16(group);
                    w.u64(account);
                    w.u32(asset);
                    w.u128(amount);
                }
            }
        }
        Cmd::Withdraw {
            account,
            asset,
            amount,
            external_id,
        } => {
            w.u8(5);
            w.u64(account);
            w.u32(asset);
            w.u128(amount);
            w.u64(external_id);
        }
        Cmd::Finalize { external_id, paid } => {
            w.u8(6);
            w.u64(external_id);
            w.u8(paid as u8);
        }
        Cmd::OutboxAck { upto } => {
            w.u8(7);
            w.u64(upto);
        }
        Cmd::Clock { now_ns } => {
            w.u8(8);
            w.u64(now_ns);
        }
        Cmd::Sweep {
            max_age_ns,
            retry_ns,
            max,
        } => {
            w.u8(9);
            w.u64(max_age_ns);
            w.u64(retry_ns);
            w.u32(max);
        }
        Cmd::Adjust {
            account,
            asset,
            amount,
            credit,
            reason,
            external_id,
        } => {
            w.u8(10);
            w.u64(account);
            w.u32(asset);
            w.u8(credit as u8);
            w.u128(amount);
            w.u32(reason);
            w.u64(external_id);
        }
        Cmd::FeeWithdraw {
            asset,
            amount,
            external_id,
        } => {
            w.u8(11);
            w.u32(asset);
            w.u128(amount);
            w.u64(external_id);
        }
        Cmd::Lend {
            group,
            account,
            asset,
            amount,
        } => {
            w.u8(12);
            w.u16(group);
            w.u64(account);
            w.u32(asset);
            w.u128(amount);
        }
        Cmd::Recall {
            group,
            account,
            asset,
            amount,
        } => {
            w.u8(13);
            w.u16(group);
            w.u64(account);
            w.u32(asset);
            w.u128(amount);
        }
    }
    w.0
}

pub fn decode_cmd(b: &[u8]) -> Result<Cmd, DecodeError> {
    let mut r = R(b);
    let c = match r.u8()? {
        1 => Cmd::Deposit {
            account: r.u64()?,
            asset: r.u32()?,
            amount: r.u128()?,
            external_id: r.u64()?,
        },
        2 => Cmd::Reserve {
            account: r.u64()?,
            symbol: r.u32()?,
            side: r.side()?,
            price: r.u64()?,
            qty: r.u64()?,
        },
        14 => Cmd::ReserveIoc {
            account: r.u64()?,
            symbol: r.u32()?,
            side: r.side()?,
            price: r.u64()?,
            qty: r.u64()?,
        },
        20 => {
            let ts_ns = r.u64()?;
            let rest = r.0;
            if rest.first() == Some(&20) {
                return Err(DecodeError("nested time stamp"));
            }
            r.0 = &[];
            Cmd::At {
                ts_ns,
                cmd: Box::new(decode_cmd(rest)?),
            }
        }
        3 => Cmd::Cancel {
            account: r.u64()?,
            order: r.u64()?,
        },
        4 => {
            let source = r.u32()?;
            let prev = r.u64()?;
            let idx = r.u64()?;
            let event = match r.u8()? {
                1 => Event::Trade {
                    buy: r.u64()?,
                    sell: r.u64()?,
                    price: r.u64()?,
                    qty: r.u64()?,
                    taker_is_buy: r.bool()?,
                },
                2 => Event::Closed { order: r.u64()? },
                3 => Event::SliceIn {
                    group: r.u16()?,
                    account: r.u64()?,
                    asset: r.u32()?,
                    amount: r.u128()?,
                },
                4 => Event::SliceOut {
                    group: r.u16()?,
                    account: r.u64()?,
                    asset: r.u32()?,
                    amount: r.u128()?,
                },
                5 => Event::SliceFee {
                    asset: r.u32()?,
                    amount: r.u128()?,
                },
                6 => Event::Returned {
                    group: r.u16()?,
                    account: r.u64()?,
                    asset: r.u32()?,
                    amount: r.u128()?,
                },
                _ => return Err(DecodeError("bad event kind")),
            };
            Cmd::Settle {
                source,
                prev,
                idx,
                event,
            }
        }
        5 => Cmd::Withdraw {
            account: r.u64()?,
            asset: r.u32()?,
            amount: r.u128()?,
            external_id: r.u64()?,
        },
        6 => Cmd::Finalize {
            external_id: r.u64()?,
            paid: r.bool()?,
        },
        7 => Cmd::OutboxAck { upto: r.u64()? },
        8 => Cmd::Clock { now_ns: r.u64()? },
        9 => Cmd::Sweep {
            max_age_ns: r.u64()?,
            retry_ns: r.u64()?,
            max: r.u32()?,
        },
        10 => Cmd::Adjust {
            account: r.u64()?,
            asset: r.u32()?,
            credit: r.bool()?,
            amount: r.u128()?,
            reason: r.u32()?,
            external_id: r.u64()?,
        },
        11 => Cmd::FeeWithdraw {
            asset: r.u32()?,
            amount: r.u128()?,
            external_id: r.u64()?,
        },
        12 => Cmd::Lend {
            group: r.u16()?,
            account: r.u64()?,
            asset: r.u32()?,
            amount: r.u128()?,
        },
        13 => Cmd::Recall {
            group: r.u16()?,
            account: r.u64()?,
            asset: r.u32()?,
            amount: r.u128()?,
        },
        _ => return Err(DecodeError("unknown command")),
    };
    r.done()?;
    Ok(c)
}

/// Written when the ledger has no phase 2 state: byte-identical to what it was
/// before phase 2 existed, so old snapshots and digests stay valid.
const MAGIC: &[u8; 4] = b"LDG2";
/// Written once phase 2 state exists: `LDG2` plus a trailing slice section
/// (`lent` entries, then the four flow maps).
const MAGIC3: &[u8; 4] = b"LDG3";
/// `LDG3` with credit groups: the `lent` entries carry a group, and a per-group flow
/// section follows the four totals. `LDG3` is still read (everything is group 0).
const MAGIC4: &[u8; 4] = b"LDG4";

fn put_map(w: &mut W, m: &BTreeMap<AssetId, Amount>) {
    w.u32(m.len() as u32);
    for (k, v) in m {
        w.u32(*k);
        w.u128(*v);
    }
}

fn get_map(r: &mut R) -> Result<BTreeMap<AssetId, Amount>, DecodeError> {
    let mut m = BTreeMap::new();
    for _ in 0..r.u32()? {
        let (k, v) = (r.u32()?, r.u128()?);
        if m.insert(k, v).is_some() {
            return Err(DecodeError("duplicate key"));
        }
    }
    Ok(m)
}

impl Ledger {
    /// The whole state, canonically ordered, so equal states give equal bytes.
    pub fn snapshot(&self) -> Vec<u8> {
        let mut w = W(Vec::new());
        let v3 = self.has_slice_state();
        w.0.extend_from_slice(if v3 { MAGIC4 } else { MAGIC });
        w.u16(self.cfg.shard);
        w.u64(self.next_order);
        w.u64(self.outbox_base);
        w.u64(self.anomalies);
        w.u64(self.clock);
        w.u32(self.balances.len() as u32);
        for ((a, asset), b) in self.balances.iter() {
            w.u64(*a);
            w.u32(*asset);
            w.u128(b.available);
            w.u128(b.held);
        }
        w.u32(self.orders.len() as u32);
        for (id, o) in self.orders.iter() {
            w.u64(*id);
            w.u64(o.account);
            w.u32(o.symbol);
            w.side(o.side);
            w.u64(o.price);
            w.u64(o.remaining);
            w.u128(o.held);
            w.u8(o.cancelling as u8);
            w.u64(o.born);
            w.u64(o.cancel_at);
        }
        w.u32(self.outbox.len() as u32);
        for m in &self.outbox {
            match *m {
                OutMsg::Submit {
                    order,
                    symbol,
                    side,
                    price,
                    qty,
                } => {
                    w.u8(1);
                    w.u64(order);
                    w.u32(symbol);
                    w.side(side);
                    w.u64(price);
                    w.u64(qty);
                }
                OutMsg::SubmitIoc {
                    order,
                    symbol,
                    side,
                    price,
                    qty,
                } => {
                    w.u8(5);
                    w.u64(order);
                    w.u32(symbol);
                    w.side(side);
                    w.u64(price);
                    w.u64(qty);
                }
                OutMsg::Cancel { order, symbol } => {
                    w.u8(2);
                    w.u64(order);
                    w.u32(symbol);
                }
                OutMsg::Grant {
                    group,
                    account,
                    asset,
                    amount,
                } => {
                    w.u8(3);
                    w.u16(group);
                    w.u64(account);
                    w.u32(asset);
                    w.u128(amount);
                }
                OutMsg::Recall {
                    group,
                    account,
                    asset,
                    amount,
                } => {
                    w.u8(4);
                    w.u16(group);
                    w.u64(account);
                    w.u32(asset);
                    w.u128(amount);
                }
            }
        }
        w.u32(self.hw.len() as u32);
        for (s, i) in &self.hw {
            w.u32(*s);
            w.u64(*i);
        }
        for set in [&self.deposit_ids, &self.withdraw_ids] {
            w.u32(set.len() as u32);
            for id in set {
                w.u64(*id);
            }
        }
        w.u32(self.pending.len() as u32);
        for (id, (a, asset, amt)) in self.pending.iter() {
            w.u64(*id);
            w.u64(*a);
            w.u32(*asset);
            w.u128(*amt);
        }
        for m in [
            &self.fees,
            &self.deposited,
            &self.withdrawn,
            &self.settled_in,
            &self.settled_out,
            &self.adjusted_in,
            &self.adjusted_out,
        ] {
            put_map(&mut w, m);
        }
        w.u32(self.adjust_ids.len() as u32);
        for id in &self.adjust_ids {
            w.u64(*id);
        }
        w.u32(self.adjust_log.len() as u32);
        for a in &self.adjust_log {
            w.u64(a.external_id);
            w.u64(a.account);
            w.u32(a.asset);
            w.u8(a.credit as u8);
            w.u128(a.amount);
            w.u32(a.reason);
        }
        if v3 {
            w.u32(self.lent_g.len() as u32);
            for ((a, asset, group), v) in self.lent_g.iter() {
                w.u64(*a);
                w.u32(*asset);
                w.u16(*group);
                w.u128(*v);
            }
            for m in [
                &self.lent_granted,
                &self.lent_returned,
                &self.slice_in,
                &self.slice_out,
            ] {
                put_map(&mut w, m);
            }
            w.u32(self.group_flow.len() as u32);
            for ((asset, group), f) in &self.group_flow {
                w.u32(*asset);
                w.u16(*group);
                for v in [f.granted, f.returned, f.slice_in, f.slice_out] {
                    w.u128(v);
                }
            }
        }
        w.0
    }

    /// SHA-256 of the whole snapshot. O(state); the cheap, incremental divergence
    /// digest is [`Ledger::digest`].
    pub fn deep_digest(&self) -> [u8; 32] {
        sha256(&self.snapshot())
    }

    /// Replace the state from a snapshot. The configuration is kept; the
    /// snapshot must be for the same shard and must satisfy [`Ledger::check`]
    /// (its anomaly counter aside).
    pub fn restore(&mut self, data: &[u8]) -> Result<(), String> {
        let l = Self::parse(self.cfg.clone(), data).map_err(|e| e.0.to_string())?;
        let anomalies = l.anomalies;
        let mut probe = l;
        probe.anomalies = 0;
        probe.check()?;
        probe.anomalies = anomalies;
        *self = probe;
        Ok(())
    }

    pub(crate) fn parse(cfg: Config, data: &[u8]) -> Result<Ledger, DecodeError> {
        let mut r = R(data);
        let (v3, v4) = match r.take(4)? {
            m if m == MAGIC => (false, false),
            m if m == MAGIC3 => (true, false),
            m if m == MAGIC4 => (true, true),
            _ => return Err(DecodeError("bad magic")),
        };
        if r.u16()? != cfg.shard {
            return Err(DecodeError("snapshot is for another shard"));
        }
        let mut l = Ledger::new_unchecked(cfg);
        l.next_order = r.u64()?;
        l.outbox_base = r.u64()?;
        l.anomalies = r.u64()?;
        l.clock = r.u64()?;
        for _ in 0..r.u32()? {
            let k = (r.u64()?, r.u32()?);
            let b = Balance {
                available: r.u128()?,
                held: r.u128()?,
            };
            if l.balances.insert(k, b).is_some() {
                return Err(DecodeError("duplicate balance"));
            }
        }
        for _ in 0..r.u32()? {
            let id = r.u64()?;
            let o = Order {
                account: r.u64()?,
                symbol: r.u32()?,
                side: r.side()?,
                price: r.u64()?,
                remaining: r.u64()?,
                held: r.u128()?,
                cancelling: r.bool()?,
                born: r.u64()?,
                cancel_at: r.u64()?,
            };
            if l.orders.insert(id, o).is_some() {
                return Err(DecodeError("duplicate order"));
            }
        }
        let mut outbox = VecDeque::new();
        for _ in 0..r.u32()? {
            outbox.push_back(match r.u8()? {
                1 => OutMsg::Submit {
                    order: r.u64()?,
                    symbol: r.u32()?,
                    side: r.side()?,
                    price: r.u64()?,
                    qty: r.u64()?,
                },
                2 => OutMsg::Cancel {
                    order: r.u64()?,
                    symbol: r.u32()?,
                },
                5 => OutMsg::SubmitIoc {
                    order: r.u64()?,
                    symbol: r.u32()?,
                    side: r.side()?,
                    price: r.u64()?,
                    qty: r.u64()?,
                },
                3 => OutMsg::Grant {
                    group: r.u16()?,
                    account: r.u64()?,
                    asset: r.u32()?,
                    amount: r.u128()?,
                },
                4 => OutMsg::Recall {
                    group: r.u16()?,
                    account: r.u64()?,
                    asset: r.u32()?,
                    amount: r.u128()?,
                },
                _ => return Err(DecodeError("bad outbox kind")),
            });
        }
        l.outbox = outbox;
        let mut acc = crate::hashed::ZERO;
        for (i, m) in l.outbox.iter().enumerate() {
            acc =
                crate::hashed::lane_add(acc, crate::ledger::out_cell(l.outbox_base + i as u64, m));
        }
        l.outbox_acc = acc;
        for _ in 0..r.u32()? {
            let (s, i) = (r.u32()?, r.u64()?);
            if l.hw.insert(s, i).is_some() {
                return Err(DecodeError("duplicate mark"));
            }
        }
        for which in 0..2 {
            let mut set = BTreeSet::new();
            for _ in 0..r.u32()? {
                if !set.insert(r.u64()?) {
                    return Err(DecodeError("duplicate id"));
                }
            }
            for id in &set {
                l.note_id(which, *id);
            }
            if which == 0 {
                l.deposit_ids = set
            } else {
                l.withdraw_ids = set
            }
        }
        for _ in 0..r.u32()? {
            let id = r.u64()?;
            let v = (r.u64()?, r.u32()?, r.u128()?);
            if l.pending.insert(id, v).is_some() {
                return Err(DecodeError("duplicate pending"));
            }
        }
        l.fees = get_map(&mut r)?;
        l.deposited = get_map(&mut r)?;
        l.withdrawn = get_map(&mut r)?;
        l.settled_in = get_map(&mut r)?;
        l.settled_out = get_map(&mut r)?;
        l.adjusted_in = get_map(&mut r)?;
        l.adjusted_out = get_map(&mut r)?;
        for _ in 0..r.u32()? {
            let id = r.u64()?;
            if !l.adjust_ids.insert(id) {
                return Err(DecodeError("duplicate adjust id"));
            }
            l.note_id(2, id);
        }
        for _ in 0..r.u32()? {
            l.adjust_log.push_back(Adjustment {
                external_id: r.u64()?,
                account: r.u64()?,
                asset: r.u32()?,
                credit: r.bool()?,
                amount: r.u128()?,
                reason: r.u32()?,
            });
        }
        if v3 {
            for _ in 0..r.u32()? {
                let (a, asset) = (r.u64()?, r.u32()?);
                let group = if v4 { r.u16()? } else { 0 };
                let v = r.u128()?;
                let prev = l.lent.get(&(a, asset)).copied().unwrap_or(0);
                if v == 0
                    || l.lent_g.insert((a, asset, group), v).is_some()
                    || prev.checked_add(v).is_none()
                {
                    return Err(DecodeError("bad lent entry"));
                }
                l.lent.insert((a, asset), prev + v);
            }
            l.lent_granted = get_map(&mut r)?;
            l.lent_returned = get_map(&mut r)?;
            l.slice_in = get_map(&mut r)?;
            l.slice_out = get_map(&mut r)?;
            if v4 {
                for _ in 0..r.u32()? {
                    let (asset, group) = (r.u32()?, r.u16()?);
                    let f = GroupFlow {
                        granted: r.u128()?,
                        returned: r.u128()?,
                        slice_in: r.u128()?,
                        slice_out: r.u128()?,
                    };
                    if l.group_flow.insert((asset, group), f).is_some() {
                        return Err(DecodeError("bad group flow"));
                    }
                }
            } else {
                // a single-group snapshot: everything belongs to group 0
                let assets: BTreeSet<AssetId> = l
                    .lent_granted
                    .keys()
                    .chain(l.lent_returned.keys())
                    .chain(l.slice_in.keys())
                    .chain(l.slice_out.keys())
                    .copied()
                    .collect();
                for a in assets {
                    let g = |m: &BTreeMap<AssetId, Amount>| m.get(&a).copied().unwrap_or(0);
                    l.group_flow.insert(
                        (a, 0),
                        GroupFlow {
                            granted: g(&l.lent_granted),
                            returned: g(&l.lent_returned),
                            slice_in: g(&l.slice_in),
                            slice_out: g(&l.slice_out),
                        },
                    );
                }
            }
            if !l.has_slice_state() {
                return Err(DecodeError("empty slice section"));
            }
        }
        r.done()?;
        l.rebuild_indexes();
        Ok(l)
    }
}
