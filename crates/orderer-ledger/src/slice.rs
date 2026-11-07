//! The group side of phase 2 (`docs/design/CREDIT-SLICES.md`): a matching group's
//! credit slices as a pure, deterministic engine.
//!
//! A [`Slice`] holds, per account and asset, the money the ledger lent the group.
//! It is a small ledger of its own (it embeds [`Ledger`], so holds, fills and fees
//! follow exactly the phase 1 rules) with three differences:
//! - money enters only through [`Slice::grant`] and leaves only through
//!   [`Slice::recall`], both applied exactly once by the caller (intake cursor);
//! - orders are entered directly by the group ([`Slice::enter`]): the pre-trade
//!   check is a lookup in its own table, no ledger round trip. Order ids carry
//!   [`SLICE_SHARD_BIT`], so no ledger shard mistakes them for its own;
//! - fills are applied from the group's archived events in batches
//!   ([`Slice::settle`]), at points the caller makes deterministic (credit epochs).
//!
//! What the slice has done is reported to the ledger as events ([`Slice::report`]):
//! `SliceIn` and `SliceOut` per `(account, asset)` for the net change of the
//! account's money in the slice, `SliceFee` for fees charged, and `Returned` for
//! recalls. The ledger applies them through the same chained settle stream as
//! phase 1, so each is applied exactly once.
//!
//! Bookkeeping that makes the reports exact without negative numbers, per key:
//! `p = granted + reported_in` and `q = returned + reported_out`; the engine's
//! total `T` satisfies `T + q = p` right after a report.

use crate::ledger::{
    is_slice_order, AccountId, Amount, AssetId, Cmd, Config, Event, Ledger, OrderId, Outcome,
    Reject, Side, SymbolId, SLICE_SHARD_BIT,
};
use orderer_cluster::sha256::sha256;
use std::collections::{BTreeMap, BTreeSet};

type Key = (AccountId, AssetId);

pub struct Slice {
    engine: Ledger,
    p: BTreeMap<Key, Amount>,
    q: BTreeMap<Key, Amount>,
    fee_reported: BTreeMap<AssetId, Amount>,
    /// Keys whose total may have changed since the last report.
    dirty: BTreeSet<Key>,
    /// Index of the next engine settle (the engine's own chain; it reads its
    /// events directly, in order).
    next_idx: u64,
}

impl Slice {
    /// `cfg` is the same reference data (symbols and fees) the ledgers use;
    /// `group` numbers the matching group (below [`SLICE_SHARD_BIT`]).
    pub fn new(mut cfg: Config, group: u16) -> Slice {
        assert!(group < SLICE_SHARD_BIT, "group number out of range");
        cfg.shard = SLICE_SHARD_BIT | group;
        if let Err(e) = cfg.validate_reference() {
            panic!("invalid slice config: {e}");
        }
        Slice {
            engine: Ledger::new_unchecked(cfg),
            p: BTreeMap::new(),
            q: BTreeMap::new(),
            fee_reported: BTreeMap::new(),
            dirty: BTreeSet::new(),
            next_idx: 1,
        }
    }

    /// The embedded ledger, for inspection and invariant checks.
    pub fn engine(&self) -> &Ledger {
        &self.engine
    }

    /// Everything the slice holds for `account` in `asset` (free and committed).
    pub fn total(&self, account: AccountId, asset: AssetId) -> Amount {
        let b = self.engine.balance(account, asset);
        b.available + b.held
    }

    /// Credit still free to commit.
    pub fn available(&self, account: AccountId, asset: AssetId) -> Amount {
        self.engine.balance(account, asset).available
    }

    /// Money in accounts' slices of `asset`, excluding the fee pool.
    pub fn accounts_total(&self, asset: AssetId) -> Amount {
        self.engine.holdings(asset) - self.engine.fees(asset)
    }

    /// Fees charged and not yet handed to the ledger's pool, plus those reported.
    pub fn fee_pool(&self, asset: AssetId) -> Amount {
        self.engine.fees(asset)
    }

    pub fn anomalies(&self) -> u64 {
        self.engine.anomalies()
    }

    pub fn open_orders(&self) -> usize {
        self.engine.open_orders()
    }

    /// Every open order: `(id, account, symbol)`.
    pub fn orders(&self) -> Vec<(OrderId, AccountId, SymbolId)> {
        self.engine
            .orders
            .iter()
            .map(|(id, o)| (*id, o.account, o.symbol))
            .collect()
    }

    pub fn order_symbol(&self, order: OrderId) -> Option<SymbolId> {
        self.engine.order(order).map(|o| o.symbol)
    }

    pub fn order_account(&self, order: OrderId) -> Option<AccountId> {
        self.engine.order(order).map(|o| o.account)
    }

    /// `Grant`: the ledger lent `amount`. The caller applies each grant once.
    pub fn grant(&mut self, account: AccountId, asset: AssetId, amount: Amount) {
        self.engine.slice_credit(account, asset, amount);
        let e = self.p.entry((account, asset)).or_insert(0);
        *e = e.saturating_add(amount);
    }

    /// `Recall`: give back up to `amount` of *uncommitted* credit. Returns the
    /// events to append to the group's stream: first whatever the account's
    /// slice gained or lost so far (so the ledger's `lent` covers the return),
    /// then the `Returned`.
    pub fn recall(&mut self, account: AccountId, asset: AssetId, amount: Amount) -> Vec<Event> {
        let mut out = Vec::new();
        self.report_key((account, asset), &mut out);
        let take = self.engine.slice_take(account, asset, amount);
        if take > 0 {
            let e = self.q.entry((account, asset)).or_insert(0);
            *e = e.saturating_add(take);
            out.push(Event::Returned {
                account,
                asset,
                amount: take,
            });
        }
        out
    }

    /// The pre-trade check and hold, locally: `Ok(order id)` once the account's
    /// credit covers the order, else the reason. Funds move from free credit to
    /// committed credit; nothing else is needed before the order enters the book.
    pub fn enter(
        &mut self,
        account: AccountId,
        symbol: SymbolId,
        side: Side,
        price: u64,
        qty: u64,
    ) -> Result<OrderId, Reject> {
        let o = self.engine.apply(&Cmd::Reserve {
            account,
            symbol,
            side,
            price,
            qty,
        });
        self.drain_outbox();
        match o {
            Outcome::Reserved { order, .. } => Ok(order),
            Outcome::Rejected(r) => Err(r),
            _ => Err(Reject::Overflow),
        }
    }

    /// Mark an order as being cancelled (ownership checked); the matcher's
    /// `Closed` event releases the hold later.
    pub fn cancel(&mut self, account: AccountId, order: OrderId) -> Result<(), Reject> {
        let o = self.engine.apply(&Cmd::Cancel { account, order });
        self.drain_outbox();
        match o {
            Outcome::CancelQueued { .. } => Ok(()),
            Outcome::Rejected(r) => Err(r),
            _ => Err(Reject::Overflow),
        }
    }

    fn drain_outbox(&mut self) {
        let next = self.engine.outbox_next();
        self.engine.apply(&Cmd::OutboxAck { upto: next });
    }

    fn touches(&self, ev: &Event) -> bool {
        match *ev {
            Event::Trade { buy, sell, .. } => {
                self.engine.is_local(buy) || self.engine.is_local(sell)
            }
            Event::Closed { order } => self.engine.is_local(order),
            _ => false,
        }
    }

    /// Apply the group's archived events, in order. Events that do not involve a
    /// slice order (phase 1 orders are settled by the ledger) are skipped.
    pub fn settle<'a>(&mut self, events: impl IntoIterator<Item = &'a Event>) {
        for ev in events {
            if !self.touches(ev) {
                continue;
            }
            if let Event::Trade { buy, sell, .. } = *ev {
                for id in [buy, sell] {
                    if !is_slice_order(id) {
                        continue;
                    }
                    if let Some(o) = self.engine.order(id) {
                        let sym = self.engine.config().symbols[&o.symbol];
                        self.dirty.insert((o.account, sym.base));
                        self.dirty.insert((o.account, sym.quote));
                    }
                }
            }
            let idx = self.next_idx;
            self.next_idx += 1;
            self.engine.apply(&Cmd::Settle {
                source: 0,
                prev: idx - 1,
                idx,
                event: *ev,
            });
        }
    }

    fn report_key(&mut self, key: Key, out: &mut Vec<Event>) {
        let t = self.total(key.0, key.1);
        let q = self.q.get(&key).copied().unwrap_or(0);
        let p = self.p.get(&key).copied().unwrap_or(0);
        let lhs = t + q;
        if lhs > p {
            let amount = lhs - p;
            self.p.insert(key, lhs);
            out.push(Event::SliceIn {
                account: key.0,
                asset: key.1,
                amount,
            });
        } else if lhs < p {
            let amount = p - lhs;
            self.q.insert(key, q + amount);
            out.push(Event::SliceOut {
                account: key.0,
                asset: key.1,
                amount,
            });
        }
        self.dirty.remove(&key);
    }

    /// The net change of every touched account's slice since the last report,
    /// then the fees charged. Deterministic order (accounts sorted, then assets).
    pub fn report(&mut self) -> Vec<Event> {
        let mut out = Vec::new();
        let keys: Vec<Key> = self.dirty.iter().copied().collect();
        for k in keys {
            self.report_key(k, &mut out);
        }
        let assets: Vec<AssetId> = self
            .engine
            .config()
            .symbols
            .values()
            .flat_map(|s| [s.base, s.quote])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        for a in assets {
            let have = self.engine.fees(a);
            let done = self.fee_reported.get(&a).copied().unwrap_or(0);
            if have > done {
                self.fee_reported.insert(a, have);
                out.push(Event::SliceFee {
                    asset: a,
                    amount: have - done,
                });
            }
        }
        out
    }

    /// The slice's complete state.
    pub fn snapshot(&self) -> Vec<u8> {
        let mut out = b"SLC1".to_vec();
        let eng = self.engine.snapshot();
        out.extend_from_slice(&(eng.len() as u64).to_le_bytes());
        out.extend_from_slice(&eng);
        for m in [&self.p, &self.q] {
            out.extend_from_slice(&(m.len() as u32).to_le_bytes());
            for ((a, asset), v) in m {
                out.extend_from_slice(&a.to_le_bytes());
                out.extend_from_slice(&asset.to_le_bytes());
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        out.extend_from_slice(&(self.fee_reported.len() as u32).to_le_bytes());
        for (asset, v) in &self.fee_reported {
            out.extend_from_slice(&asset.to_le_bytes());
            out.extend_from_slice(&v.to_le_bytes());
        }
        out.extend_from_slice(&(self.dirty.len() as u32).to_le_bytes());
        for (a, asset) in &self.dirty {
            out.extend_from_slice(&a.to_le_bytes());
            out.extend_from_slice(&asset.to_le_bytes());
        }
        out.extend_from_slice(&self.next_idx.to_le_bytes());
        out
    }

    pub fn digest(&self) -> [u8; 32] {
        sha256(&self.snapshot())
    }

    pub fn restore(&mut self, data: &[u8]) -> Result<(), String> {
        let bad = || "slice snapshot: truncated".to_string();
        let mut pos = 0usize;
        let mut take = |n: usize| -> Result<&[u8], String> {
            let s = data.get(pos..pos + n).ok_or_else(bad)?;
            pos += n;
            Ok(s)
        };
        if take(4)? != b"SLC1" {
            return Err("not a slice snapshot".into());
        }
        let n = u64::from_le_bytes(take(8)?.try_into().unwrap()) as usize;
        let eng = take(n)?.to_vec();
        let mut maps = Vec::new();
        for _ in 0..2 {
            let c = u32::from_le_bytes(take(4)?.try_into().unwrap());
            let mut m = BTreeMap::new();
            for _ in 0..c {
                let a = u64::from_le_bytes(take(8)?.try_into().unwrap());
                let asset = u32::from_le_bytes(take(4)?.try_into().unwrap());
                let v = u128::from_le_bytes(take(16)?.try_into().unwrap());
                if m.insert((a, asset), v).is_some() {
                    return Err("slice snapshot: duplicate key".into());
                }
            }
            maps.push(m);
        }
        let c = u32::from_le_bytes(take(4)?.try_into().unwrap());
        let mut fee_reported = BTreeMap::new();
        for _ in 0..c {
            let asset = u32::from_le_bytes(take(4)?.try_into().unwrap());
            let v = u128::from_le_bytes(take(16)?.try_into().unwrap());
            fee_reported.insert(asset, v);
        }
        let c = u32::from_le_bytes(take(4)?.try_into().unwrap());
        let mut dirty = BTreeSet::new();
        for _ in 0..c {
            let a = u64::from_le_bytes(take(8)?.try_into().unwrap());
            let asset = u32::from_le_bytes(take(4)?.try_into().unwrap());
            dirty.insert((a, asset));
        }
        let next_idx = u64::from_le_bytes(take(8)?.try_into().unwrap());
        if pos != data.len() {
            return Err("slice snapshot: trailing bytes".into());
        }
        self.engine.restore(&eng)?;
        self.q = maps.pop().unwrap();
        self.p = maps.pop().unwrap();
        self.fee_reported = fee_reported;
        self.dirty = dirty;
        self.next_idx = next_idx;
        Ok(())
    }

    /// The engine's own invariants (holds, conservation inside the slice, no
    /// anomalies) plus the report bookkeeping: `q` and `p` only ever grow, and
    /// no key reports more than it has.
    pub fn check(&self) -> Result<(), String> {
        self.engine.check()?;
        for (k, p) in &self.p {
            let q = self.q.get(k).copied().unwrap_or(0);
            let t = self.total(k.0, k.1);
            // not yet reported changes live in `dirty`; otherwise T + q == p
            if !self.dirty.contains(k) && t + q != *p {
                return Err(format!(
                    "account {} asset {}: total {t} + returned/out {q} != granted/in {p} with no pending change",
                    k.0, k.1
                ));
            }
        }
        Ok(())
    }
}
