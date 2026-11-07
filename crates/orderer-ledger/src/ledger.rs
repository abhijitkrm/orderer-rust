//! The ledger state machine: balances, holds, open orders, the outbox and the
//! settlement marks. See `docs/design/LEDGER-RISK.md` sections 3-6.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub type AccountId = u64;
pub type AssetId = u32;
pub type SymbolId = u32;
pub type OrderId = u64;
pub type Amount = u128;

/// Order ids carry the ledger shard in their high 16 bits, so a settler can
/// route a trade event to the right ledger shards from the ids alone.
pub const SHARD_SHIFT: u32 = 48;

pub fn order_shard(id: OrderId) -> u16 {
    (id >> SHARD_SHIFT) as u16
}

const PPM: u128 = 1_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

/// A tradable pair. `lot` is how many base atoms one unit of `qty` is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Symbol {
    pub base: AssetId,
    pub quote: AssetId,
    pub lot: Amount,
}

/// Reference data. Every replica of a shard must be given an identical one.
#[derive(Clone, Debug)]
pub struct Config {
    pub shard: u16,
    /// Fee on what each side receives, in parts per million, floored.
    pub fee_ppm: u32,
    pub symbols: BTreeMap<SymbolId, Symbol>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Balance {
    pub available: Amount,
    pub held: Amount,
}

/// An open order and what is still held for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Order {
    pub account: AccountId,
    pub symbol: SymbolId,
    pub side: Side,
    pub price: u64,
    pub remaining: u64,
    pub held: Amount,
    pub cancelling: bool,
}

/// An event from a matching group, as the ledger needs it. `Closed` covers an
/// order leaving the book without (further) filling: cancelled, expired,
/// rejected by matching.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Trade {
        buy: OrderId,
        sell: OrderId,
        price: u64,
        qty: u64,
    },
    Closed {
        order: OrderId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cmd {
    Deposit {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
        external_id: u64,
    },
    Reserve {
        account: AccountId,
        symbol: SymbolId,
        side: Side,
        price: u64,
        qty: u64,
    },
    Cancel {
        account: AccountId,
        order: OrderId,
    },
    /// Apply stream event `idx` of matching group `source`. `prev` is the index
    /// of the previous event of that stream routed to this shard (0 for the
    /// first); the ledger applies it only if that is exactly its mark, so a
    /// reordered, skipped or zombie delivery can never advance the mark past
    /// an event that was not applied.
    Settle {
        source: u32,
        prev: u64,
        idx: u64,
        event: Event,
    },
    Withdraw {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
        external_id: u64,
    },
    Finalize {
        external_id: u64,
        paid: bool,
    },
    /// The matching group has accepted outbox entries below `upto`.
    OutboxAck {
        upto: u64,
    },
}

/// A message for a matching group, delivered at least once by a forwarder
/// whose session sequence number is the outbox index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutMsg {
    Submit {
        order: OrderId,
        symbol: SymbolId,
        side: Side,
        price: u64,
        qty: u64,
    },
    Cancel {
        order: OrderId,
        symbol: SymbolId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reject {
    UnknownSymbol,
    BadAmount,
    BadPrice,
    BadQuantity,
    Insufficient,
    Overflow,
    UnknownOrder,
    NotOwner,
    AlreadyCancelling,
    DuplicateId,
    UnknownId,
    /// A settle whose `prev` is not this shard's mark: events are missing or
    /// out of order. Nothing changed; the sender resynchronises.
    Gap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Deposited,
    Reserved {
        order: OrderId,
        outbox: u64,
    },
    CancelQueued {
        outbox: u64,
    },
    Settled,
    /// A replay of something already applied; nothing changed.
    Duplicate,
    WithdrawHeld,
    WithdrawDone,
    Acked,
    Rejected(Reject),
    /// The command could not be applied consistently (a matching event that
    /// contradicts ledger state, or an undecodable entry). Nothing changed
    /// except the anomaly counter. Correct operation never produces one.
    Anomaly,
}

pub struct Ledger {
    pub(crate) cfg: Config,
    pub(crate) balances: BTreeMap<(AccountId, AssetId), Balance>,
    pub(crate) orders: BTreeMap<OrderId, Order>,
    pub(crate) next_order: u64,
    pub(crate) outbox: VecDeque<OutMsg>,
    pub(crate) outbox_base: u64,
    pub(crate) hw: BTreeMap<u32, u64>,
    pub(crate) deposit_ids: BTreeSet<u64>,
    pub(crate) withdraw_ids: BTreeSet<u64>,
    pub(crate) pending: BTreeMap<u64, (AccountId, AssetId, Amount)>,
    pub(crate) fees: BTreeMap<AssetId, Amount>,
    pub(crate) deposited: BTreeMap<AssetId, Amount>,
    pub(crate) withdrawn: BTreeMap<AssetId, Amount>,
    pub(crate) settled_in: BTreeMap<AssetId, Amount>,
    pub(crate) settled_out: BTreeMap<AssetId, Amount>,
    pub(crate) anomalies: u64,
}

fn bump(map: &mut BTreeMap<AssetId, Amount>, asset: AssetId, by: Amount) {
    let e = map.entry(asset).or_insert(0);
    *e = e.saturating_add(by);
}

/// floor(amount * ppm / 1_000_000) without overflowing for any `amount`.
fn fee_of(amount: Amount, ppm: u32) -> Amount {
    let ppm = ppm as u128;
    (amount / PPM) * ppm + (amount % PPM) * ppm / PPM
}

impl Ledger {
    pub fn new(cfg: Config) -> Ledger {
        Ledger {
            cfg,
            balances: BTreeMap::new(),
            orders: BTreeMap::new(),
            next_order: 1,
            outbox: VecDeque::new(),
            outbox_base: 0,
            hw: BTreeMap::new(),
            deposit_ids: BTreeSet::new(),
            withdraw_ids: BTreeSet::new(),
            pending: BTreeMap::new(),
            fees: BTreeMap::new(),
            deposited: BTreeMap::new(),
            withdrawn: BTreeMap::new(),
            settled_in: BTreeMap::new(),
            settled_out: BTreeMap::new(),
            anomalies: 0,
        }
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }
    pub fn balance(&self, account: AccountId, asset: AssetId) -> Balance {
        self.balances
            .get(&(account, asset))
            .copied()
            .unwrap_or_default()
    }
    pub fn order(&self, id: OrderId) -> Option<&Order> {
        self.orders.get(&id)
    }
    pub fn open_orders(&self) -> usize {
        self.orders.len()
    }
    pub fn fees(&self, asset: AssetId) -> Amount {
        self.fees.get(&asset).copied().unwrap_or(0)
    }
    pub fn anomalies(&self) -> u64 {
        self.anomalies
    }
    pub fn high_water(&self, source: u32) -> Option<u64> {
        self.hw.get(&source).copied()
    }
    /// Index of the first outbox entry still held, and the entries from it.
    pub fn outbox(&self) -> (u64, impl Iterator<Item = &OutMsg>) {
        (self.outbox_base, self.outbox.iter())
    }
    pub fn outbox_next(&self) -> u64 {
        self.outbox_base + self.outbox.len() as u64
    }
    /// Total deposited minus withdrawn, per asset, on this shard.
    pub fn external_net(&self, asset: AssetId) -> (Amount, Amount) {
        (
            self.deposited.get(&asset).copied().unwrap_or(0),
            self.withdrawn.get(&asset).copied().unwrap_or(0),
        )
    }
    /// What trades moved into and out of this shard's accounts and fees, per
    /// asset. Across all shards, once every event has been settled on every
    /// shard involved, the two sums are equal.
    pub fn settled_flow(&self, asset: AssetId) -> (Amount, Amount) {
        (
            self.settled_in.get(&asset).copied().unwrap_or(0),
            self.settled_out.get(&asset).copied().unwrap_or(0),
        )
    }
    /// Everything this shard holds of `asset`: balances, fees and funds in
    /// pending withdrawals.
    pub fn holdings(&self, asset: AssetId) -> Amount {
        let mut t: Amount = self.fees(asset);
        for ((_, a), b) in &self.balances {
            if *a == asset {
                t += b.available + b.held;
            }
        }
        for (_, a, amt) in self.pending.values() {
            if *a == asset {
                t += amt;
            }
        }
        t
    }

    pub fn note_undecodable(&mut self) -> Outcome {
        self.anomalies += 1;
        Outcome::Anomaly
    }

    pub fn apply(&mut self, cmd: &Cmd) -> Outcome {
        match *cmd {
            Cmd::Deposit {
                account,
                asset,
                amount,
                external_id,
            } => self.deposit(account, asset, amount, external_id),
            Cmd::Reserve {
                account,
                symbol,
                side,
                price,
                qty,
            } => self.reserve(account, symbol, side, price, qty),
            Cmd::Cancel { account, order } => self.cancel(account, order),
            Cmd::Settle {
                source,
                prev,
                idx,
                event,
            } => self.settle(source, prev, idx, event),
            Cmd::Withdraw {
                account,
                asset,
                amount,
                external_id,
            } => self.withdraw(account, asset, amount, external_id),
            Cmd::Finalize { external_id, paid } => self.finalize(external_id, paid),
            Cmd::OutboxAck { upto } => {
                let upto = upto.min(self.outbox_next());
                while self.outbox_base < upto {
                    self.outbox.pop_front();
                    self.outbox_base += 1;
                }
                Outcome::Acked
            }
        }
    }

    fn bal(&mut self, a: AccountId, asset: AssetId) -> &mut Balance {
        self.balances.entry((a, asset)).or_default()
    }

    fn deposit(&mut self, account: AccountId, asset: AssetId, amount: Amount, id: u64) -> Outcome {
        if amount == 0 {
            return Outcome::Rejected(Reject::BadAmount);
        }
        if self.deposit_ids.contains(&id) {
            return Outcome::Duplicate;
        }
        let b = self.balance(account, asset);
        let dep = self.deposited.get(&asset).copied().unwrap_or(0);
        if b.available.checked_add(amount).is_none() || dep.checked_add(amount).is_none() {
            return Outcome::Rejected(Reject::Overflow);
        }
        self.deposit_ids.insert(id);
        self.bal(account, asset).available += amount;
        bump(&mut self.deposited, asset, amount);
        Outcome::Deposited
    }

    /// The asset and amount a new order must hold, or why it cannot.
    fn hold_for(
        &self,
        sym: &Symbol,
        side: Side,
        price: u64,
        qty: u64,
    ) -> Result<(AssetId, Amount), Reject> {
        match side {
            Side::Buy => Ok((sym.quote, price as u128 * qty as u128)),
            Side::Sell => sym
                .lot
                .checked_mul(qty as u128)
                .map(|a| (sym.base, a))
                .ok_or(Reject::Overflow),
        }
    }

    /// A market buy has no limit price of its own; the caller must give a
    /// protection price, which is what is held. That is a limit order.
    fn reserve(
        &mut self,
        account: AccountId,
        symbol: SymbolId,
        side: Side,
        price: u64,
        qty: u64,
    ) -> Outcome {
        let Some(sym) = self.cfg.symbols.get(&symbol).copied() else {
            return Outcome::Rejected(Reject::UnknownSymbol);
        };
        if qty == 0 {
            return Outcome::Rejected(Reject::BadQuantity);
        }
        if price == 0 {
            return Outcome::Rejected(Reject::BadPrice);
        }
        if self.next_order >= 1 << SHARD_SHIFT {
            return Outcome::Rejected(Reject::Overflow);
        }
        let (asset, hold) = match self.hold_for(&sym, side, price, qty) {
            Ok(x) => x,
            Err(r) => return Outcome::Rejected(r),
        };
        if self.balance(account, asset).available < hold {
            return Outcome::Rejected(Reject::Insufficient);
        }
        let order = ((self.cfg.shard as u64) << SHARD_SHIFT) | self.next_order;
        self.next_order += 1;
        let b = self.bal(account, asset);
        b.available -= hold;
        b.held += hold;
        self.orders.insert(
            order,
            Order {
                account,
                symbol,
                side,
                price,
                remaining: qty,
                held: hold,
                cancelling: false,
            },
        );
        let outbox = self.outbox_next();
        self.outbox.push_back(OutMsg::Submit {
            order,
            symbol,
            side,
            price,
            qty,
        });
        Outcome::Reserved { order, outbox }
    }

    fn cancel(&mut self, account: AccountId, order: OrderId) -> Outcome {
        let Some(o) = self.orders.get_mut(&order) else {
            return Outcome::Rejected(Reject::UnknownOrder);
        };
        if o.account != account {
            return Outcome::Rejected(Reject::NotOwner);
        }
        if o.cancelling {
            return Outcome::Rejected(Reject::AlreadyCancelling);
        }
        o.cancelling = true;
        let symbol = o.symbol;
        let outbox = self.outbox_next();
        self.outbox.push_back(OutMsg::Cancel { order, symbol });
        Outcome::CancelQueued { outbox }
    }

    fn withdraw(&mut self, account: AccountId, asset: AssetId, amount: Amount, id: u64) -> Outcome {
        if amount == 0 {
            return Outcome::Rejected(Reject::BadAmount);
        }
        if self.withdraw_ids.contains(&id) {
            return Outcome::Rejected(Reject::DuplicateId);
        }
        if self.balance(account, asset).available < amount {
            return Outcome::Rejected(Reject::Insufficient);
        }
        self.withdraw_ids.insert(id);
        self.bal(account, asset).available -= amount;
        self.pending.insert(id, (account, asset, amount));
        Outcome::WithdrawHeld
    }

    fn finalize(&mut self, id: u64, paid: bool) -> Outcome {
        let Some((account, asset, amount)) = self.pending.remove(&id) else {
            return if self.withdraw_ids.contains(&id) {
                Outcome::Duplicate
            } else {
                Outcome::Rejected(Reject::UnknownId)
            };
        };
        if paid {
            bump(&mut self.withdrawn, asset, amount);
        } else {
            self.bal(account, asset).available += amount;
        }
        Outcome::WithdrawDone
    }

    fn local(&self, id: OrderId) -> bool {
        order_shard(id) == self.cfg.shard
    }

    fn settle(&mut self, source: u32, prev: u64, idx: u64, event: Event) -> Outcome {
        let hw = self.hw.get(&source).copied().unwrap_or(0);
        if idx <= hw {
            return Outcome::Duplicate;
        }
        if prev != hw || idx <= prev {
            return Outcome::Rejected(Reject::Gap);
        }
        let ok = match event {
            Event::Trade {
                buy,
                sell,
                price,
                qty,
            } => self.settle_trade(buy, sell, price, qty),
            Event::Closed { order } => self.settle_closed(order),
        };
        // The mark advances even for an anomaly: a replay must not retry it.
        self.hw.insert(source, idx);
        if ok {
            Outcome::Settled
        } else {
            self.anomalies += 1;
            Outcome::Anomaly
        }
    }

    fn settle_closed(&mut self, id: OrderId) -> bool {
        if !self.local(id) {
            return false;
        }
        let Some(o) = self.orders.remove(&id) else {
            return false;
        };
        let asset = self.hold_asset(&o);
        let b = self.bal(o.account, asset);
        b.held -= o.held;
        b.available += o.held;
        true
    }

    fn hold_asset(&self, o: &Order) -> AssetId {
        let sym = &self.cfg.symbols[&o.symbol];
        match o.side {
            Side::Buy => sym.quote,
            Side::Sell => sym.base,
        }
    }

    /// Checks one local side of a trade without changing anything.
    fn trade_side_ok(&self, id: OrderId, side: Side, price: u64, qty: u64) -> Option<Symbol> {
        let o = self.orders.get(&id)?;
        let sym = *self.cfg.symbols.get(&o.symbol)?;
        let limit_ok = match side {
            Side::Buy => price <= o.price,
            Side::Sell => price >= o.price,
        };
        (o.side == side && o.remaining >= qty && limit_ok && qty > 0).then_some(sym)
    }

    fn settle_trade(&mut self, buy: OrderId, sell: OrderId, price: u64, qty: u64) -> bool {
        let (lb, ls) = (self.local(buy), self.local(sell));
        if !lb && !ls || buy == sell {
            return false;
        }
        let sb = if lb {
            self.trade_side_ok(buy, Side::Buy, price, qty)
        } else {
            None
        };
        let ss = if ls {
            self.trade_side_ok(sell, Side::Sell, price, qty)
        } else {
            None
        };
        if lb && sb.is_none() || ls && ss.is_none() {
            return false;
        }
        if let (Some(a), Some(b)) = (sb, ss) {
            if a != b {
                return false;
            }
        }
        if lb {
            self.apply_buy(buy, &sb.unwrap(), price, qty);
        }
        if ls {
            self.apply_sell(sell, &ss.unwrap(), price, qty);
        }
        true
    }

    fn fill(&mut self, id: OrderId, qty: u64, hold_used: Amount) {
        let o = self.orders.get_mut(&id).unwrap();
        o.remaining -= qty;
        o.held -= hold_used;
        if o.remaining == 0 {
            let o = self.orders.remove(&id).unwrap();
            if o.held > 0 {
                let asset = self.hold_asset(&o);
                let b = self.bal(o.account, asset);
                b.held -= o.held;
                b.available += o.held;
            }
        }
    }

    fn apply_buy(&mut self, id: OrderId, sym: &Symbol, price: u64, qty: u64) {
        let o = self.orders[&id].clone();
        let held = o.price as u128 * qty as u128;
        let paid = price as u128 * qty as u128;
        let recv = sym.lot * qty as u128;
        let fee = fee_of(recv, self.cfg.fee_ppm);
        let b = self.bal(o.account, sym.quote);
        b.held -= held;
        b.available += held - paid;
        let b = self.bal(o.account, sym.base);
        b.available = b.available.saturating_add(recv - fee);
        bump(&mut self.fees, sym.base, fee);
        bump(&mut self.settled_out, sym.quote, paid);
        bump(&mut self.settled_in, sym.base, recv);
        self.fill(id, qty, held);
    }

    fn apply_sell(&mut self, id: OrderId, sym: &Symbol, price: u64, qty: u64) {
        let o = self.orders[&id].clone();
        let sold = sym.lot * qty as u128;
        let recv = price as u128 * qty as u128;
        let fee = fee_of(recv, self.cfg.fee_ppm);
        let b = self.bal(o.account, sym.base);
        b.held -= sold;
        let b = self.bal(o.account, sym.quote);
        b.available = b.available.saturating_add(recv - fee);
        bump(&mut self.fees, sym.quote, fee);
        bump(&mut self.settled_out, sym.base, sold);
        bump(&mut self.settled_in, sym.quote, recv);
        self.fill(id, qty, sold);
    }

    /// Checks everything that must hold on one shard at every step:
    /// no order without its exact hold, no hold without an order, balances
    /// that add up against deposits, withdrawals and settled flows (I1 local),
    /// and no anomalies. Returns the first violation.
    pub fn check(&self) -> Result<(), String> {
        let mut held: BTreeMap<(AccountId, AssetId), Amount> = BTreeMap::new();
        for (id, o) in &self.orders {
            if order_shard(*id) != self.cfg.shard
                || (*id & ((1 << SHARD_SHIFT) - 1)) >= self.next_order
            {
                return Err(format!("order {id:#x} has a foreign or future id"));
            }
            if o.remaining == 0 {
                return Err(format!("order {id:#x} is open with nothing remaining"));
            }
            let sym = self
                .cfg
                .symbols
                .get(&o.symbol)
                .ok_or("order on unknown symbol")?;
            let want = match o.side {
                Side::Buy => o.price as u128 * o.remaining as u128,
                Side::Sell => sym.lot * o.remaining as u128,
            };
            if o.held != want {
                return Err(format!("order {id:#x} holds {} but needs {want}", o.held));
            }
            *held.entry((o.account, self.hold_asset(o))).or_insert(0) += o.held;
        }
        for (k, b) in &self.balances {
            if b.held != held.get(k).copied().unwrap_or(0) {
                return Err(format!(
                    "account {} asset {}: held {} != open orders {}",
                    k.0,
                    k.1,
                    b.held,
                    held.get(k).copied().unwrap_or(0)
                ));
            }
        }
        for k in held.keys() {
            if !self.balances.contains_key(k) {
                return Err(format!(
                    "account {} asset {}: orders without a balance",
                    k.0, k.1
                ));
            }
        }
        let mut assets: BTreeSet<AssetId> = BTreeSet::new();
        assets.extend(self.balances.keys().map(|k| k.1));
        assets.extend(self.deposited.keys());
        assets.extend(self.settled_in.keys());
        assets.extend(self.settled_out.keys());
        for a in assets {
            let (dep, wd) = self.external_net(a);
            let (inn, out) = self.settled_flow(a);
            let have = self.holdings(a);
            if have + wd + out != dep + inn {
                return Err(format!("asset {a}: holdings {have} + withdrawn {wd} + out {out} != deposited {dep} + in {inn}"));
            }
        }
        if self.anomalies != 0 {
            return Err(format!("{} anomalies", self.anomalies));
        }
        Ok(())
    }
}
