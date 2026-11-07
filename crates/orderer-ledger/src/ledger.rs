//! The ledger state machine: balances, holds, open orders, the outbox and the
//! settlement marks. See `docs/design/LEDGER-RISK.md` sections 3-6.

use crate::hashed::{lane_add, lane_bytes, lane_sub, Lane, Mix, Tracked, ZERO};
use orderer_cluster::sha256::sha256;
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

/// Ledger shard numbers are below this. Orders entered through a credit slice
/// (phase 2) are numbered by their matching group with this bit set in the shard
/// field, so no ledger shard ever mistakes one for its own order.
pub const SLICE_SHARD_BIT: u16 = 0x8000;

/// True for an order id assigned by a matching group's credit slice.
pub fn is_slice_order(id: OrderId) -> bool {
    order_shard(id) & SLICE_SHARD_BIT != 0
}

const PPM: u128 = 1_000_000;

/// The system account that fees accrue to; no user command may name it.
pub const FEE_ACCOUNT: AccountId = u64::MAX;

/// How many recent adjustments the audit ring keeps.
pub const ADJUST_LOG: usize = 256;

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
    /// Default fee on what each side receives, in parts per million, floored.
    pub fee_ppm: u32,
    pub symbols: BTreeMap<SymbolId, Symbol>,
    /// Per-symbol `(maker_ppm, taker_ppm)`, overriding `fee_ppm`.
    pub fees: BTreeMap<SymbolId, (u32, u32)>,
}

/// Why a [`Config`] cannot be used.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Ledger shard numbers are below [`SLICE_SHARD_BIT`]: orders entered through a
    /// credit slice carry that bit in the shard field of their id, so a ledger shard
    /// at or above it would mistake them for its own.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.shard >= SLICE_SHARD_BIT {
            return Err(ConfigError(format!(
                "ledger shard {} is out of range: shard numbers must be below {:#x} \
                 (bit 15 of the order-id shard field marks credit-slice orders)",
                self.shard, SLICE_SHARD_BIT
            )));
        }
        self.validate_reference()
    }

    /// The checks that also apply to a slice engine's configuration.
    pub(crate) fn validate_reference(&self) -> Result<(), ConfigError> {
        if self.fee_ppm > PPM as u32 {
            return Err(ConfigError(format!(
                "fee_ppm {} exceeds 1,000,000",
                self.fee_ppm
            )));
        }
        for (id, (m, t)) in &self.fees {
            if *m > PPM as u32 || *t > PPM as u32 {
                return Err(ConfigError(format!(
                    "fees for symbol {id} exceed 1,000,000 ppm"
                )));
            }
        }
        for (id, sym) in &self.symbols {
            if sym.lot == 0 {
                return Err(ConfigError(format!("symbol {id} has a zero lot")));
            }
            if sym.base == sym.quote {
                return Err(ConfigError(format!(
                    "symbol {id} has the same base and quote asset"
                )));
            }
        }
        Ok(())
    }

    fn fee_pair(&self, symbol: SymbolId) -> (u32, u32) {
        self.fees
            .get(&symbol)
            .copied()
            .unwrap_or((self.fee_ppm, self.fee_ppm))
    }
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
    /// Ledger clock when the order was reserved.
    pub born: u64,
    /// Ledger clock when a cancel was last queued (0 unless `cancelling`).
    pub cancel_at: u64,
}

/// An event from a matching group, as the ledger needs it. `Closed` covers an
/// order leaving the book without (further) filling: cancelled, expired,
/// rejected by matching.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// `taker_is_buy`: the buy order was the aggressor (the sell was resting),
    /// which decides who pays the taker and who the maker fee.
    Trade {
        buy: OrderId,
        sell: OrderId,
        price: u64,
        qty: u64,
        taker_is_buy: bool,
    },
    Closed {
        order: OrderId,
    },
    /// Phase 2, from a matching group's credit slice (`CREDIT-SLICES.md`): the
    /// account's money in the slice grew by `amount` (trade proceeds, net of fee).
    SliceIn {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
    },
    /// The account's money in the slice shrank by `amount` (trades paid).
    SliceOut {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
    },
    /// Fees the slice charged, to be booked in this shard's fee pool.
    SliceFee {
        asset: AssetId,
        amount: Amount,
    },
    /// The slice handed back `amount` of uncommitted credit (answer to a recall).
    Returned {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
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
    /// Like `Reserve`, for an immediate-or-cancel order: whatever does not trade at
    /// once is closed by matching (a `Closed` event releases its hold). A market
    /// order is an IOC order priced at its protection price.
    ReserveIoc {
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
    /// Advance the ledger clock to `now_ns` (never backwards). The only source
    /// of time in the state machine, so replicas agree on every age.
    Clock {
        now_ns: u64,
    },
    /// The reconciler: queue a cancel for open orders older than `max_age_ns`
    /// that have none queued, and re-queue one for orders whose cancel was
    /// queued at least `retry_ns` ago (0 = never retry), at most `max` in all.
    /// Re-sent cancels are harmless: matching treats a cancel of an order that
    /// is not resting as a no-op. It never releases a hold itself; only matching
    /// events do, because an order may still be live.
    Sweep {
        max_age_ns: u64,
        retry_ns: u64,
        max: u32,
    },
    /// Administrative credit or debit with an audit reason, idempotent by
    /// `external_id`, never below zero, counted in conservation.
    Adjust {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
        credit: bool,
        reason: u32,
        external_id: u64,
    },
    /// Move accrued fees of `asset` out through the withdrawal machinery
    /// (`Finalize` pays or returns them to the fee pool).
    FeeWithdraw {
        asset: AssetId,
        amount: Amount,
        external_id: u64,
    },
    /// Phase 2: lend `amount` of the account's available balance to the matching
    /// group as a credit slice (a `Grant` goes out through the outbox).
    Lend {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
    },
    /// Phase 2: ask the group to give back up to `amount` of the account's
    /// uncommitted slice. The funds return as a `Returned` event; a withdrawal
    /// can only draw on `available`, never on lent funds.
    Recall {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
    },
    /// The time sidecar: `cmd` stamped with the proposer's clock reading `ts_ns`.
    /// The state machine raises its clock to `ts_ns` (never lowers it) and then
    /// applies `cmd`, so replicas never read a local clock in `apply` and every
    /// order's `born` is a function of the log. Does not nest.
    At {
        ts_ns: u64,
        cmd: Box<Cmd>,
    },
}

impl Cmd {
    /// `self` stamped with `ts_ns` (see [`Cmd::At`]).
    pub fn at(self, ts_ns: u64) -> Cmd {
        Cmd::At {
            ts_ns,
            cmd: Box::new(self),
        }
    }
}

/// One entry of the adjustment audit ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Adjustment {
    pub external_id: u64,
    pub account: AccountId,
    pub asset: AssetId,
    pub credit: bool,
    pub amount: Amount,
    pub reason: u32,
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
    /// An immediate-or-cancel order (`Cmd::ReserveIoc`).
    SubmitIoc {
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
    /// Phase 2: `credit += amount` for the account in the group's slice.
    Grant {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
    },
    /// Phase 2: return up to `amount` of the account's uncommitted credit.
    Recall {
        account: AccountId,
        asset: AssetId,
        amount: Amount,
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
    /// The command names the fee system account.
    ReservedAccount,
    /// A time-stamped command inside a time-stamped command.
    Nested,
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
    Ticked,
    /// Cancels queued by a sweep.
    Swept {
        queued: u32,
    },
    Adjusted,
    /// Funds lent to the group; the `Grant` is at this outbox index.
    Lent {
        outbox: u64,
    },
    RecallQueued {
        outbox: u64,
    },
    Rejected(Reject),
    /// The command could not be applied consistently (a matching event that
    /// contradicts ledger state, or an undecodable entry). Nothing changed
    /// except the anomaly counter. Correct operation never produces one.
    Anomaly,
}

pub struct Ledger {
    pub(crate) cfg: Config,
    pub(crate) balances: Tracked<(AccountId, AssetId), Balance>,
    pub(crate) orders: Tracked<OrderId, Order>,
    /// Open orders with no cancel queued, by `(born, id)`; and those with one, by
    /// `(cancel_at, id)`. Derived from `orders`, so not in the snapshot: they let a
    /// sweep find what is due without visiting every order.
    pub(crate) by_born: BTreeSet<(u64, OrderId)>,
    pub(crate) by_cancel: BTreeSet<(u64, OrderId)>,
    pub(crate) next_order: u64,
    pub(crate) outbox: VecDeque<OutMsg>,
    pub(crate) outbox_base: u64,
    pub(crate) hw: BTreeMap<u32, u64>,
    pub(crate) deposit_ids: BTreeSet<u64>,
    pub(crate) withdraw_ids: BTreeSet<u64>,
    pub(crate) pending: Tracked<u64, (AccountId, AssetId, Amount)>,
    /// Accumulators of the outbox entries and of the three id sets (see `hashed`).
    pub(crate) outbox_acc: Lane,
    pub(crate) ids_acc: [Lane; 3],
    pub(crate) fees: BTreeMap<AssetId, Amount>,
    pub(crate) deposited: BTreeMap<AssetId, Amount>,
    pub(crate) withdrawn: BTreeMap<AssetId, Amount>,
    pub(crate) settled_in: BTreeMap<AssetId, Amount>,
    pub(crate) settled_out: BTreeMap<AssetId, Amount>,
    pub(crate) anomalies: u64,
    pub(crate) clock: u64,
    pub(crate) adjust_ids: BTreeSet<u64>,
    pub(crate) adjusted_in: BTreeMap<AssetId, Amount>,
    pub(crate) adjusted_out: BTreeMap<AssetId, Amount>,
    pub(crate) adjust_log: VecDeque<Adjustment>,
    /// Phase 2: what each account has lent to the group, by asset.
    pub(crate) lent: Tracked<(AccountId, AssetId), Amount>,
    pub(crate) lent_granted: BTreeMap<AssetId, Amount>,
    pub(crate) lent_returned: BTreeMap<AssetId, Amount>,
    pub(crate) slice_in: BTreeMap<AssetId, Amount>,
    pub(crate) slice_out: BTreeMap<AssetId, Amount>,
}

fn side_u(s: Side) -> u64 {
    match s {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn balance_cell(k: &(AccountId, AssetId), b: &Balance) -> Lane {
    Mix::new(1)
        .u64(k.0)
        .u64(k.1 as u64)
        .u128(b.available)
        .u128(b.held)
        .finish()
}

fn order_cell(id: &OrderId, o: &Order) -> Lane {
    Mix::new(2)
        .u64(*id)
        .u64(o.account)
        .u64(o.symbol as u64)
        .u64(side_u(o.side))
        .u64(o.price)
        .u64(o.remaining)
        .u128(o.held)
        .u64(o.cancelling as u64)
        .u64(o.born)
        .u64(o.cancel_at)
        .finish()
}

fn lent_cell(k: &(AccountId, AssetId), v: &Amount) -> Lane {
    Mix::new(3).u64(k.0).u64(k.1 as u64).u128(*v).finish()
}

fn pending_cell(id: &u64, v: &(AccountId, AssetId, Amount)) -> Lane {
    Mix::new(4)
        .u64(*id)
        .u64(v.0)
        .u64(v.1 as u64)
        .u128(v.2)
        .finish()
}

/// Outbox entry `idx`: position matters, so a reordered outbox hashes differently.
pub(crate) fn out_cell(idx: u64, m: &OutMsg) -> Lane {
    let mut x = Mix::new(5);
    x.u64(idx);
    match *m {
        OutMsg::Submit {
            order,
            symbol,
            side,
            price,
            qty,
        } => x
            .u64(1)
            .u64(order)
            .u64(symbol as u64)
            .u64(side_u(side))
            .u64(price)
            .u64(qty),
        OutMsg::SubmitIoc {
            order,
            symbol,
            side,
            price,
            qty,
        } => x
            .u64(6)
            .u64(order)
            .u64(symbol as u64)
            .u64(side_u(side))
            .u64(price)
            .u64(qty),
        OutMsg::Cancel { order, symbol } => x.u64(2).u64(order).u64(symbol as u64),
        OutMsg::Grant {
            account,
            asset,
            amount,
        } => x.u64(3).u64(account).u64(asset as u64).u128(amount),
        OutMsg::Recall {
            account,
            asset,
            amount,
        } => x.u64(4).u64(account).u64(asset as u64).u128(amount),
    };
    x.finish()
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
    /// A ledger for `cfg`, which must be valid ([`Config::validate`]).
    pub fn try_new(cfg: Config) -> Result<Ledger, ConfigError> {
        cfg.validate()?;
        Ok(Ledger::new_unchecked(cfg))
    }

    /// Like [`Ledger::try_new`], panicking with the reason when `cfg` is invalid.
    pub fn new(cfg: Config) -> Ledger {
        match Ledger::try_new(cfg) {
            Ok(l) => l,
            Err(e) => panic!("invalid ledger config: {e}"),
        }
    }

    /// For the slice engine, whose shard field legitimately carries the slice bit.
    pub(crate) fn new_unchecked(cfg: Config) -> Ledger {
        Ledger {
            cfg,
            balances: Tracked::new(),
            orders: Tracked::new(),
            by_born: BTreeSet::new(),
            by_cancel: BTreeSet::new(),
            outbox_acc: ZERO,
            ids_acc: [ZERO; 3],
            next_order: 1,
            outbox: VecDeque::new(),
            outbox_base: 0,
            hw: BTreeMap::new(),
            deposit_ids: BTreeSet::new(),
            withdraw_ids: BTreeSet::new(),
            pending: Tracked::new(),
            fees: BTreeMap::new(),
            deposited: BTreeMap::new(),
            withdrawn: BTreeMap::new(),
            settled_in: BTreeMap::new(),
            settled_out: BTreeMap::new(),
            anomalies: 0,
            clock: 0,
            adjust_ids: BTreeSet::new(),
            adjusted_in: BTreeMap::new(),
            adjusted_out: BTreeMap::new(),
            adjust_log: VecDeque::new(),
            lent: Tracked::new(),
            lent_granted: BTreeMap::new(),
            lent_returned: BTreeMap::new(),
            slice_in: BTreeMap::new(),
            slice_out: BTreeMap::new(),
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
    pub fn clock(&self) -> u64 {
        self.clock
    }
    /// Administrative credits and debits applied, per asset: `(in, out)`.
    pub fn adjusted_flow(&self, asset: AssetId) -> (Amount, Amount) {
        (
            self.adjusted_in.get(&asset).copied().unwrap_or(0),
            self.adjusted_out.get(&asset).copied().unwrap_or(0),
        )
    }
    /// The most recent adjustments, oldest first (at most [`ADJUST_LOG`]).
    pub fn adjustments(&self) -> impl Iterator<Item = &Adjustment> {
        self.adjust_log.iter()
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
    /// What `account` has lent to the matching group, of `asset`.
    pub fn lent(&self, account: AccountId, asset: AssetId) -> Amount {
        self.lent.get(&(account, asset)).copied().unwrap_or(0)
    }
    /// Every `(account, asset)` with funds lent to the matching group, and how much.
    pub fn lent_entries(&self) -> impl Iterator<Item = ((AccountId, AssetId), Amount)> + '_ {
        self.lent.iter().map(|(k, v)| (*k, *v))
    }
    /// Everything lent of `asset` on this shard.
    pub fn lent_total(&self, asset: AssetId) -> Amount {
        self.lent
            .iter()
            .filter(|(k, _)| k.1 == asset)
            .map(|(_, v)| *v)
            .sum()
    }
    /// Phase 2 flows of `asset`: `(granted, returned, slice_in, slice_out)`.
    /// Locally, `lent_total + returned + out == granted + in` always holds.
    pub fn slice_flow(&self, asset: AssetId) -> (Amount, Amount, Amount, Amount) {
        let g = |m: &BTreeMap<AssetId, Amount>| m.get(&asset).copied().unwrap_or(0);
        (
            g(&self.lent_granted),
            g(&self.lent_returned),
            g(&self.slice_in),
            g(&self.slice_out),
        )
    }
    /// True once any phase 2 state exists (decides the snapshot format).
    pub(crate) fn has_slice_state(&self) -> bool {
        !(self.lent.is_empty()
            && self.lent_granted.is_empty()
            && self.lent_returned.is_empty()
            && self.slice_in.is_empty()
            && self.slice_out.is_empty())
    }
    pub(crate) fn is_local(&self, id: OrderId) -> bool {
        self.local(id)
    }
    /// Slice engine: money delivered to the group (a `Grant` applied).
    pub(crate) fn slice_credit(&mut self, account: AccountId, asset: AssetId, amount: Amount) {
        let b = self.bal(account, asset);
        b.available = b.available.saturating_add(amount);
        bump(&mut self.deposited, asset, amount);
    }
    /// Slice engine: take back up to `amount` of uncommitted money.
    pub(crate) fn slice_take(
        &mut self,
        account: AccountId,
        asset: AssetId,
        amount: Amount,
    ) -> Amount {
        let take = self.balance(account, asset).available.min(amount);
        if take > 0 {
            self.bal(account, asset).available -= take;
            bump(&mut self.withdrawn, asset, take);
        }
        take
    }
    fn add_lent(&mut self, account: AccountId, asset: AssetId, amount: Amount) -> bool {
        let e = self.lent.get_or_insert_with((account, asset), || 0);
        match e.checked_add(amount) {
            Some(v) => {
                *e = v;
                true
            }
            None => false,
        }
    }
    fn sub_lent(&mut self, account: AccountId, asset: AssetId, amount: Amount) -> bool {
        let have = self.lent(account, asset);
        if have < amount {
            return false;
        }
        if have == amount {
            self.lent.remove(&(account, asset));
        } else {
            self.lent.insert((account, asset), have - amount);
        }
        true
    }

    /// Everything this shard holds of `asset`: balances, fees, funds in
    /// pending withdrawals and funds lent to the matching group.
    pub fn holdings(&self, asset: AssetId) -> Amount {
        let mut t: Amount = self.fees(asset) + self.lent_total(asset);
        for ((_, a), b) in self.balances.iter() {
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

    /// Append to the outbox, keeping its accumulator current.
    fn out_push(&mut self, m: OutMsg) {
        let idx = self.outbox_next();
        self.outbox_acc = lane_add(self.outbox_acc, out_cell(idx, &m));
        self.outbox.push_back(m);
    }

    fn out_pop(&mut self) {
        if let Some(m) = self.outbox.pop_front() {
            self.outbox_acc = lane_sub(self.outbox_acc, out_cell(self.outbox_base, &m));
            self.outbox_base += 1;
        }
    }

    /// Count `id` in the accumulator of id set `which` (0 deposits, 1 withdrawals,
    /// 2 adjustments).
    pub(crate) fn note_id(&mut self, which: usize, id: u64) {
        let l = Mix::new(100 + which as u64).u64(id).finish();
        self.ids_acc[which] = lane_add(self.ids_acc[which], l);
    }

    /// Remove an open order and its age-index entry.
    fn drop_order(&mut self, id: OrderId) -> Option<Order> {
        let o = self.orders.remove(&id)?;
        if o.cancelling {
            self.by_cancel.remove(&(o.cancel_at, id));
        } else {
            self.by_born.remove(&(o.born, id));
        }
        Some(o)
    }

    /// Rebuild the age indexes from `orders` (after a restore).
    pub(crate) fn rebuild_indexes(&mut self) {
        self.by_born.clear();
        self.by_cancel.clear();
        for (id, o) in self.orders.iter() {
            if o.cancelling {
                self.by_cancel.insert((o.cancel_at, *id));
            } else {
                self.by_born.insert((o.born, *id));
            }
        }
    }

    /// The divergence digest: SHA-256 over a small summary (counters, per-asset
    /// totals, marks) and the accumulators of every large table. Cost is the number
    /// of entries changed since the last call, not the size of the state, which
    /// makes it cheap enough to take every few hundred log entries. It detects
    /// divergence, it is not collision-resistant against an adversary: use
    /// [`Ledger::deep_digest`] (SHA-256 of the whole snapshot, O(state)) for that.
    pub fn digest(&self) -> [u8; 32] {
        let mut b = Vec::with_capacity(512);
        b.extend_from_slice(b"orderer-ledger-digest/2");
        self.small_state(&mut b);
        let lanes = [
            self.balances.lane(balance_cell),
            self.orders.lane(order_cell),
            self.lent.lane(lent_cell),
            self.pending.lane(pending_cell),
            self.outbox_acc,
            self.ids_acc[0],
            self.ids_acc[1],
            self.ids_acc[2],
        ];
        for l in lanes {
            b.extend_from_slice(&lane_bytes(l));
        }
        for n in [
            self.balances.len(),
            self.orders.len(),
            self.lent.len(),
            self.pending.len(),
            self.outbox.len(),
            self.deposit_ids.len(),
            self.withdraw_ids.len(),
            self.adjust_ids.len(),
        ] {
            b.extend_from_slice(&(n as u64).to_le_bytes());
        }
        sha256(&b)
    }

    /// Everything in the digest that is not a large table: scalars, the per-asset
    /// maps, the marks and the adjustment ring (all bounded).
    fn small_state(&self, b: &mut Vec<u8>) {
        b.extend_from_slice(&self.cfg.shard.to_le_bytes());
        for v in [
            self.next_order,
            self.outbox_base,
            self.anomalies,
            self.clock,
        ] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b.extend_from_slice(&(self.hw.len() as u32).to_le_bytes());
        for (s, i) in &self.hw {
            b.extend_from_slice(&s.to_le_bytes());
            b.extend_from_slice(&i.to_le_bytes());
        }
        for m in [
            &self.fees,
            &self.deposited,
            &self.withdrawn,
            &self.settled_in,
            &self.settled_out,
            &self.adjusted_in,
            &self.adjusted_out,
            &self.lent_granted,
            &self.lent_returned,
            &self.slice_in,
            &self.slice_out,
        ] {
            b.extend_from_slice(&(m.len() as u32).to_le_bytes());
            for (k, v) in m {
                b.extend_from_slice(&k.to_le_bytes());
                b.extend_from_slice(&v.to_le_bytes());
            }
        }
        b.extend_from_slice(&(self.adjust_log.len() as u32).to_le_bytes());
        for a in &self.adjust_log {
            b.extend_from_slice(&a.external_id.to_le_bytes());
            b.extend_from_slice(&a.account.to_le_bytes());
            b.extend_from_slice(&a.asset.to_le_bytes());
            b.push(a.credit as u8);
            b.extend_from_slice(&a.amount.to_le_bytes());
            b.extend_from_slice(&a.reason.to_le_bytes());
        }
    }

    /// The accumulators recomputed from scratch, ignoring every cache, for tests
    /// that pin the incremental bookkeeping to its definition.
    pub fn digest_from_scratch(&self) -> [u8; 32] {
        let mut fresh = Ledger::new_unchecked(self.cfg.clone());
        fresh.copy_state_from(self);
        fresh.digest()
    }

    fn copy_state_from(&mut self, o: &Ledger) {
        // A snapshot round trip rebuilds every table and accumulator.
        let snap = o.snapshot();
        let l = Ledger::parse(self.cfg.clone(), &snap).expect("own snapshot parses");
        *self = l;
    }

    pub fn note_undecodable(&mut self) -> Outcome {
        self.anomalies += 1;
        Outcome::Anomaly
    }

    pub fn apply(&mut self, cmd: &Cmd) -> Outcome {
        match *cmd {
            Cmd::Deposit { account, .. }
            | Cmd::Reserve { account, .. }
            | Cmd::ReserveIoc { account, .. }
            | Cmd::Withdraw { account, .. }
            | Cmd::Adjust { account, .. }
            | Cmd::Lend { account, .. }
            | Cmd::Recall { account, .. }
                if account == FEE_ACCOUNT =>
            {
                Outcome::Rejected(Reject::ReservedAccount)
            }
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
            } => self.reserve(account, symbol, side, price, qty, false),
            Cmd::ReserveIoc {
                account,
                symbol,
                side,
                price,
                qty,
            } => self.reserve(account, symbol, side, price, qty, true),
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
                    self.out_pop();
                }
                Outcome::Acked
            }
            Cmd::Clock { now_ns } => {
                self.clock = self.clock.max(now_ns);
                Outcome::Ticked
            }
            Cmd::Sweep {
                max_age_ns,
                retry_ns,
                max,
            } => self.sweep(max_age_ns, retry_ns, max),
            Cmd::Adjust {
                account,
                asset,
                amount,
                credit,
                reason,
                external_id,
            } => self.adjust(account, asset, amount, credit, reason, external_id),
            Cmd::FeeWithdraw {
                asset,
                amount,
                external_id,
            } => self.fee_withdraw(asset, amount, external_id),
            Cmd::Lend {
                account,
                asset,
                amount,
            } => self.lend(account, asset, amount),
            Cmd::Recall {
                account,
                asset,
                amount,
            } => self.recall(account, asset, amount),
            Cmd::At {
                ts_ns,
                cmd: ref inner,
            } => {
                if matches!(**inner, Cmd::At { .. }) {
                    return Outcome::Rejected(Reject::Nested);
                }
                self.clock = self.clock.max(ts_ns);
                self.apply(inner)
            }
        }
    }

    fn lend(&mut self, account: AccountId, asset: AssetId, amount: Amount) -> Outcome {
        if amount == 0 {
            return Outcome::Rejected(Reject::BadAmount);
        }
        if self.balance(account, asset).available < amount {
            return Outcome::Rejected(Reject::Insufficient);
        }
        let granted = self.lent_granted.get(&asset).copied().unwrap_or(0);
        if self.lent(account, asset).checked_add(amount).is_none()
            || granted.checked_add(amount).is_none()
        {
            return Outcome::Rejected(Reject::Overflow);
        }
        self.bal(account, asset).available -= amount;
        self.add_lent(account, asset, amount);
        bump(&mut self.lent_granted, asset, amount);
        let outbox = self.outbox_next();
        self.out_push(OutMsg::Grant {
            account,
            asset,
            amount,
        });
        Outcome::Lent { outbox }
    }

    fn recall(&mut self, account: AccountId, asset: AssetId, amount: Amount) -> Outcome {
        if amount == 0 {
            return Outcome::Rejected(Reject::BadAmount);
        }
        let lent = self.lent(account, asset);
        if lent == 0 {
            return Outcome::Rejected(Reject::Insufficient);
        }
        let outbox = self.outbox_next();
        self.out_push(OutMsg::Recall {
            account,
            asset,
            amount: amount.min(lent),
        });
        Outcome::RecallQueued { outbox }
    }

    fn sweep(&mut self, max_age_ns: u64, retry_ns: u64, max: u32) -> Outcome {
        let now = self.clock;
        let max = max as usize;
        let mut todo: Vec<OrderId> = Vec::new();
        // Cancels queued long ago and still open come first, oldest first, then
        // orders that have never had one, oldest first. Both are read off the age
        // indexes, so the cost is the number swept, not the number of open orders.
        if retry_ns > 0 {
            if let Some(limit) = now.checked_sub(retry_ns) {
                todo.extend(
                    self.by_cancel
                        .range(..=(limit, u64::MAX))
                        .take(max)
                        .map(|&(_, id)| id),
                );
            }
        }
        if todo.len() < max {
            if let Some(limit) = now.checked_sub(max_age_ns) {
                let room = max - todo.len();
                todo.extend(
                    self.by_born
                        .range(..=(limit, u64::MAX))
                        .take(room)
                        .map(|&(_, id)| id),
                );
            }
        }
        for id in &todo {
            let o = self.orders.get_mut(id).unwrap();
            if o.cancelling {
                self.by_cancel.remove(&(o.cancel_at, *id));
            } else {
                self.by_born.remove(&(o.born, *id));
            }
            o.cancelling = true;
            o.cancel_at = now;
            self.by_cancel.insert((now, *id));
            let symbol = o.symbol;
            self.out_push(OutMsg::Cancel { order: *id, symbol });
        }
        Outcome::Swept {
            queued: todo.len() as u32,
        }
    }

    fn adjust(
        &mut self,
        account: AccountId,
        asset: AssetId,
        amount: Amount,
        credit: bool,
        reason: u32,
        id: u64,
    ) -> Outcome {
        if amount == 0 {
            return Outcome::Rejected(Reject::BadAmount);
        }
        if self.adjust_ids.contains(&id) {
            return Outcome::Duplicate;
        }
        let b = self.balance(account, asset);
        if credit {
            let tot = self.adjusted_in.get(&asset).copied().unwrap_or(0);
            if b.available.checked_add(amount).is_none() || tot.checked_add(amount).is_none() {
                return Outcome::Rejected(Reject::Overflow);
            }
            self.bal(account, asset).available += amount;
            bump(&mut self.adjusted_in, asset, amount);
        } else {
            if b.available < amount {
                return Outcome::Rejected(Reject::Insufficient);
            }
            self.bal(account, asset).available -= amount;
            bump(&mut self.adjusted_out, asset, amount);
        }
        self.adjust_ids.insert(id);
        self.note_id(2, id);
        if self.adjust_log.len() == ADJUST_LOG {
            self.adjust_log.pop_front();
        }
        self.adjust_log.push_back(Adjustment {
            external_id: id,
            account,
            asset,
            credit,
            amount,
            reason,
        });
        Outcome::Adjusted
    }

    fn fee_withdraw(&mut self, asset: AssetId, amount: Amount, id: u64) -> Outcome {
        if amount == 0 {
            return Outcome::Rejected(Reject::BadAmount);
        }
        if self.withdraw_ids.contains(&id) {
            return Outcome::Rejected(Reject::DuplicateId);
        }
        if self.fees(asset) < amount {
            return Outcome::Rejected(Reject::Insufficient);
        }
        self.withdraw_ids.insert(id);
        self.note_id(1, id);
        *self.fees.get_mut(&asset).unwrap() -= amount;
        self.pending.insert(id, (FEE_ACCOUNT, asset, amount));
        Outcome::WithdrawHeld
    }

    fn bal(&mut self, a: AccountId, asset: AssetId) -> &mut Balance {
        self.balances
            .get_or_insert_with((a, asset), Balance::default)
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
        self.note_id(0, id);
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
        ioc: bool,
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
        self.by_born.insert((self.clock, order));
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
                born: self.clock,
                cancel_at: 0,
            },
        );
        let outbox = self.outbox_next();
        let m = if ioc {
            OutMsg::SubmitIoc {
                order,
                symbol,
                side,
                price,
                qty,
            }
        } else {
            OutMsg::Submit {
                order,
                symbol,
                side,
                price,
                qty,
            }
        };
        self.out_push(m);
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
        o.cancel_at = self.clock;
        let symbol = o.symbol;
        self.by_born.remove(&(o.born, order));
        self.by_cancel.insert((self.clock, order));
        let outbox = self.outbox_next();
        self.out_push(OutMsg::Cancel { order, symbol });
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
        self.note_id(1, id);
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
        } else if account == FEE_ACCOUNT {
            bump(&mut self.fees, asset, amount);
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
                taker_is_buy,
            } => self.settle_trade(buy, sell, price, qty, taker_is_buy),
            Event::Closed { order } => self.settle_closed(order),
            Event::SliceIn {
                account,
                asset,
                amount,
            } => self.settle_slice_in(account, asset, amount),
            Event::SliceOut {
                account,
                asset,
                amount,
            } => self.settle_slice_out(account, asset, amount),
            Event::SliceFee { asset, amount } => {
                bump(&mut self.fees, asset, amount);
                bump(&mut self.settled_in, asset, amount);
                amount > 0
            }
            Event::Returned {
                account,
                asset,
                amount,
            } => self.settle_returned(account, asset, amount),
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

    fn settle_slice_in(&mut self, account: AccountId, asset: AssetId, amount: Amount) -> bool {
        if amount == 0 || account == FEE_ACCOUNT || !self.add_lent(account, asset, amount) {
            return false;
        }
        bump(&mut self.slice_in, asset, amount);
        bump(&mut self.settled_in, asset, amount);
        true
    }

    fn settle_slice_out(&mut self, account: AccountId, asset: AssetId, amount: Amount) -> bool {
        if amount == 0 || !self.sub_lent(account, asset, amount) {
            return false;
        }
        bump(&mut self.slice_out, asset, amount);
        bump(&mut self.settled_out, asset, amount);
        true
    }

    fn settle_returned(&mut self, account: AccountId, asset: AssetId, amount: Amount) -> bool {
        if amount == 0 || !self.sub_lent(account, asset, amount) {
            return false;
        }
        self.bal(account, asset).available += amount;
        bump(&mut self.lent_returned, asset, amount);
        true
    }

    fn settle_closed(&mut self, id: OrderId) -> bool {
        if !self.local(id) {
            return false;
        }
        let Some(o) = self.drop_order(id) else {
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

    fn settle_trade(
        &mut self,
        buy: OrderId,
        sell: OrderId,
        price: u64,
        qty: u64,
        taker_is_buy: bool,
    ) -> bool {
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
            self.apply_buy(buy, &sb.unwrap(), price, qty, taker_is_buy);
        }
        if ls {
            self.apply_sell(sell, &ss.unwrap(), price, qty, !taker_is_buy);
        }
        true
    }

    fn fill(&mut self, id: OrderId, qty: u64, hold_used: Amount) {
        let o = self.orders.get_mut(&id).unwrap();
        o.remaining -= qty;
        o.held -= hold_used;
        if o.remaining == 0 {
            let o = self.drop_order(id).unwrap();
            if o.held > 0 {
                let asset = self.hold_asset(&o);
                let b = self.bal(o.account, asset);
                b.held -= o.held;
                b.available += o.held;
            }
        }
    }

    fn apply_buy(&mut self, id: OrderId, sym: &Symbol, price: u64, qty: u64, taker: bool) {
        let o = self.orders.get(&id).unwrap().clone();
        let held = o.price as u128 * qty as u128;
        let paid = price as u128 * qty as u128;
        let recv = sym.lot * qty as u128;
        let (maker, taker_ppm) = self.cfg.fee_pair(o.symbol);
        let fee = fee_of(recv, if taker { taker_ppm } else { maker });
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

    fn apply_sell(&mut self, id: OrderId, sym: &Symbol, price: u64, qty: u64, taker: bool) {
        let o = self.orders.get(&id).unwrap().clone();
        let sold = sym.lot * qty as u128;
        let recv = price as u128 * qty as u128;
        let (maker, taker_ppm) = self.cfg.fee_pair(o.symbol);
        let fee = fee_of(recv, if taker { taker_ppm } else { maker });
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
        let (mut born, mut canc) = (BTreeSet::new(), BTreeSet::new());
        for (id, o) in self.orders.iter() {
            if o.cancelling {
                canc.insert((o.cancel_at, *id));
            } else {
                born.insert((o.born, *id));
            }
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
            if o.born > self.clock
                || o.cancel_at > self.clock
                || (!o.cancelling && o.cancel_at != 0)
            {
                return Err(format!("order {id:#x} has inconsistent times"));
            }
            *held.entry((o.account, self.hold_asset(o))).or_insert(0) += o.held;
        }
        if born != self.by_born || canc != self.by_cancel {
            return Err("age indexes disagree with the open orders".into());
        }
        for (k, b) in self.balances.iter() {
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
        assets.extend(self.adjusted_in.keys());
        assets.extend(self.adjusted_out.keys());
        assets.extend(self.fees.keys());
        assets.extend(self.lent.keys().map(|k| k.1));
        assets.extend(self.lent_granted.keys());
        for a in assets {
            let (granted, returned, sin, sout) = self.slice_flow(a);
            if self.lent_total(a) + returned + sout != granted + sin {
                return Err(format!(
                    "asset {a}: lent {} + returned {returned} + slice out {sout} != granted {granted} + slice in {sin}",
                    self.lent_total(a)
                ));
            }
            let (dep, wd) = self.external_net(a);
            let (inn, out) = self.settled_flow(a);
            let (ain, aout) = self.adjusted_flow(a);
            let have = self.holdings(a);
            if have + wd + out + aout != dep + inn + ain {
                return Err(format!("asset {a}: holdings {have} + withdrawn {wd} + out {out} + adjusted out {aout} != deposited {dep} + in {inn} + adjusted in {ain}"));
            }
        }
        if self.anomalies != 0 {
            return Err(format!("{} anomalies", self.anomalies));
        }
        Ok(())
    }
}
