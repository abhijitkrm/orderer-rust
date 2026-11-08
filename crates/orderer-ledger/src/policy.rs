//! Credit-slice policy as a pure function (`CREDIT-SLICES.md` section 8): when to
//! lend an account's funds to the matching group, when to top a slice up, and
//! when to take idle credit back. It only *proposes* ledger commands (`Lend`,
//! `Recall`); the ledger enforces what is safe (`available` is checked, a recall
//! can only return uncommitted credit), so a stale or wrong view costs efficiency,
//! never money. That is also why a driver may keep this state in memory and lose
//! it at any time: a fresh [`PolicyState`] merely restarts the idle clocks.
//!
//! Rules, per `(account, asset)` with a configured chunk:
//! - **first use**: an account that is about to trade (`demand`) and has nothing
//!   lent gets `chunk`;
//! - **refill**: a slice whose free credit has fallen below the low-water mark
//!   (`low_water_pct` of a chunk) and whose account is demanded gets another chunk;
//! - **caps**: a grant never takes the account's lent total above `account_cap`, nor
//!   the group's above `group_cap`, nor more than the account's `available`;
//! - **idle return**: a slice whose lent total and free credit have not moved for
//!   `idle_ns` while credit is free is recalled (the free part; committed funds are
//!   never recalled).

use crate::ledger::{AccountId, Amount, AssetId, Cmd, Ledger};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct PolicyConfig {
    /// The credit group this policy feeds; `Lend`/`Recall` name it, and every
    /// "lent" figure it uses is the ledger's for this group alone.
    pub group: u16,
    /// Grant size by asset; an asset without one is never lent.
    pub chunk: BTreeMap<AssetId, Amount>,
    /// Refill when free credit is below this percent of the chunk.
    pub low_water_pct: u32,
    /// Most one account may have lent of an asset (absent: unlimited).
    pub account_cap: BTreeMap<AssetId, Amount>,
    /// Most all accounts together may have lent of an asset to this group (absent: unlimited).
    pub group_cap: BTreeMap<AssetId, Amount>,
    /// Return free credit after this long without movement (0: never).
    pub idle_ns: u64,
    /// Most commands one call proposes.
    pub max_actions: usize,
}

/// What the policy remembers between calls. Disposable.
#[derive(Clone, Debug, Default)]
pub struct PolicyState {
    seen: BTreeMap<(AccountId, AssetId), Seen>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Seen {
    lent: Amount,
    credit: Amount,
    since: u64,
    /// When a recall was last proposed for this unchanged slice.
    recalled: Option<u64>,
}

impl PolicyState {
    pub fn new() -> PolicyState {
        PolicyState::default()
    }
    pub fn tracked(&self) -> usize {
        self.seen.len()
    }
}

/// `pct` percent of `a`, without overflow.
fn pct(a: Amount, pct: u32) -> Amount {
    a / 100 * pct as u128 + a % 100 * pct as u128 / 100
}

/// Propose commands for `ledger` at time `now` (any monotonic unit; `idle_ns` is in
/// the same one). `demand` lists accounts that are about to trade, with the asset
/// they will spend; `credit(account, asset)` is the group's free credit for the
/// slice (`None` when unknown, which suppresses refills and idle returns for it).
pub fn plan(
    cfg: &PolicyConfig,
    st: &mut PolicyState,
    ledger: &Ledger,
    credit: &dyn Fn(AccountId, AssetId) -> Option<Amount>,
    demand: &[(AccountId, AssetId)],
    now: u64,
) -> Vec<Cmd> {
    let mut out: Vec<Cmd> = Vec::new();
    // what this call has already decided to lend, so caps and balances hold
    let mut planned: BTreeMap<(AccountId, AssetId), Amount> = BTreeMap::new();
    let mut group_planned: BTreeMap<AssetId, Amount> = BTreeMap::new();

    let mut seen_demand = std::collections::BTreeSet::new();
    for &(account, asset) in demand {
        if out.len() >= cfg.max_actions {
            break;
        }
        if !seen_demand.insert((account, asset)) {
            continue;
        }
        let Some(&chunk) = cfg.chunk.get(&asset) else {
            continue;
        };
        let lent = ledger.lent_to(account, asset, cfg.group);
        let want = if lent == 0 {
            true
        } else {
            match credit(account, asset) {
                Some(c) => c < pct(chunk, cfg.low_water_pct),
                None => false,
            }
        };
        if !want {
            continue;
        }
        let account_room = cfg
            .account_cap
            .get(&asset)
            .map_or(Amount::MAX, |c| c.saturating_sub(lent));
        let group_room = cfg.group_cap.get(&asset).map_or(Amount::MAX, |c| {
            c.saturating_sub(
                ledger.lent_total_to(asset, cfg.group)
                    + group_planned.get(&asset).copied().unwrap_or(0),
            )
        });
        let avail = ledger.balance(account, asset).available;
        let amount = chunk.min(account_room).min(group_room).min(avail);
        if amount == 0 {
            continue;
        }
        *planned.entry((account, asset)).or_insert(0) += amount;
        *group_planned.entry(asset).or_insert(0) += amount;
        out.push(Cmd::Lend {
            group: cfg.group,
            account,
            asset,
            amount,
        });
    }

    // idle returns, and forgetting slices that no longer exist
    let live: BTreeMap<(AccountId, AssetId), Amount> = ledger.lent_entries_of(cfg.group).collect();
    st.seen.retain(|k, _| live.contains_key(k));
    for (&k, &lent) in &live {
        let Some(c) = credit(k.0, k.1) else {
            continue;
        };
        let e = st.seen.entry(k).or_insert(Seen {
            lent,
            credit: c,
            since: now,
            recalled: None,
        });
        if e.lent != lent || e.credit != c {
            *e = Seen {
                lent,
                credit: c,
                since: now,
                recalled: None,
            };
            continue;
        }
        if cfg.idle_ns == 0
            || c == 0
            || now.saturating_sub(e.since) < cfg.idle_ns
            || e.recalled
                .is_some_and(|r| now.saturating_sub(r) < cfg.idle_ns)
            || out.len() >= cfg.max_actions
            || planned.contains_key(&k)
        {
            continue;
        }
        e.recalled = Some(now);
        out.push(Cmd::Recall {
            group: cfg.group,
            account: k.0,
            asset: k.1,
            amount: c,
        });
    }
    out
}
