//! The slice policy as a pure function.

use orderer_ledger::policy::{plan, PolicyConfig, PolicyState};
use orderer_ledger::*;
use std::collections::BTreeMap;

const BTC: u32 = 1;
const USD: u32 = 2;

fn ledger() -> Ledger {
    let mut symbols = BTreeMap::new();
    symbols.insert(
        7,
        Symbol {
            base: BTC,
            quote: USD,
            lot: 10,
        },
    );
    let mut l = Ledger::new(Config {
        shard: 0,
        fee_ppm: 0,
        symbols,
        fees: BTreeMap::new(),
    });
    for a in 1..=4u64 {
        l.apply(&Cmd::Deposit {
            account: a,
            asset: USD,
            amount: 1_000,
            external_id: a,
        });
    }
    l
}

fn cfg() -> PolicyConfig {
    PolicyConfig {
        chunk: [(USD, 400)].into(),
        low_water_pct: 25,
        account_cap: [(USD, 700)].into(),
        group_cap: [(USD, 1_000)].into(),
        idle_ns: 100,
        max_actions: 16,
    }
}

fn none(_: u64, _: u32) -> Option<u128> {
    None
}

#[test]
fn first_use_lends_one_chunk_within_the_account_balance() {
    let mut l = ledger();
    let mut st = PolicyState::new();
    let cmds = plan(&cfg(), &mut st, &l, &none, &[(1, USD)], 0);
    assert_eq!(
        cmds,
        vec![Cmd::Lend {
            account: 1,
            asset: USD,
            amount: 400
        }]
    );
    // a poor account gets what it has
    l.apply(&Cmd::Withdraw {
        account: 2,
        asset: USD,
        amount: 800,
        external_id: 9,
    });
    let cmds = plan(&cfg(), &mut st, &l, &none, &[(2, USD)], 0);
    assert_eq!(
        cmds,
        vec![Cmd::Lend {
            account: 2,
            asset: USD,
            amount: 200
        }]
    );
    // an asset with no chunk is never lent, and an empty account gets nothing
    assert!(plan(&cfg(), &mut st, &l, &none, &[(1, BTC)], 0).is_empty());
    l.apply(&Cmd::Withdraw {
        account: 3,
        asset: USD,
        amount: 1_000,
        external_id: 10,
    });
    assert!(plan(&cfg(), &mut st, &l, &none, &[(3, USD)], 0).is_empty());
}

#[test]
fn refill_needs_demand_and_a_credit_below_the_low_water_mark() {
    let mut l = ledger();
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 400,
    });
    let mut st = PolicyState::new();
    // low water is 25% of 400 = 100
    let credit_at = |c: u128| move |_: u64, _: u32| Some(c);
    assert!(plan(&cfg(), &mut st, &l, &credit_at(100), &[(1, USD)], 0).is_empty());
    let cmds = plan(&cfg(), &mut st, &l, &credit_at(99), &[(1, USD)], 0);
    assert_eq!(
        cmds,
        vec![Cmd::Lend {
            account: 1,
            asset: USD,
            amount: 300
        }],
        "limited by the per-account cap of 700"
    );
    // not demanded: no refill however low
    assert!(plan(&cfg(), &mut st, &l, &credit_at(0), &[], 0).is_empty());
    // unknown credit never refills
    assert!(plan(&cfg(), &mut st, &l, &none, &[(1, USD)], 0).is_empty());
}

#[test]
fn caps_hold_across_one_plan() {
    let l = ledger();
    let mut st = PolicyState::new();
    let cmds = plan(
        &cfg(),
        &mut st,
        &l,
        &none,
        &[(1, USD), (2, USD), (3, USD), (4, USD)],
        0,
    );
    // 400 + 400 + 200 reaches the group cap of 1,000; the fourth gets nothing
    let amounts: Vec<u128> = cmds
        .iter()
        .map(|c| match c {
            Cmd::Lend { amount, .. } => *amount,
            c => panic!("{c:?}"),
        })
        .collect();
    assert_eq!(amounts, vec![400, 400, 200]);
    // and every proposed lend is accepted by the ledger
    let mut l = l;
    for c in &cmds {
        assert!(matches!(l.apply(c), Outcome::Lent { .. }), "{c:?}");
    }
    l.check().unwrap();
    // a duplicate demand entry is one lend
    let l = ledger();
    let cmds = plan(
        &cfg(),
        &mut PolicyState::new(),
        &l,
        &none,
        &[(1, USD), (1, USD)],
        0,
    );
    assert_eq!(cmds.len(), 1);
}

#[test]
fn max_actions_bounds_a_plan() {
    let l = ledger();
    let mut c = cfg();
    c.max_actions = 2;
    let cmds = plan(
        &c,
        &mut PolicyState::new(),
        &l,
        &none,
        &[(1, USD), (2, USD), (3, USD)],
        0,
    );
    assert_eq!(cmds.len(), 2);
}

#[test]
fn an_unmoving_slice_with_free_credit_is_recalled_once_per_idle_period() {
    let mut l = ledger();
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 400,
    });
    let mut st = PolicyState::new();
    let credit = |_: u64, _: u32| Some(300u128);
    assert!(
        plan(&cfg(), &mut st, &l, &credit, &[], 0).is_empty(),
        "first sight starts the clock"
    );
    assert!(plan(&cfg(), &mut st, &l, &credit, &[], 99).is_empty());
    let cmds = plan(&cfg(), &mut st, &l, &credit, &[], 100);
    assert_eq!(
        cmds,
        vec![Cmd::Recall {
            account: 1,
            asset: USD,
            amount: 300
        }],
        "only the free credit, never the committed part"
    );
    // not again until another idle period passes
    assert!(plan(&cfg(), &mut st, &l, &credit, &[], 150).is_empty());
    assert_eq!(plan(&cfg(), &mut st, &l, &credit, &[], 200).len(), 1);
}

#[test]
fn movement_resets_the_idle_clock_and_nothing_free_is_not_recalled() {
    let mut l = ledger();
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 400,
    });
    let mut st = PolicyState::new();
    let _ = plan(&cfg(), &mut st, &l, &|_, _| Some(300), &[], 0);
    // the credit moved (some was spent), so the clock restarts at 90
    assert!(plan(&cfg(), &mut st, &l, &|_, _| Some(250), &[], 90).is_empty());
    assert!(plan(&cfg(), &mut st, &l, &|_, _| Some(250), &[], 189).is_empty());
    assert_eq!(
        plan(&cfg(), &mut st, &l, &|_, _| Some(250), &[], 190).len(),
        1
    );
    // all credit committed: nothing to give back, however idle
    let mut st = PolicyState::new();
    let _ = plan(&cfg(), &mut st, &l, &|_, _| Some(0), &[], 0);
    assert!(plan(&cfg(), &mut st, &l, &|_, _| Some(0), &[], 10_000).is_empty());
    // a slice that was fully returned is forgotten
    let mut l2 = ledger();
    l2.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 400,
    });
    let mut st = PolicyState::new();
    let _ = plan(&cfg(), &mut st, &l2, &|_, _| Some(1), &[], 0);
    assert_eq!(st.tracked(), 1);
    let l3 = ledger();
    let _ = plan(&cfg(), &mut st, &l3, &|_, _| Some(1), &[], 1);
    assert_eq!(st.tracked(), 0);
}

#[test]
fn losing_the_state_only_restarts_the_idle_clocks() {
    let mut l = ledger();
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 400,
    });
    let credit = |_: u64, _: u32| Some(300u128);
    let mut st = PolicyState::new();
    let _ = plan(&cfg(), &mut st, &l, &credit, &[], 0);
    assert_eq!(plan(&cfg(), &mut st, &l, &credit, &[], 100).len(), 1);
    // a restarted driver does not recall at once on its first look
    let mut fresh = PolicyState::new();
    assert!(plan(&cfg(), &mut fresh, &l, &credit, &[], 150).is_empty());
    assert_eq!(plan(&cfg(), &mut fresh, &l, &credit, &[], 250).len(), 1);
}

#[test]
fn idle_zero_never_returns() {
    let mut l = ledger();
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 400,
    });
    let mut c = cfg();
    c.idle_ns = 0;
    let mut st = PolicyState::new();
    let _ = plan(&c, &mut st, &l, &|_, _| Some(300), &[], 0);
    assert!(plan(&c, &mut st, &l, &|_, _| Some(300), &[], u64::MAX).is_empty());
}

#[test]
fn the_group_cap_counts_what_is_already_lent() {
    let mut l = ledger();
    // 700 already lent by accounts 1 and 2 (within each account's cap)
    for (a, amt) in [(1u64, 400u128), (2, 300)] {
        l.apply(&Cmd::Lend {
            account: a,
            asset: USD,
            amount: amt,
        });
    }
    let cmds = plan(
        &cfg(),
        &mut PolicyState::new(),
        &l,
        &none,
        &[(3, USD), (4, USD)],
        0,
    );
    // only 300 of the group's 1,000 is left, for the first account that asks
    assert_eq!(
        cmds,
        vec![Cmd::Lend {
            account: 3,
            asset: USD,
            amount: 300
        }]
    );
}
