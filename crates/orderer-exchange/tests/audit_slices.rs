//! The audit's per-group slice checks, without processes: the identity of each credit
//! group's slice against the ledgers' per-group figures, and the "settled" predicate.

use orderer_exchange::audit::{grants_delivered, slice_identity_violations, slice_settled};
use orderer_ledger::{Cmd, Config, Event, Ledger, Side, Slice, Symbol};
use std::collections::BTreeMap;

const BTC: u32 = 1;
const USD: u32 = 2;
const ASSETS: [u32; 2] = [BTC, USD];

fn config(shard: u16) -> Config {
    let mut symbols = BTreeMap::new();
    symbols.insert(
        10,
        Symbol {
            base: BTC,
            quote: USD,
            lot: 100,
        },
    );
    Config {
        shard,
        fee_ppm: 100_000,
        symbols,
        fees: BTreeMap::new(),
    }
}

/// Two ledger shards (account `a` lives on shard `a % 2`) and two credit groups. Group
/// 0 is lent 300 USD of account 1 (shard 1), then 100 of it comes back; group 1 is lent
/// 250 USD of account 2 (shard 0). Every grant is delivered to its slice once and the
/// return is settled into the ledger through the group's report stream.
struct World {
    ledgers: Vec<Ledger>,
    slices: Vec<Slice>,
}

fn world() -> World {
    let mut ledgers = vec![Ledger::new(config(0)), Ledger::new(config(1))];
    let mut slices = vec![Slice::new(config(0), 0), Slice::new(config(0), 1)];
    for (account, shard) in [(1u64, 1usize), (2, 0)] {
        ledgers[shard].apply(&Cmd::Deposit {
            account,
            asset: USD,
            amount: 1_000,
            external_id: account,
        });
    }
    ledgers[0].apply(&Cmd::Deposit {
        account: 4,
        asset: BTC,
        amount: 10_000,
        external_id: 4,
    });
    for (group, account, shard, asset, amount) in [
        (0u16, 1u64, 1usize, USD, 300u128),
        (1, 2, 0, USD, 250),
        (1, 4, 0, BTC, 1_000),
    ] {
        ledgers[shard].apply(&Cmd::Lend {
            group,
            account,
            asset,
            amount,
        });
        slices[group as usize].grant(account, asset, amount);
    }
    // group 1: account 2 buys 2 and account 4 sells 2 at 95 inside the slice, and
    // the slice's report (slice-in, slice-out) is settled into shard 0
    let buy = slices[1].enter(2, 10, Side::Buy, 100, 2).unwrap();
    let sell = slices[1].enter(4, 10, Side::Sell, 90, 2).unwrap();
    slices[1].settle(&[Event::Trade {
        buy,
        sell,
        price: 95,
        qty: 2,
        taker_is_buy: true,
    }]);
    let report = slices[1].report();
    assert!(report
        .iter()
        .any(|e| matches!(e, Event::SliceOut { amount: 190, .. })));
    let mut prev = 0;
    for (i, event) in report.into_iter().enumerate() {
        let idx = i as u64 + 1;
        ledgers[0].apply(&Cmd::Settle {
            source: 8,
            prev,
            idx,
            event,
        });
        prev = idx;
    }
    let events = slices[0].recall(1, USD, 100);
    assert!(matches!(
        events.last(),
        Some(Event::Returned { amount: 100, .. })
    ));
    // group 0's report stream is source 7; settle its events in order
    let mut prev = 0;
    for (i, event) in events.into_iter().enumerate() {
        let idx = i as u64 + 1;
        ledgers[1].apply(&Cmd::Settle {
            source: 7,
            prev,
            idx,
            event,
        });
        prev = idx;
    }
    World { ledgers, slices }
}

fn violations(w: &World, group: u16, slice: usize) -> Vec<String> {
    slice_identity_violations(group, &ASSETS, &w.slices[slice], &w.ledgers)
}

#[test]
fn a_consistent_two_group_state_passes_for_each_group() {
    let w = world();
    assert_eq!(w.ledgers[1].lent_total_to(USD, 0), 200);
    assert_eq!(w.ledgers[0].lent_total_to(USD, 1), 231); // 250 less the 19 fee the slice paid
    assert!(
        violations(&w, 0, 0).is_empty(),
        "{:?}",
        violations(&w, 0, 0)
    );
    assert!(
        violations(&w, 1, 1).is_empty(),
        "{:?}",
        violations(&w, 1, 1)
    );
}

#[test]
fn money_lent_to_one_group_but_not_granted_to_its_slice_is_found_in_that_group_only() {
    let mut w = world();
    // the ledger lends group 0 another 50 that the slice never receives
    w.ledgers[1].apply(&Cmd::Lend {
        group: 0,
        account: 1,
        asset: USD,
        amount: 50,
    });
    let v = violations(&w, 0, 0);
    assert_eq!(v.len(), 1, "{v:?}");
    assert!(v[0].starts_with("group 0 asset 2:"), "{v:?}");
    assert!(v[0].contains("slice side 200 != ledger side 250"), "{v:?}");
    assert!(violations(&w, 1, 1).is_empty());
}

#[test]
fn a_slice_credited_without_a_lend_is_found() {
    let mut w = world();
    w.slices[1].grant(2, USD, 40);
    let v = violations(&w, 1, 1);
    assert_eq!(v.len(), 1, "{v:?}");
    assert!(v[0].contains("slice side 290 != ledger side 250"), "{v:?}");
    assert!(violations(&w, 0, 0).is_empty());
}

#[test]
fn checking_a_slice_against_the_other_groups_figures_is_found() {
    let w = world();
    // group swap: slice 0 against group 1's ledger side, and the reverse
    assert!(!violations(&w, 1, 0).is_empty());
    assert!(!violations(&w, 0, 1).is_empty());
    // a group nobody was lent anything: the slice side is nonzero, the ledger side zero
    assert!(!violations(&w, 5, 0).is_empty());
}

#[test]
fn a_return_the_ledger_never_settled_is_found() {
    let mut w = world();
    // group 1 gives 60 back, but the Returned never reaches the ledger
    let _lost = w.slices[1].recall(2, USD, 60);
    let v = violations(&w, 1, 1);
    assert_eq!(v.len(), 1, "{v:?}");
    assert!(v[0].contains("slice side 190 != ledger side 250"), "{v:?}");
}

#[test]
fn settled_needs_every_condition() {
    assert!(slice_settled(true, false, false, true, 9, 9));
    assert!(
        !slice_settled(false, false, false, true, 9, 9),
        "streams not caught up"
    );
    assert!(
        !slice_settled(true, true, false, true, 9, 9),
        "unreported slice events"
    );
    assert!(
        !slice_settled(true, false, true, true, 9, 9),
        "group behind its stream"
    );
    assert!(
        !slice_settled(true, false, false, false, 9, 9),
        "grants undelivered"
    );
    assert!(
        !slice_settled(true, false, false, true, 8, 9),
        "group stream not read to its end"
    );
    assert!(
        !slice_settled(true, false, false, true, 10, 9),
        "group ahead of the stream read"
    );
}

#[test]
fn grants_delivered_is_per_group_and_per_shard() {
    let w = world();
    let next: Vec<u64> = w.ledgers.iter().map(|l| l.outbox_next()).collect();
    assert!(next.iter().all(|&n| n >= 1));
    // cursors[s][g]: both groups level with every shard's outbox
    let level: Vec<Vec<u64>> = next.iter().map(|&n| vec![n, n]).collect();
    assert!(grants_delivered(&w.ledgers, &level, 0));
    assert!(grants_delivered(&w.ledgers, &level, 1));
    // group 1 one behind on shard 0 only
    let mut behind = level.clone();
    behind[0][1] -= 1;
    assert!(grants_delivered(&w.ledgers, &behind, 0));
    assert!(!grants_delivered(&w.ledgers, &behind, 1));
    // a group index with no cursor counts as zero (not delivered)
    assert!(!grants_delivered(&w.ledgers, &level, 2));
}
