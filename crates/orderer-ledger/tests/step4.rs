//! Step 4: clock, reconciler, per-symbol maker/taker fees, fee withdrawal,
//! adjustments, and their wire formats.

use orderer_ledger::*;
use std::collections::BTreeMap;

const BTC: u32 = 1;
const USD: u32 = 2;
const SYM: u32 = 7;
const SYM2: u32 = 8;

fn cfg(shard: u16) -> Config {
    let mut symbols = BTreeMap::new();
    symbols.insert(
        SYM,
        Symbol {
            base: BTC,
            quote: USD,
            lot: 1_000_000,
        },
    );
    symbols.insert(
        SYM2,
        Symbol {
            base: BTC,
            quote: USD,
            lot: 1_000_000,
        },
    );
    let mut fees = BTreeMap::new();
    fees.insert(SYM, (1_000, 3_000)); // maker 0.1%, taker 0.3%
    Config {
        shard,
        fee_ppm: 2_000,
        symbols,
        fees,
    } // SYM2 falls back to 0.2% for both
}

fn dep(l: &mut Ledger, account: u64, asset: u32, amount: u128, id: u64) {
    assert_eq!(
        l.apply(&Cmd::Deposit {
            account,
            asset,
            amount,
            external_id: id
        }),
        Outcome::Deposited
    );
}

fn reserve(l: &mut Ledger, account: u64, symbol: u32, side: Side, price: u64, qty: u64) -> u64 {
    match l.apply(&Cmd::Reserve {
        account,
        symbol,
        side,
        price,
        qty,
    }) {
        Outcome::Reserved { order, .. } => order,
        o => panic!("{o:?}"),
    }
}

fn clock(l: &mut Ledger, now_ns: u64) {
    assert_eq!(l.apply(&Cmd::Clock { now_ns }), Outcome::Ticked);
}

#[test]
fn the_clock_only_moves_forward_and_orders_remember_their_birth() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000, 1);
    clock(&mut l, 500);
    clock(&mut l, 100);
    assert_eq!(l.clock(), 500);
    let o = reserve(&mut l, 1, SYM, Side::Buy, 10, 1);
    assert_eq!(l.order(o).unwrap().born, 500);
    clock(&mut l, 900);
    assert_eq!(l.order(o).unwrap().born, 500);
    let snap = l.snapshot();
    let mut r = Ledger::new(cfg(0));
    r.restore(&snap).unwrap();
    assert_eq!(r.clock(), 900);
    assert_eq!(r.digest(), l.digest());
    l.check().unwrap();
}

#[test]
fn sweep_cancels_old_orders_once_and_retries_on_request() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 10_000, 1);
    clock(&mut l, 1_000);
    let old = reserve(&mut l, 1, SYM, Side::Buy, 10, 1);
    clock(&mut l, 5_000);
    let young = reserve(&mut l, 1, SYM, Side::Buy, 10, 1);
    clock(&mut l, 6_000);
    let held = l.balance(1, USD);
    let out0 = l.outbox_next();
    // old is 5000 old, young 1000
    let sweep = |l: &mut Ledger, age, retry, max| {
        l.apply(&Cmd::Sweep {
            max_age_ns: age,
            retry_ns: retry,
            max,
        })
    };
    assert_eq!(sweep(&mut l, 3_000, 0, 10), Outcome::Swept { queued: 1 });
    assert_eq!(l.outbox_next(), out0 + 1);
    assert!(l.order(old).unwrap().cancelling && !l.order(young).unwrap().cancelling);
    // the hold stays until matching says the order is gone
    assert_eq!(l.balance(1, USD), held);
    // again at the same time: nothing new (cancel already queued, no retry asked)
    assert_eq!(sweep(&mut l, 3_000, 0, 10), Outcome::Swept { queued: 0 });
    // a retry age that has not elapsed re-queues nothing; one that has, does
    clock(&mut l, 7_000);
    assert_eq!(
        sweep(&mut l, 3_000, 2_000, 10),
        Outcome::Swept { queued: 0 }
    );
    clock(&mut l, 8_000);
    // old: cancel queued at 6000, now 8000 -> retry; young: 3000 old -> first cancel
    assert_eq!(
        sweep(&mut l, 3_000, 2_000, 10),
        Outcome::Swept { queued: 2 }
    );
    // a user cancel on an order the sweep already cancelled is refused as usual
    assert_eq!(
        l.apply(&Cmd::Cancel {
            account: 1,
            order: old
        }),
        Outcome::Rejected(Reject::AlreadyCancelling)
    );
    // Closed events release the holds
    for (i, o) in [old, young].iter().enumerate() {
        let prev = i as u64;
        assert_eq!(
            l.apply(&Cmd::Settle {
                source: 0,
                prev,
                idx: prev + 1,
                event: Event::Closed { order: *o }
            }),
            Outcome::Settled
        );
    }
    assert_eq!(
        l.balance(1, USD),
        Balance {
            available: 10_000,
            held: 0
        }
    );
    l.check().unwrap();
}

#[test]
fn sweep_respects_max_and_visits_orders_in_id_order() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 10_000, 1);
    let ids: Vec<u64> = (0..5)
        .map(|_| reserve(&mut l, 1, SYM, Side::Buy, 1, 1))
        .collect();
    clock(&mut l, 100);
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 1,
            retry_ns: 0,
            max: 2
        }),
        Outcome::Swept { queued: 2 }
    );
    assert!(l.order(ids[0]).unwrap().cancelling && l.order(ids[1]).unwrap().cancelling);
    assert!(!l.order(ids[2]).unwrap().cancelling);
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 1,
            retry_ns: 0,
            max: 0
        }),
        Outcome::Swept { queued: 0 }
    );
    // each sweep moves on to the next ones
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 1,
            retry_ns: 0,
            max: 10
        }),
        Outcome::Swept { queued: 3 }
    );
    l.check().unwrap();
}

#[test]
fn maker_and_taker_pay_their_own_rates_per_symbol() {
    // SYM: maker 0.1%, taker 0.3%. 1 lot = 1_000_000 base atoms; price 2 => 2 quote atoms per lot... use large price.
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 100_000_000, 1);
    dep(&mut l, 2, BTC, 100_000_000, 2);
    // resting sell (maker), aggressive buy (taker)
    let sell = reserve(&mut l, 2, SYM, Side::Sell, 1_000_000, 10);
    let buy = reserve(&mut l, 1, SYM, Side::Buy, 1_000_000, 10);
    let ev = Event::Trade {
        buy,
        sell,
        price: 1_000_000,
        qty: 10,
        taker_is_buy: true,
    };
    assert_eq!(
        l.apply(&Cmd::Settle {
            source: 0,
            prev: 0,
            idx: 1,
            event: ev
        }),
        Outcome::Settled
    );
    // buyer (taker) receives 10_000_000 base less 0.3% = 30_000
    assert_eq!(l.balance(1, BTC).available, 10_000_000 - 30_000);
    // seller (maker) receives 10_000_000 quote less 0.1% = 10_000
    assert_eq!(l.balance(2, USD).available, 10_000_000 - 10_000);
    assert_eq!((l.fees(BTC), l.fees(USD)), (30_000, 10_000));
    l.check().unwrap();

    // the same trade with the sell as aggressor swaps the rates
    let sell = reserve(&mut l, 2, SYM, Side::Sell, 1_000_000, 10);
    let buy = reserve(&mut l, 1, SYM, Side::Buy, 1_000_000, 10);
    let ev = Event::Trade {
        buy,
        sell,
        price: 1_000_000,
        qty: 10,
        taker_is_buy: false,
    };
    let (b0, s0) = (l.balance(1, BTC).available, l.balance(2, USD).available);
    assert_eq!(
        l.apply(&Cmd::Settle {
            source: 0,
            prev: 1,
            idx: 2,
            event: ev
        }),
        Outcome::Settled
    );
    assert_eq!(l.balance(1, BTC).available - b0, 10_000_000 - 10_000); // buyer is maker
    assert_eq!(l.balance(2, USD).available - s0, 10_000_000 - 30_000); // seller is taker
    l.check().unwrap();
}

#[test]
fn a_symbol_without_a_schedule_uses_the_default_for_both_sides() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 100_000_000, 1);
    dep(&mut l, 2, BTC, 100_000_000, 2);
    let sell = reserve(&mut l, 2, SYM2, Side::Sell, 1_000_000, 10);
    let buy = reserve(&mut l, 1, SYM2, Side::Buy, 1_000_000, 10);
    let ev = Event::Trade {
        buy,
        sell,
        price: 1_000_000,
        qty: 5,
        taker_is_buy: true,
    };
    assert_eq!(
        l.apply(&Cmd::Settle {
            source: 0,
            prev: 0,
            idx: 1,
            event: ev
        }),
        Outcome::Settled
    );
    // 5 lots = 5_000_000 atoms, 0.2% = 10_000 each side
    assert_eq!((l.fees(BTC), l.fees(USD)), (10_000, 10_000));
    l.check().unwrap();
}

#[test]
fn fees_are_withdrawn_through_the_pending_machinery_and_conserved() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 100_000_000, 1);
    dep(&mut l, 2, BTC, 100_000_000, 2);
    let sell = reserve(&mut l, 2, SYM, Side::Sell, 1_000_000, 10);
    let buy = reserve(&mut l, 1, SYM, Side::Buy, 1_000_000, 10);
    let ev = Event::Trade {
        buy,
        sell,
        price: 1_000_000,
        qty: 10,
        taker_is_buy: true,
    };
    l.apply(&Cmd::Settle {
        source: 0,
        prev: 0,
        idx: 1,
        event: ev,
    });
    assert_eq!(l.fees(BTC), 30_000);
    let fw = |asset, amount, id| Cmd::FeeWithdraw {
        asset,
        amount,
        external_id: id,
    };
    assert_eq!(
        l.apply(&fw(BTC, 30_001, 1)),
        Outcome::Rejected(Reject::Insufficient)
    );
    assert_eq!(
        l.apply(&fw(BTC, 0, 1)),
        Outcome::Rejected(Reject::BadAmount)
    );
    assert_eq!(l.apply(&fw(BTC, 20_000, 1)), Outcome::WithdrawHeld);
    assert_eq!(
        l.apply(&fw(BTC, 1, 1)),
        Outcome::Rejected(Reject::DuplicateId)
    );
    assert_eq!(l.fees(BTC), 10_000);
    l.check().unwrap(); // pending fees still count as held funds
                        // an unpaid withdrawal goes back to the pool, a paid one leaves the system
    assert_eq!(
        l.apply(&Cmd::Finalize {
            external_id: 1,
            paid: false
        }),
        Outcome::WithdrawDone
    );
    assert_eq!(l.fees(BTC), 30_000);
    assert_eq!(l.apply(&fw(BTC, 30_000, 2)), Outcome::WithdrawHeld);
    assert_eq!(
        l.apply(&Cmd::Finalize {
            external_id: 2,
            paid: true
        }),
        Outcome::WithdrawDone
    );
    assert_eq!(l.fees(BTC), 0);
    assert_eq!(l.external_net(BTC).1, 30_000);
    l.check().unwrap();
    // a fee withdrawal id and a user withdrawal id share one namespace
    dep(&mut l, 3, USD, 10, 3);
    assert_eq!(
        l.apply(&Cmd::Withdraw {
            account: 3,
            asset: USD,
            amount: 1,
            external_id: 2
        }),
        Outcome::Rejected(Reject::DuplicateId)
    );
}

#[test]
fn the_fee_account_cannot_be_named_by_user_commands() {
    let mut l = Ledger::new(cfg(0));
    let r = Outcome::Rejected(Reject::ReservedAccount);
    assert_eq!(
        l.apply(&Cmd::Deposit {
            account: FEE_ACCOUNT,
            asset: USD,
            amount: 5,
            external_id: 1
        }),
        r
    );
    assert_eq!(
        l.apply(&Cmd::Reserve {
            account: FEE_ACCOUNT,
            symbol: SYM,
            side: Side::Buy,
            price: 1,
            qty: 1
        }),
        r
    );
    assert_eq!(
        l.apply(&Cmd::Withdraw {
            account: FEE_ACCOUNT,
            asset: USD,
            amount: 5,
            external_id: 1
        }),
        r
    );
    assert_eq!(
        l.apply(&Cmd::Adjust {
            account: FEE_ACCOUNT,
            asset: USD,
            amount: 5,
            credit: true,
            reason: 1,
            external_id: 1
        }),
        r
    );
    assert_eq!(l.balance(FEE_ACCOUNT, USD), Balance::default());
    l.check().unwrap();
}

#[test]
fn adjustments_are_idempotent_bounded_by_zero_audited_and_conserved() {
    let mut l = Ledger::new(cfg(0));
    let adj = |account, amount, credit, reason, id| Cmd::Adjust {
        account,
        asset: USD,
        amount,
        credit,
        reason,
        external_id: id,
    };
    assert_eq!(l.apply(&adj(1, 100, true, 77, 1)), Outcome::Adjusted);
    assert_eq!(l.apply(&adj(1, 100, true, 77, 1)), Outcome::Duplicate);
    assert_eq!(l.balance(1, USD).available, 100);
    assert_eq!(
        l.apply(&adj(1, 101, false, 78, 2)),
        Outcome::Rejected(Reject::Insufficient)
    );
    assert_eq!(
        l.apply(&adj(1, 0, true, 78, 2)),
        Outcome::Rejected(Reject::BadAmount)
    );
    // a rejected id is not consumed
    assert_eq!(l.apply(&adj(1, 40, false, 78, 2)), Outcome::Adjusted);
    assert_eq!(l.balance(1, USD).available, 60);
    assert_eq!(l.adjusted_flow(USD), (100, 40));
    l.check().unwrap();
    let log: Vec<_> = l.adjustments().copied().collect();
    assert_eq!(log.len(), 2);
    assert_eq!(
        (
            log[1].external_id,
            log[1].reason,
            log[1].credit,
            log[1].amount
        ),
        (2, 78, false, 40)
    );
    // funds held by an order cannot be adjusted away
    l.apply(&Cmd::Reserve {
        account: 1,
        symbol: SYM,
        side: Side::Buy,
        price: 10,
        qty: 6,
    });
    assert_eq!(
        l.apply(&adj(1, 1, false, 1, 3)),
        Outcome::Rejected(Reject::Insufficient)
    );
    // the audit ring is bounded and keeps the newest
    for i in 0..(ADJUST_LOG as u64 + 10) {
        l.apply(&adj(2, 1, true, 5, 1000 + i));
    }
    assert_eq!(l.adjustments().count(), ADJUST_LOG);
    assert_eq!(
        l.adjustments().last().unwrap().external_id,
        1000 + ADJUST_LOG as u64 + 9
    );
    l.check().unwrap();
    // it survives a snapshot
    let mut r = Ledger::new(cfg(0));
    r.restore(&l.snapshot()).unwrap();
    assert_eq!(r.digest(), l.digest());
    assert_eq!(r.adjustments().count(), ADJUST_LOG);
}

fn le(parts: &[(u128, usize)]) -> Vec<u8> {
    parts
        .iter()
        .flat_map(|(v, n)| v.to_le_bytes()[..*n].to_vec())
        .collect()
}

#[test]
fn new_commands_have_pinned_encodings_and_round_trip() {
    let cases: Vec<(Cmd, Vec<u8>)> = vec![
        (Cmd::Clock { now_ns: 0x0102 }, le(&[(8, 1), (0x0102, 8)])),
        (
            Cmd::Sweep {
                max_age_ns: 3,
                retry_ns: 4,
                max: 5,
            },
            le(&[(9, 1), (3, 8), (4, 8), (5, 4)]),
        ),
        (
            Cmd::Adjust {
                account: 1,
                asset: 2,
                amount: 3,
                credit: true,
                reason: 4,
                external_id: 5,
            },
            le(&[(10, 1), (1, 8), (2, 4), (1, 1), (3, 16), (4, 4), (5, 8)]),
        ),
        (
            Cmd::FeeWithdraw {
                asset: 2,
                amount: 3,
                external_id: 4,
            },
            le(&[(11, 1), (2, 4), (3, 16), (4, 8)]),
        ),
        (
            Cmd::Settle {
                source: 1,
                prev: 2,
                idx: 3,
                event: Event::Trade {
                    buy: 4,
                    sell: 5,
                    price: 6,
                    qty: 7,
                    taker_is_buy: true,
                },
            },
            le(&[
                (4, 1),
                (1, 4),
                (2, 8),
                (3, 8),
                (1, 1),
                (4, 8),
                (5, 8),
                (6, 8),
                (7, 8),
                (1, 1),
            ]),
        ),
    ];
    for (c, golden) in cases {
        let b = encode_cmd(&c);
        assert_eq!(b, golden, "{c:?}");
        assert_eq!(decode_cmd(&b).unwrap(), c);
        for cut in 0..b.len() {
            assert!(decode_cmd(&b[..cut]).is_err());
        }
    }
    // a bad taker flag is refused
    let mut b = encode_cmd(&Cmd::Settle {
        source: 0,
        prev: 0,
        idx: 1,
        event: Event::Trade {
            buy: 1,
            sell: 2,
            price: 1,
            qty: 1,
            taker_is_buy: false,
        },
    });
    *b.last_mut().unwrap() = 2;
    assert!(decode_cmd(&b).is_err());
}

#[test]
fn a_snapshot_with_impossible_times_is_refused() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000, 1);
    clock(&mut l, 50);
    reserve(&mut l, 1, SYM, Side::Buy, 10, 1);
    let snap = l.snapshot();
    // the clock is the u64 after magic(4) shard(2) next_order(8) outbox_base(8) anomalies(8)
    let at = 4 + 2 + 8 + 8 + 8;
    let mut bad = snap.clone();
    bad[at..at + 8].copy_from_slice(&10u64.to_le_bytes()); // clock before the order's birth
    assert!(Ledger::new(cfg(0)).restore(&bad).is_err());
    Ledger::new(cfg(0)).restore(&snap).unwrap();
}
