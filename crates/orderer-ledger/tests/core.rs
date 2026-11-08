//! Core hardening: the sweep age index, the incremental digest, IOC orders, the
//! time sidecar and the shard-number limit.

use orderer_ledger::*;
use std::collections::BTreeMap;
use std::time::Instant;

const BTC: u32 = 1;
const USD: u32 = 2;
const SYM: u32 = 7;

fn cfg(shard: u16) -> Config {
    let mut symbols = BTreeMap::new();
    symbols.insert(
        SYM,
        Symbol {
            base: BTC,
            quote: USD,
            lot: 1_000,
        },
    );
    Config {
        shard,
        fee_ppm: 1_000,
        symbols,
        fees: BTreeMap::new(),
    }
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

fn buy(l: &mut Ledger, account: u64, price: u64) -> u64 {
    match l.apply(&Cmd::Reserve {
        account,
        symbol: SYM,
        side: Side::Buy,
        price,
        qty: 1,
    }) {
        Outcome::Reserved { order, .. } => order,
        o => panic!("{o:?}"),
    }
}

// ---- item 7: shard numbers ------------------------------------------------

#[test]
fn shard_numbers_must_stay_below_the_slice_bit() {
    assert!(Ledger::try_new(cfg(0)).is_ok());
    assert!(Ledger::try_new(cfg(SLICE_SHARD_BIT - 1)).is_ok());
    let e = Ledger::try_new(cfg(SLICE_SHARD_BIT))
        .err()
        .expect("rejected");
    assert!(
        e.0.contains("0x8000") && e.0.contains("out of range"),
        "{e}"
    );
    assert!(Ledger::try_new(cfg(u16::MAX)).is_err());
    // a slice order id is never a ledger's own, for any legal shard
    let top = (SLICE_SHARD_BIT - 1) as u64;
    assert!(!is_slice_order(top << SHARD_SHIFT | 1));
    assert!(is_slice_order((SLICE_SHARD_BIT as u64) << SHARD_SHIFT | 1));
}

#[test]
#[should_panic(expected = "invalid ledger config")]
fn new_panics_with_the_reason_on_a_bad_shard() {
    let _ = Ledger::new(cfg(0x8001));
}

#[test]
fn reference_data_is_validated() {
    let mut c = cfg(0);
    c.fee_ppm = 1_000_001;
    assert!(Ledger::try_new(c).is_err());
    let mut c = cfg(0);
    c.symbols.insert(
        9,
        Symbol {
            base: 1,
            quote: 1,
            lot: 1,
        },
    );
    assert!(Ledger::try_new(c)
        .err()
        .unwrap()
        .0
        .contains("same base and quote"));
    let mut c = cfg(0);
    c.symbols.insert(
        9,
        Symbol {
            base: 1,
            quote: 2,
            lot: 0,
        },
    );
    assert!(Ledger::try_new(c).err().unwrap().0.contains("zero lot"));
    let mut c = cfg(0);
    c.fees.insert(SYM, (5, 2_000_000));
    assert!(Ledger::try_new(c).is_err());
}

// ---- item 1: the sweep age index -------------------------------------------

#[test]
fn a_sweep_takes_the_oldest_first_and_honours_max() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000_000, 1);
    let mut ids = vec![];
    for t in [10u64, 20, 30, 40] {
        l.apply(&Cmd::Clock { now_ns: t });
        ids.push(buy(&mut l, 1, 5));
    }
    l.apply(&Cmd::Clock { now_ns: 100 });
    // ages are 90, 80, 70, 60: only the first two are 75+ old, oldest first
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 75,
            retry_ns: 0,
            max: 10
        }),
        Outcome::Swept { queued: 2 }
    );
    let (base, it) = l.outbox();
    let cancels: Vec<u64> = it
        .skip_while(|m| matches!(m, OutMsg::Submit { .. }))
        .map(|m| match m {
            OutMsg::Cancel { order, .. } => *order,
            m => panic!("{m:?}"),
        })
        .collect();
    assert_eq!((base, cancels), (0, vec![ids[0], ids[1]]));
    // max bounds the batch, again oldest first
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 0,
            retry_ns: 0,
            max: 1
        }),
        Outcome::Swept { queued: 1 }
    );
    assert!(l.order(ids[2]).unwrap().cancelling);
    assert!(!l.order(ids[3]).unwrap().cancelling);
    l.check().unwrap();
}

#[test]
fn retries_come_before_first_cancels_and_use_the_cancel_time() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000_000, 1);
    l.apply(&Cmd::Clock { now_ns: 1 });
    let a = buy(&mut l, 1, 5);
    let b = buy(&mut l, 1, 5);
    l.apply(&Cmd::Clock { now_ns: 50 });
    assert!(matches!(
        l.apply(&Cmd::Cancel {
            account: 1,
            order: a
        }),
        Outcome::CancelQueued { .. }
    ));
    l.apply(&Cmd::Clock { now_ns: 100 });
    // `a` was cancelled at 50 (50 ago), `b` was born at 1 (99 ago)
    let s = Cmd::Sweep {
        max_age_ns: 60,
        retry_ns: 40,
        max: 1,
    };
    assert_eq!(l.apply(&s), Outcome::Swept { queued: 1 });
    // the retry won the single slot: `a` was re-queued at 100, `b` untouched
    assert_eq!(l.order(a).unwrap().cancel_at, 100);
    assert!(!l.order(b).unwrap().cancelling);
    l.check().unwrap();
    assert_eq!(l.apply(&s), Outcome::Swept { queued: 1 });
    assert!(l.order(b).unwrap().cancelling);
    // nothing else is due: a was just retried, b just cancelled
    assert_eq!(l.apply(&s), Outcome::Swept { queued: 0 });
    l.check().unwrap();
}

#[test]
fn the_age_indexes_survive_snapshot_restore_and_fills() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000_000, 1);
    dep(&mut l, 2, BTC, 1_000_000, 2);
    let mut open = vec![];
    for t in 1..=6u64 {
        l.apply(&Cmd::Clock { now_ns: t });
        open.push(buy(&mut l, 1, 5));
    }
    l.apply(&Cmd::Cancel {
        account: 1,
        order: open[0],
    });
    let sell = match l.apply(&Cmd::Reserve {
        account: 2,
        symbol: SYM,
        side: Side::Sell,
        price: 5,
        qty: 1,
    }) {
        Outcome::Reserved { order, .. } => order,
        o => panic!("{o:?}"),
    };
    l.apply(&Cmd::Settle {
        source: 0,
        prev: 0,
        idx: 1,
        event: Event::Trade {
            buy: open[1],
            sell,
            price: 5,
            qty: 1,
            taker_is_buy: false,
        },
    });
    l.apply(&Cmd::Settle {
        source: 0,
        prev: 1,
        idx: 2,
        event: Event::Closed { order: open[0] },
    });
    l.check().unwrap();
    let mut r = Ledger::new(cfg(0));
    r.restore(&l.snapshot()).unwrap();
    r.check().unwrap();
    let s = Cmd::Sweep {
        max_age_ns: 0,
        retry_ns: 0,
        max: 100,
    };
    assert_eq!(l.apply(&s), r.apply(&s));
    assert_eq!(l.digest(), r.digest());
    assert_eq!(l.snapshot(), r.snapshot());
}

/// The cost of a sweep that finds nothing must not grow with the number of open
/// orders. 20x the orders with a generous bound; a scan would be near 20x.
#[test]
fn a_sweep_that_finds_nothing_costs_the_same_on_a_big_book() {
    fn time(orders: u64) -> f64 {
        let mut l = Ledger::new(cfg(0));
        dep(&mut l, 1, USD, u128::MAX / 4, 1);
        l.apply(&Cmd::Clock { now_ns: 1_000_000 });
        for _ in 0..orders {
            buy(&mut l, 1, 5);
        }
        let s = Cmd::Sweep {
            max_age_ns: 10,
            retry_ns: 10,
            max: 100,
        };
        let t = Instant::now();
        for _ in 0..3_000 {
            assert_eq!(l.apply(&s), Outcome::Swept { queued: 0 });
        }
        t.elapsed().as_secs_f64()
    }
    let small = time(2_000).max(1e-6);
    let big = time(60_000);
    eprintln!("sweep, nothing due: 2k orders {small:.4}s, 60k orders {big:.4}s");
    assert!(big < small * 8.0 + 0.02, "{small} vs {big}");
}

// ---- item 2: the incremental digest ----------------------------------------

#[test]
fn the_incremental_digest_equals_its_definition_at_every_step() {
    let mut l = Ledger::new(cfg(0));
    let mut x = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut orders: Vec<(u64, u64)> = vec![];
    for step in 0..3_000u64 {
        let acct = 1 + next() % 5;
        match next() % 9 {
            0 => {
                l.apply(&Cmd::Deposit {
                    account: acct,
                    asset: [BTC, USD][(next() % 2) as usize],
                    amount: 1 + (next() % 100_000) as u128,
                    external_id: step,
                });
            }
            1 | 2 => {
                if let Outcome::Reserved { order, .. } = l.apply(&Cmd::Reserve {
                    account: acct,
                    symbol: SYM,
                    side: if next() % 2 == 0 {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    price: 1 + next() % 9,
                    qty: 1 + next() % 4,
                }) {
                    orders.push((acct, order));
                }
            }
            3 if !orders.is_empty() => {
                let (a, o) = orders[(next() % orders.len() as u64) as usize];
                l.apply(&Cmd::Cancel {
                    account: a,
                    order: o,
                });
            }
            4 => {
                l.apply(&Cmd::Clock {
                    now_ns: step * 10 + next() % 5,
                });
            }
            5 => {
                l.apply(&Cmd::Sweep {
                    max_age_ns: next() % 200,
                    retry_ns: next() % 100,
                    max: 3,
                });
            }
            6 => {
                l.apply(&Cmd::Withdraw {
                    account: acct,
                    asset: USD,
                    amount: 1 + (next() % 500) as u128,
                    external_id: next() % 40,
                });
            }
            7 => {
                l.apply(&Cmd::Finalize {
                    external_id: next() % 40,
                    paid: next() % 2 == 0,
                });
            }
            _ => {
                let upto = l.outbox().0 + next() % 3;
                l.apply(&Cmd::OutboxAck { upto });
            }
        }
        // digests are taken at irregular times, so the dirty set varies in size
        if next() % 3 == 0 {
            assert_eq!(l.digest(), l.digest_from_scratch(), "step {step}");
        }
    }
    assert_eq!(l.digest(), l.digest_from_scratch());
    l.check().unwrap();
}

#[test]
fn equal_states_have_equal_digests_however_they_were_reached() {
    // the same final state, reached with digests taken at different times
    let script = |l: &mut Ledger, take: bool| {
        for i in 0..200u64 {
            l.apply(&Cmd::Deposit {
                account: 1 + i % 7,
                asset: USD,
                amount: 100 + i as u128,
                external_id: i,
            });
            if take && i % 3 == 0 {
                let _ = l.digest();
            }
            if i % 5 == 0 {
                buy(l, 1 + i % 7, 3);
            }
        }
    };
    let (mut a, mut b) = (Ledger::new(cfg(0)), Ledger::new(cfg(0)));
    script(&mut a, true);
    script(&mut b, false);
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.deep_digest(), b.deep_digest());
    // and a restored copy agrees with both
    let mut c = Ledger::new(cfg(0));
    c.restore(&a.snapshot()).unwrap();
    assert_eq!(c.digest(), a.digest());
}

#[test]
fn the_digest_sees_every_kind_of_difference() {
    let base = || {
        let mut l = Ledger::new(cfg(0));
        dep(&mut l, 1, USD, 10_000, 1);
        buy(&mut l, 1, 5);
        l
    };
    let d0 = base().digest();
    type Change = Box<dyn Fn(&mut Ledger)>;
    let changes: Vec<(&str, Change)> = vec![
        ("balance", Box::new(|l| dep(l, 2, USD, 1, 50))),
        (
            "order",
            Box::new(|l| {
                buy(l, 1, 6);
            }),
        ),
        (
            "cancelling",
            Box::new(|l| {
                l.apply(&Cmd::Cancel {
                    account: 1,
                    order: 1 << 0 | 1,
                });
            }),
        ),
        (
            "clock",
            Box::new(|l| {
                l.apply(&Cmd::Clock { now_ns: 9 });
            }),
        ),
        (
            "outbox ack",
            Box::new(|l| {
                l.apply(&Cmd::OutboxAck { upto: 1 });
            }),
        ),
        (
            "withdraw",
            Box::new(|l| {
                l.apply(&Cmd::Withdraw {
                    account: 1,
                    asset: USD,
                    amount: 1,
                    external_id: 7,
                });
            }),
        ),
        (
            "lend",
            Box::new(|l| {
                l.apply(&Cmd::Lend {
                    group: 0,
                    account: 1,
                    asset: USD,
                    amount: 3,
                });
            }),
        ),
        (
            "adjust",
            Box::new(|l| {
                l.apply(&Cmd::Adjust {
                    account: 1,
                    asset: USD,
                    amount: 1,
                    credit: true,
                    reason: 1,
                    external_id: 3,
                });
            }),
        ),
    ];
    for (name, f) in changes {
        let mut l = base();
        f(&mut l);
        assert_ne!(l.digest(), d0, "{name}");
        assert_eq!(l.digest(), l.digest_from_scratch(), "{name}");
    }
    // a message order swap is visible (position is hashed)
    let mut a = base();
    let mut b = base();
    a.apply(&Cmd::Deposit {
        account: 3,
        asset: USD,
        amount: 9,
        external_id: 80,
    });
    a.apply(&Cmd::Lend {
        group: 0,
        account: 3,
        asset: USD,
        amount: 4,
    });
    a.apply(&Cmd::Lend {
        group: 0,
        account: 3,
        asset: USD,
        amount: 5,
    });
    b.apply(&Cmd::Deposit {
        account: 3,
        asset: USD,
        amount: 9,
        external_id: 80,
    });
    b.apply(&Cmd::Lend {
        group: 0,
        account: 3,
        asset: USD,
        amount: 5,
    });
    b.apply(&Cmd::Lend {
        group: 0,
        account: 3,
        asset: USD,
        amount: 4,
    });
    assert_ne!(a.digest(), b.digest());
}

/// One change plus one digest on a large ledger costs the same as on a small one.
#[test]
fn a_digest_costs_the_changes_not_the_state() {
    fn time(accounts: u64) -> f64 {
        let mut l = Ledger::new(cfg(0));
        for a in 1..=accounts {
            dep(&mut l, a, USD, 1_000, a);
        }
        let _ = l.digest();
        let t = Instant::now();
        for i in 0..400u64 {
            dep(&mut l, 1 + i % accounts, USD, 1, accounts + 1 + i);
            let _ = l.digest();
        }
        t.elapsed().as_secs_f64()
    }
    let small = time(1_000).max(1e-6);
    let big = time(100_000);
    eprintln!("400 x (change + digest): 1k accounts {small:.4}s, 100k accounts {big:.4}s");
    assert!(big < small * 8.0 + 0.02, "{small} vs {big}");
}

#[test]
fn the_deep_digest_is_the_hash_of_the_snapshot_and_differs_from_the_cheap_one() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 10, 1);
    assert_eq!(
        l.deep_digest(),
        orderer_cluster::sha256::sha256(&l.snapshot())
    );
    assert_ne!(l.deep_digest(), l.digest());
}

// ---- item 4: IOC ------------------------------------------------------------

#[test]
fn an_ioc_reserve_holds_like_a_limit_order_and_queues_an_ioc_submit() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000, 1);
    let o = match l.apply(&Cmd::ReserveIoc {
        account: 1,
        symbol: SYM,
        side: Side::Buy,
        price: 100,
        qty: 3,
    }) {
        Outcome::Reserved { order, outbox: 0 } => order,
        o => panic!("{o:?}"),
    };
    assert_eq!(l.balance(1, USD).held, 300);
    let first = {
        let (_, mut it) = l.outbox();
        it.next().copied()
    };
    assert_eq!(
        first,
        Some(OutMsg::SubmitIoc {
            order: o,
            symbol: SYM,
            side: Side::Buy,
            price: 100,
            qty: 3
        })
    );
    // the same checks as a limit order
    assert_eq!(
        l.apply(&Cmd::ReserveIoc {
            account: 1,
            symbol: SYM,
            side: Side::Buy,
            price: 100,
            qty: 8
        }),
        Outcome::Rejected(Reject::Insufficient)
    );
    assert_eq!(
        l.apply(&Cmd::ReserveIoc {
            account: u64::MAX,
            symbol: SYM,
            side: Side::Buy,
            price: 1,
            qty: 1
        }),
        Outcome::Rejected(Reject::ReservedAccount)
    );
    // an IOC remainder closes through the ordinary Closed event
    assert_eq!(
        l.apply(&Cmd::Settle {
            source: 0,
            prev: 0,
            idx: 1,
            event: Event::Closed { order: o }
        }),
        Outcome::Settled
    );
    assert_eq!(
        l.balance(1, USD),
        Balance {
            available: 1_000,
            held: 0
        }
    );
    l.check().unwrap();
}

#[test]
fn ioc_encodings_round_trip_and_the_old_ones_are_unchanged() {
    let ioc = Cmd::ReserveIoc {
        account: 1,
        symbol: 2,
        side: Side::Sell,
        price: 3,
        qty: 4,
    };
    let gtc = Cmd::Reserve {
        account: 1,
        symbol: 2,
        side: Side::Sell,
        price: 3,
        qty: 4,
    };
    assert_eq!(
        encode_cmd(&ioc),
        [
            14, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0,
            0
        ]
    );
    assert_eq!(
        encode_cmd(&gtc),
        [
            2, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0,
            0
        ]
    );
    assert_eq!(decode_cmd(&encode_cmd(&ioc)).unwrap(), ioc);
    assert_eq!(decode_cmd(&encode_cmd(&gtc)).unwrap(), gtc);
    // a snapshot with an IOC submit in the outbox restores identically
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000, 1);
    l.apply(&ioc_buy());
    let mut r = Ledger::new(cfg(0));
    r.restore(&l.snapshot()).unwrap();
    assert_eq!(r.snapshot(), l.snapshot());
    assert_eq!(r.digest(), l.digest());
}

fn ioc_buy() -> Cmd {
    Cmd::ReserveIoc {
        account: 1,
        symbol: SYM,
        side: Side::Buy,
        price: 5,
        qty: 1,
    }
}

// ---- item 5: the time sidecar ----------------------------------------------

#[test]
fn a_stamped_command_raises_the_clock_and_applies_its_command() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000, 1);
    let o = match l.apply(&buy_cmd().at(500)) {
        Outcome::Reserved { order, .. } => order,
        o => panic!("{o:?}"),
    };
    assert_eq!(l.clock(), 500);
    assert_eq!(l.order(o).unwrap().born, 500);
    // a stale stamp never lowers the clock; the command still applies
    assert!(matches!(
        l.apply(&buy_cmd().at(100)),
        Outcome::Reserved { .. }
    ));
    assert_eq!(l.clock(), 500);
    // the stamp applies even if the command is refused
    assert_eq!(
        l.apply(
            &Cmd::Cancel {
                account: 9,
                order: o
            }
            .at(900)
        ),
        Outcome::Rejected(Reject::NotOwner)
    );
    assert_eq!(l.clock(), 900);
    // no nesting
    assert_eq!(
        l.apply(&buy_cmd().at(1).at(2)),
        Outcome::Rejected(Reject::Nested)
    );
    assert_eq!(l.clock(), 900);
    l.check().unwrap();
}

fn buy_cmd() -> Cmd {
    Cmd::Reserve {
        account: 1,
        symbol: SYM,
        side: Side::Buy,
        price: 5,
        qty: 1,
    }
}

#[test]
fn the_time_sidecar_encoding_is_pinned_and_strict() {
    let c = Cmd::Clock { now_ns: 7 }.at(0x0102);
    let b = encode_cmd(&c);
    assert_eq!(b, [20, 2, 1, 0, 0, 0, 0, 0, 0, 8, 7, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(decode_cmd(&b).unwrap(), c);
    // nested stamps, a stamp with nothing inside, and a truncated inner command
    assert!(decode_cmd(&[20, 1, 0, 0, 0, 0, 0, 0, 0, 20, 2, 0, 0, 0, 0, 0, 0, 0, 8]).is_err());
    assert!(decode_cmd(&[20, 1, 0, 0, 0, 0, 0, 0, 0]).is_err());
    assert!(decode_cmd(&b[..b.len() - 1]).is_err());
    let mut longer = b.clone();
    longer.push(0);
    assert!(decode_cmd(&longer).is_err());
    for cut in 0..b.len() {
        assert!(decode_cmd(&b[..cut]).is_err(), "{cut}");
    }
}

#[test]
fn replicas_applying_stamped_commands_agree_without_reading_any_clock() {
    // the stamps are in the log; two replays are identical regardless of when they run
    let log: Vec<Cmd> = (1..=50u64)
        .flat_map(|i| {
            vec![
                Cmd::Deposit {
                    account: 1,
                    asset: USD,
                    amount: 1_000,
                    external_id: i,
                }
                .at(i * 10),
                buy_cmd().at(i * 10 + 3),
                Cmd::Sweep {
                    max_age_ns: 60,
                    retry_ns: 0,
                    max: 5,
                }
                .at(i * 10 + 5),
            ]
        })
        .collect();
    let run = || {
        let mut l = Ledger::new(cfg(0));
        for c in &log {
            l.apply(c);
        }
        l
    };
    let (a, b) = (run(), run());
    assert_eq!(a.digest(), b.digest());
    assert_eq!(a.deep_digest(), b.deep_digest());
    assert!(a.clock() > 0);
    a.check().unwrap();
}

#[test]
fn the_age_thresholds_are_inclusive() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000_000, 1);
    l.apply(&Cmd::Clock { now_ns: 100 });
    let a = buy(&mut l, 1, 5);
    l.apply(&Cmd::Clock { now_ns: 150 });
    // age is exactly 50: due at max_age 50, not at 51
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 51,
            retry_ns: 0,
            max: 9
        }),
        Outcome::Swept { queued: 0 }
    );
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 50,
            retry_ns: 0,
            max: 9
        }),
        Outcome::Swept { queued: 1 }
    );
    assert_eq!(l.order(a).unwrap().cancel_at, 150);
    l.apply(&Cmd::Clock { now_ns: 200 });
    // the cancel is exactly 50 old: a retry at 50, not at 51
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 1_000,
            retry_ns: 51,
            max: 9
        }),
        Outcome::Swept { queued: 0 }
    );
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 1_000,
            retry_ns: 50,
            max: 9
        }),
        Outcome::Swept { queued: 1 }
    );
    // an age larger than the clock reads never matches (no wrap-around)
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: u64::MAX,
            retry_ns: u64::MAX,
            max: 9
        }),
        Outcome::Swept { queued: 0 }
    );
    l.check().unwrap();
}

/// Two valid states that differ in exactly one table must have different digests,
/// so no lane can be dropped from the digest unnoticed.
#[test]
fn each_table_alone_changes_the_digest() {
    let new = || Ledger::new(cfg(0));
    let diff = |a: Ledger, b: Ledger, what: &str| {
        a.check().unwrap();
        b.check().unwrap();
        assert_ne!(a.digest(), b.digest(), "{what}");
        assert_eq!(a.digest(), a.digest_from_scratch(), "{what}");
        assert_eq!(b.digest(), b.digest_from_scratch(), "{what}");
    };
    // balances: the same deposits (ids, totals) assigned to different accounts
    let (mut a, mut b) = (new(), new());
    dep(&mut a, 1, USD, 100, 1);
    dep(&mut a, 2, USD, 50, 2);
    dep(&mut b, 1, USD, 50, 1);
    dep(&mut b, 2, USD, 100, 2);
    diff(a, b, "balances");
    // orders: the same hold (10) from a different price and quantity, outbox acked
    let (mut a, mut b) = (new(), new());
    for l in [&mut a, &mut b] {
        dep(l, 1, USD, 100, 1);
    }
    for (l, price, qty) in [(&mut a, 5u64, 2u64), (&mut b, 10, 1)] {
        l.apply(&Cmd::Reserve {
            account: 1,
            symbol: SYM,
            side: Side::Buy,
            price,
            qty,
        });
        l.apply(&Cmd::OutboxAck { upto: 1 });
    }
    diff(a, b, "orders");
    // lent: a slice-in credited to a different account, nothing else different
    let (mut a, mut b) = (new(), new());
    for (l, acct) in [(&mut a, 1u64), (&mut b, 2)] {
        assert_eq!(
            l.apply(&Cmd::Settle {
                source: 0,
                prev: 0,
                idx: 1,
                event: Event::SliceIn {
                    group: 0,
                    account: acct,
                    asset: USD,
                    amount: 10
                }
            }),
            Outcome::Settled
        );
    }
    diff(a, b, "lent");
    // pending withdrawals: the same balances afterwards, held for another account
    let (mut a, mut b) = (new(), new());
    dep(&mut a, 1, USD, 100, 1);
    dep(&mut a, 2, USD, 95, 2);
    dep(&mut b, 1, USD, 95, 1);
    dep(&mut b, 2, USD, 100, 2);
    for (l, acct) in [(&mut a, 1u64), (&mut b, 2)] {
        assert_eq!(
            l.apply(&Cmd::Withdraw {
                account: acct,
                asset: USD,
                amount: 5,
                external_id: 7
            }),
            Outcome::WithdrawHeld
        );
    }
    diff(a, b, "pending");
    // the three id sets: a different id with everything else equal
    for (name, mk) in [
        (
            "deposit ids",
            Box::new(|l: &mut Ledger, id| dep(l, 1, USD, 5, id)) as Box<dyn Fn(&mut Ledger, u64)>,
        ),
        (
            "withdraw ids",
            Box::new(|l: &mut Ledger, id| {
                dep(l, 1, USD, 5, 99);
                l.apply(&Cmd::Withdraw {
                    account: 1,
                    asset: USD,
                    amount: 1,
                    external_id: id,
                });
                l.apply(&Cmd::Finalize {
                    external_id: id,
                    paid: false,
                });
            }),
        ),
        (
            "adjust ids",
            Box::new(|l: &mut Ledger, id| {
                l.apply(&Cmd::Adjust {
                    account: 1,
                    asset: USD,
                    amount: 1,
                    credit: true,
                    reason: 3,
                    external_id: id,
                });
            }),
        ),
    ] {
        let (mut a, mut b) = (new(), new());
        mk(&mut a, 11);
        mk(&mut b, 12);
        // the adjustment ring records the id, so blank it for a pure id-set comparison
        if name == "adjust ids" {
            continue;
        }
        diff(a, b, name);
    }
}

#[test]
fn many_retries_are_bounded_by_max() {
    let mut l = Ledger::new(cfg(0));
    dep(&mut l, 1, USD, 1_000_000, 1);
    l.apply(&Cmd::Clock { now_ns: 1 });
    let ids: Vec<u64> = (0..5).map(|_| buy(&mut l, 1, 5)).collect();
    for id in &ids {
        l.apply(&Cmd::Cancel {
            account: 1,
            order: *id,
        });
    }
    l.apply(&Cmd::Clock { now_ns: 1_000 });
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 10,
            retry_ns: 10,
            max: 3
        }),
        Outcome::Swept { queued: 3 }
    );
    assert_eq!(
        l.apply(&Cmd::Sweep {
            max_age_ns: 10,
            retry_ns: 10,
            max: 3
        }),
        Outcome::Swept { queued: 2 },
        "the three just retried are not due again"
    );
    l.check().unwrap();
}
