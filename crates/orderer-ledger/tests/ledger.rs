//! Unit tests for the ledger state machine and the wire formats.

use orderer_ledger::*;
use std::collections::BTreeMap;

const BTC: u32 = 1;
const USD: u32 = 2;
const SYM: u32 = 7;

fn cfg(shard: u16, fee_ppm: u32) -> Config {
    let mut symbols = BTreeMap::new();
    // 1 qty = 1000 satoshi-ish atoms of base; price in quote atoms per lot.
    symbols.insert(
        SYM,
        Symbol {
            base: BTC,
            quote: USD,
            lot: 1000,
        },
    );
    Config {
        shard,
        fee_ppm,
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

fn reserve(l: &mut Ledger, account: u64, side: Side, price: u64, qty: u64) -> u64 {
    match l.apply(&Cmd::Reserve {
        account,
        symbol: SYM,
        side,
        price,
        qty,
    }) {
        Outcome::Reserved { order, .. } => order,
        o => panic!("{o:?}"),
    }
}

#[test]
fn deposit_is_exactly_once_by_external_id() {
    let mut l = Ledger::new(cfg(0, 0));
    dep(&mut l, 1, USD, 500, 10);
    let c = Cmd::Deposit {
        account: 1,
        asset: USD,
        amount: 500,
        external_id: 10,
    };
    assert_eq!(l.apply(&c), Outcome::Duplicate);
    assert_eq!(l.balance(1, USD).available, 500);
    assert_eq!(
        l.apply(&Cmd::Deposit {
            account: 1,
            asset: USD,
            amount: 0,
            external_id: 11
        }),
        Outcome::Rejected(Reject::BadAmount)
    );
    assert_eq!(
        l.apply(&Cmd::Deposit {
            account: 1,
            asset: USD,
            amount: u128::MAX,
            external_id: 12
        }),
        Outcome::Rejected(Reject::Overflow)
    );
    l.check().unwrap();
}

#[test]
fn reserve_holds_funds_and_never_overdrafts() {
    let mut l = Ledger::new(cfg(3, 0));
    dep(&mut l, 1, USD, 1000, 1);
    // buy 5 lots at 100 needs 500
    let o = reserve(&mut l, 1, Side::Buy, 100, 5);
    assert_eq!(order_shard(o), 3);
    assert_eq!(
        l.balance(1, USD),
        Balance {
            available: 500,
            held: 500
        }
    );
    // 6 more lots at 100 needs 600 > 500 available
    assert_eq!(
        l.apply(&Cmd::Reserve {
            account: 1,
            symbol: SYM,
            side: Side::Buy,
            price: 100,
            qty: 6
        }),
        Outcome::Rejected(Reject::Insufficient)
    );
    // selling needs base
    assert_eq!(
        l.apply(&Cmd::Reserve {
            account: 1,
            symbol: SYM,
            side: Side::Sell,
            price: 100,
            qty: 1
        }),
        Outcome::Rejected(Reject::Insufficient)
    );
    assert_eq!(
        l.apply(&Cmd::Reserve {
            account: 1,
            symbol: 99,
            side: Side::Buy,
            price: 1,
            qty: 1
        }),
        Outcome::Rejected(Reject::UnknownSymbol)
    );
    assert_eq!(
        l.apply(&Cmd::Reserve {
            account: 1,
            symbol: SYM,
            side: Side::Buy,
            price: 0,
            qty: 1
        }),
        Outcome::Rejected(Reject::BadPrice)
    );
    assert_eq!(
        l.apply(&Cmd::Reserve {
            account: 1,
            symbol: SYM,
            side: Side::Buy,
            price: 1,
            qty: 0
        }),
        Outcome::Rejected(Reject::BadQuantity)
    );
    // a rejected reserve leaves no trace in the outbox
    assert_eq!(l.outbox_next(), 1);
    l.check().unwrap();
}

#[test]
fn reserve_arithmetic_cannot_overflow() {
    let mut l = Ledger::new(cfg(0, 0));
    dep(&mut l, 1, BTC, u128::MAX / 2, 1);
    // lot * qty would exceed u128 for a huge lot
    let mut c = cfg(0, 0);
    c.symbols.insert(
        8,
        Symbol {
            base: BTC,
            quote: USD,
            lot: u128::MAX / 2,
        },
    );
    let mut l2 = Ledger::new(c);
    dep(&mut l2, 1, BTC, u128::MAX / 2, 1);
    assert_eq!(
        l2.apply(&Cmd::Reserve {
            account: 1,
            symbol: 8,
            side: Side::Sell,
            price: 1,
            qty: 3
        }),
        Outcome::Rejected(Reject::Overflow)
    );
    l.check().unwrap();
    l2.check().unwrap();
}

#[test]
fn cancel_checks_ownership_and_queues_once() {
    let mut l = Ledger::new(cfg(0, 0));
    dep(&mut l, 1, USD, 1000, 1);
    let o = reserve(&mut l, 1, Side::Buy, 10, 1);
    assert_eq!(
        l.apply(&Cmd::Cancel {
            account: 2,
            order: o
        }),
        Outcome::Rejected(Reject::NotOwner)
    );
    assert_eq!(
        l.apply(&Cmd::Cancel {
            account: 1,
            order: 12345
        }),
        Outcome::Rejected(Reject::UnknownOrder)
    );
    assert!(matches!(
        l.apply(&Cmd::Cancel {
            account: 1,
            order: o
        }),
        Outcome::CancelQueued { outbox: 1 }
    ));
    assert_eq!(
        l.apply(&Cmd::Cancel {
            account: 1,
            order: o
        }),
        Outcome::Rejected(Reject::AlreadyCancelling)
    );
    // funds stay held until matching confirms
    assert_eq!(l.balance(1, USD).held, 10);
    assert_eq!(
        l.apply(&Cmd::Settle {
            source: 0,
            prev: 0,
            idx: 5,
            event: Event::Closed { order: o }
        }),
        Outcome::Settled
    );
    assert_eq!(
        l.balance(1, USD),
        Balance {
            available: 1000,
            held: 0
        }
    );
    assert_eq!(l.open_orders(), 0);
    l.check().unwrap();
}

#[test]
fn trade_settles_both_sides_with_price_improvement_and_fees() {
    let mut l = Ledger::new(cfg(0, 1_000)); // 0.1%
    dep(&mut l, 1, USD, 10_000, 1); // buyer
    dep(&mut l, 2, BTC, 10_000, 2); // seller
    let buy = reserve(&mut l, 1, Side::Buy, 120, 10); // holds 1200 USD
    let sell = reserve(&mut l, 2, Side::Sell, 100, 4); // holds 4000 BTC atoms
    assert_eq!(
        l.balance(1, USD),
        Balance {
            available: 8_800,
            held: 1_200
        }
    );
    assert_eq!(
        l.balance(2, BTC),
        Balance {
            available: 6_000,
            held: 4_000
        }
    );
    // 4 lots trade at 110: buyer pays 440, sheds 480 of hold, gets 40 back
    let ev = Event::Trade {
        buy,
        sell,
        price: 110,
        qty: 4,
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
    // buyer: held 1200-480=720; available 8800+40; base 4000 less 0.1% fee (4)
    assert_eq!(
        l.balance(1, USD),
        Balance {
            available: 8_840,
            held: 720
        }
    );
    assert_eq!(
        l.balance(1, BTC),
        Balance {
            available: 3_996,
            held: 0
        }
    );
    // seller: base held gone; quote 440 less fee floor(0.44)=0
    assert_eq!(
        l.balance(2, BTC),
        Balance {
            available: 6_000,
            held: 0
        }
    );
    assert_eq!(
        l.balance(2, USD),
        Balance {
            available: 440,
            held: 0
        }
    );
    assert_eq!(l.fees(BTC), 4);
    assert_eq!(l.fees(USD), 0);
    assert_eq!(l.order(buy).unwrap().remaining, 6);
    assert!(l.order(sell).is_none(), "fully filled order is gone");
    l.check().unwrap();
    // the rest of the buy is cancelled: its remaining hold comes back
    assert_eq!(
        l.apply(&Cmd::Settle {
            source: 0,
            prev: 1,
            idx: 2,
            event: Event::Closed { order: buy }
        }),
        Outcome::Settled
    );
    assert_eq!(
        l.balance(1, USD),
        Balance {
            available: 9_560,
            held: 0
        }
    );
    l.check().unwrap();
}

#[test]
fn settle_is_exactly_once_per_source_mark() {
    let mut l = Ledger::new(cfg(0, 0));
    dep(&mut l, 1, USD, 1000, 1);
    dep(&mut l, 2, BTC, 1000, 2);
    let buy = reserve(&mut l, 1, Side::Buy, 100, 1);
    let sell = reserve(&mut l, 2, Side::Sell, 100, 1);
    let c = Cmd::Settle {
        source: 4,
        prev: 0,
        idx: 9,
        event: Event::Trade {
            buy,
            sell,
            price: 100,
            qty: 1,
            taker_is_buy: true,
        },
    };
    assert_eq!(l.apply(&c), Outcome::Settled);
    let d = l.digest();
    assert_eq!(l.apply(&c), Outcome::Duplicate);
    // an older index from the same source is also a duplicate
    assert_eq!(
        l.apply(&Cmd::Settle {
            source: 4,
            prev: 0,
            idx: 3,
            event: Event::Closed { order: buy }
        }),
        Outcome::Duplicate
    );
    assert_eq!(l.digest(), d);
    assert_eq!(l.high_water(4), Some(9));
    assert_eq!(l.high_water(5), None);
    // marks are per source
    dep(&mut l, 1, USD, 100, 3);
    let b2 = reserve(&mut l, 1, Side::Buy, 100, 1);
    assert_eq!(
        l.apply(&Cmd::Settle {
            source: 5,
            prev: 0,
            idx: 1,
            event: Event::Closed { order: b2 }
        }),
        Outcome::Settled
    );
    l.check().unwrap();
}

#[test]
fn contradictory_events_are_anomalies_that_change_nothing() {
    let mut l = Ledger::new(cfg(0, 0));
    dep(&mut l, 1, USD, 1000, 1);
    dep(&mut l, 2, BTC, 10_000, 2);
    let buy = reserve(&mut l, 1, Side::Buy, 100, 2);
    let sell = reserve(&mut l, 2, Side::Sell, 100, 5);
    let before = l.snapshot();
    let bad = [
        Event::Trade {
            buy,
            sell,
            price: 101,
            qty: 1,
            taker_is_buy: true,
        }, // above the buyer's limit
        Event::Trade {
            buy,
            sell,
            price: 99,
            qty: 3,
            taker_is_buy: true,
        }, // more than the buyer has left
        Event::Trade {
            buy: sell,
            sell: buy,
            price: 100,
            qty: 1,
            taker_is_buy: true,
        }, // sides swapped
        Event::Trade {
            buy,
            sell: buy,
            price: 100,
            qty: 1,
            taker_is_buy: true,
        },
        Event::Trade {
            buy,
            sell,
            price: 100,
            qty: 0,
            taker_is_buy: true,
        },
        Event::Closed { order: 0xdead },
    ];
    for (i, ev) in bad.iter().enumerate() {
        assert_eq!(
            l.apply(&Cmd::Settle {
                source: 0,
                prev: i as u64,
                idx: i as u64 + 1,
                event: *ev
            }),
            Outcome::Anomaly,
            "{ev:?}"
        );
    }
    assert_eq!(l.anomalies(), bad.len() as u64);
    assert!(l.check().is_err(), "anomalies are reported by check");
    // balances and orders are untouched (only marks and the counter moved)
    let mut fresh = Ledger::new(cfg(0, 0));
    fresh.restore(&before).unwrap();
    for a in [1, 2] {
        for asset in [BTC, USD] {
            assert_eq!(l.balance(a, asset), fresh.balance(a, asset));
        }
    }
    assert_eq!(l.order(buy), fresh.order(buy));
    assert_eq!(l.order(sell), fresh.order(sell));
}

#[test]
fn a_replayed_anomaly_is_not_retried() {
    let mut l = Ledger::new(cfg(0, 0));
    let c = Cmd::Settle {
        source: 0,
        prev: 0,
        idx: 1,
        event: Event::Closed { order: 77 },
    };
    assert_eq!(l.apply(&c), Outcome::Anomaly);
    assert_eq!(l.apply(&c), Outcome::Duplicate);
    assert_eq!(l.anomalies(), 1);
}

#[test]
fn a_trade_between_accounts_on_two_shards_applies_only_the_local_side() {
    let mut a = Ledger::new(cfg(0, 0));
    let mut b = Ledger::new(cfg(1, 0));
    dep(&mut a, 1, USD, 1000, 1);
    dep(&mut b, 2, BTC, 10_000, 2);
    let buy = reserve(&mut a, 1, Side::Buy, 100, 2);
    let sell = reserve(&mut b, 2, Side::Sell, 100, 2);
    assert_ne!(order_shard(buy), order_shard(sell));
    let c = Cmd::Settle {
        source: 0,
        prev: 0,
        idx: 1,
        event: Event::Trade {
            buy,
            sell,
            price: 100,
            qty: 2,
            taker_is_buy: true,
        },
    };
    assert_eq!(a.apply(&c), Outcome::Settled);
    // until the other shard settles, only its own books are consistent
    a.check().unwrap();
    b.check().unwrap();
    assert_eq!(b.apply(&c), Outcome::Settled);
    a.check().unwrap();
    b.check().unwrap();
    for asset in [BTC, USD] {
        let (ai, ao) = a.settled_flow(asset);
        let (bi, bo) = b.settled_flow(asset);
        assert_eq!(ai + bi, ao + bo, "asset {asset}");
    }
    assert_eq!(a.balance(1, BTC).available, 2000);
    assert_eq!(b.balance(2, BTC).available, 8000);
    assert_eq!(b.balance(2, USD).available, 200);
}

#[test]
fn withdrawals_hold_then_pay_or_return() {
    let mut l = Ledger::new(cfg(0, 0));
    dep(&mut l, 1, USD, 100, 1);
    let w = |id| Cmd::Withdraw {
        account: 1,
        asset: USD,
        amount: 60,
        external_id: id,
    };
    assert_eq!(l.apply(&w(9)), Outcome::WithdrawHeld);
    assert_eq!(l.apply(&w(9)), Outcome::Rejected(Reject::DuplicateId));
    assert_eq!(l.apply(&w(10)), Outcome::Rejected(Reject::Insufficient));
    l.check().unwrap();
    assert_eq!(
        l.apply(&Cmd::Finalize {
            external_id: 9,
            paid: false
        }),
        Outcome::WithdrawDone
    );
    assert_eq!(l.balance(1, USD).available, 100);
    assert_eq!(
        l.apply(&Cmd::Finalize {
            external_id: 9,
            paid: true
        }),
        Outcome::Duplicate
    );
    assert_eq!(
        l.apply(&Cmd::Finalize {
            external_id: 1234,
            paid: true
        }),
        Outcome::Rejected(Reject::UnknownId)
    );
    assert_eq!(l.apply(&w(11)), Outcome::WithdrawHeld);
    assert_eq!(
        l.apply(&Cmd::Finalize {
            external_id: 11,
            paid: true
        }),
        Outcome::WithdrawDone
    );
    assert_eq!(l.balance(1, USD).available, 40);
    l.check().unwrap();
}

#[test]
fn outbox_is_trimmed_by_ack_and_keeps_indices() {
    let mut l = Ledger::new(cfg(0, 0));
    dep(&mut l, 1, USD, 1000, 1);
    for _ in 0..4 {
        reserve(&mut l, 1, Side::Buy, 10, 1);
    }
    assert_eq!(l.outbox_next(), 4);
    l.apply(&Cmd::OutboxAck { upto: 3 });
    let (base, it) = l.outbox();
    assert_eq!((base, it.count()), (3, 1));
    l.apply(&Cmd::OutboxAck { upto: 99 }); // clamped
    assert_eq!(l.outbox_next(), 4);
    assert_eq!(reserve(&mut l, 1, Side::Buy, 10, 1) & 0xffff, 5);
    assert_eq!(l.outbox_next(), 5);
}

#[test]
fn commands_round_trip_and_garbage_never_panics() {
    let cmds = [
        Cmd::Deposit {
            account: u64::MAX,
            asset: 3,
            amount: u128::MAX,
            external_id: 9,
        },
        Cmd::Reserve {
            account: 1,
            symbol: 2,
            side: Side::Sell,
            price: 3,
            qty: 4,
        },
        Cmd::Cancel {
            account: 1,
            order: 2,
        },
        Cmd::Settle {
            source: 1,
            prev: 1,
            idx: 2,
            event: Event::Trade {
                buy: 1,
                sell: 2,
                price: 3,
                qty: 4,
                taker_is_buy: true,
            },
        },
        Cmd::Settle {
            source: 1,
            prev: 1,
            idx: 2,
            event: Event::Closed { order: 5 },
        },
        Cmd::Withdraw {
            account: 1,
            asset: 2,
            amount: 3,
            external_id: 4,
        },
        Cmd::Finalize {
            external_id: 1,
            paid: true,
        },
        Cmd::OutboxAck { upto: 8 },
    ];
    for c in &cmds {
        let b = encode_cmd(c);
        assert_eq!(&decode_cmd(&b).unwrap(), c);
        for cut in 0..b.len() {
            assert!(decode_cmd(&b[..cut]).is_err());
        }
        let mut longer = b.clone();
        longer.push(0);
        assert!(decode_cmd(&longer).is_err());
    }
    let mut x = 0x1234_5678_9abc_def0u64;
    for _ in 0..20_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let n = (x % 60) as usize;
        let bytes: Vec<u8> = (0..n)
            .map(|i| (x >> ((i % 8) * 8)) as u8 ^ i as u8)
            .collect();
        let _ = decode_cmd(&bytes);
    }
}

#[test]
fn a_golden_command_encoding_is_stable() {
    // Pinned so other implementations (gateways) can rely on it.
    let b = encode_cmd(&Cmd::Reserve {
        account: 1,
        symbol: 2,
        side: Side::Sell,
        price: 3,
        qty: 4,
    });
    assert_eq!(
        b,
        [
            2, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0,
            0
        ]
    );
}

#[test]
fn snapshot_restores_exactly_and_rejects_corruption() {
    let mut l = Ledger::new(cfg(2, 500));
    dep(&mut l, 1, USD, 5000, 1);
    dep(&mut l, 2, BTC, 5000, 2);
    let buy = reserve(&mut l, 1, Side::Buy, 100, 3);
    let sell = reserve(&mut l, 2, Side::Sell, 90, 3);
    l.apply(&Cmd::Settle {
        source: 1,
        prev: 0,
        idx: 4,
        event: Event::Trade {
            buy,
            sell,
            price: 95,
            qty: 2,
            taker_is_buy: true,
        },
    });
    l.apply(&Cmd::Cancel {
        account: 1,
        order: buy,
    });
    l.apply(&Cmd::Withdraw {
        account: 1,
        asset: USD,
        amount: 7,
        external_id: 3,
    });
    l.check().unwrap();
    let snap = l.snapshot();
    let mut r = Ledger::new(cfg(2, 500));
    r.restore(&snap).unwrap();
    assert_eq!(r.snapshot(), snap);
    assert_eq!(r.digest(), l.digest());
    // same commands after restore give the same results
    let next = Cmd::Settle {
        source: 1,
        prev: 4,
        idx: 5,
        event: Event::Closed { order: buy },
    };
    assert_eq!(r.apply(&next), l.apply(&next));
    assert_eq!(r.digest(), l.digest());
    // wrong shard, truncation and a hold that does not match its order are refused
    assert!(Ledger::new(cfg(3, 500)).restore(&snap).is_err());
    for cut in [0, 3, snap.len() / 2, snap.len() - 1] {
        assert!(Ledger::new(cfg(2, 500)).restore(&snap[..cut]).is_err());
    }
    let mut bad = Ledger::new(cfg(2, 500));
    let mut tampered = snap.clone();
    // balances start at byte 4+2+8+8+8+4; flip a low byte of the first `held`
    let held_at = 4 + 2 + 8 + 8 + 8 + 8 + 4 + 8 + 4 + 16;
    tampered[held_at] ^= 1;
    assert!(bad.restore(&tampered).is_err());
}

#[test]
fn digest_differs_when_state_differs() {
    let mut a = Ledger::new(cfg(0, 0));
    let mut b = Ledger::new(cfg(0, 0));
    assert_eq!(a.digest(), b.digest());
    dep(&mut a, 1, USD, 1, 1);
    assert_ne!(a.digest(), b.digest());
    dep(&mut b, 1, USD, 1, 1);
    assert_eq!(a.digest(), b.digest());
}
