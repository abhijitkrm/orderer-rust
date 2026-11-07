//! Phase 2 (credit slices): the ledger's side, the slice engine, wire and snapshot.

mod common;
use common::*;
use orderer_ledger::*;

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

fn settle(l: &mut Ledger, idx: u64, ev: Event) -> Outcome {
    l.apply(&Cmd::Settle {
        source: 9,
        prev: idx - 1,
        idx,
        event: ev,
    })
}

#[test]
fn lend_moves_available_to_lent_and_queues_a_grant() {
    let mut l = Ledger::new(config(0));
    dep(&mut l, 1, USD, 1000, 1);
    assert_eq!(
        l.apply(&Cmd::Lend {
            account: 1,
            asset: USD,
            amount: 600
        }),
        Outcome::Lent { outbox: 0 }
    );
    assert_eq!(l.balance(1, USD).available, 400);
    assert_eq!(l.lent(1, USD), 600);
    assert_eq!(l.outbox().0, 0);
    assert_eq!(
        l.outbox().1.next().copied(),
        Some(OutMsg::Grant {
            account: 1,
            asset: USD,
            amount: 600
        })
    );
    // lent funds still count as the account's money, so the books balance
    assert_eq!(l.holdings(USD), 1000);
    assert_eq!(l.slice_flow(USD), (600, 0, 0, 0));
    l.check().unwrap();
    // cannot lend what is not available, or nothing, or the fee account's money
    let lend = |account, amount| Cmd::Lend {
        account,
        asset: USD,
        amount,
    };
    assert_eq!(
        l.apply(&lend(1, 401)),
        Outcome::Rejected(Reject::Insufficient)
    );
    assert_eq!(l.apply(&lend(1, 0)), Outcome::Rejected(Reject::BadAmount));
    assert_eq!(
        l.apply(&lend(FEE_ACCOUNT, 1)),
        Outcome::Rejected(Reject::ReservedAccount)
    );
    // S4: a withdrawal draws on available only, never on lent funds
    let w = |amount, id| Cmd::Withdraw {
        account: 1,
        asset: USD,
        amount,
        external_id: id,
    };
    assert_eq!(l.apply(&w(401, 1)), Outcome::Rejected(Reject::Insufficient));
    assert_eq!(l.apply(&w(400, 2)), Outcome::WithdrawHeld);
    l.check().unwrap();
}

#[test]
fn recall_is_a_request_bounded_by_what_is_lent() {
    let mut l = Ledger::new(config(0));
    dep(&mut l, 1, USD, 1000, 1);
    let recall = |amount| Cmd::Recall {
        account: 1,
        asset: USD,
        amount,
    };
    assert_eq!(l.apply(&recall(5)), Outcome::Rejected(Reject::Insufficient));
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 300,
    });
    assert_eq!(l.apply(&recall(0)), Outcome::Rejected(Reject::BadAmount));
    assert_eq!(l.apply(&recall(9999)), Outcome::RecallQueued { outbox: 1 });
    let msgs: Vec<_> = l.outbox().1.copied().collect();
    assert_eq!(
        msgs[1],
        OutMsg::Recall {
            account: 1,
            asset: USD,
            amount: 300
        }
    );
    // asking changes no balance: only the group's answer does
    assert_eq!(l.balance(1, USD).available, 700);
    assert_eq!(l.lent(1, USD), 300);
    l.check().unwrap();
}

#[test]
fn slice_events_move_lent_money_and_the_books_still_balance() {
    let mut l = Ledger::new(config(0));
    dep(&mut l, 1, USD, 1000, 1);
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 600,
    });
    let ev = |e| e;
    assert_eq!(
        settle(
            &mut l,
            1,
            ev(Event::SliceOut {
                account: 1,
                asset: USD,
                amount: 100
            })
        ),
        Outcome::Settled
    );
    assert_eq!(l.lent(1, USD), 500);
    assert_eq!(
        settle(
            &mut l,
            2,
            Event::SliceIn {
                account: 1,
                asset: BTC,
                amount: 7
            }
        ),
        Outcome::Settled
    );
    assert_eq!(l.lent(1, BTC), 7);
    assert_eq!(
        settle(
            &mut l,
            3,
            Event::SliceFee {
                asset: USD,
                amount: 3
            }
        ),
        Outcome::Settled
    );
    assert_eq!(l.fees(USD), 3);
    assert_eq!(
        settle(
            &mut l,
            4,
            Event::Returned {
                account: 1,
                asset: USD,
                amount: 200
            }
        ),
        Outcome::Settled
    );
    assert_eq!(l.lent(1, USD), 300);
    assert_eq!(l.balance(1, USD).available, 400 + 200);
    l.check().unwrap();
    assert_eq!(l.slice_flow(USD), (600, 200, 0, 100));
    // lent entries disappear at zero
    settle(
        &mut l,
        5,
        Event::Returned {
            account: 1,
            asset: USD,
            amount: 300,
        },
    );
    assert_eq!(l.lent(1, USD), 0);
    l.check().unwrap();
}

#[test]
fn a_slice_event_that_would_overdraw_lent_money_is_an_anomaly_that_changes_nothing() {
    let mut l = Ledger::new(config(0));
    dep(&mut l, 1, USD, 1000, 1);
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 50,
    });
    let before = l.snapshot();
    for (i, e) in [
        Event::SliceOut {
            account: 1,
            asset: USD,
            amount: 51,
        },
        Event::Returned {
            account: 1,
            asset: USD,
            amount: 51,
        },
        Event::Returned {
            account: 2,
            asset: USD,
            amount: 1,
        },
        Event::SliceIn {
            account: 1,
            asset: USD,
            amount: 0,
        },
        Event::SliceFee {
            asset: USD,
            amount: 0,
        },
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(settle(&mut l, i as u64 + 1, e), Outcome::Anomaly, "{e:?}");
    }
    assert_eq!(l.anomalies(), 5);
    assert_eq!(l.lent(1, USD), 50);
    assert_eq!(l.balance(1, USD).available, 950);
    assert_eq!(l.fees(USD), 0);
    assert_eq!(l.slice_flow(USD), (50, 0, 0, 0));
    assert_ne!(l.snapshot(), before); // marks and the counter moved, nothing else
}

#[test]
fn phase_two_wire_formats_round_trip_and_are_pinned() {
    let cmds = [
        Cmd::Lend {
            account: 1,
            asset: 2,
            amount: u128::MAX,
        },
        Cmd::Recall {
            account: 1,
            asset: 2,
            amount: 3,
        },
        Cmd::Settle {
            source: 1,
            prev: 2,
            idx: 3,
            event: Event::SliceIn {
                account: 4,
                asset: 5,
                amount: 6,
            },
        },
        Cmd::Settle {
            source: 1,
            prev: 2,
            idx: 3,
            event: Event::SliceOut {
                account: 4,
                asset: 5,
                amount: 6,
            },
        },
        Cmd::Settle {
            source: 1,
            prev: 2,
            idx: 3,
            event: Event::SliceFee {
                asset: 5,
                amount: 6,
            },
        },
        Cmd::Settle {
            source: 1,
            prev: 2,
            idx: 3,
            event: Event::Returned {
                account: 4,
                asset: 5,
                amount: 6,
            },
        },
    ];
    for c in &cmds {
        let b = encode_cmd(c);
        assert_eq!(&decode_cmd(&b).unwrap(), c);
        for cut in 0..b.len() {
            assert!(decode_cmd(&b[..cut]).is_err());
        }
    }
    let mut want = vec![12u8, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3];
    want.extend_from_slice(&[0; 15]);
    assert_eq!(
        encode_cmd(&Cmd::Lend {
            account: 1,
            asset: 2,
            amount: 3
        }),
        want
    );
    want[0] = 13;
    assert_eq!(
        encode_cmd(&Cmd::Recall {
            account: 1,
            asset: 2,
            amount: 3
        }),
        want
    );
}

#[test]
fn the_snapshot_format_changes_only_when_phase_two_state_exists() {
    let mut l = Ledger::new(config(0));
    dep(&mut l, 1, USD, 1000, 1);
    let phase1 = l.snapshot();
    assert_eq!(
        &phase1[..4],
        b"LDG2",
        "no slice state: the old format, byte for byte"
    );
    l.apply(&Cmd::Lend {
        account: 1,
        asset: USD,
        amount: 10,
    });
    let snap = l.snapshot();
    assert_eq!(&snap[..4], b"LDG3");
    let mut r = Ledger::new(config(0));
    r.restore(&snap).unwrap();
    assert_eq!(r.snapshot(), snap);
    assert_eq!(r.lent(1, USD), 10);
    // an old snapshot still restores into a ledger that has slice state
    r.restore(&phase1).unwrap();
    assert_eq!(r.lent(1, USD), 0);
    // corruption of the lent section is refused (the books would not balance)
    let mut bad = snap.clone();
    let at = bad.len() - 1;
    // the last map is slice_out (empty): its count is 4 bytes; change the
    // granted amount instead by flipping a byte inside the granted map
    let granted_amount_lo = at - (4 + 4) /*slice_out count*/ - (4 + 4) /*slice_in count*/ - (4 + 4) /*returned count*/ - 16 /*granted amount*/ + 1;
    bad[granted_amount_lo] ^= 1;
    assert!(Ledger::new(config(0)).restore(&bad).is_err());
    for cut in [snap.len() - 1, snap.len() - 20] {
        assert!(Ledger::new(config(0)).restore(&snap[..cut]).is_err());
    }
}

// ---- the slice engine ----

const S0: u32 = SYMS[0]; // BTC/USD, lot 1000, fee 1.5% both sides

fn slice() -> Slice {
    Slice::new(config(0), 0)
}

#[test]
fn the_pre_trade_check_is_a_local_lookup_against_credit() {
    let mut s = slice();
    s.grant(1, USD, 1000);
    s.grant(2, BTC, 5000);
    let o = s.enter(1, S0, Side::Buy, 100, 4).unwrap();
    assert!(is_slice_order(o));
    assert_eq!(s.available(1, USD), 600);
    assert_eq!(s.total(1, USD), 1000);
    // not enough credit left
    assert_eq!(s.enter(1, S0, Side::Buy, 100, 7), Err(Reject::Insufficient));
    assert_eq!(
        s.enter(1, S0, Side::Sell, 100, 1),
        Err(Reject::Insufficient)
    );
    assert_eq!(s.enter(9, S0, Side::Buy, 1, 1), Err(Reject::Insufficient));
    assert_eq!(s.enter(1, 99, Side::Buy, 1, 1), Err(Reject::UnknownSymbol));
    // ownership of cancels
    assert_eq!(s.cancel(2, o), Err(Reject::NotOwner));
    assert_eq!(s.cancel(1, o), Ok(()));
    assert_eq!(s.cancel(1, o), Err(Reject::AlreadyCancelling));
    s.check().unwrap();
}

#[test]
fn fills_are_reported_to_the_ledger_exactly() {
    let mut s = slice();
    s.grant(1, USD, 1000);
    s.grant(2, BTC, 5000);
    let buy = s.enter(1, S0, Side::Buy, 100, 4).unwrap();
    let sell = s.enter(2, S0, Side::Sell, 90, 3).unwrap();
    // nothing to report until fills are settled
    assert!(s.report().is_empty());
    s.settle(&[Event::Trade {
        buy,
        sell,
        price: 95,
        qty: 3,
        taker_is_buy: true,
    }]);
    s.check().unwrap();
    let rep = s.report();
    // buyer paid 285 USD, got 3000 BTC less 1.5% = 45; seller the mirror
    let find = |want: Event| assert!(rep.contains(&want), "{want:?} missing from {rep:?}");
    find(Event::SliceOut {
        account: 1,
        asset: USD,
        amount: 285,
    });
    find(Event::SliceIn {
        account: 1,
        asset: BTC,
        amount: 2955,
    });
    find(Event::SliceOut {
        account: 2,
        asset: BTC,
        amount: 3000,
    });
    find(Event::SliceIn {
        account: 2,
        asset: USD,
        amount: 281,
    });
    find(Event::SliceFee {
        asset: BTC,
        amount: 45,
    });
    find(Event::SliceFee {
        asset: USD,
        amount: 4,
    });
    assert_eq!(rep.len(), 6);
    // value moved, none created: per asset, in + fee == out
    for a in [BTC, USD] {
        let mut net = 0i128;
        for e in &rep {
            match *e {
                Event::SliceIn { asset, amount, .. } | Event::SliceFee { asset, amount }
                    if asset == a =>
                {
                    net += amount as i128
                }
                Event::SliceOut { asset, amount, .. } if asset == a => net -= amount as i128,
                _ => {}
            }
        }
        assert_eq!(net, 0, "asset {a}");
    }
    // a second report has nothing new
    assert!(s.report().is_empty());
    // the filled seller's order is gone, the buyer's remainder is still held
    assert_eq!(s.open_orders(), 1);
    assert_eq!(s.available(1, USD), 600 + 15, "price improvement came back");
    s.check().unwrap();
}

#[test]
fn only_uncommitted_credit_is_ever_recalled_and_net_is_reported_first() {
    let mut s = slice();
    s.grant(1, USD, 1000);
    s.grant(2, BTC, 5000);
    let buy = s.enter(1, S0, Side::Buy, 100, 4).unwrap(); // commits 400
    let sell = s.enter(2, S0, Side::Sell, 90, 3).unwrap();
    s.settle(&[Event::Trade {
        buy,
        sell,
        price: 95,
        qty: 3,
        taker_is_buy: false,
    }]);
    // account 1 asks to recall everything: only the free part comes back, and
    // what its slice gained or lost is reported before the return
    let ev = s.recall(1, USD, 10_000);
    assert!(
        matches!(
            ev[0],
            Event::SliceOut {
                account: 1,
                asset: USD,
                ..
            }
        ),
        "{ev:?}"
    );
    let ret = ev.last().unwrap();
    // 1000 granted, 285 paid, 100 still committed for the unfilled lot: 615 free
    assert_eq!(
        *ret,
        Event::Returned {
            account: 1,
            asset: USD,
            amount: 615
        }
    );
    assert_eq!(s.available(1, USD), 0);
    assert_eq!(s.total(1, USD), 100, "the committed part stays");
    // nothing free: nothing returned
    assert!(s.recall(1, USD, 5).is_empty());
    s.check().unwrap();
}

#[test]
fn a_slice_ignores_events_about_phase_one_orders() {
    let mut s = slice();
    s.grant(1, USD, 1000);
    let slice_buy = s.enter(1, S0, Side::Buy, 100, 2).unwrap();
    let ledger_sell = 5; // shard 0, counter 5: a phase 1 order
    s.settle(&[
        Event::Trade {
            buy: slice_buy,
            sell: ledger_sell,
            price: 100,
            qty: 1,
            taker_is_buy: true,
        },
        Event::Closed { order: ledger_sell },
        Event::Trade {
            buy: 6,
            sell: 7,
            price: 1,
            qty: 1,
            taker_is_buy: true,
        },
    ]);
    assert_eq!(s.anomalies(), 0);
    let rep = s.report();
    // only the slice side moved: the buyer paid 100 USD and got 1000 BTC less fee
    assert!(rep.contains(&Event::SliceOut {
        account: 1,
        asset: USD,
        amount: 100
    }));
    assert!(rep.contains(&Event::SliceIn {
        account: 1,
        asset: BTC,
        amount: 985
    }));
    s.check().unwrap();
}

#[test]
fn a_slice_snapshot_restores_exactly_and_continues_identically() {
    let mut a = slice();
    a.grant(1, USD, 1000);
    a.grant(2, BTC, 5000);
    let buy = a.enter(1, S0, Side::Buy, 100, 4).unwrap();
    let sell = a.enter(2, S0, Side::Sell, 90, 3).unwrap();
    a.settle(&[Event::Trade {
        buy,
        sell,
        price: 95,
        qty: 3,
        taker_is_buy: true,
    }]);
    a.report();
    a.settle(&[Event::Trade {
        buy,
        sell: 3,
        price: 99,
        qty: 1,
        taker_is_buy: true,
    }]);
    let snap = a.snapshot();
    let mut b = slice();
    b.restore(&snap).unwrap();
    assert_eq!(b.snapshot(), snap);
    assert_eq!(b.digest(), a.digest());
    assert_eq!(a.report(), b.report());
    assert_eq!(a.recall(1, USD, 50), b.recall(1, USD, 50));
    assert_eq!(a.digest(), b.digest());
    for cut in [0, 3, snap.len() / 2, snap.len() - 1] {
        assert!(slice().restore(&snap[..cut]).is_err());
    }
    // a snapshot of another group is refused
    assert!(Slice::new(config(0), 3).restore(&snap).is_err());
}
