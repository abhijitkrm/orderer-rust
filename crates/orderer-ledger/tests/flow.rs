//! Unit tests for the relays' pure logic and the ledger's gap rule.

use orderer_ledger::*;
use std::collections::BTreeMap;

fn cfg(shard: u16) -> Config {
    let mut symbols = BTreeMap::new();
    symbols.insert(
        1,
        Symbol {
            base: 1,
            quote: 2,
            lot: 10,
        },
    );
    Config {
        shard,
        fee_ppm: 0,
        symbols,
        fees: BTreeMap::new(),
    }
}

fn closed(shard: u16, n: u64) -> Event {
    Event::Closed {
        order: ((shard as u64) << SHARD_SHIFT) | n,
    }
}

#[test]
fn intake_accepts_only_the_next_index() {
    let mut i = Intake::new();
    assert_eq!(i.accept(0, 1), Accept::Gap);
    assert_eq!(i.accept(0, 0), Accept::Applied);
    assert_eq!(i.accept(0, 0), Accept::Duplicate);
    assert_eq!(i.accept(0, 2), Accept::Gap);
    assert_eq!(i.accept(0, 1), Accept::Applied);
    assert_eq!(i.cursor(0), 2);
    // shards are independent
    assert_eq!(i.cursor(1), 0);
    assert_eq!(i.accept(1, 0), Accept::Applied);
}

#[test]
fn a_fresh_forwarder_resumes_from_the_ledger_outbox() {
    let mut l = Ledger::new(cfg(0));
    l.apply(&Cmd::Deposit {
        account: 1,
        asset: 2,
        amount: 1000,
        external_id: 1,
    });
    for _ in 0..5 {
        l.apply(&Cmd::Reserve {
            account: 1,
            symbol: 1,
            side: Side::Buy,
            price: 1,
            qty: 1,
        });
    }
    let mut f = Forwarder::new(0);
    let b = f.poll(&l, 3);
    assert_eq!(b.iter().map(|s| s.idx).collect::<Vec<_>>(), [0, 1, 2]);
    assert_eq!(
        f.poll(&l, 10).iter().map(|s| s.idx).collect::<Vec<_>>(),
        [3, 4]
    );
    assert!(f.poll(&l, 10).is_empty());
    // matching accepted 0..3 only; a gap reply sends the forwarder back
    assert_eq!(f.on_reply(3, true), Some(Cmd::OutboxAck { upto: 3 }));
    assert_eq!(
        f.poll(&l, 10).iter().map(|s| s.idx).collect::<Vec<_>>(),
        [3, 4]
    );
    // a stale reply never lowers what is acked
    assert_eq!(f.on_reply(2, false), None);
    // after the ledger trims, a brand-new forwarder starts at the base
    l.apply(&Cmd::OutboxAck { upto: 3 });
    let mut g = Forwarder::new(0);
    assert_eq!(
        g.poll(&l, 10).iter().map(|s| s.idx).collect::<Vec<_>>(),
        [3, 4]
    );
    // and a reply ahead of it (matching already has more) skips ahead
    assert_eq!(g.on_reply(5, false), Some(Cmd::OutboxAck { upto: 5 }));
    g.rewind();
    assert!(g.poll(&l, 10).is_empty());
}

#[test]
fn settle_cmds_chain_only_this_shards_events_after_the_mark() {
    let events = vec![
        (1, closed(0, 1)),
        (2, closed(1, 1)),
        (
            3,
            Event::Trade {
                buy: (1 << SHARD_SHIFT) | 2,
                sell: 2,
                price: 5,
                qty: 1,
                taker_is_buy: true,
            },
        ),
        (4, closed(1, 3)),
        (5, closed(2, 1)),
    ];
    let prevs = |shard, hw, max| -> Vec<(u64, u64)> {
        settle_cmds(7, shard, hw, events.clone(), max)
            .into_iter()
            .map(|c| match c {
                Cmd::Settle {
                    source: 7,
                    prev,
                    idx,
                    ..
                } => (prev, idx),
                c => panic!("{c:?}"),
            })
            .collect()
    };
    assert_eq!(prevs(0, 0, 10), [(0, 1), (1, 3)]);
    assert_eq!(prevs(1, 0, 10), [(0, 2), (2, 3), (3, 4)]);
    assert_eq!(prevs(1, 2, 10), [(2, 3), (3, 4)]);
    assert_eq!(prevs(1, 0, 2), [(0, 2), (2, 3)]);
    assert_eq!(prevs(2, 0, 10), [(0, 5)]);
    assert!(prevs(3, 0, 10).is_empty());
}

#[test]
fn the_ledger_refuses_a_settle_that_skips_or_is_out_of_order() {
    let mut l = Ledger::new(cfg(0));
    l.apply(&Cmd::Deposit {
        account: 1,
        asset: 2,
        amount: 100,
        external_id: 1,
    });
    let mut orders = vec![];
    for _ in 0..3 {
        if let Outcome::Reserved { order, .. } = l.apply(&Cmd::Reserve {
            account: 1,
            symbol: 1,
            side: Side::Buy,
            price: 1,
            qty: 1,
        }) {
            orders.push(order);
        }
    }
    let s = |prev, idx, o| Cmd::Settle {
        source: 0,
        prev,
        idx,
        event: Event::Closed { order: o },
    };
    // event 8 (prev 5) arrives before event 5 (prev 0): refused, nothing changes
    let d = l.digest();
    assert_eq!(l.apply(&s(5, 8, orders[1])), Outcome::Rejected(Reject::Gap));
    assert_eq!(l.digest(), d);
    assert_eq!(l.high_water(0), None);
    assert_eq!(l.apply(&s(0, 5, orders[0])), Outcome::Settled);
    // a message built from an older view (prev 0) is now a duplicate or a gap, never applied
    assert_eq!(l.apply(&s(0, 5, orders[0])), Outcome::Duplicate);
    assert_eq!(l.apply(&s(0, 9, orders[1])), Outcome::Rejected(Reject::Gap));
    assert_eq!(l.apply(&s(5, 8, orders[1])), Outcome::Settled);
    // idx must exceed prev
    assert_eq!(l.apply(&s(8, 8, orders[2])), Outcome::Duplicate);
    assert_eq!(l.apply(&s(8, 9, orders[2])), Outcome::Settled);
    l.check().unwrap();
}

#[test]
fn route_settles_equals_the_per_shard_chains() {
    // random streams over phase 1 and phase 2 events, shards 0..3, marks anywhere
    let mut x = 0x2545_F491_4F6C_DD1Du64;
    let mut rnd = |n: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % n
    };
    for _ in 0..300 {
        let shards = 1 + rnd(4) as usize;
        let shard_of = |a: u64| (a % shards as u64) as u16;
        let events: Vec<(u64, Event)> = (1..=40)
            .map(|i| {
                let id = |r: u64| ((r % (shards as u64 + 1)) << SHARD_SHIFT) | (1 + r);
                let e = match rnd(6) {
                    0 | 1 => Event::Trade {
                        buy: id(rnd(9)),
                        sell: id(rnd(9)),
                        price: 5,
                        qty: 1,
                        taker_is_buy: false,
                    },
                    2 => Event::Closed { order: id(rnd(9)) },
                    3 => Event::SliceIn {
                        group: 0,
                        account: rnd(7),
                        asset: 1,
                        amount: 1,
                    },
                    4 => Event::Returned {
                        group: 0,
                        account: rnd(7),
                        asset: 1,
                        amount: 1,
                    },
                    _ => Event::SliceFee {
                        asset: 2,
                        amount: 1,
                    },
                };
                (i, e)
            })
            .collect();
        let marks: Vec<u64> = (0..shards).map(|_| rnd(30)).collect();
        let max = 1 + rnd(8) as usize;
        let fee_shard = rnd(shards as u64) as u16;
        let routed = route_settles(3, &marks, events.clone(), max, shard_of, fee_shard);
        for s in 0..shards {
            let want = settle_cmds_routed(
                3,
                s as u16,
                marks[s],
                events.clone(),
                max,
                shard_of,
                fee_shard,
            );
            assert_eq!(routed[s], want, "shard {s}");
        }
    }
}
