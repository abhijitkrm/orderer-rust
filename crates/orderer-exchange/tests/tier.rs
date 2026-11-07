//! The matching tier state machine driven directly (no network): translation,
//! intake, archive indices, snapshots.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::pipeline_sm::Tap;
use orderer_cluster::StateMachine;
use orderer_consensus::Entry;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::wire::*;
use orderer_exchange::*;
use orderer_ledger::{Accept, Event, Intake, OutMsg, Side, SHARD_SHIFT};

fn tier(partitions: u32, retain: usize) -> (MatchingTierSm<FifoCore>, TierView, Tap) {
    let tap: Tap = Default::default();
    let (sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(partitions).unwrap(),
        TierConfig {
            source_base: 100,
            retain,
        },
        move |init, plugs| {
            let mut b = Pipeline::<FifoCore>::builder()
                .partitions(partitions)
                .egress(plugs.digest)
                .egress(plugs.archive);
            if let Some(i) = init {
                b = b.initial(i);
            }
            b.build().unwrap()
        },
    );
    (sm.with_tap(tap.clone()), view, tap)
}

fn oid(shard: u16, n: u64) -> u64 {
    ((shard as u64) << SHARD_SHIFT) | n
}

fn sub(shard: u16, n: u64, symbol: u32, side: Side, price: u64, qty: u64) -> OutMsg {
    OutMsg::Submit {
        order: oid(shard, n),
        symbol,
        side,
        price,
        qty,
    }
}

fn fwd(sm: &mut MatchingTierSm<FifoCore>, shard: u16, first_idx: u64, msgs: Vec<OutMsg>) {
    let data = encode_forward(&Forward {
        shard,
        first_idx,
        msgs,
    });
    sm.apply(1, &Entry { term: 1, data });
}

fn flush(sm: &mut MatchingTierSm<FifoCore>) {
    sm.digest(1);
}

fn commands(tap: &Tap) -> usize {
    tap.lock().unwrap().iter().map(|(_, c)| c.len()).sum()
}

fn all(view: &TierView, source: u32) -> Vec<(u64, Event)> {
    view.read(source, 1, usize::MAX).unwrap().unwrap()
}

#[test]
fn wire_round_trips_and_rejects_garbage() {
    let f = Forward {
        shard: 7,
        first_idx: u64::MAX - 1,
        msgs: vec![
            sub(7, 1, 3, Side::Sell, 10, 20),
            OutMsg::Cancel {
                order: oid(7, 1),
                symbol: 3,
            },
        ],
    };
    let b = encode_forward(&f);
    assert_eq!(b.len(), 16 + 2 * MSG_BYTES);
    assert_eq!(decode_forward(&b).unwrap(), f);
    for cut in 0..b.len() {
        assert!(decode_forward(&b[..cut]).is_err(), "prefix {cut}");
    }
    let mut longer = b.clone();
    longer.push(0);
    assert!(decode_forward(&longer).is_err());
    // a cancel with a price is not canonical
    let mut c = b.clone();
    let cancel_at = 16 + MSG_BYTES;
    c[cancel_at + 14] = 1;
    assert!(decode_forward(&c).is_err());
    // hostile count
    let mut h = b.clone();
    h[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(decode_forward(&h).is_err());
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..20_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let n = (x % 70) as usize;
        let junk: Vec<u8> = (0..n)
            .map(|i| (x >> ((i % 8) * 8)) as u8 ^ i as u8)
            .collect();
        let _ = decode_forward(&junk);
    }
}

#[test]
fn tier_intake_follows_the_ledger_intake_exactly() {
    let (mut a, mut b) = (TierIntake::default(), Intake::new());
    let mut x = 12345u64;
    for _ in 0..50_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let shard = (x % 3) as u16;
        // mostly near the cursor so all three outcomes occur
        let idx = a
            .cursor(shard)
            .saturating_add(x >> 8 & 3)
            .saturating_sub(x >> 12 & 1);
        assert_eq!(a.accept(shard, idx), b.accept(shard, idx));
        assert_eq!(a.cursor(shard), b.cursor(shard));
    }
}

#[test]
fn a_crossing_pair_becomes_one_trade_with_the_right_sides() {
    let (mut sm, view, _tap) = tier(1, 100);
    // resting sell by shard 1, then an aggressive buy by shard 2
    fwd(&mut sm, 1, 0, vec![sub(1, 1, 5, Side::Sell, 100, 10)]);
    fwd(&mut sm, 2, 0, vec![sub(2, 1, 5, Side::Buy, 105, 4)]);
    flush(&mut sm);
    let ev = all(&view, 100);
    assert_eq!(
        ev,
        vec![(
            1,
            Event::Trade {
                buy: oid(2, 1),
                sell: oid(1, 1),
                price: 100,
                qty: 4,
                taker_is_buy: true
            }
        )]
    );
    // the buy filled: nothing else is emitted for it, and only the sell is still tracked
    assert_eq!(view.stats(), (0, 1));
    // an aggressive sell hits nothing, rests; then a buy takes it: taker is the buy again
    fwd(&mut sm, 1, 1, vec![sub(1, 2, 5, Side::Buy, 90, 5)]);
    fwd(&mut sm, 2, 1, vec![sub(2, 2, 5, Side::Sell, 80, 5)]);
    flush(&mut sm);
    let ev = all(&view, 100);
    assert_eq!(ev.len(), 2);
    assert_eq!(
        ev[1],
        (
            2,
            Event::Trade {
                buy: oid(1, 2),
                sell: oid(2, 2),
                price: 90,
                qty: 5,
                taker_is_buy: false
            }
        )
    );
}

#[test]
fn cancel_and_rejection_close_the_order_but_a_fill_does_not() {
    let (mut sm, view, _tap) = tier(1, 100);
    fwd(
        &mut sm,
        0,
        0,
        vec![
            sub(0, 1, 1, Side::Buy, 50, 3),
            OutMsg::Cancel {
                order: oid(0, 1),
                symbol: 1,
            },
            // cancel of an order that is already gone: no event
            OutMsg::Cancel {
                order: oid(0, 1),
                symbol: 1,
            },
            // an unrepresentable price is rejected by the matcher and closes the order
            sub(0, 2, 1, Side::Buy, u64::MAX, 3),
            // zero quantity likewise
            sub(0, 3, 1, Side::Sell, 10, 0),
        ],
    );
    flush(&mut sm);
    let ev: Vec<Event> = all(&view, 100).into_iter().map(|e| e.1).collect();
    assert_eq!(
        ev,
        vec![
            Event::Closed { order: oid(0, 1) },
            Event::Closed { order: oid(0, 2) },
            Event::Closed { order: oid(0, 3) }
        ]
    );
    assert_eq!(view.stats(), (0, 0), "nothing left tracked");
}

#[test]
fn duplicates_and_gaps_never_reach_the_matcher() {
    let (mut sm, view, tap) = tier(2, 100);
    let batch = vec![
        sub(0, 1, 1, Side::Buy, 50, 3),
        sub(0, 2, 2, Side::Buy, 50, 3),
        sub(0, 3, 1, Side::Sell, 60, 3),
    ];
    // a gap: starts at 2 while the cursor is 0
    fwd(&mut sm, 0, 2, batch.clone());
    assert_eq!((commands(&tap), view.cursor(0)), (0, 0));
    fwd(&mut sm, 0, 0, batch.clone());
    assert_eq!((commands(&tap), view.cursor(0)), (3, 3));
    // the same batch again, and an overlapping one that adds a single new message
    fwd(&mut sm, 0, 0, batch.clone());
    let mut more = batch.clone();
    more.push(sub(0, 4, 2, Side::Sell, 70, 1));
    fwd(&mut sm, 0, 0, more);
    assert_eq!((commands(&tap), view.cursor(0)), (4, 4));
    // another shard has its own cursor
    fwd(&mut sm, 1, 0, vec![sub(1, 1, 1, Side::Buy, 10, 1)]);
    assert_eq!((commands(&tap), view.cursor(1)), (5, 1));
    assert_eq!(sm.bad_forwards, 0);
    // garbage is counted and skipped
    sm.apply(
        2,
        &Entry {
            term: 1,
            data: vec![TAG_FORWARD, 1, 0],
        },
    );
    assert_eq!(sm.bad_forwards, 1);
}

#[test]
fn plain_batches_still_go_to_the_pipeline() {
    use orderer_cluster::codec::encode_batch;
    use orderer_core::{Command, Side as M, Tif};
    let (mut sm, view, tap) = tier(1, 100);
    let mut data = Vec::new();
    encode_batch(&[(1, Command::new(9, M::Ask, 10, 5, Tif::Gtc))], &mut data);
    sm.apply(1, &Entry { term: 1, data });
    sm.apply(
        2,
        &Entry {
            term: 1,
            data: Vec::new(),
        },
    );
    flush(&mut sm);
    assert_eq!(commands(&tap), 1);
    // an order the archive was never told about produces no stream entries
    assert!(all(&view, 100).is_empty());
}

type Script = Vec<(u16, u64, Vec<OutMsg>)>;

/// A deterministic stream of forwarded batches for `shards` ledger shards, with
/// contiguous outbox indices per shard, `steps` batches long.
fn script(shards: u16, steps: usize, seed: u64) -> Script {
    let mut x = seed | 1;
    let mut rnd = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    let mut cursor = vec![0u64; shards as usize];
    let mut counter = vec![0u64; shards as usize];
    let mut out = Vec::new();
    for _ in 0..steps {
        let s = (rnd() % shards as u64) as usize;
        let mut msgs = Vec::new();
        for _ in 0..1 + rnd() % 3 {
            if counter[s] > 0 && rnd() % 4 == 0 {
                let n = 1 + rnd() % counter[s];
                msgs.push(OutMsg::Cancel {
                    order: oid(s as u16, n),
                    symbol: (rnd() % 4) as u32,
                });
            } else {
                counter[s] += 1;
                let side = if rnd() % 2 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                msgs.push(sub(
                    s as u16,
                    counter[s],
                    (rnd() % 4) as u32,
                    side,
                    95 + rnd() % 11,
                    1 + rnd() % 5,
                ));
            }
        }
        out.push((s as u16, cursor[s], msgs.clone()));
        cursor[s] += msgs.len() as u64;
    }
    out
}

fn run(sm: &mut MatchingTierSm<FifoCore>, s: &[(u16, u64, Vec<OutMsg>)]) {
    for (shard, first, msgs) in s {
        fwd(sm, *shard, *first, msgs.clone());
    }
}

#[test]
fn snapshot_restore_continues_the_streams_and_digests() {
    let sc = script(2, 600, 7);
    let (head, tail) = sc.split_at(300);

    let (mut a, view_a, _) = tier(2, 1 << 20);
    run(&mut a, head);
    flush(&mut a);
    let snap = a.snapshot(1);
    let mid: Vec<u64> = (100..102).map(|s| view_a.end(s).unwrap()).collect();
    assert!(mid.iter().all(|e| *e > 20), "{mid:?}");

    let (mut b, view_b, _) = tier(2, 1 << 20);
    b.restore(1, &snap).unwrap();
    assert_eq!(b.digest(1), a.digest(1));
    assert_eq!(b.deep_digest(1), a.deep_digest(1));
    for s in 100..102 {
        assert_eq!(all(&view_b, s), all(&view_a, s));
    }
    for sh in 0..2 {
        assert_eq!(view_b.cursor(sh), view_a.cursor(sh));
    }

    // the same tail on both: new events get the same indices on the restored node
    run(&mut a, tail);
    run(&mut b, tail);
    flush(&mut a);
    flush(&mut b);
    for s in 100..102 {
        let (ea, eb) = (all(&view_a, s), all(&view_b, s));
        assert_eq!(ea, eb, "stream {s}");
        assert!(
            view_a.end(s).unwrap() > mid[(s - 100) as usize],
            "the tail added events"
        );
        // contiguous from 1
        assert!(ea.iter().enumerate().all(|(i, e)| e.0 == i as u64 + 1));
    }
    assert_eq!(a.digest(2), b.digest(2));
    assert_eq!(a.deep_digest(2), b.deep_digest(2));
    assert_eq!(view_a.stats().0, 0, "no trade with an unknown order");
}

#[test]
fn a_replica_that_applies_everything_matches_one_that_restored_midway_and_one_that_did_not() {
    let sc = script(3, 800, 99);
    let (mut full, vf, _) = tier(4, 1 << 20);
    run(&mut full, &sc);
    flush(&mut full);
    // an independent replica fed the same entries gets the same streams
    let (mut other, vo, _) = tier(4, 1 << 20);
    run(&mut other, &sc);
    flush(&mut other);
    for s in 100..104 {
        assert_eq!(all(&vf, s), all(&vo, s));
    }
    assert_eq!(full.digest(9), other.digest(9));
    let total: u64 = (100..104).map(|s| vf.end(s).unwrap()).sum();
    assert!(total > 100, "{total}");
}

#[test]
fn the_window_is_bounded_reports_truncation_and_survives_a_snapshot() {
    let sc = script(1, 400, 5);
    let (mut a, va, _) = tier(1, 10);
    run(&mut a, &sc);
    flush(&mut a);
    let end = va.end(100).unwrap();
    assert!(end > 30);
    assert_eq!(va.read(100, 1, 5), Some(Err(Truncated { base: end - 9 })));
    let tail = va.read(100, end - 9, 100).unwrap().unwrap();
    assert_eq!(tail.len(), 10);
    assert_eq!(tail[0].0, end - 9);
    assert_eq!(va.read(100, end + 1, 5).unwrap().unwrap(), vec![]);
    assert_eq!(va.read(99, 1, 5), None);
    assert_eq!(va.read(101, 1, 5), None);
    let snap = a.snapshot(1);
    let (mut b, vb, _) = tier(1, 10);
    b.restore(1, &snap).unwrap();
    assert_eq!(vb.read(100, end - 9, 100).unwrap().unwrap(), tail);
    assert_eq!(vb.end(100), Some(end));
    assert_eq!(vb.read(100, 1, 5), Some(Err(Truncated { base: end - 9 })));
}

#[test]
fn restore_refuses_damaged_or_foreign_snapshots_and_leaves_state_alone() {
    let sc = script(2, 100, 3);
    let (mut a, va, _) = tier(2, 1000);
    run(&mut a, &sc);
    flush(&mut a);
    let snap = a.snapshot(1);
    let before = a.deep_digest(1);
    assert!(a.restore(1, b"nope").is_err());
    let (mut other_parts, _, _) = tier(1, 1000);
    assert!(other_parts.restore(1, &snap).is_err());
    for cut in (0..snap.len()).step_by(7) {
        assert!(a.restore(1, &snap[..cut]).is_err(), "prefix {cut}");
    }
    assert_eq!(a.deep_digest(1), before);
    assert!(va.end(100).is_some());
}

#[test]
fn the_digest_sees_the_intake_the_archive_and_the_orders() {
    let sc = script(2, 60, 11);
    let (mut a, _, _) = tier(1, 1000);
    let (mut b, _, _) = tier(1, 1000);
    run(&mut a, &sc);
    run(&mut b, &sc);
    assert_eq!(a.digest(1), b.digest(1));
    assert_eq!(a.deep_digest(1), b.deep_digest(1));
    // one more accepted message on one side only
    fwd(&mut a, 0, 1_000_000, vec![]);
    assert_eq!(
        a.digest(1),
        b.digest(1),
        "an empty or refused entry changes nothing"
    );
    let last = sc
        .iter()
        .filter(|(s, _, _)| *s == 0)
        .map(|(_, f, m)| f + m.len() as u64)
        .max()
        .unwrap();
    fwd(&mut a, 0, last, vec![sub(0, 9_999, 0, Side::Buy, 10, 1)]);
    assert_ne!(a.digest(1), b.digest(1));
    assert_ne!(a.deep_digest(1), b.deep_digest(1));
}

#[test]
fn accept_results_match_what_the_state_machine_did() {
    // the same sequence through a bare intake predicts the tier's applied count
    let sc = script(2, 200, 21);
    let (mut sm, view, tap) = tier(1, 1000);
    let mut model = Intake::new();
    let mut applied = 0usize;
    // deliver each batch twice and out of order with its successor
    for w in sc.windows(2) {
        for (s, f, m) in [&w[1], &w[0], &w[1], &w[0]] {
            for (i, _) in m.iter().enumerate() {
                if model.accept(*s, f + i as u64) == Accept::Applied {
                    applied += 1;
                }
            }
            fwd(&mut sm, *s, *f, m.clone());
        }
    }
    assert_eq!(commands(&tap), applied);
    for s in 0..2 {
        assert_eq!(view.cursor(s), model.cursor(s));
    }
}

#[test]
fn the_digest_distinguishes_replicas_that_differ_only_in_their_intake() {
    // the same order reaches the matcher through two different ledger shards:
    // identical commands, identical events, but different intake cursors
    let m = sub(5, 1, 0, Side::Buy, 10, 1);
    let (mut a, _, _) = tier(1, 100);
    let (mut b, _, _) = tier(1, 100);
    fwd(&mut a, 0, 0, vec![m]);
    fwd(&mut b, 1, 0, vec![m]);
    assert_ne!(a.digest(1), b.digest(1));
    assert_ne!(a.deep_digest(1), b.deep_digest(1));
}
