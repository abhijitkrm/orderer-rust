//! The matching tier in credit mode (phase 2): orders entered at the group against
//! a slice of lent balances, credit epochs, the group stream, recalls, snapshots and
//! determinism across replicas. Driven directly (no network).

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::StateMachine;
use orderer_consensus::Entry;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::tier::credit::CreditConfig;
use orderer_exchange::wire::*;
use orderer_exchange::*;
use orderer_ledger::{
    is_slice_order, settle_cmds_routed, Cmd, Config, Event, Ledger, OutMsg, Outcome, Reject, Side,
    Symbol,
};
use std::collections::BTreeMap;

const BTC: u32 = 1;
const USD: u32 = 2;
const ETH: u32 = 3;
const EPOCH: u64 = 4;

fn ledger_cfg(shard: u16) -> Config {
    let mut symbols = BTreeMap::new();
    for (sym, base, lot) in [
        (10, BTC, 1000),
        (11, ETH, 10),
        (12, BTC, 1000),
        (13, ETH, 10),
    ] {
        symbols.insert(
            sym,
            Symbol {
                base,
                quote: USD,
                lot,
            },
        );
    }
    Config {
        shard,
        fee_ppm: 15_000,
        symbols,
        fees: BTreeMap::new(),
    }
}

struct Node {
    sm: MatchingTierSm<FifoCore>,
    view: TierView,
    idx: u64,
}

fn node(partitions: u32, epoch: u64) -> Node {
    let (sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(partitions).unwrap(),
        TierConfig {
            source_base: 100,
            retain: 1 << 16,
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
    let sm = sm.with_credit(CreditConfig {
        ledger: ledger_cfg(0),
        group: 0,
        epoch,
        retain: 1 << 16,
    });
    Node { sm, view, idx: 0 }
}

impl Node {
    fn put(&mut self, data: Vec<u8>) {
        self.idx += 1;
        self.sm.apply(self.idx, &Entry { term: 1, data });
    }
    fn forward(&mut self, shard: u16, first_idx: u64, msgs: Vec<OutMsg>) {
        self.put(encode_forward(&Forward {
            shard,
            first_idx,
            msgs,
        }));
    }
    fn req(&mut self, client: u64, seq: u64, body: SliceBody) {
        self.put(encode_slice_req(&SliceReq { client, seq, body }));
    }
    #[allow(clippy::too_many_arguments)]
    fn enter(
        &mut self,
        client: u64,
        seq: u64,
        account: u64,
        symbol: u32,
        side: Side,
        price: u64,
        qty: u64,
    ) {
        self.req(
            client,
            seq,
            SliceBody::Enter {
                account,
                symbol,
                side,
                price,
                qty,
            },
        );
    }
    /// Apply no-op entries up to the next epoch boundary and a full epoch more, so
    /// every earlier fill has been settled into the slice and reported.
    fn settle(&mut self) {
        for _ in 0..(2 * EPOCH.max(self.epoch_len())) {
            self.put(Vec::new());
        }
    }
    fn epoch_len(&self) -> u64 {
        EPOCH
    }
    fn stream(&self) -> Vec<Event> {
        let g = self.view.group_source().unwrap();
        self.view
            .read(g, 1, usize::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|(_, e)| e)
            .collect()
    }
}

fn grant(account: u64, asset: u32, amount: u128) -> OutMsg {
    OutMsg::Grant {
        group: 0,
        account,
        asset,
        amount,
    }
}

#[test]
fn slice_wire_round_trips_and_rejects_garbage() {
    for q in [
        SliceReq {
            client: u64::MAX,
            seq: 7,
            body: SliceBody::Enter {
                account: 3,
                symbol: 10,
                side: Side::Sell,
                price: u64::MAX,
                qty: 5,
            },
        },
        SliceReq {
            client: 1,
            seq: 2,
            body: SliceBody::Cancel {
                account: 3,
                order: 4,
            },
        },
    ] {
        let b = encode_slice_req(&q);
        assert_eq!(decode_slice_req(&b).unwrap(), q);
        for cut in 0..b.len() {
            assert!(decode_slice_req(&b[..cut]).is_err());
        }
        let mut longer = b.clone();
        longer.push(0);
        assert!(decode_slice_req(&longer).is_err());
    }
    // grants and recalls travel in the ordinary forwarded batch
    let f = Forward {
        shard: 3,
        first_idx: 9,
        msgs: vec![
            grant(7, USD, u128::MAX),
            OutMsg::Recall {
                group: 0,
                account: 7,
                asset: USD,
                amount: (1u128 << 64) + 5,
            },
        ],
    };
    assert_eq!(decode_forward(&encode_forward(&f)).unwrap(), f);
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..20_000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let n = (x % 60) as usize;
        let junk: Vec<u8> = (0..n)
            .map(|i| (x >> ((i % 8) * 8)) as u8 ^ i as u8)
            .collect();
        let _ = decode_slice_req(&junk);
    }
}

#[test]
fn an_order_is_checked_against_credit_and_enters_in_one_entry() {
    let mut n = node(1, EPOCH);
    n.forward(1, 0, vec![grant(7, USD, 1000), grant(8, BTC, 5000)]);
    n.enter(1, 1, 7, 10, Side::Buy, 100, 4);
    let (seq, res) = n.view.slice_result(1).unwrap();
    assert_eq!(seq, 1);
    let order = res.unwrap();
    assert!(is_slice_order(order));
    // credit is committed at once: no ledger round trip, no epoch needed
    assert_eq!(n.view.with_slice(|s| s.available(7, USD)).unwrap(), 600);
    // more than the credit left: refused locally
    n.enter(1, 2, 7, 10, Side::Buy, 100, 20);
    assert_eq!(
        n.view.slice_result(1).unwrap(),
        (2, Err(Reject::Insufficient))
    );
    // no credit at all
    n.enter(1, 3, 9, 10, Side::Buy, 1, 1);
    assert_eq!(
        n.view.slice_result(1).unwrap(),
        (3, Err(Reject::Insufficient))
    );
    // the same request again (a retry) changes nothing, an older one is ignored
    n.enter(1, 3, 7, 10, Side::Buy, 100, 1);
    assert_eq!(
        n.view.slice_result(1).unwrap(),
        (3, Err(Reject::Insufficient))
    );
    n.enter(1, 1, 7, 10, Side::Buy, 100, 1);
    assert_eq!(n.view.slice_result(1).unwrap().0, 3);
    assert_eq!(n.view.with_slice(|s| s.available(7, USD)).unwrap(), 600);
    assert_eq!(n.view.with_slice(|s| s.open_orders()).unwrap(), 1);
}

#[test]
fn fills_become_group_stream_reports_at_the_epoch() {
    let mut n = node(1, EPOCH);
    n.forward(1, 0, vec![grant(7, USD, 1000), grant(8, BTC, 5000)]);
    n.enter(1, 1, 7, 10, Side::Buy, 100, 4);
    n.enter(2, 1, 8, 10, Side::Sell, 90, 3);
    // before an epoch nothing has been reported
    assert!(n.stream().is_empty());
    n.settle();
    let ev = n.stream();
    // the trade is at the resting buy's price, 100; the seller was the taker
    let has = |e: Event| assert!(ev.contains(&e), "{e:?} missing from {ev:?}");
    has(Event::SliceOut {
        group: 0,
        account: 7,
        asset: USD,
        amount: 300,
    });
    has(Event::SliceIn {
        group: 0,
        account: 7,
        asset: BTC,
        amount: 2955,
    });
    has(Event::SliceOut {
        group: 0,
        account: 8,
        asset: BTC,
        amount: 3000,
    });
    has(Event::SliceIn {
        group: 0,
        account: 8,
        asset: USD,
        amount: 296,
    });
    has(Event::SliceFee {
        asset: BTC,
        amount: 45,
    });
    has(Event::SliceFee {
        asset: USD,
        amount: 4,
    });
    assert_eq!(ev.len(), 6);
    // proceeds are credit again, and the group stream is readable by index
    assert_eq!(n.view.with_slice(|s| s.available(8, USD)).unwrap(), 296);
    let g = n.view.group_source().unwrap();
    assert_eq!(g, 101); // source_base + partitions
    assert_eq!(n.view.end(g), Some(6));
    let tail = n.view.read(g, 5, 10).unwrap().unwrap();
    assert_eq!(tail.iter().map(|t| t.0).collect::<Vec<_>>(), [5, 6]);
    n.view.with_slice(|s| s.check().unwrap()).unwrap();
}

#[test]
fn a_recall_returns_only_free_credit_after_reporting_what_was_gained() {
    let mut n = node(1, EPOCH);
    n.forward(1, 0, vec![grant(7, USD, 1000), grant(8, BTC, 5000)]);
    n.enter(1, 1, 7, 10, Side::Buy, 100, 4);
    n.enter(2, 1, 8, 10, Side::Sell, 90, 3);
    n.settle();
    let before = n.stream().len();
    n.forward(
        1,
        2,
        vec![OutMsg::Recall {
            group: 0,
            account: 7,
            asset: USD,
            amount: 100_000,
        }],
    );
    let ev = n.stream();
    // 1000 granted, 300 paid, 100 still committed for the unfilled lot: 600 free
    assert_eq!(
        ev[before..],
        [Event::Returned {
            group: 0,
            account: 7,
            asset: USD,
            amount: 600
        }]
    );
    assert_eq!(n.view.with_slice(|s| s.total(7, USD)).unwrap(), 100);
    // a cancel releases the committed part; after the epoch it can be recalled
    let order = n.view.slice_result(1).unwrap().1.unwrap();
    n.req(1, 2, SliceBody::Cancel { account: 7, order });
    assert_eq!(n.view.slice_result(1).unwrap(), (2, Ok(order)));
    n.req(1, 3, SliceBody::Cancel { account: 8, order });
    assert_eq!(n.view.slice_result(1).unwrap(), (3, Err(Reject::NotOwner)));
    n.req(1, 4, SliceBody::Cancel { account: 7, order });
    assert_eq!(
        n.view.slice_result(1).unwrap(),
        (4, Err(Reject::AlreadyCancelling))
    );
    n.settle();
    n.forward(
        1,
        3,
        vec![OutMsg::Recall {
            group: 0,
            account: 7,
            asset: USD,
            amount: 100_000,
        }],
    );
    assert_eq!(
        n.stream().last(),
        Some(&Event::Returned {
            group: 0,
            account: 7,
            asset: USD,
            amount: 100
        })
    );
    assert_eq!(n.view.with_slice(|s| s.total(7, USD)).unwrap(), 0);
}

#[test]
fn a_duplicate_grant_batch_is_applied_once() {
    let mut n = node(1, EPOCH);
    for _ in 0..3 {
        n.forward(1, 0, vec![grant(7, USD, 1000)]);
    }
    assert_eq!(n.view.with_slice(|s| s.total(7, USD)).unwrap(), 1000);
    // a gap is refused too
    n.forward(1, 5, vec![grant(7, USD, 1000)]);
    assert_eq!(n.view.with_slice(|s| s.total(7, USD)).unwrap(), 1000);
    n.forward(1, 1, vec![grant(7, USD, 1)]);
    assert_eq!(n.view.with_slice(|s| s.total(7, USD)).unwrap(), 1001);
}

#[test]
fn grants_and_recalls_for_another_group_are_consumed_without_effect() {
    // this node is credit group 0; messages addressed to group 1 are consumed (the
    // intake cursor moves, so the stream stays contiguous) but touch no credit
    let mut n = node(1, EPOCH);
    let other = |account, amount| OutMsg::Grant {
        group: 1,
        account,
        asset: USD,
        amount,
    };
    n.forward(1, 0, vec![grant(7, USD, 1000), other(7, 5000), other(8, 9)]);
    assert_eq!(n.view.cursor(1), 3);
    assert_eq!(n.view.with_slice(|s| s.total(7, USD)).unwrap(), 1000);
    assert_eq!(n.view.with_slice(|s| s.total(8, USD)).unwrap(), 0);
    let before = n.stream().len();
    n.forward(
        1,
        3,
        vec![OutMsg::Recall {
            group: 1,
            account: 7,
            asset: USD,
            amount: 1000,
        }],
    );
    assert_eq!(n.view.cursor(1), 4);
    assert_eq!(n.view.with_slice(|s| s.total(7, USD)).unwrap(), 1000);
    assert_eq!(n.stream().len(), before, "a foreign recall reports nothing");
    // its own recall still works
    n.forward(
        1,
        4,
        vec![OutMsg::Recall {
            group: 0,
            account: 7,
            asset: USD,
            amount: 400,
        }],
    );
    assert_eq!(n.view.with_slice(|s| s.total(7, USD)).unwrap(), 600);
}

#[test]
fn group_numbers_survive_the_wire() {
    for group in [0u16, 1, 63, 0x7fff] {
        for m in [
            OutMsg::Grant {
                group,
                account: 5,
                asset: USD,
                amount: u128::MAX - 3,
            },
            OutMsg::Recall {
                group,
                account: 5,
                asset: USD,
                amount: 17,
            },
        ] {
            let b = encode_forward(&Forward {
                shard: 2,
                first_idx: 9,
                msgs: vec![m],
            });
            let f = decode_forward(&b).unwrap();
            assert_eq!(f.msgs, vec![m]);
        }
        for e in [
            Event::SliceIn {
                group,
                account: 1,
                asset: BTC,
                amount: 9,
            },
            Event::SliceOut {
                group,
                account: 1,
                asset: BTC,
                amount: 9,
            },
            Event::Returned {
                group,
                account: 1,
                asset: BTC,
                amount: 9,
            },
        ] {
            let mut b = Vec::new();
            put_event(&mut b, &e);
            assert_eq!(get_event(&mut Rd::new(&b)).unwrap(), e);
        }
    }
    // a group number on a message that has none is not canonical
    let mut b = encode_forward(&Forward {
        shard: 0,
        first_idx: 0,
        msgs: vec![OutMsg::Cancel {
            order: 1,
            symbol: 10,
        }],
    });
    let n = b.len();
    b[n - 2] = 1;
    assert!(decode_forward(&b).is_err());
}

#[test]
fn slice_orders_trade_against_orders_forwarded_by_a_ledger() {
    let mut n = node(1, EPOCH);
    n.forward(1, 0, vec![grant(7, USD, 1000)]);
    // a phase 1 sell from ledger shard 2, resting
    let sell = (2u64 << 48) | 1;
    n.forward(
        2,
        0,
        vec![OutMsg::Submit {
            order: sell,
            symbol: 10,
            side: Side::Sell,
            price: 100,
            qty: 2,
        }],
    );
    n.enter(1, 1, 7, 10, Side::Buy, 100, 2);
    n.settle();
    let g = n.stream();
    // the slice reports only its own side: the buyer paid 200 and got 2000 BTC less fee
    assert!(g.contains(&Event::SliceOut {
        group: 0,
        account: 7,
        asset: USD,
        amount: 200
    }));
    assert!(g.contains(&Event::SliceIn {
        group: 0,
        account: 7,
        asset: BTC,
        amount: 1970
    }));
    // the trade itself is on the partition stream for ledger shard 2 to settle
    let part = n.view.read(100, 1, 10).unwrap().unwrap();
    assert!(part
        .iter()
        .any(|(_, e)| matches!(e, Event::Trade { sell: s, .. } if *s == sell)));
    n.view.with_slice(|s| {
        s.check().unwrap();
        assert_eq!(s.anomalies(), 0);
    });
}

// ---- determinism across replicas, snapshots, and a real ledger ----

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// One workload step as log entries, so identical replicas get identical logs.
fn workload(seed: u64, n: usize) -> Vec<Vec<u8>> {
    let mut r = Rng(seed | 1);
    let mut log = Vec::new();
    let mut seqs = [0u64; 3];
    let mut next_grant = 0u64;
    let mut orders: Vec<(u64, u64)> = Vec::new(); // (account, order id): unknown to the log, so cancel by guess
    let _ = &mut orders;
    for i in 0..n {
        match r.below(10) {
            0..=1 => {
                let account = 1 + r.below(4);
                let asset = [BTC, USD, ETH][r.below(3) as usize];
                log.push(encode_forward(&Forward {
                    shard: 1,
                    first_idx: next_grant,
                    msgs: vec![grant(account, asset, 1 + r.below(30_000) as u128)],
                }));
                next_grant += 1;
            }
            2 => {
                log.push(encode_forward(&Forward {
                    shard: 1,
                    first_idx: next_grant,
                    msgs: vec![OutMsg::Recall {
                        group: 0,
                        account: 1 + r.below(4),
                        asset: [BTC, USD, ETH][r.below(3) as usize],
                        amount: 1 + r.below(20_000) as u128,
                    }],
                }));
                next_grant += 1;
            }
            3 => {
                // a phase 1 order from another ledger shard
                log.push(encode_forward(&Forward {
                    shard: 2,
                    first_idx: i as u64, // gaps are refused: only the first lands, on purpose
                    msgs: vec![OutMsg::Submit {
                        order: (2u64 << 48) | (i as u64 + 1),
                        symbol: 10 + r.below(4) as u32,
                        side: if r.below(2) == 0 {
                            Side::Buy
                        } else {
                            Side::Sell
                        },
                        price: 95 + r.below(11),
                        qty: 1 + r.below(4),
                    }],
                }));
            }
            _ => {
                let c = r.below(3) as usize;
                seqs[c] += 1;
                log.push(encode_slice_req(&SliceReq {
                    client: c as u64,
                    seq: seqs[c],
                    body: SliceBody::Enter {
                        account: 1 + r.below(4),
                        symbol: 10 + r.below(4) as u32,
                        side: if r.below(2) == 0 {
                            Side::Buy
                        } else {
                            Side::Sell
                        },
                        price: 95 + r.below(11),
                        qty: 1 + r.below(5),
                    },
                }));
            }
        }
    }
    log
}

fn digests(n: &mut Node) -> ([u8; 32], [u8; 32]) {
    (
        n.sm.digest(n.idx).unwrap(),
        n.sm.deep_digest(n.idx).unwrap(),
    )
}

#[test]
fn three_replicas_and_a_restored_one_agree_across_epochs() {
    for seed in [1u64, 7, 99] {
        let log = workload(seed, 400);
        let mut reps: Vec<Node> = (0..3).map(|_| node(4, 8)).collect();
        for e in &log {
            for r in reps.iter_mut() {
                r.put(e.clone());
            }
        }
        for r in reps.iter_mut() {
            r.settle();
            r.settle();
        }
        let d0 = digests(&mut reps[0]);
        let stream0 = reps[0].stream();
        for r in reps.iter_mut().skip(1) {
            assert_eq!(digests(r), d0, "seed {seed}: replicas diverged");
            assert_eq!(r.stream(), stream0);
        }
        // a replica restored from a mid-stream snapshot and fed the tail agrees
        let cut = 213;
        let mut a = node(4, 8);
        for e in &log[..cut] {
            a.put(e.clone());
        }
        let snap = a.sm.snapshot(a.idx);
        let mut b = node(4, 8);
        b.idx = a.idx;
        b.sm.restore(b.idx, &snap).unwrap();
        assert_eq!(digests(&mut a), digests(&mut b), "seed {seed}: restore");
        for e in &log[cut..] {
            a.put(e.clone());
            b.put(e.clone());
        }
        a.settle();
        a.settle();
        b.settle();
        b.settle();
        assert_eq!(
            digests(&mut a),
            d0,
            "seed {seed}: uninterrupted vs replicas"
        );
        assert_eq!(digests(&mut b), d0, "seed {seed}: restored vs replicas");
        assert_eq!(a.stream(), b.stream());
        a.view.with_slice(|s| {
            s.check().unwrap();
            assert_eq!(s.anomalies(), 0);
        });
        assert!(
            !a.stream().is_empty(),
            "the workload must reach the group stream"
        );
    }
}

#[test]
fn a_snapshot_does_not_cross_modes() {
    let mut c = node(1, EPOCH);
    c.forward(1, 0, vec![grant(1, USD, 5)]);
    let snap = c.sm.snapshot(c.idx);
    let (mut plain, _view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(1).unwrap(),
        TierConfig {
            source_base: 100,
            retain: 1 << 16,
        },
        |init, plugs| {
            let mut b = Pipeline::<FifoCore>::builder()
                .partitions(1)
                .egress(plugs.digest)
                .egress(plugs.archive);
            if let Some(i) = init {
                b = b.initial(i);
            }
            b.build().unwrap()
        },
    );
    assert!(plain.restore(1, &snap).is_err());
    let psnap = plain.snapshot(1);
    assert!(c.sm.restore(1, &psnap).is_err());
    assert_eq!(&snap[..4], b"XTS2");
    assert_eq!(&psnap[..4], b"XTS1");
}

/// The group stream settled into a real ledger that did the lending: the books balance.
#[test]
fn the_group_stream_settles_into_a_ledger_and_everything_comes_back() {
    let mut n = node(2, 4);
    let mut l = Ledger::new(ledger_cfg(1));
    let accounts = [3u64, 4, 5];
    for (i, a) in accounts.iter().enumerate() {
        for (j, asset) in [BTC, USD, ETH].iter().enumerate() {
            l.apply(&Cmd::Deposit {
                account: *a,
                asset: *asset,
                amount: 100_000,
                external_id: (i * 10 + j) as u64,
            });
            assert!(matches!(
                l.apply(&Cmd::Lend {
                    group: 0,
                    account: *a,
                    asset: *asset,
                    amount: 60_000
                }),
                Outcome::Lent { .. }
            ));
        }
    }
    // the ledger's outbox goes to the group exactly as a forwarder would send it
    let mut sent = 0u64;
    let forward = |n: &mut Node, l: &mut Ledger, sent: &mut u64| {
        let (base, it) = l.outbox();
        let msgs: Vec<OutMsg> = it.copied().collect();
        let from = (*sent).max(base);
        let fresh: Vec<OutMsg> = msgs[(from - base) as usize..].to_vec();
        if !fresh.is_empty() {
            n.forward(1, from, fresh.clone());
            *sent = from + fresh.len() as u64;
        }
    };
    forward(&mut n, &mut l, &mut sent);
    let mut r = Rng(5);
    let mut seq = 0;
    for _ in 0..300 {
        seq += 1;
        n.enter(
            1,
            seq,
            accounts[r.below(3) as usize],
            10 + r.below(4) as u32,
            if r.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            },
            95 + r.below(11),
            1 + r.below(5),
        );
    }
    n.settle();
    // settle the group stream into the ledger (every event involves a known account)
    let settle_all = |n: &Node, l: &mut Ledger| {
        let g = n.view.group_source().unwrap();
        let events = n.view.read(g, 1, usize::MAX).unwrap().unwrap();
        let hw = l.high_water(g).unwrap_or(0);
        for c in settle_cmds_routed(g, 1, hw, events, usize::MAX, |_| 1, 1) {
            assert_eq!(l.apply(&c), Outcome::Settled, "{c:?}");
        }
        l.check().unwrap();
    };
    settle_all(&n, &mut l);
    assert!(
        l.slice_flow(USD).2 + l.slice_flow(USD).3 > 0,
        "trades must have happened"
    );
    // cancel every order, settle the epoch, recall everything, settle the returns
    let open: Vec<(u64, u64)> = n
        .view
        .with_slice(|s| s.orders().into_iter().map(|o| (o.0, o.1)).collect())
        .unwrap();
    for (order, account) in open {
        seq += 1;
        n.req(1, seq, SliceBody::Cancel { account, order });
    }
    n.settle();
    for a in accounts {
        for asset in [BTC, USD, ETH] {
            let lent = l.lent(a, asset);
            if lent > 0 {
                l.apply(&Cmd::Recall {
                    group: 0,
                    account: a,
                    asset,
                    amount: lent,
                });
            }
        }
    }
    forward(&mut n, &mut l, &mut sent);
    settle_all(&n, &mut l);
    for asset in [BTC, USD, ETH] {
        assert_eq!(
            l.lent_total(asset),
            0,
            "asset {asset}: everything lent came back"
        );
        let (i, o) = l.settled_flow(asset);
        assert_eq!(i, o, "asset {asset}: trades moved exactly what they took");
        assert_eq!(
            l.holdings(asset),
            300_000 - o + i,
            "asset {asset}: money conserved"
        );
    }
    assert_eq!(l.anomalies(), 0);
    n.view.with_slice(|s| {
        assert_eq!(s.open_orders(), 0);
        for asset in [BTC, USD, ETH] {
            assert_eq!(s.accounts_total(asset), 0);
        }
    });
}
