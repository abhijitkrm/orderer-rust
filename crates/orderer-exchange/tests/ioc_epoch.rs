//! IOC orders (forwarded from a ledger, and entered at a credit slice) and the
//! epoch tick entry that lets a quiet group close its credit epoch.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::StateMachine;
use orderer_consensus::Entry;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::tier::credit::CreditConfig;
use orderer_exchange::wire::*;
use orderer_exchange::*;
use orderer_ledger::{Config, Event, OutMsg, Side, Symbol, SHARD_SHIFT};
use std::collections::BTreeMap;

const BTC: u32 = 1;
const USD: u32 = 2;
const SYM: u32 = 10;

fn ledger_cfg() -> Config {
    let mut symbols = BTreeMap::new();
    symbols.insert(
        SYM,
        Symbol {
            base: BTC,
            quote: USD,
            lot: 1000,
        },
    );
    Config {
        shard: 0,
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

fn node(credit_epoch: Option<u64>) -> Node {
    let (sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(1).unwrap(),
        TierConfig {
            source_base: 100,
            retain: 1 << 16,
        },
        move |init, plugs| {
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
    let sm = match credit_epoch {
        Some(epoch) => sm.with_credit(CreditConfig {
            ledger: ledger_cfg(),
            group: 0,
            epoch,
            retain: 1 << 16,
        }),
        None => sm,
    };
    Node { sm, view, idx: 0 }
}

impl Node {
    fn put(&mut self, data: Vec<u8>) {
        self.idx += 1;
        self.sm.apply(self.idx, &Entry { term: 1, data });
    }
    fn forward(&mut self, first_idx: u64, msgs: Vec<OutMsg>) {
        self.put(encode_forward(&Forward {
            shard: 1,
            first_idx,
            msgs,
        }));
    }
    fn req(&mut self, client: u64, seq: u64, body: SliceBody) {
        self.put(encode_slice_req(&SliceReq { client, seq, body }));
    }
    fn tick(&mut self) {
        self.put(encode_epoch_tick());
    }
    /// The matcher's events as the archive maps them (stream 0).
    fn trades(&mut self) -> Vec<Event> {
        let _ = self.sm.digest(self.idx);
        self.view
            .read(100, 1, usize::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|(_, e)| e)
            .collect()
    }
    fn group(&self) -> Vec<Event> {
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

fn oid(n: u64) -> u64 {
    (1u64 << SHARD_SHIFT) | n
}

fn submit(n: u64, side: Side, price: u64, qty: u64, ioc: bool) -> OutMsg {
    let (order, symbol) = (oid(n), SYM);
    if ioc {
        OutMsg::SubmitIoc {
            order,
            symbol,
            side,
            price,
            qty,
        }
    } else {
        OutMsg::Submit {
            order,
            symbol,
            side,
            price,
            qty,
        }
    }
}

// ---- IOC from a ledger -------------------------------------------------------

#[test]
fn an_ioc_order_trades_what_it_can_and_the_rest_is_closed() {
    let mut n = node(None);
    n.forward(0, vec![submit(1, Side::Sell, 90, 3, false)]);
    n.forward(1, vec![submit(2, Side::Buy, 100, 5, true)]);
    let ev = n.trades();
    assert_eq!(
        ev,
        vec![
            Event::Trade {
                buy: oid(2),
                sell: oid(1),
                price: 90,
                qty: 3,
                taker_is_buy: true
            },
            Event::Closed { order: oid(2) },
        ]
    );
}

#[test]
fn the_same_order_as_gtc_rests_instead() {
    let mut n = node(None);
    n.forward(0, vec![submit(1, Side::Sell, 90, 3, false)]);
    n.forward(1, vec![submit(2, Side::Buy, 100, 5, false)]);
    assert_eq!(
        n.trades().len(),
        1,
        "a trade, and no close: the remainder rests"
    );
    // cancelling it closes it
    n.forward(
        2,
        vec![OutMsg::Cancel {
            order: oid(2),
            symbol: SYM,
        }],
    );
    assert_eq!(n.trades().last(), Some(&Event::Closed { order: oid(2) }));
}

#[test]
fn an_ioc_order_that_cannot_trade_is_closed_at_once() {
    let mut n = node(None);
    n.forward(0, vec![submit(1, Side::Buy, 100, 5, true)]);
    assert_eq!(n.trades(), vec![Event::Closed { order: oid(1) }]);
    // and an IOC sell against a lower bid trades fully, with no close
    n.forward(1, vec![submit(2, Side::Buy, 100, 4, false)]);
    n.forward(2, vec![submit(3, Side::Sell, 90, 4, true)]);
    let ev = n.trades();
    assert_eq!(ev.len(), 2);
    assert!(matches!(
        ev[1],
        Event::Trade {
            qty: 4,
            taker_is_buy: false,
            ..
        }
    ));
}

#[test]
fn an_ioc_with_an_unrepresentable_price_is_rejected_and_closed() {
    let mut n = node(None);
    n.forward(0, vec![submit(1, Side::Buy, u64::MAX, 1, true)]);
    assert_eq!(n.trades(), vec![Event::Closed { order: oid(1) }]);
}

#[test]
fn ioc_messages_survive_the_wire_and_replays_are_not_applied_twice() {
    let f = Forward {
        shard: 1,
        first_idx: 7,
        msgs: vec![
            submit(1, Side::Buy, 5, 6, true),
            submit(2, Side::Sell, 7, 8, false),
        ],
    };
    let b = encode_forward(&f);
    assert_eq!(decode_forward(&b).unwrap(), f);
    for cut in 0..b.len() {
        assert!(decode_forward(&b[..cut]).is_err());
    }
    // the IOC kind is 5; kind 1 stays a GTC submit
    assert_eq!(b[16], 5);
    assert_eq!(b[16 + MSG_BYTES], 1);
    let mut n = node(None);
    n.forward(0, vec![submit(1, Side::Sell, 90, 3, false)]);
    n.forward(1, vec![submit(2, Side::Buy, 100, 5, true)]);
    n.forward(1, vec![submit(2, Side::Buy, 100, 5, true)]);
    n.forward(0, vec![submit(1, Side::Sell, 90, 3, false)]);
    assert_eq!(n.trades().len(), 2, "the replays changed nothing");
}

// ---- IOC at a credit slice -----------------------------------------------------

fn grant(account: u64, asset: u32, amount: u128) -> OutMsg {
    OutMsg::Grant {
        group: 0,
        account,
        asset,
        amount,
    }
}

fn enter(ioc: bool, account: u64, side: Side, price: u64, qty: u64) -> SliceBody {
    if ioc {
        SliceBody::EnterIoc {
            account,
            symbol: SYM,
            side,
            price,
            qty,
        }
    } else {
        SliceBody::Enter {
            account,
            symbol: SYM,
            side,
            price,
            qty,
        }
    }
}

fn slice_run(ioc: bool) -> (u128, usize) {
    let mut n = node(Some(4));
    n.forward(0, vec![grant(7, USD, 1000), grant(8, BTC, 5000)]);
    n.req(1, 1, enter(false, 8, Side::Sell, 90, 3));
    n.req(2, 1, enter(ioc, 7, Side::Buy, 100, 5));
    for _ in 0..16 {
        n.put(Vec::new());
    }
    n.view.with_slice(|s| s.check().unwrap()).unwrap();
    n.view
        .with_slice(|s| (s.available(7, USD), s.open_orders()))
        .unwrap()
}

#[test]
fn an_ioc_slice_order_releases_its_unfilled_hold_but_a_gtc_one_keeps_it() {
    // bought 3 at 90 = 270; the remaining 2 lots at limit 100 = 200 are held only if GTC
    assert_eq!(slice_run(true), (1000 - 270, 0));
    assert_eq!(slice_run(false), (1000 - 270 - 200, 1));
}

#[test]
fn slice_request_wire_for_ioc() {
    let q = SliceReq {
        client: 3,
        seq: 9,
        body: SliceBody::EnterIoc {
            account: 4,
            symbol: 5,
            side: Side::Sell,
            price: 6,
            qty: 7,
        },
    };
    let b = encode_slice_req(&q);
    assert_eq!(decode_slice_req(&b).unwrap(), q);
    assert_eq!(b[18], 3, "IOC is request kind 3");
    for cut in 0..b.len() {
        assert!(decode_slice_req(&b[..cut]).is_err());
    }
}

// ---- the epoch tick -----------------------------------------------------------

#[test]
fn a_quiet_group_closes_its_epoch_on_a_tick() {
    let mut n = node(Some(1_000_000)); // an epoch that no count would reach
    n.forward(0, vec![grant(7, USD, 1000), grant(8, BTC, 5000)]);
    n.req(1, 1, enter(false, 8, Side::Sell, 90, 3));
    n.req(2, 1, enter(false, 7, Side::Buy, 100, 3));
    for _ in 0..50 {
        n.put(Vec::new());
    }
    let _ = n.trades();
    assert!(
        n.group().is_empty(),
        "no epoch has closed: nothing reported"
    );
    n.tick();
    let g = n.group();
    assert!(
        g.iter()
            .any(|e| matches!(e, Event::SliceOut { account: 7, .. })),
        "{g:?}"
    );
    assert!(g
        .iter()
        .any(|e| matches!(e, Event::SliceIn { account: 8, .. })));
    // a second tick with nothing new reports nothing more
    let len = g.len();
    n.tick();
    assert_eq!(n.group().len(), len);
    assert_eq!(n.sm.bad_forwards, 0);
}

#[test]
fn a_malformed_tick_is_counted_and_closes_nothing() {
    let mut n = node(Some(1_000_000));
    n.forward(0, vec![grant(7, USD, 1000), grant(8, BTC, 5000)]);
    n.req(1, 1, enter(false, 8, Side::Sell, 90, 3));
    n.req(2, 1, enter(false, 7, Side::Buy, 100, 3));
    let _ = n.trades();
    n.put(vec![TAG_EPOCH]);
    n.put(vec![TAG_EPOCH, 2]);
    n.put(vec![TAG_EPOCH, 1, 0]);
    assert_eq!(n.sm.bad_forwards, 3);
    assert!(n.group().is_empty());
}

#[test]
fn a_tick_outside_credit_mode_is_a_no_op() {
    let mut n = node(None);
    n.forward(0, vec![submit(1, Side::Sell, 90, 3, false)]);
    let d0 = n.sm.digest(n.idx);
    n.tick();
    n.tick();
    assert_eq!(n.sm.digest(n.idx), d0);
    assert_eq!(n.sm.bad_forwards, 0);
}

#[test]
fn replicas_agree_with_ticks_in_the_log_including_across_a_snapshot() {
    let script = |n: &mut Node, from: u64, to: u64| {
        for k in from..to {
            match k % 5 {
                0 => n.forward(k / 5 * 2, vec![grant(7 + k % 2, USD, 500)]),
                1 => n.req(1, k, enter(k % 2 == 0, 7, Side::Buy, 100, 1 + k % 3)),
                2 => n.req(2, k, enter(false, 8, Side::Sell, 95, 1 + k % 3)),
                3 => n.tick(),
                _ => n.put(Vec::new()),
            }
        }
    };
    let (mut a, mut b) = (node(Some(7)), node(Some(7)));
    // both receive grants first so there is credit to use
    for n in [&mut a, &mut b] {
        n.forward(1_000_000, vec![]);
    }
    script(&mut a, 0, 120);
    script(&mut b, 0, 60);
    let snap = b.sm.snapshot(b.idx);
    let mut c = node(Some(7));
    c.sm.restore(b.idx, &snap).unwrap();
    c.idx = b.idx;
    script(&mut b, 60, 120);
    script(&mut c, 60, 120);
    let (da, db, dc) = (a.sm.digest(a.idx), b.sm.digest(b.idx), c.sm.digest(c.idx));
    assert_eq!(da, db);
    assert_eq!(db, dc);
    assert_eq!(a.group(), b.group());
    assert_eq!(b.group(), c.group());
    assert_eq!(a.sm.deep_digest(a.idx), c.sm.deep_digest(c.idx));
}
