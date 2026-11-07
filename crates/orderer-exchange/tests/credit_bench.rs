//! State-machine-level cost of one order, phase 1 against phase 2. Ignored by
//! default (timing is noise in debug builds and on a busy machine):
//!
//!   cargo test --release -p orderer-exchange --test credit_bench -- --ignored --nocapture
//!
//! What it measures: the CPU time of the `apply` calls an order needs on its way
//! to the book, in one thread, no network, no log, no fsync, no replication.
//! Phase 1: a ledger `Reserve` (one apply on the ledger) and the forwarded
//! `Submit` (one apply on the matching tier). Phase 2: one gateway request apply
//! on the tier, which includes the amortised credit epochs. Settlement of fills
//! into the ledger (phase 1) happens off the order path and is not counted;
//! phase 2's own settlement (the epochs) is.
//!
//! What it does not show: latency. The point of phase 2 is one replicated hop
//! instead of two, which is network round trips and fsyncs this test cannot see.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::StateMachine;
use orderer_consensus::Entry;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::tier::credit::CreditConfig;
use orderer_exchange::wire::*;
use orderer_exchange::*;
use orderer_ledger::{Cmd, Config, Ledger, OutMsg, Outcome, Side, Symbol};
use std::collections::BTreeMap;
use std::time::Instant;

const BTC: u32 = 1;
const USD: u32 = 2;
const ACCOUNTS: u64 = 1_000;
const ORDERS: usize = 100_000;

fn cfg(shard: u16) -> Config {
    let mut symbols = BTreeMap::new();
    for s in 10..14u32 {
        symbols.insert(
            s,
            Symbol {
                base: BTC,
                quote: USD,
                lot: 1000,
            },
        );
    }
    Config {
        shard,
        fee_ppm: 1_000,
        symbols,
        fees: BTreeMap::new(),
    }
}

fn tier(partitions: u32, credit: Option<u64>) -> MatchingTierSm<FifoCore> {
    let (sm, _view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(partitions).unwrap(),
        TierConfig {
            source_base: 100,
            retain: 1 << 12,
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
    match credit {
        Some(epoch) => sm.with_credit(CreditConfig {
            ledger: cfg(0),
            group: 0,
            epoch,
            retain: 1 << 12,
        }),
        None => sm,
    }
}

struct Order {
    account: u64,
    symbol: u32,
    side: Side,
    price: u64,
    qty: u64,
}

fn orders() -> Vec<Order> {
    let mut x = 0x2545_f491_4f6c_dd1du64;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    (0..ORDERS)
        .map(|i| Order {
            account: 1 + next() % ACCOUNTS,
            symbol: 10 + (next() % 4) as u32,
            side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
            // a tight band so a good share of orders cross
            price: 1_000 + next() % 8,
            qty: 1 + next() % 3,
        })
        .collect()
}

fn phase1(partitions: u32, batch: usize) -> (f64, f64) {
    let mut ledger = Ledger::new(cfg(1));
    for a in 1..=ACCOUNTS {
        for asset in [BTC, USD] {
            ledger.apply(&Cmd::Deposit {
                account: a,
                asset,
                amount: 1 << 100,
                external_id: a * 10 + asset as u64,
            });
        }
    }
    let mut sm = tier(partitions, None);
    let orders = orders();
    let mut idx = 0u64;
    let t0 = Instant::now();
    let mut pending: Vec<OutMsg> = Vec::new();
    let mut first = 0u64;
    for (i, o) in orders.iter().enumerate() {
        let sent = i as u64 + 1;
        let r = ledger.apply(&Cmd::Reserve {
            account: o.account,
            symbol: o.symbol,
            side: o.side,
            price: o.price,
            qty: o.qty,
        });
        assert!(matches!(r, Outcome::Reserved { .. }));
        let (base, it) = ledger.outbox();
        let all: Vec<OutMsg> = it.copied().collect();
        let _ = base;
        if pending.is_empty() {
            first = sent - 1;
        }
        pending.push(*all.last().unwrap());
        if pending.len() >= batch {
            idx += 1;
            let data = encode_forward(&Forward {
                shard: 1,
                first_idx: first,
                msgs: std::mem::take(&mut pending),
            });
            sm.apply(idx, &Entry { term: 1, data });
            ledger.apply(&Cmd::OutboxAck { upto: sent });
        }
    }
    let apply = t0.elapsed();
    sm.digest(idx); // drain the matcher
    let total = t0.elapsed();
    (
        apply.as_nanos() as f64 / ORDERS as f64,
        total.as_nanos() as f64 / ORDERS as f64,
    )
}

fn phase2(partitions: u32, epoch: u64) -> (f64, f64) {
    let mut sm = tier(partitions, Some(epoch));
    let mut idx = 0u64;
    let grants: Vec<OutMsg> = (1..=ACCOUNTS)
        .flat_map(|a| {
            [BTC, USD].map(|asset| OutMsg::Grant {
                account: a,
                asset,
                amount: 1 << 100,
            })
        })
        .collect();
    idx += 1;
    sm.apply(
        idx,
        &Entry {
            term: 1,
            data: encode_forward(&Forward {
                shard: 1,
                first_idx: 0,
                msgs: grants[..grants.len().min(60_000)].to_vec(),
            }),
        },
    );
    let orders = orders();
    let t0 = Instant::now();
    for (i, o) in orders.iter().enumerate() {
        idx += 1;
        let data = encode_slice_req(&SliceReq {
            client: 1,
            seq: i as u64 + 1,
            body: SliceBody::Enter {
                account: o.account,
                symbol: o.symbol,
                side: o.side,
                price: o.price,
                qty: o.qty,
            },
        });
        sm.apply(idx, &Entry { term: 1, data });
    }
    let apply = t0.elapsed();
    sm.digest(idx);
    let total = t0.elapsed();
    (
        apply.as_nanos() as f64 / ORDERS as f64,
        total.as_nanos() as f64 / ORDERS as f64,
    )
}

#[test]
#[ignore]
fn per_order_cost_phase_one_vs_phase_two() {
    for partitions in [1u32, 4] {
        let (a1, t1) = phase1(partitions, 1);
        let (a32, t32) = phase1(partitions, 32);
        eprintln!(
            "partitions {partitions}: ns/order (apply path / incl. matcher drain)\n  \
             phase 1, forwarded one at a time : {a1:8.0} / {t1:8.0}\n  \
             phase 1, forwarded in batches 32 : {a32:8.0} / {t32:8.0}"
        );
        for epoch in [64u64, 1_024, 16_384, 1 << 40] {
            let (a2, t2) = phase2(partitions, epoch);
            eprintln!("  phase 2, epoch {epoch:>13}        : {a2:8.0} / {t2:8.0}");
        }
    }
}
