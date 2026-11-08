//! Outbox thinning for several matching groups: every group is sent only the
//! messages of its own symbols, as spans that still move its intake cursor over every
//! outbox index. Simulated against real `MatchingTierSm` groups (one in credit mode)
//! with lost, duplicated, reordered, overlapping and gapped spans; the group must apply
//! exactly its own messages, once each, in order, however the spans arrive.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::pipeline_sm::Tap;
use orderer_cluster::shard::ShardMap;
use orderer_cluster::StateMachine;
use orderer_consensus::Entry;
use orderer_core::{BookConfig, Command, FifoCore};
use orderer_exchange::relay::{thin_span, Thinning};
use orderer_exchange::tier::credit::CreditConfig;
use orderer_exchange::wire::{decode_span, encode_span, Span};
use orderer_exchange::*;
use orderer_ledger::{Config, OutMsg, Side, Symbol};
use std::collections::BTreeMap;

const GROUPS: u32 = 3;
const SYMS: [u32; 6] = [10, 11, 12, 13, 14, 15];
const USD: u32 = 2;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn ledger_cfg() -> Config {
    let mut symbols = BTreeMap::new();
    for (i, s) in SYMS.iter().enumerate() {
        symbols.insert(
            *s,
            Symbol {
                base: 3 + i as u32,
                quote: USD,
                lot: 10,
            },
        );
    }
    Config {
        shard: 0,
        fee_ppm: 0,
        symbols,
        fees: BTreeMap::new(),
    }
}

struct Group {
    sm: MatchingTierSm<FifoCore>,
    view: TierView,
    tap: Tap,
    idx: u64,
}

fn group(g: u32, credit: bool) -> Group {
    let tap: Tap = Default::default();
    let (sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(2).unwrap(),
        TierConfig {
            source_base: g * 3,
            retain: 1 << 16,
        },
        |init, plugs| {
            let mut b = Pipeline::<FifoCore>::builder()
                .partitions(2)
                .egress(plugs.digest)
                .egress(plugs.archive);
            if let Some(i) = init {
                b = b.initial(i);
            }
            b.build().unwrap()
        },
    );
    let mut sm = sm.with_group(g, GROUPS).with_tap(tap.clone());
    if credit {
        sm = sm.with_credit(CreditConfig {
            ledger: ledger_cfg(),
            group: g as u16,
            epoch: 1 << 30,
            retain: 1 << 16,
        });
    }
    Group {
        sm,
        view,
        tap,
        idx: 0,
    }
}

impl Group {
    fn put(&mut self, data: Vec<u8>) {
        self.idx += 1;
        self.sm.apply(self.idx, &Entry { term: 1, data });
    }
    /// What reached the matcher, in order: (is cancel, order id).
    fn applied(&mut self) -> Vec<(bool, u64)> {
        // the tap is filled as batches are applied to the pipeline
        let _ = self.sm.digest(self.idx);
        self.tap
            .lock()
            .unwrap()
            .iter()
            .flat_map(|(_, cmds)| cmds.iter())
            .map(|(_, c)| match c {
                Command::New { order_id, .. } => (false, *order_id),
                Command::Cancel { order_id } => (true, *order_id),
                other => panic!("{other:?}"),
            })
            .collect()
    }
}

struct Sim {
    outbox: Vec<OutMsg>,
    symbol_of: BTreeMap<u64, u32>,
    next_order: u64,
    groups: Vec<Group>,
    map: ShardMap,
    thin: Thinning,
    /// the forwarder's belief of each group's cursor (volatile)
    known: Vec<u64>,
    net: Vec<(usize, Vec<u8>)>,
    rng: Rng,
    faults: bool,
    /// credit model of group 0: free credit per (account, asset)
    credit: BTreeMap<(u64, u32), u128>,
}

impl Sim {
    fn new(seed: u64) -> Sim {
        let map = ShardMap::new(GROUPS);
        Sim {
            outbox: Vec::new(),
            symbol_of: BTreeMap::new(),
            next_order: 1,
            groups: (0..GROUPS).map(|g| group(g, g == 0)).collect(),
            map,
            thin: Thinning {
                map,
                credit_group: Some(0),
            },
            known: vec![0; GROUPS as usize],
            net: Vec::new(),
            rng: Rng(seed | 1),
            faults: true,
            credit: BTreeMap::new(),
        }
    }

    fn owner(&self, m: &OutMsg) -> Option<usize> {
        match *m {
            OutMsg::Submit { symbol, .. }
            | OutMsg::SubmitIoc { symbol, .. }
            | OutMsg::Cancel { symbol, .. } => Some(self.map.group_of(symbol) as usize),
            OutMsg::Grant { .. } | OutMsg::Recall { .. } => Some(0),
        }
    }

    /// What group `g` must have applied once every message up to `upto` is in.
    fn expected(&self, g: usize, upto: u64) -> Vec<(bool, u64)> {
        self.outbox[..upto as usize]
            .iter()
            .filter(|m| self.owner(m) == Some(g))
            .filter_map(|m| match *m {
                OutMsg::Submit { order, .. } | OutMsg::SubmitIoc { order, .. } => {
                    Some((false, order))
                }
                OutMsg::Cancel { order, .. } => Some((true, order)),
                _ => None,
            })
            .collect()
    }

    fn produce(&mut self) {
        let m = match self.rng.below(10) {
            0..=6 => {
                let symbol = SYMS[self.rng.below(6) as usize];
                let order = self.next_order;
                self.next_order += 1;
                self.symbol_of.insert(order, symbol);
                OutMsg::Submit {
                    order,
                    symbol,
                    side: if self.rng.below(2) == 0 {
                        Side::Buy
                    } else {
                        Side::Sell
                    },
                    price: 90 + self.rng.below(20),
                    qty: 1 + self.rng.below(5),
                }
            }
            7 | 8 if self.next_order > 1 => {
                let order = 1 + self.rng.below(self.next_order - 1);
                OutMsg::Cancel {
                    order,
                    symbol: self.symbol_of[&order],
                }
            }
            _ => {
                let (account, asset) = (1 + self.rng.below(4), USD);
                if self.rng.below(3) == 0 {
                    OutMsg::Recall {
                        account,
                        asset,
                        amount: 1 + self.rng.below(300) as u128,
                    }
                } else {
                    OutMsg::Grant {
                        account,
                        asset,
                        amount: 1 + self.rng.below(500) as u128,
                    }
                }
            }
        };
        match m {
            OutMsg::Grant {
                account,
                asset,
                amount,
            } => *self.credit.entry((account, asset)).or_default() += amount,
            OutMsg::Recall {
                account,
                asset,
                amount,
            } => {
                let c = self.credit.entry((account, asset)).or_default();
                *c -= amount.min(*c);
            }
            _ => {}
        }
        self.outbox.push(m);
    }

    /// Indices `a..b` of the outbox as group `g`'s span.
    fn span_for(&self, g: usize, a: u64, b: u64) -> Vec<u8> {
        let msgs: Vec<(u64, OutMsg)> = (a..b)
            .filter(|i| self.owner(&self.outbox[*i as usize]) == Some(g))
            .map(|i| (i, self.outbox[i as usize]))
            .collect();
        encode_span(&Span {
            shard: 0,
            first: a,
            end: b,
            msgs,
        })
    }

    fn send(&mut self, g: usize, entry: Vec<u8>) {
        if self.faults {
            match self.rng.below(100) {
                0..=7 => return,
                8..=15 => self.net.push((g, entry.clone())),
                _ => {}
            }
        }
        self.net.push((g, entry));
    }

    /// The forwarder's tick: a window from the lowest believed cursor, a thinned span
    /// per group; and, with faults, spans of arbitrary extent (older, overlapping,
    /// gapped) as a confused or zombie sender would produce.
    fn forward(&mut self) {
        let n = self.outbox.len() as u64;
        if n == 0 {
            return;
        }
        let low = *self.known.iter().min().unwrap();
        let w = 1 + self.rng.below(12) as usize;
        let hi = (low as usize + w).min(self.outbox.len());
        let window = self.outbox[low as usize..hi].to_vec();
        for g in 0..GROUPS as usize {
            if let Some((first, end, msgs)) = thin_span(&self.thin, g, self.known[g], low, &window)
            {
                let e = encode_span(&Span {
                    shard: 0,
                    first,
                    end,
                    msgs,
                });
                self.send(g, e);
            }
            if self.faults && self.rng.below(3) == 0 {
                let a = self.rng.below(n + 1);
                let b = a + self.rng.below(n + 1 - a);
                let e = self.span_for(g, a, b);
                self.send(g, e);
            }
        }
    }

    fn deliver(&mut self) {
        if self.net.is_empty() {
            return;
        }
        let i = if self.faults {
            self.rng.below(self.net.len() as u64) as usize
        } else {
            0
        };
        let (g, e) = self.net.remove(i);
        self.groups[g].put(e);
        // the reply (the cursor) may be lost
        if !self.faults || self.rng.below(4) != 0 {
            self.known[g] = self.known[g].max(self.groups[g].view.cursor(0));
        }
        self.check(g);
    }

    fn check(&mut self, g: usize) {
        let cursor = self.groups[g].view.cursor(0);
        let got = self.groups[g].applied();
        let want = self.expected(g, cursor);
        assert_eq!(
            got, want,
            "group {g}: what reached the matcher is not exactly its own messages below cursor {cursor}"
        );
    }

    fn step(&mut self) {
        match self.rng.below(10) {
            0..=2 => self.produce(),
            3..=5 => self.forward(),
            6..=8 => self.deliver(),
            _ => {
                // the forwarder crashes: its belief is gone, the groups' state is not
                if self.faults && self.rng.below(4) == 0 {
                    self.known = vec![0; GROUPS as usize];
                }
            }
        }
    }

    fn drain(&mut self) {
        self.faults = false;
        for _ in 0..10_000 {
            self.forward();
            while !self.net.is_empty() {
                self.deliver();
            }
            let n = self.outbox.len() as u64;
            if (0..GROUPS as usize).all(|g| self.groups[g].view.cursor(0) == n) {
                return;
            }
        }
        panic!("did not converge");
    }
}

fn run(seed: u64, steps: usize) {
    let mut s = Sim::new(seed);
    for _ in 0..steps {
        s.step();
    }
    s.drain();
    let n = s.outbox.len() as u64;
    for g in 0..GROUPS as usize {
        s.check(g);
        assert_eq!(s.groups[g].view.cursor(0), n);
    }
    // the credit group applied every grant and recall exactly once
    let slice_total: u128 = s.groups[0]
        .view
        .with_slice(|sl| (1..=4u64).map(|a| sl.total(a, USD)).sum())
        .unwrap();
    let model: u128 = (1..=4u64)
        .map(|a| s.credit.get(&(a, USD)).copied().unwrap_or(0))
        .sum();
    assert_eq!(
        slice_total, model,
        "seed {seed}: credit after grants and recalls"
    );
    // the other groups hold no credit state to speak of: grants never reach them
    assert!(s.groups[1].view.with_slice(|_| ()).is_none());
}

#[test]
fn each_group_applies_exactly_its_own_messages_under_faults() {
    for seed in 1..=16u64 {
        run(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15), 700);
    }
}

#[test]
fn thinning_sends_a_fraction_of_the_outbox() {
    // 3 groups: each gets about a third of the orders, not all of them
    let map = ShardMap::new(GROUPS);
    let t = Thinning {
        map,
        credit_group: Some(0),
    };
    let msgs: Vec<OutMsg> = (0..300u64)
        .map(|i| OutMsg::Submit {
            order: i + 1,
            symbol: SYMS[(i % 6) as usize],
            side: Side::Buy,
            price: 10,
            qty: 1,
        })
        .collect();
    let mut sent = 0;
    for g in 0..GROUPS as usize {
        let (first, end, own) = thin_span(&t, g, 0, 0, &msgs).unwrap();
        assert_eq!((first, end), (0, 300));
        assert!(own.iter().all(|(_, m)| t.owns(g, m)));
        sent += own.len();
    }
    assert_eq!(sent, 300, "every message goes to exactly one group");
    // already past the window: nothing to send
    assert!(thin_span(&t, 0, 300, 0, &msgs).is_none());
    // a group behind the window start cannot be served from it
    assert!(thin_span(&t, 0, 5, 10, &msgs).is_none());
}

#[test]
fn span_wire_format_is_strict() {
    let m = OutMsg::Submit {
        order: 9,
        symbol: 10,
        side: Side::Sell,
        price: 5,
        qty: 2,
    };
    let sp = Span {
        shard: 3,
        first: 100,
        end: 110,
        msgs: vec![(101, m), (105, m)],
    };
    let b = encode_span(&sp);
    assert_eq!(decode_span(&b).unwrap(), sp);
    // an empty span is a valid cursor move
    let empty = Span {
        shard: 0,
        first: 5,
        end: 9,
        msgs: vec![],
    };
    assert_eq!(decode_span(&encode_span(&empty)).unwrap(), empty);
    for cut in 0..b.len() {
        assert!(decode_span(&b[..cut]).is_err(), "cut {cut}");
    }
    let mut longer = b.clone();
    longer.push(0);
    assert!(decode_span(&longer).is_err());
    // offsets must be strictly increasing and below the end; end must not precede first
    let bad_order = Span {
        msgs: vec![(105, m), (101, m)],
        ..sp.clone()
    };
    let mut raw = encode_span(&Span {
        msgs: vec![(101, m), (105, m)],
        ..sp.clone()
    });
    // swap the two offsets in place
    let first_off = 24;
    let second_off = 24 + 4 + 30;
    let (a, c) = (raw[first_off], raw[second_off]);
    raw[first_off] = c;
    raw[second_off] = a;
    assert!(decode_span(&raw).is_err(), "decreasing offsets");
    let _ = bad_order;
    let mut past_end = encode_span(&sp);
    past_end[12..20].copy_from_slice(&105u64.to_le_bytes()); // end = 105 < the last index
    assert!(decode_span(&past_end).is_err());
    let mut inverted = encode_span(&empty);
    inverted[4..12].copy_from_slice(&50u64.to_le_bytes()); // first = 50 > end
    assert!(decode_span(&inverted).is_err());
    // version 1 is not a span
    assert!(decode_span(&encode_forward(&Forward {
        shard: 0,
        first_idx: 0,
        msgs: vec![]
    }))
    .is_err());
}
