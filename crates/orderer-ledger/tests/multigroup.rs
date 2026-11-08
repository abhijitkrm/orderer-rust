//! Phase 2 with several credit groups: two matching groups, each with its own credit
//! slice, one owning symbol 10 (BTC/USD) and the other symbol 11 (ETH/USD), against
//! two ledger shards. Both slices lend and trade the same accounts and the same USD.
//!
//! Safety at every step: every ledger's `check()` (which now also holds the identity
//! `lent + returned + out == granted + in` for each group separately), each slice's
//! `check()`, in-order exactly-once intake of the outbox at every group (grants and
//! recalls addressed to the other group are consumed without effect), and marks that
//! never go back. At quiescence, per group and account, what the ledgers say is lent to
//! the group equals what its slice holds, exactly; then a recall of everything returns
//! every unit and money is conserved.
//!
//! The unit tests at the end pin the rules the simulator relies on: a group can return
//! or spend only what it was lent, whatever other groups hold of the same account.

mod common;
use common::*;
use orderer_ledger::*;
use std::collections::BTreeMap;

const SHARDS: usize = 2;
const EPOCH: u64 = 4;
const ASSETS: [u32; 3] = [BTC, USD, ETH];
const SRC_MATCH: u32 = 10; // + group
const SRC_REPORT: u32 = 20; // + group

fn shard_of(a: AccountId) -> u16 {
    (a % SHARDS as u64) as u16
}

fn group_of_symbol(sym: u32) -> usize {
    if sym == SYMS[0] {
        0
    } else {
        1
    }
}

struct Group {
    id: u16,
    matcher: Matcher,
    slice: Slice,
    reports: Vec<Event>,
    cursor: usize,
    entries: u64,
    /// grants and recalls that arrived addressed to another group (must stay 0 here)
    foreign: u64,
}

impl Group {
    fn new(id: u16) -> Group {
        Group {
            id,
            matcher: Matcher::default(),
            slice: Slice::new(config(0), id),
            reports: Vec::new(),
            cursor: 0,
            entries: 0,
            foreign: 0,
        }
    }

    fn epoch(&mut self) {
        let evs: Vec<Event> = self.matcher.events[self.cursor..].to_vec();
        self.cursor = self.matcher.events.len();
        self.slice.settle(evs.iter());
        let rep = self.slice.report();
        self.reports.extend(rep);
    }

    fn forward(&mut self, shard: u16, idx: u64, m: OutMsg) {
        if self.matcher.intake.accept(shard, idx) != Accept::Applied {
            return;
        }
        match m {
            OutMsg::Submit {
                order,
                symbol,
                side,
                price,
                qty,
            } if group_of_symbol(symbol) == self.id as usize => {
                self.matcher.submit(order, symbol, side, price, qty)
            }
            OutMsg::SubmitIoc {
                order,
                symbol,
                side,
                price,
                qty,
            } if group_of_symbol(symbol) == self.id as usize => self
                .matcher
                .submit_tif(order, symbol, side, price, qty, true),
            OutMsg::Cancel { order, symbol } if group_of_symbol(symbol) == self.id as usize => {
                self.matcher.cancel(order, symbol)
            }
            OutMsg::Submit { .. } | OutMsg::SubmitIoc { .. } | OutMsg::Cancel { .. } => {}
            OutMsg::Grant {
                group,
                account,
                asset,
                amount,
            } => {
                if group == self.id {
                    self.slice.grant(account, asset, amount);
                } else {
                    self.foreign += 1;
                }
            }
            OutMsg::Recall {
                group,
                account,
                asset,
                amount,
            } => {
                if group == self.id {
                    let ev = self.slice.recall(account, asset, amount);
                    self.reports.extend(ev);
                } else {
                    self.foreign += 1;
                }
            }
        }
        self.entries += 1;
        if self.entries % EPOCH == 0 {
            self.epoch();
        }
    }
}

struct World {
    rng: Rng,
    ledgers: Vec<Ledger>,
    fwd: Vec<Forwarder>,
    groups: Vec<Group>,
    marks: Vec<BTreeMap<u32, u64>>,
    ext: u64,
    open: Vec<(usize, u64, u64, u32)>, // (group, account, order, symbol) entered at a group
}

impl World {
    fn new(seed: u64) -> World {
        let mut w = World {
            rng: Rng(seed | 1),
            ledgers: (0..SHARDS).map(|s| Ledger::new(config(s as u16))).collect(),
            fwd: (0..SHARDS).map(|s| Forwarder::new(s as u16)).collect(),
            groups: vec![Group::new(0), Group::new(1)],
            marks: vec![BTreeMap::new(); SHARDS],
            ext: 0,
            open: Vec::new(),
        };
        for a in 1..=ACCOUNTS {
            for asset in ASSETS {
                w.ext += 1;
                let ext = w.ext;
                w.ledgers[shard_of(a) as usize].apply(&Cmd::Deposit {
                    account: a,
                    asset,
                    amount: 2_000_000,
                    external_id: ext,
                });
            }
        }
        w
    }

    fn account(&mut self) -> u64 {
        1 + self.rng.below(ACCOUNTS)
    }

    fn step(&mut self) {
        let r = self.rng.below(100);
        match r {
            0..=17 => {
                let (g, a) = (self.rng.below(2) as u16, self.account());
                let asset = ASSETS[self.rng.below(3) as usize];
                let amount = 1 + self.rng.below(60_000) as u128;
                self.ledgers[shard_of(a) as usize].apply(&Cmd::Lend {
                    group: g,
                    account: a,
                    asset,
                    amount,
                });
            }
            18..=23 => {
                let (g, a) = (self.rng.below(2) as u16, self.account());
                let asset = ASSETS[self.rng.below(3) as usize];
                let amount = 1 + self.rng.below(60_000) as u128;
                self.ledgers[shard_of(a) as usize].apply(&Cmd::Recall {
                    group: g,
                    account: a,
                    asset,
                    amount,
                });
            }
            24..=51 => self.enter(),
            52..=57 => self.cancel(),
            58..=62 => {
                // a phase 1 order through the ledger, to the group that owns the symbol
                let (a, sym) = (self.account(), SYMS[self.rng.below(2) as usize]);
                let side = if self.rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let (price, qty) = (95 + self.rng.below(11), 1 + self.rng.below(4));
                self.ledgers[shard_of(a) as usize].apply(&Cmd::Reserve {
                    account: a,
                    symbol: sym,
                    side,
                    price,
                    qty,
                });
            }
            63..=77 => self.forward(),
            78..=85 => {
                let g = self.rng.below(2) as usize;
                self.groups[g].epoch();
            }
            _ => self.settle(),
        }
    }

    fn enter(&mut self) {
        let g = self.rng.below(2) as usize;
        let sym = SYMS[g];
        let a = self.account();
        let side = if self.rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let (price, qty) = (95 + self.rng.below(11), 1 + self.rng.below(5));
        if let Ok(order) = self.groups[g].slice.enter(a, sym, side, price, qty) {
            self.groups[g].matcher.submit(order, sym, side, price, qty);
            self.groups[g].entries += 1;
            self.open.push((g, a, order, sym));
        }
    }

    fn cancel(&mut self) {
        if self.open.is_empty() {
            return;
        }
        let i = self.rng.below(self.open.len() as u64) as usize;
        let (g, a, order, sym) = self.open.swap_remove(i);
        if self.groups[g].slice.cancel(a, order).is_ok() {
            self.groups[g].matcher.cancel(order, sym);
        }
    }

    fn forward(&mut self) {
        let s = self.rng.below(SHARDS as u64) as usize;
        let sends = self.fwd[s].poll(&self.ledgers[s], 1 + self.rng.below(4) as usize);
        for send in sends {
            // duplicates and a delivery order the intake must tolerate
            let times = if self.rng.below(5) == 0 { 2 } else { 1 };
            for _ in 0..times {
                for g in self.groups.iter_mut() {
                    g.forward(send.shard, send.idx, send.msg);
                }
            }
        }
        let low = self
            .groups
            .iter()
            .map(|g| g.matcher.intake.cursor(s as u16))
            .min()
            .unwrap();
        if let Some(c) = self.fwd[s].on_reply(low, false) {
            self.ledgers[s].apply(&c);
        }
        if self.rng.below(15) == 0 {
            self.fwd[s].rewind();
        }
    }

    fn settle(&mut self) {
        for s in 0..SHARDS {
            for g in 0..2 {
                for (src, stream) in [
                    (SRC_MATCH + g as u32, &self.groups[g].matcher.events),
                    (SRC_REPORT + g as u32, &self.groups[g].reports),
                ] {
                    let hw = self.ledgers[s].high_water(src).unwrap_or(0);
                    let max = 1 + self.rng.below(6) as usize;
                    let evs: Vec<(u64, Event)> = stream
                        .iter()
                        .enumerate()
                        .map(|(i, e)| (i as u64 + 1, *e))
                        .collect();
                    for c in settle_cmds_routed(src, s as u16, hw, evs, max, shard_of, 0) {
                        self.ledgers[s].apply(&c);
                    }
                }
            }
        }
    }

    fn check(&mut self, ctx: &str) {
        for (s, l) in self.ledgers.iter().enumerate() {
            if let Err(e) = l.check() {
                panic!("{ctx}: shard {s}: {e}");
            }
            for src in 0..40u32 {
                let hw = l.high_water(src).unwrap_or(0);
                let m = self.marks[s].entry(src).or_insert(0);
                assert!(hw >= *m, "{ctx}: mark of source {src} went back");
                *m = hw;
            }
        }
        for g in &self.groups {
            if let Err(e) = g.slice.check() {
                panic!("{ctx}: slice {}: {e}", g.id);
            }
            assert_eq!(g.slice.anomalies(), 0, "{ctx}: slice {} anomalies", g.id);
        }
    }

    /// Everything delivered, every epoch closed, every report and event settled.
    fn quiesce(&mut self) {
        for _ in 0..40 {
            for s in 0..SHARDS {
                self.fwd[s].rewind();
            }
            for _ in 0..6 {
                self.forward();
            }
            for g in 0..2 {
                self.groups[g].epoch();
            }
            for _ in 0..4 {
                self.settle();
            }
        }
    }
}

fn run(seed: u64, steps: usize) {
    let mut w = World::new(seed);
    for i in 0..steps {
        w.step();
        w.check(&format!("seed {seed} step {i}"));
    }
    w.quiesce();
    w.check(&format!("seed {seed} quiesced"));
    // per group and account: the ledgers' figure for the group is exactly the slice's
    for g in 0..2u16 {
        for a in 1..=ACCOUNTS {
            for asset in ASSETS {
                let lent = w.ledgers[shard_of(a) as usize].lent_to(a, asset, g);
                let held = w.groups[g as usize].slice.total(a, asset);
                assert_eq!(
                    lent, held,
                    "seed {seed}: account {a} asset {asset} group {g}: ledger lent {lent}, slice holds {held}"
                );
            }
        }
    }
    for gr in &w.groups {
        // every grant and recall of the other group reached this group and was ignored
        assert!(
            gr.foreign > 0,
            "seed {seed}: group {} saw no foreign grants",
            gr.id
        );
    }
    // recall everything; every unit comes back and money is conserved
    for a in 1..=ACCOUNTS {
        for asset in ASSETS {
            for g in 0..2u16 {
                let l = &mut w.ledgers[shard_of(a) as usize];
                let lent = l.lent_to(a, asset, g);
                if lent > 0 {
                    l.apply(&Cmd::Recall {
                        group: g,
                        account: a,
                        asset,
                        amount: lent,
                    });
                }
            }
        }
    }
    // cancel whatever rests so committed credit becomes returnable, then recall again
    let open = std::mem::take(&mut w.open);
    for (g, a, order, sym) in open {
        if w.groups[g].slice.cancel(a, order).is_ok() {
            w.groups[g].matcher.cancel(order, sym);
        }
    }
    w.quiesce();
    for a in 1..=ACCOUNTS {
        for asset in ASSETS {
            for g in 0..2u16 {
                let l = &mut w.ledgers[shard_of(a) as usize];
                let lent = l.lent_to(a, asset, g);
                if lent > 0 {
                    l.apply(&Cmd::Recall {
                        group: g,
                        account: a,
                        asset,
                        amount: lent,
                    });
                }
            }
        }
    }
    w.quiesce();
    w.check(&format!("seed {seed} drained"));
    for asset in ASSETS {
        let (mut dep, mut inn, mut out, mut have, mut wd) = (0u128, 0u128, 0u128, 0u128, 0u128);
        for l in &w.ledgers {
            let (d, x) = l.external_net(asset);
            dep += d;
            wd += x;
            let (i, o) = l.settled_flow(asset);
            inn += i;
            out += o;
            have += l.holdings(asset);
            assert_eq!(
                l.lent_total(asset),
                0,
                "seed {seed}: asset {asset} still lent"
            );
        }
        assert_eq!(
            inn, out,
            "seed {seed}: asset {asset}: trades took {out}, delivered {inn}"
        );
        assert_eq!(have + wd, dep, "seed {seed}: asset {asset} not conserved");
    }
}

#[test]
fn two_credit_groups_hold_every_invariant() {
    for seed in [1u64, 7, 42, 1234, 99991] {
        run(seed, 6000);
    }
}

#[test]
fn trades_really_happen_in_both_groups() {
    let mut w = World::new(5);
    for _ in 0..6000 {
        w.step();
    }
    w.quiesce();
    for g in &w.groups {
        let trades = g
            .matcher
            .events
            .iter()
            .filter(|e| matches!(e, Event::Trade { .. }))
            .count();
        assert!(trades > 20, "group {} traded only {trades} times", g.id);
        assert!(!g.reports.is_empty(), "group {} reported nothing", g.id);
    }
    for g in 0..2u16 {
        let flow: u128 = ASSETS
            .iter()
            .map(|&a| {
                w.ledgers
                    .iter()
                    .map(|l| l.group_flow(a, g).slice_in)
                    .sum::<u128>()
            })
            .sum();
        assert!(flow > 0, "ledgers saw no slice-in from group {g}");
    }
}

// ---- unit tests of the per-group rules ----

fn ledger_with(deposit: u128) -> Ledger {
    let mut l = Ledger::new(config(0));
    l.apply(&Cmd::Deposit {
        account: 1,
        asset: USD,
        amount: deposit,
        external_id: 1,
    });
    l
}

fn settle(l: &mut Ledger, source: u32, idx: u64, event: Event) -> Outcome {
    let prev = l.high_water(source).unwrap_or(0);
    l.apply(&Cmd::Settle {
        source,
        prev,
        idx,
        event,
    })
}

#[test]
fn lend_names_the_group_and_the_grant_carries_it() {
    let mut l = ledger_with(1_000);
    for (group, amount) in [(0u16, 100u128), (1, 250)] {
        assert!(matches!(
            l.apply(&Cmd::Lend {
                group,
                account: 1,
                asset: USD,
                amount
            }),
            Outcome::Lent { .. }
        ));
    }
    assert_eq!(l.lent(1, USD), 350);
    assert_eq!(l.lent_to(1, USD, 0), 100);
    assert_eq!(l.lent_to(1, USD, 1), 250);
    assert_eq!(l.lent_total_to(USD, 1), 250);
    let msgs: Vec<OutMsg> = l.outbox().1.copied().collect();
    assert_eq!(
        msgs,
        vec![
            OutMsg::Grant {
                group: 0,
                account: 1,
                asset: USD,
                amount: 100
            },
            OutMsg::Grant {
                group: 1,
                account: 1,
                asset: USD,
                amount: 250
            },
        ]
    );
    l.check().unwrap();
}

#[test]
fn a_group_cannot_return_or_spend_what_another_group_holds() {
    let mut l = ledger_with(1_000);
    for (group, amount) in [(0u16, 100u128), (1, 250)] {
        l.apply(&Cmd::Lend {
            group,
            account: 1,
            asset: USD,
            amount,
        });
    }
    let before = l.deep_digest();
    // group 0 holds 100; returning 150 would need group 1's money
    assert_eq!(
        settle(
            &mut l,
            20,
            1,
            Event::Returned {
                group: 0,
                account: 1,
                asset: USD,
                amount: 150
            }
        ),
        Outcome::Anomaly
    );
    assert_eq!(
        settle(
            &mut l,
            20,
            2,
            Event::SliceOut {
                group: 0,
                account: 1,
                asset: USD,
                amount: 101
            }
        ),
        Outcome::Anomaly
    );
    assert_eq!(l.lent_to(1, USD, 0), 100);
    assert_eq!(l.lent_to(1, USD, 1), 250);
    assert_eq!(l.balance(1, USD).available, 650);
    assert_eq!(l.anomalies(), 2);
    assert_ne!(before, l.deep_digest(), "the marks advanced");
    // a legitimate return from each group
    for (group, amount) in [(0u16, 100u128), (1, 250)] {
        assert_eq!(
            settle(
                &mut l,
                21 + group as u32,
                1,
                Event::Returned {
                    group,
                    account: 1,
                    asset: USD,
                    amount
                }
            ),
            Outcome::Settled
        );
    }
    assert_eq!(l.lent(1, USD), 0);
    assert_eq!(l.balance(1, USD).available, 1_000);
}

#[test]
fn slice_in_is_credited_to_its_own_group_only() {
    let mut l = ledger_with(1_000);
    l.apply(&Cmd::Lend {
        group: 1,
        account: 1,
        asset: USD,
        amount: 100,
    });
    assert_eq!(
        settle(
            &mut l,
            21,
            1,
            Event::SliceIn {
                group: 1,
                account: 1,
                asset: USD,
                amount: 40
            }
        ),
        Outcome::Settled
    );
    assert_eq!(l.lent_to(1, USD, 1), 140);
    assert_eq!(l.lent_to(1, USD, 0), 0);
    assert_eq!(l.group_flow(USD, 1).slice_in, 40);
    assert_eq!(l.group_flow(USD, 0), GroupFlow::default());
    assert_eq!(l.credit_groups().into_iter().collect::<Vec<_>>(), vec![1]);
    l.check().unwrap();
}

#[test]
fn group_numbers_must_leave_the_slice_bit_alone() {
    let mut l = ledger_with(1_000);
    for group in [SLICE_SHARD_BIT, u16::MAX] {
        assert_eq!(
            l.apply(&Cmd::Lend {
                group,
                account: 1,
                asset: USD,
                amount: 1
            }),
            Outcome::Rejected(Reject::BadGroup)
        );
        assert_eq!(
            l.apply(&Cmd::Recall {
                group,
                account: 1,
                asset: USD,
                amount: 1
            }),
            Outcome::Rejected(Reject::BadGroup)
        );
    }
    assert_eq!(
        settle(
            &mut l,
            20,
            1,
            Event::SliceIn {
                group: SLICE_SHARD_BIT,
                account: 1,
                asset: USD,
                amount: 1
            }
        ),
        Outcome::Anomaly
    );
    assert_eq!(l.lent(1, USD), 0);
}

#[test]
fn recall_asks_only_for_what_that_group_holds() {
    let mut l = ledger_with(1_000);
    for (group, amount) in [(0u16, 100u128), (1, 250)] {
        l.apply(&Cmd::Lend {
            group,
            account: 1,
            asset: USD,
            amount,
        });
    }
    match l.apply(&Cmd::Recall {
        group: 0,
        account: 1,
        asset: USD,
        amount: 10_000,
    }) {
        Outcome::RecallQueued { outbox } => {
            let (base, mut msgs) = l.outbox();
            let m = msgs.nth((outbox - base) as usize).copied().unwrap();
            assert_eq!(
                m,
                OutMsg::Recall {
                    group: 0,
                    account: 1,
                    asset: USD,
                    amount: 100
                }
            );
        }
        o => panic!("{o:?}"),
    }
    // a group that holds nothing of the account has nothing to recall
    assert_eq!(
        l.apply(&Cmd::Recall {
            group: 7,
            account: 1,
            asset: USD,
            amount: 1
        }),
        Outcome::Rejected(Reject::Insufficient)
    );
}

#[test]
fn snapshots_keep_per_group_state_and_old_ones_read_as_group_zero() {
    let mut l = ledger_with(1_000);
    for (group, amount) in [(0u16, 100u128), (1, 250), (3, 5)] {
        l.apply(&Cmd::Lend {
            group,
            account: 1,
            asset: USD,
            amount,
        });
    }
    settle(
        &mut l,
        21,
        1,
        Event::SliceIn {
            group: 1,
            account: 1,
            asset: USD,
            amount: 9,
        },
    );
    let snap = l.snapshot();
    assert_eq!(&snap[..4], b"LDG4");
    let mut r = Ledger::new(config(0));
    r.restore(&snap).unwrap();
    assert_eq!(r.snapshot(), snap);
    assert_eq!(r.digest(), l.digest());
    assert_eq!(r.deep_digest(), l.deep_digest());
    assert_eq!(r.lent_to(1, USD, 1), 259);
    assert_eq!(r.lent_to(1, USD, 3), 5);
    r.check().unwrap();
    // the incremental digest tracks per-group state
    let d0 = r.digest();
    r.apply(&Cmd::Lend {
        group: 3,
        account: 1,
        asset: USD,
        amount: 1,
    });
    assert_ne!(d0, r.digest());
}

#[test]
fn an_old_single_group_snapshot_restores_as_group_zero() {
    let mut l = ledger_with(1_000);
    l.apply(&Cmd::Lend {
        group: 0,
        account: 1,
        asset: USD,
        amount: 10,
    });
    let v4 = l.snapshot();
    assert_eq!(&v4[..4], b"LDG4");
    // rebuild what the previous format wrote: no group in the lent entry, no per-group section
    let flow = 4 + (4 + 2 + 4 * 16); // count, one (asset, group, four amounts)
    let maps = 24 + 4 + 4 + 4; // granted (one entry), returned, in, out
    let lent = 4 + (8 + 4 + 2 + 16); // count, one (account, asset, group, amount)
    let head = &v4[..v4.len() - flow - maps - lent];
    let mut v3 = head.to_vec();
    v3[..4].copy_from_slice(b"LDG3");
    v3.extend_from_slice(&1u32.to_le_bytes());
    v3.extend_from_slice(&1u64.to_le_bytes());
    v3.extend_from_slice(&USD.to_le_bytes());
    v3.extend_from_slice(&10u128.to_le_bytes());
    v3.extend_from_slice(&v4[v4.len() - flow - maps..v4.len() - flow]);
    let mut r = Ledger::new(config(0));
    r.restore(&v3).unwrap();
    assert_eq!(r.lent_to(1, USD, 0), 10);
    assert_eq!(r.group_flow(USD, 0).granted, 10);
    r.check().unwrap();
    assert_eq!(
        r.snapshot(),
        v4,
        "it re-encodes in the new format, identically"
    );
    assert_eq!(r.digest(), l.digest());
}
