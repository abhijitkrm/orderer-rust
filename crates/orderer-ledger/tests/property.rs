//! Property tests: random command streams against 1-3 ledger shards fed by a
//! small matching model, with duplicated and delayed delivery. After every
//! step each shard satisfies `check()`; at quiescence the whole system
//! conserves money and every hold matches a resting order.

mod common;
use common::*;
use orderer_ledger::*;

struct World {
    shards: Vec<Ledger>,
    logs: Vec<Vec<Cmd>>,
    matcher: Matcher,
    /// per shard: events routed to it, in stream order, and how many were delivered
    routed: Vec<Vec<(u64, Event)>>,
    delivered: Vec<usize>,
    routed_upto: usize,
    forwarded: Vec<u64>,
    known_orders: Vec<(u64, u64)>, // (account, order id)
    next_ext: u64,
    now: u64,
    rng: Rng,
}

impl World {
    fn new(n: usize, seed: u64) -> World {
        World {
            shards: (0..n).map(|s| Ledger::new(config(s as u16))).collect(),
            logs: vec![Vec::new(); n],
            matcher: Matcher::default(),
            routed: vec![Vec::new(); n],
            delivered: vec![0; n],
            routed_upto: 0,
            forwarded: vec![0; n],
            known_orders: Vec::new(),
            next_ext: 1,
            now: 0,
            rng: Rng(seed | 1),
        }
    }

    fn shard_of(&self, account: u64) -> usize {
        (account % self.shards.len() as u64) as usize
    }

    fn apply(&mut self, shard: usize, c: Cmd) -> Outcome {
        let o = self.shards[shard].apply(&c);
        self.logs[shard].push(c);
        o
    }

    fn route_new_events(&mut self) {
        while self.routed_upto < self.matcher.events.len() {
            let idx = self.routed_upto as u64 + 1;
            let ev = self.matcher.events[self.routed_upto];
            self.routed_upto += 1;
            let ids: Vec<u64> = match ev {
                Event::Trade { buy, sell, .. } => vec![buy, sell],
                Event::Closed { order } => vec![order],
            };
            let mut shards: Vec<usize> = ids.iter().map(|i| order_shard(*i) as usize).collect();
            shards.sort();
            shards.dedup();
            for s in shards {
                self.routed[s].push((idx, ev));
            }
        }
    }

    fn forward(&mut self, shard: usize, how_many: usize, resend: bool) {
        let (base, msgs): (u64, Vec<OutMsg>) = {
            let (b, it) = self.shards[shard].outbox();
            (b, it.copied().collect())
        };
        let mut idx = if resend {
            base
        } else {
            self.forwarded[shard].max(base)
        };
        let mut sent = 0;
        while sent < how_many && idx < base + msgs.len() as u64 {
            let m = msgs[(idx - base) as usize];
            if self.matcher.deliver(shard as u16, idx, m) == Accept::Applied {
                self.forwarded[shard] = idx + 1;
            }
            idx += 1;
            sent += 1;
        }
        self.route_new_events();
        if self.rng.below(3) == 0 {
            let upto = self.matcher.intake.cursor(shard as u16);
            self.apply(shard, Cmd::OutboxAck { upto });
        }
    }

    fn settle(&mut self, shard: usize, how_many: usize, resend_old: bool) {
        for _ in 0..how_many {
            let pos = if resend_old && self.delivered[shard] > 0 {
                self.rng.below(self.delivered[shard] as u64) as usize
            } else if self.delivered[shard] < self.routed[shard].len() {
                let p = self.delivered[shard];
                self.delivered[shard] += 1;
                p
            } else {
                return;
            };
            let (idx, event) = self.routed[shard][pos];
            let prev = if pos == 0 {
                0
            } else {
                self.routed[shard][pos - 1].0
            };
            self.apply(
                shard,
                Cmd::Settle {
                    source: 0,
                    prev,
                    idx,
                    event,
                },
            );
        }
    }

    fn step(&mut self) {
        let account = 1 + self.rng.below(ACCOUNTS);
        let shard = self.shard_of(account);
        let kind = self.rng.below(26);
        let r = &mut self.rng;
        match kind {
            0..=2 => {
                let asset = [BTC, USD, ETH][r.below(3) as usize];
                let amount = 1 + r.below(50_000) as u128;
                let id = if r.below(6) == 0 && self.next_ext > 1 {
                    1 + r.below(self.next_ext - 1)
                } else {
                    self.next_ext
                };
                self.next_ext += 1;
                // a repeated id may reach another shard, which has not seen it
                self.apply(
                    shard,
                    Cmd::Deposit {
                        account,
                        asset,
                        amount,
                        external_id: id * 100 + shard as u64,
                    },
                );
            }
            3..=8 => {
                let symbol = SYMS[r.below(2) as usize];
                let side = if r.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let price = 95 + r.below(11);
                let qty = 1 + r.below(6);
                if let Outcome::Reserved { order, .. } = self.apply(
                    shard,
                    Cmd::Reserve {
                        account,
                        symbol,
                        side,
                        price,
                        qty,
                    },
                ) {
                    self.known_orders.push((account, order));
                }
            }
            9..=10 if !self.known_orders.is_empty() => {
                let (owner, order) =
                    self.known_orders[r.below(self.known_orders.len() as u64) as usize];
                // sometimes by someone else: must be refused
                let who = if r.below(5) == 0 {
                    1 + r.below(ACCOUNTS)
                } else {
                    owner
                };
                let s = order_shard(order) as usize;
                let o = self.apply(
                    s,
                    Cmd::Cancel {
                        account: who,
                        order,
                    },
                );
                if who != owner {
                    assert!(matches!(o, Outcome::Rejected(_)), "{o:?}");
                }
            }
            11..=13 => {
                let s = r.below(self.shards.len() as u64) as usize;
                let n = 1 + r.below(4) as usize;
                let resend = r.below(5) == 0;
                self.forward(s, n, resend);
            }
            14..=17 => {
                let s = r.below(self.shards.len() as u64) as usize;
                let n = 1 + r.below(4) as usize;
                let resend = r.below(4) == 0;
                self.settle(s, n, resend);
            }
            18 => {
                let asset = [BTC, USD, ETH][r.below(3) as usize];
                let id = r.below(40);
                let amount = 1 + r.below(2_000) as u128;
                self.apply(
                    shard,
                    Cmd::Withdraw {
                        account,
                        asset,
                        amount,
                        external_id: id,
                    },
                );
            }
            20 => {
                self.now += r.below(5_000);
                let s = r.below(self.shards.len() as u64) as usize;
                let jitter = r.below(2_000);
                self.apply(
                    s,
                    Cmd::Clock {
                        now_ns: self.now.saturating_sub(jitter),
                    },
                );
            }
            21 => {
                let s = r.below(self.shards.len() as u64) as usize;
                let c = Cmd::Sweep {
                    max_age_ns: r.below(20_000),
                    retry_ns: r.below(20_000),
                    max: 1 + r.below(5) as u32,
                };
                self.apply(s, c);
            }
            22..=23 => {
                let asset = [BTC, USD, ETH][r.below(3) as usize];
                let c = Cmd::Adjust {
                    account,
                    asset,
                    amount: 1 + r.below(500) as u128,
                    credit: r.below(2) == 0,
                    reason: r.below(5) as u32,
                    external_id: r.below(60),
                };
                self.apply(shard, c);
            }
            24 => {
                let s = r.below(self.shards.len() as u64) as usize;
                let asset = [BTC, USD, ETH][r.below(3) as usize];
                let c = Cmd::FeeWithdraw {
                    asset,
                    amount: 1 + r.below(300) as u128,
                    external_id: r.below(40),
                };
                self.apply(s, c);
            }
            _ => {
                let id = r.below(40);
                let paid = r.below(2) == 0;
                self.apply(
                    shard,
                    Cmd::Finalize {
                        external_id: id,
                        paid,
                    },
                );
            }
        }
        for (i, l) in self.shards.iter().enumerate() {
            if let Err(e) = l.check() {
                panic!("shard {i}: {e}");
            }
        }
    }

    /// Deliver everything, cancel everything resting, deliver again.
    fn quiesce(&mut self) {
        for round in 0..2 {
            for _ in 0..8 {
                for s in 0..self.shards.len() {
                    self.forward(s, 10_000, false);
                    self.settle(s, 10_000, false);
                }
            }
            if round == 0 {
                let open: Vec<(u64, u64)> = self.known_orders.clone();
                for (acct, order) in open {
                    let s = order_shard(order) as usize;
                    self.apply(
                        s,
                        Cmd::Cancel {
                            account: acct,
                            order,
                        },
                    );
                }
            }
        }
        // finish pending withdrawals so nothing is in flight
        for id in 0..40 {
            for s in 0..self.shards.len() {
                self.apply(
                    s,
                    Cmd::Finalize {
                        external_id: id,
                        paid: id % 2 == 0,
                    },
                );
            }
        }
    }

    fn assert_conserved(&self) {
        for asset in [BTC, USD, ETH] {
            let (mut have, mut wd, mut dep, mut inn, mut out) = (0u128, 0u128, 0u128, 0u128, 0u128);
            let (mut ain, mut aout) = (0u128, 0u128);
            for l in &self.shards {
                let (ai, ao) = l.adjusted_flow(asset);
                ain += ai;
                aout += ao;
                have += l.holdings(asset);
                let (d, w) = l.external_net(asset);
                dep += d;
                wd += w;
                let (i, o) = l.settled_flow(asset);
                inn += i;
                out += o;
            }
            assert_eq!(
                inn, out,
                "asset {asset}: trades must move exactly what they take"
            );
            assert_eq!(
                have + wd + aout,
                dep + ain,
                "asset {asset}: money is conserved"
            );
        }
    }
}

fn replay(n: usize, logs: &[Vec<Cmd>], upto: Option<usize>) -> Vec<Ledger> {
    (0..n)
        .map(|s| {
            let mut l = Ledger::new(config(s as u16));
            for c in logs[s].iter().take(upto.unwrap_or(usize::MAX)) {
                l.apply(c);
            }
            l
        })
        .collect()
}

fn run(n: usize, seed: u64, steps: usize) {
    let mut w = World::new(n, seed);
    for step in 0..steps {
        w.step();
        if step % 97 == 0 {
            // a replica that restores a snapshot and then applies the same tail agrees
            let s = w.rng.below(n as u64) as usize;
            let snap = w.shards[s].snapshot();
            let mut r = Ledger::new(config(s as u16));
            r.restore(&snap)
                .unwrap_or_else(|e| panic!("seed {seed} restore: {e}"));
            assert_eq!(r.digest(), w.shards[s].digest());
        }
    }
    // every log, replayed from nothing, reaches the same state
    let replayed = replay(n, &w.logs, None);
    for (s, (r, l)) in replayed.iter().zip(&w.shards).enumerate() {
        assert_eq!(
            r.digest(),
            l.digest(),
            "seed {seed} shard {s}: replay diverged"
        );
    }
    w.quiesce();
    for l in &w.shards {
        l.check().unwrap();
    }
    w.assert_conserved();
    // every open order in a ledger is resting in the matcher, and vice versa
    let open: usize = w.shards.iter().map(|l| l.open_orders()).sum();
    assert_eq!(
        open,
        w.matcher.resting(),
        "seed {seed}: ledger orders vs book"
    );
    for l in &w.shards {
        assert_eq!(l.anomalies(), 0);
    }
    // the whole log replays to the same final state, too
    let replayed = replay(n, &w.logs, None);
    for (r, l) in replayed.iter().zip(&w.shards) {
        assert_eq!(r.digest(), l.digest());
    }
}

#[test]
fn one_shard() {
    for seed in 1..=60 {
        run(1, seed * 0x9E37_79B9, 1_500);
    }
}

#[test]
fn three_shards() {
    for seed in 1..=60 {
        run(3, seed * 0x85EB_CA6B, 1_500);
    }
}

#[test]
fn every_account_ends_with_nothing_held() {
    let mut w = World::new(3, 99);
    for _ in 0..4_000 {
        w.step();
    }
    w.quiesce();
    // the model cancels every known order; whatever still rests did not come from a ledger
    let held_total: u128 = w
        .shards
        .iter()
        .map(|l| {
            let mut t = 0;
            for a in 1..=ACCOUNTS {
                for asset in [BTC, USD, ETH] {
                    t += l.balance(a, asset).held;
                }
            }
            t
        })
        .sum();
    assert_eq!(
        held_total, 0,
        "I5: every hold is released once its order is done"
    );
}
