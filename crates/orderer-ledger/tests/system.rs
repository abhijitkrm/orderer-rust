//! Whole-system simulation: clients, ledger shards, forwarders, a matching
//! group and settlers, joined by a network that loses, duplicates and reorders
//! messages, with crashes of the volatile roles and failover of a ledger shard.
//!
//! Safety is checked at every step; liveness and conservation once faults stop.

mod common;
use common::*;
use orderer_ledger::*;

#[derive(Clone, Debug)]
enum Msg {
    Submit {
        shard: usize,
        idx: u64,
        msg: OutMsg,
    },
    SubmitReply {
        shard: usize,
        cursor: u64,
        gap: bool,
    },
    Ack {
        shard: usize,
        upto: u64,
    },
    HwQuery {
        shard: usize,
    },
    HwReply {
        shard: usize,
        hw: u64,
    },
    Settle {
        shard: usize,
        cmd: Cmd,
    },
    SettleReply {
        shard: usize,
        hw: u64,
        gap: bool,
    },
}

/// Per shard settler memory, all of it disposable.
#[derive(Clone, Default)]
struct SettlerSt {
    /// None until the ledger's mark has been learned (after a start or crash).
    acked: Option<u64>,
    /// Last event index sent, the `prev` for the next one.
    pos: u64,
}

struct Sys {
    shards: Vec<Ledger>,
    logs: Vec<Vec<Cmd>>,
    matcher: Matcher,
    forwarders: Vec<Forwarder>,
    settlers: Vec<SettlerSt>,
    net: Vec<Msg>,
    rng: Rng,
    faults: bool,
    // bookkeeping for the safety checks
    outbox_all: Vec<Vec<OutMsg>>,
    matcher_applied: Vec<u64>,
    settled_applied: Vec<usize>,
    hw_seen: Vec<u64>,
    known_orders: Vec<(u64, u64)>,
    next_ext: u64,
    stats: Stats,
}

#[derive(Default, Debug)]
struct Stats {
    dropped: u64,
    duplicated: u64,
    crashes: u64,
    failovers: u64,
    gaps_refused: u64,
    intake_gaps: u64,
}

impl Sys {
    fn new(n: usize, seed: u64) -> Sys {
        Sys {
            shards: (0..n).map(|s| Ledger::new(config(s as u16))).collect(),
            logs: vec![Vec::new(); n],
            matcher: Matcher::default(),
            forwarders: (0..n).map(|s| Forwarder::new(s as u16)).collect(),
            settlers: vec![SettlerSt::default(); n],
            net: Vec::new(),
            rng: Rng(seed | 1),
            faults: true,
            outbox_all: vec![Vec::new(); n],
            matcher_applied: vec![0; n],
            settled_applied: vec![0; n],
            hw_seen: vec![0; n],
            known_orders: Vec::new(),
            next_ext: 1,
            stats: Stats::default(),
        }
    }

    fn n(&self) -> usize {
        self.shards.len()
    }

    /// Everything the matching group has emitted that involves `shard`.
    fn routed(&self, shard: usize) -> Vec<(u64, Event)> {
        self.matcher
            .events
            .iter()
            .enumerate()
            .map(|(i, e)| (i as u64 + 1, *e))
            .filter(|(_, e)| match *e {
                Event::Trade { buy, sell, .. } => {
                    order_shard(buy) as usize == shard || order_shard(sell) as usize == shard
                }
                Event::Closed { order } => order_shard(order) as usize == shard,
            })
            .collect()
    }

    fn ledger_apply(&mut self, shard: usize, c: Cmd) -> Outcome {
        let before = self.shards[shard].outbox_next();
        let o = self.shards[shard].apply(&c);
        self.logs[shard].push(c.clone());
        // record new outbox entries for the content check
        let (base, it) = self.shards[shard].outbox();
        let msgs: Vec<OutMsg> = it.copied().collect();
        let after = base + msgs.len() as u64;
        for idx in before..after {
            self.outbox_all[shard].push(msgs[(idx - base) as usize]);
        }
        if let Cmd::Settle { idx, .. } = c {
            if matches!(o, Outcome::Settled | Outcome::Anomaly) {
                let routed = self.routed(shard);
                let k = self.settled_applied[shard];
                assert!(
                    k < routed.len() && routed[k].0 == idx,
                    "shard {shard}: applied event {idx} but the next one owed was {:?}",
                    routed.get(k).map(|r| r.0)
                );
                self.settled_applied[shard] += 1;
            }
        }
        let hw = self.shards[shard].high_water(0).unwrap_or(0);
        assert!(hw >= self.hw_seen[shard], "mark went backwards");
        self.hw_seen[shard] = hw;
        o
    }

    fn send(&mut self, m: Msg) {
        if self.faults {
            match self.rng.below(100) {
                0..=7 => {
                    self.stats.dropped += 1;
                    return;
                }
                8..=14 => {
                    self.stats.duplicated += 1;
                    self.net.push(m.clone());
                }
                _ => {}
            }
        }
        self.net.push(m);
    }

    fn deliver_one(&mut self) {
        if self.net.is_empty() {
            return;
        }
        let i = if self.faults {
            self.rng.below(self.net.len() as u64) as usize
        } else {
            0
        };
        let m = self.net.remove(i);
        match m {
            Msg::Submit { shard, idx, msg } => {
                let a = self.matcher.deliver(shard as u16, idx, msg);
                if a == Accept::Applied {
                    assert_eq!(
                        idx, self.matcher_applied[shard],
                        "intake applied out of order"
                    );
                    assert_eq!(
                        msg, self.outbox_all[shard][idx as usize],
                        "intake applied the wrong message"
                    );
                    self.matcher_applied[shard] += 1;
                }
                if a == Accept::Gap {
                    self.stats.intake_gaps += 1;
                }
                let cursor = self.matcher.intake.cursor(shard as u16);
                self.send(Msg::SubmitReply {
                    shard,
                    cursor,
                    gap: a == Accept::Gap,
                });
            }
            Msg::SubmitReply { shard, cursor, gap } => {
                if let Some(Cmd::OutboxAck { upto }) = self.forwarders[shard].on_reply(cursor, gap)
                {
                    self.send(Msg::Ack { shard, upto });
                }
            }
            Msg::Ack { shard, upto } => {
                self.ledger_apply(shard, Cmd::OutboxAck { upto });
            }
            Msg::HwQuery { shard } => {
                let hw = self.shards[shard].high_water(0).unwrap_or(0);
                self.send(Msg::HwReply { shard, hw });
            }
            Msg::HwReply { shard, hw } => {
                let st = &mut self.settlers[shard];
                if st.acked.is_none() {
                    st.acked = Some(hw);
                    st.pos = hw;
                }
            }
            Msg::Settle { shard, cmd } => {
                let o = self.ledger_apply(shard, cmd);
                let gap = o == Outcome::Rejected(Reject::Gap);
                if gap {
                    self.stats.gaps_refused += 1;
                }
                let hw = self.shards[shard].high_water(0).unwrap_or(0);
                self.send(Msg::SettleReply { shard, hw, gap });
            }
            Msg::SettleReply { shard, hw, gap } => {
                let st = &mut self.settlers[shard];
                if let Some(a) = st.acked {
                    st.acked = Some(a.max(hw));
                    if gap || hw > st.pos {
                        st.pos = hw.max(if gap { 0 } else { st.pos });
                    }
                }
            }
        }
    }

    fn tick_forwarder(&mut self, shard: usize) {
        let batch = self.forwarders[shard].poll(&self.shards[shard], 4);
        for s in batch {
            self.send(Msg::Submit {
                shard,
                idx: s.idx,
                msg: s.msg,
            });
        }
    }

    fn tick_settler(&mut self, shard: usize) {
        match self.settlers[shard].acked {
            None => self.send(Msg::HwQuery { shard }),
            Some(_) => {
                let pos = self.settlers[shard].pos;
                let events: Vec<(u64, Event)> = self
                    .matcher
                    .events
                    .iter()
                    .enumerate()
                    .map(|(i, e)| (i as u64 + 1, *e))
                    .collect();
                let cmds = settle_cmds(0, shard as u16, pos, events, 4);
                for c in cmds {
                    if let Cmd::Settle { idx, .. } = c {
                        self.settlers[shard].pos = idx;
                    }
                    self.send(Msg::Settle { shard, cmd: c });
                }
            }
        }
    }

    fn client_op(&mut self) {
        let account = 1 + self.rng.below(ACCOUNTS);
        let shard = (account % self.n() as u64) as usize;
        let kind = self.rng.below(12);
        match kind {
            0..=2 => {
                let asset = [BTC, USD, ETH][self.rng.below(3) as usize];
                let amount = 1 + self.rng.below(50_000) as u128;
                let id = self.next_ext * 100 + shard as u64;
                self.next_ext += 1;
                self.ledger_apply(
                    shard,
                    Cmd::Deposit {
                        account,
                        asset,
                        amount,
                        external_id: id,
                    },
                );
            }
            3..=8 => {
                let symbol = SYMS[self.rng.below(2) as usize];
                let side = if self.rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let price = 95 + self.rng.below(11);
                let qty = 1 + self.rng.below(6);
                if let Outcome::Reserved { order, .. } = self.ledger_apply(
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
                    self.known_orders[self.rng.below(self.known_orders.len() as u64) as usize];
                self.ledger_apply(
                    order_shard(order) as usize,
                    Cmd::Cancel {
                        account: owner,
                        order,
                    },
                );
            }
            _ => {
                let asset = [BTC, USD, ETH][self.rng.below(3) as usize];
                let id = self.rng.below(30);
                let amount = 1 + self.rng.below(2_000) as u128;
                self.ledger_apply(
                    shard,
                    Cmd::Withdraw {
                        account,
                        asset,
                        amount,
                        external_id: id,
                    },
                );
                let id = self.rng.below(30);
                let paid = self.rng.below(2) == 0;
                self.ledger_apply(
                    shard,
                    Cmd::Finalize {
                        external_id: id,
                        paid,
                    },
                );
            }
        }
    }

    fn crash_or_failover(&mut self) {
        let shard = self.rng.below(self.n() as u64) as usize;
        match self.rng.below(5) {
            0 | 1 => {
                self.forwarders[shard] = Forwarder::new(shard as u16);
                self.stats.crashes += 1;
            }
            2 | 3 => {
                self.settlers[shard] = SettlerSt::default();
                self.stats.crashes += 1;
            }
            _ => {
                // ledger failover: a new leader restores the replicated state and
                // replays the log tail; nothing committed is lost, in-flight messages may be
                let digest = self.shards[shard].digest();
                let snap = self.shards[shard].snapshot();
                let mut fresh = Ledger::new(config(shard as u16));
                fresh.restore(&snap).expect("restore");
                assert_eq!(fresh.digest(), digest);
                self.shards[shard] = fresh;
                let keep = self.net.len() / 2;
                self.net.truncate(keep);
                self.stats.failovers += 1;
            }
        }
    }

    fn step(&mut self) {
        match self.rng.below(100) {
            0..=24 => self.client_op(),
            25..=64 => self.deliver_one(),
            65..=73 => {
                let s = self.rng.below(self.n() as u64) as usize;
                self.tick_forwarder(s)
            }
            74..=82 => {
                let s = self.rng.below(self.n() as u64) as usize;
                self.tick_settler(s)
            }
            83..=86 => {
                // timeouts: go back to what is known to be accepted
                let s = self.rng.below(self.n() as u64) as usize;
                if self.rng.below(2) == 0 {
                    self.forwarders[s].rewind();
                } else if let Some(a) = self.settlers[s].acked {
                    self.settlers[s].pos = a;
                }
            }
            87..=89 => self.crash_or_failover(),
            _ => {}
        }
        for (i, l) in self.shards.iter().enumerate() {
            if let Err(e) = l.check() {
                panic!("shard {i}: {e}");
            }
        }
    }

    /// Stop faults and run the relays until nothing is left to do.
    fn drain(&mut self, what: &str) {
        self.faults = false;
        for round in 0..400 {
            for s in 0..self.n() {
                if round % 5 == 0 {
                    self.forwarders[s].rewind();
                    if let Some(a) = self.settlers[s].acked {
                        self.settlers[s].pos = a;
                    }
                }
                self.tick_forwarder(s);
                self.tick_settler(s);
            }
            let mut guard = 0;
            while !self.net.is_empty() && guard < 100_000 {
                self.deliver_one();
                guard += 1;
            }
            if self.converged() {
                return;
            }
        }
        panic!("{what}: did not converge: {:?}", self.progress());
    }

    fn progress(&self) -> Vec<String> {
        (0..self.n())
            .map(|s| {
                format!(
                    "shard {s}: outbox {}..{} cursor {} hw {} of {}",
                    self.shards[s].outbox().0,
                    self.shards[s].outbox_next(),
                    self.matcher.intake.cursor(s as u16),
                    self.shards[s].high_water(0).unwrap_or(0),
                    self.routed(s).last().map(|r| r.0).unwrap_or(0)
                )
            })
            .collect()
    }

    fn converged(&self) -> bool {
        (0..self.n()).all(|s| {
            self.matcher.intake.cursor(s as u16) == self.shards[s].outbox_next()
                && self.shards[s].outbox().0 == self.shards[s].outbox_next()
                && self.shards[s].high_water(0).unwrap_or(0)
                    == self.routed(s).last().map(|r| r.0).unwrap_or(0)
        })
    }

    fn assert_conserved(&self) {
        for asset in [BTC, USD, ETH] {
            let (mut have, mut wd, mut dep, mut inn, mut out) = (0u128, 0u128, 0u128, 0u128, 0u128);
            for l in &self.shards {
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
            assert_eq!(have + wd, dep, "asset {asset}: money is conserved");
        }
    }
}

fn run(n: usize, seed: u64, steps: usize) -> Stats {
    let mut s = Sys::new(n, seed);
    for _ in 0..steps {
        s.step();
    }
    s.drain("first drain");
    // every outbox message was applied by matching exactly once and in order
    for sh in 0..n {
        assert_eq!(
            s.matcher_applied[sh] as usize,
            s.outbox_all[sh].len(),
            "seed {seed}: shard {sh} outbox"
        );
    }
    // cancel everything, drain again, and everything is released
    let orders = s.known_orders.clone();
    for (account, order) in orders {
        s.ledger_apply(order_shard(order) as usize, Cmd::Cancel { account, order });
    }
    s.drain("second drain");
    for id in 0..30 {
        for sh in 0..n {
            s.ledger_apply(
                sh,
                Cmd::Finalize {
                    external_id: id,
                    paid: id % 2 == 0,
                },
            );
        }
    }
    for l in &s.shards {
        l.check().unwrap();
        assert_eq!(l.anomalies(), 0);
    }
    s.assert_conserved();
    let open: usize = s.shards.iter().map(|l| l.open_orders()).sum();
    assert_eq!(
        open,
        s.matcher.resting(),
        "seed {seed}: ledger orders vs book"
    );
    for l in &s.shards {
        for a in 1..=ACCOUNTS {
            for asset in [BTC, USD, ETH] {
                let b = l.balance(a, asset);
                let resting_hold = s.matcher.resting() > 0;
                assert!(resting_hold || b.held == 0, "seed {seed}: hold left behind");
            }
        }
    }
    // logs replay to the same states
    for (sh, log) in s.logs.iter().enumerate() {
        let mut r = Ledger::new(config(sh as u16));
        for c in log {
            r.apply(c);
        }
        assert_eq!(
            r.digest(),
            s.shards[sh].digest(),
            "seed {seed}: replay of shard {sh}"
        );
    }
    s.stats
}

#[test]
fn one_shard_under_faults() {
    for seed in 1..=40 {
        run(1, seed * 0x9E37_79B9, 3_000);
    }
}

#[test]
fn three_shards_under_faults() {
    for seed in 1..=40 {
        run(3, seed * 0x85EB_CA6B, 3_000);
    }
}

#[test]
fn the_faults_actually_happen() {
    let mut t = Stats::default();
    for seed in 1..=10 {
        let s = run(3, seed * 0xC2B2_AE35, 3_000);
        t.dropped += s.dropped;
        t.duplicated += s.duplicated;
        t.crashes += s.crashes;
        t.failovers += s.failovers;
        t.gaps_refused += s.gaps_refused;
        t.intake_gaps += s.intake_gaps;
    }
    eprintln!("{t:?}");
    assert!(t.dropped > 100 && t.duplicated > 100 && t.crashes > 50 && t.failovers > 10);
    assert!(
        t.gaps_refused > 10,
        "reordering must reach the ledger's gap check"
    );
    assert!(
        t.intake_gaps > 10,
        "reordering must reach the intake's gap check"
    );
}
