//! Whole-system simulation of phase 2 (credit slices) next to phase 1: clients,
//! ledger shards, forwarders, a matching group with a credit slice, settlers for
//! two streams (the matcher's events and the group's slice reports) and gateways
//! that enter orders directly at the group, over a network that loses, duplicates
//! and reorders, with crashes of every volatile role and failovers.
//!
//! Safety each step: every ledger's `check()` (lent funds count as the account's
//! money), the slice's `check()`, in-order exactly-once delivery of every outbox
//! message (grants and recalls included) and every settled event, marks that never
//! go back, and an exact conservation identity that ties ledgers, slice and the
//! messages in flight together. After faults stop everything converges, money is
//! conserved across all shards, and a full recall returns every lent unit.

mod common;
use common::*;
use orderer_ledger::policy::{plan, PolicyConfig, PolicyState};
use orderer_ledger::*;
use std::collections::BTreeMap;

const SRC_MATCH: u32 = 0;
const SRC_GROUP: u32 = 1;
const EPOCH: u64 = 5;
const GATEWAYS: usize = 2;
const ASSETS: [u32; 3] = [BTC, USD, ETH];

#[derive(Clone, Debug)]
enum Body {
    Enter {
        account: u64,
        symbol: u32,
        side: Side,
        price: u64,
        qty: u64,
    },
    Cancel {
        account: u64,
        order: u64,
    },
}

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
        src: u32,
    },
    HwReply {
        shard: usize,
        src: u32,
        hw: u64,
    },
    Settle {
        shard: usize,
        src: u32,
        cmd: Cmd,
    },
    SettleReply {
        shard: usize,
        src: u32,
        hw: u64,
        gap: bool,
    },
    Req {
        gw: usize,
        seq: u64,
        body: Body,
    },
    Rep {
        gw: usize,
        seq: u64,
        res: Result<u64, Reject>,
    },
}

/// The matching group as a deterministic state machine: books, intake, credit
/// slice, the group stream, and the credit epoch (every `EPOCH` entries it settles
/// the matcher's new events into the slice and reports).
struct Group {
    matcher: Matcher,
    slice: Slice,
    gstream: Vec<Event>,
    cursor: usize,
    entries: u64,
    last: BTreeMap<u64, (u64, Result<u64, Reject>)>,
}

impl Group {
    fn new() -> Group {
        Group {
            matcher: Matcher::default(),
            slice: Slice::new(config(0), 0),
            gstream: Vec::new(),
            cursor: 0,
            entries: 0,
            last: BTreeMap::new(),
        }
    }

    fn tick(&mut self) {
        self.entries += 1;
        if self.entries % EPOCH == 0 {
            self.epoch();
        }
    }

    fn epoch(&mut self) {
        let evs: Vec<Event> = self.matcher.events[self.cursor..].to_vec();
        self.cursor = self.matcher.events.len();
        self.slice.settle(evs.iter());
        let rep = self.slice.report();
        self.gstream.extend(rep);
    }

    /// An outbox message from a ledger shard, applied once per `(shard, idx)`.
    fn forward(&mut self, shard: u16, idx: u64, m: OutMsg) -> Accept {
        let a = self.matcher.intake.accept(shard, idx);
        if a == Accept::Applied {
            match m {
                OutMsg::Submit {
                    order,
                    symbol,
                    side,
                    price,
                    qty,
                } => self.matcher.submit(order, symbol, side, price, qty),
                OutMsg::SubmitIoc {
                    order,
                    symbol,
                    side,
                    price,
                    qty,
                } => self
                    .matcher
                    .submit_tif(order, symbol, side, price, qty, true),
                OutMsg::Cancel { order, symbol } => self.matcher.cancel(order, symbol),
                OutMsg::Grant {
                    group: _,
                    account,
                    asset,
                    amount,
                } => self.slice.grant(account, asset, amount),
                OutMsg::Recall {
                    group: _,
                    account,
                    asset,
                    amount,
                } => {
                    let ev = self.slice.recall(account, asset, amount);
                    self.gstream.extend(ev);
                }
            }
            self.tick();
        }
        a
    }

    /// A gateway request; `None` for a stale retry of an older request.
    fn handle(&mut self, gw: usize, seq: u64, body: &Body) -> Option<Result<u64, Reject>> {
        let client = gw as u64;
        if let Some((last, res)) = self.last.get(&client) {
            if seq < *last {
                return None;
            }
            if seq == *last {
                return Some(*res);
            }
        }
        let res = match *body {
            Body::Enter {
                account,
                symbol,
                side,
                price,
                qty,
            } => match self.slice.enter(account, symbol, side, price, qty) {
                Ok(order) => {
                    self.matcher.submit(order, symbol, side, price, qty);
                    Ok(order)
                }
                Err(r) => Err(r),
            },
            Body::Cancel { account, order } => match self.slice.cancel(account, order) {
                Ok(()) => {
                    let sym = self.slice.order_symbol(order).unwrap();
                    self.matcher.cancel(order, sym);
                    Ok(order)
                }
                Err(r) => Err(r),
            },
        };
        self.last.insert(client, (seq, res));
        self.tick();
        Some(res)
    }
}

#[derive(Clone, Default)]
struct SettlerSt {
    acked: Option<u64>,
    pos: u64,
}

#[derive(Default)]
struct Gw {
    seq: u64,
    pending: Option<(u64, Body)>,
    orders: Vec<(u64, u64)>,
}

#[derive(Default, Debug)]
struct Stats {
    dropped: u64,
    duplicated: u64,
    crashes: u64,
    failovers: u64,
    group_failovers: u64,
    entered: u64,
    rejected: u64,
    slice_trades: u64,
    mixed_trades: u64,
    returns: u64,
    gaps: u64,
    policy_lends: u64,
    policy_recalls: u64,
}

struct Sys {
    n: usize,
    shards: Vec<Ledger>,
    logs: Vec<Vec<Cmd>>,
    group: Group,
    forwarders: Vec<Forwarder>,
    settlers: Vec<[SettlerSt; 2]>,
    gws: Vec<Gw>,
    net: Vec<Msg>,
    rng: Rng,
    faults: bool,
    outbox_all: Vec<Vec<OutMsg>>,
    delivered: Vec<u64>,
    settled_applied: Vec<[usize; 2]>,
    hw_seen: Vec<[u64; 2]>,
    phase1: Vec<(u64, u64)>,
    next_ext: u64,
    stats: Stats,
    /// Policy drivers (volatile, one per shard), the time they read (steps), and
    /// the accounts the gateways have lately entered orders for.
    policies: Vec<PolicyState>,
    tclock: u64,
    demand: Vec<(u64, u32)>,
}

fn owes(n: usize, shard: usize, src: u32, e: &Event) -> bool {
    let shard_of = |a: u64| (a % n as u64) as usize;
    match (src, *e) {
        (SRC_MATCH, Event::Trade { buy, sell, .. }) => {
            order_shard(buy) as usize == shard || order_shard(sell) as usize == shard
        }
        (SRC_MATCH, Event::Closed { order }) => order_shard(order) as usize == shard,
        (SRC_GROUP, Event::SliceIn { account, .. })
        | (SRC_GROUP, Event::SliceOut { account, .. })
        | (SRC_GROUP, Event::Returned { account, .. }) => shard_of(account) == shard,
        (SRC_GROUP, Event::SliceFee { .. }) => shard == 0,
        _ => false,
    }
}

impl Sys {
    fn new(n: usize, seed: u64) -> Sys {
        Sys {
            n,
            shards: (0..n).map(|s| Ledger::new(config(s as u16))).collect(),
            logs: vec![Vec::new(); n],
            group: Group::new(),
            forwarders: (0..n).map(|s| Forwarder::new(s as u16)).collect(),
            settlers: vec![[SettlerSt::default(), SettlerSt::default()]; n],
            gws: (0..GATEWAYS).map(|_| Gw::default()).collect(),
            net: Vec::new(),
            rng: Rng(seed | 1),
            faults: true,
            outbox_all: vec![Vec::new(); n],
            delivered: vec![0; n],
            settled_applied: vec![[0; 2]; n],
            hw_seen: vec![[0; 2]; n],
            phase1: Vec::new(),
            next_ext: 1,
            stats: Stats::default(),
            policies: vec![PolicyState::new(); n],
            tclock: 0,
            demand: Vec::new(),
        }
    }

    fn shard_of(&self, account: u64) -> usize {
        (account % self.n as u64) as usize
    }

    fn stream(&self, src: u32) -> &Vec<Event> {
        if src == SRC_MATCH {
            &self.group.matcher.events
        } else {
            &self.group.gstream
        }
    }

    fn routed(&self, shard: usize, src: u32) -> Vec<(u64, Event)> {
        self.stream(src)
            .iter()
            .enumerate()
            .map(|(i, e)| (i as u64 + 1, *e))
            .filter(|(_, e)| owes(self.n, shard, src, e))
            .collect()
    }

    fn ledger_apply(&mut self, shard: usize, c: Cmd) -> Outcome {
        let before = self.shards[shard].outbox_next();
        let o = self.shards[shard].apply(&c);
        self.logs[shard].push(c.clone());
        let (base, it) = self.shards[shard].outbox();
        let msgs: Vec<OutMsg> = it.copied().collect();
        for idx in before..base + msgs.len() as u64 {
            self.outbox_all[shard].push(msgs[(idx - base) as usize]);
        }
        if let Cmd::Settle { source, idx, .. } = c {
            let s = source as usize;
            if matches!(o, Outcome::Settled | Outcome::Anomaly) {
                let routed = self.routed(shard, source);
                let k = self.settled_applied[shard][s];
                assert!(
                    k < routed.len() && routed[k].0 == idx,
                    "shard {shard} source {source}: applied event {idx}, next owed was {:?}",
                    routed.get(k).map(|r| r.0)
                );
                self.settled_applied[shard][s] += 1;
            }
            let hw = self.shards[shard].high_water(source).unwrap_or(0);
            assert!(hw >= self.hw_seen[shard][s], "mark went backwards");
            self.hw_seen[shard][s] = hw;
        }
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
        match self.net.remove(i) {
            Msg::Submit { shard, idx, msg } => {
                let a = self.group.forward(shard as u16, idx, msg);
                if a == Accept::Applied {
                    assert_eq!(idx, self.delivered[shard], "intake applied out of order");
                    assert_eq!(
                        msg, self.outbox_all[shard][idx as usize],
                        "intake applied the wrong message"
                    );
                    self.delivered[shard] += 1;
                }
                let cursor = self.group.matcher.intake.cursor(shard as u16);
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
            Msg::HwQuery { shard, src } => {
                let hw = self.shards[shard].high_water(src).unwrap_or(0);
                self.send(Msg::HwReply { shard, src, hw });
            }
            Msg::HwReply { shard, src, hw } => {
                let st = &mut self.settlers[shard][src as usize];
                if st.acked.is_none() {
                    st.acked = Some(hw);
                    st.pos = hw;
                }
            }
            Msg::Settle { shard, src, cmd } => {
                let o = self.ledger_apply(shard, cmd);
                let gap = o == Outcome::Rejected(Reject::Gap);
                if gap {
                    self.stats.gaps += 1;
                }
                let hw = self.shards[shard].high_water(src).unwrap_or(0);
                self.send(Msg::SettleReply {
                    shard,
                    src,
                    hw,
                    gap,
                });
            }
            Msg::SettleReply {
                shard,
                src,
                hw,
                gap,
            } => {
                let st = &mut self.settlers[shard][src as usize];
                if let Some(a) = st.acked {
                    st.acked = Some(a.max(hw));
                    if gap || hw > st.pos {
                        st.pos = hw;
                    }
                }
            }
            Msg::Req { gw, seq, body } => {
                if let Some(res) = self.group.handle(gw, seq, &body) {
                    self.send(Msg::Rep { gw, seq, res });
                }
            }
            Msg::Rep { gw, seq, res } => {
                let g = &mut self.gws[gw];
                if g.pending.as_ref().map(|p| p.0) == Some(seq) {
                    let (_, body) = g.pending.take().unwrap();
                    if let (Body::Enter { account, .. }, Ok(order)) = (&body, res) {
                        g.orders.push((*account, order));
                        self.stats.entered += 1;
                    } else if res.is_err() {
                        self.stats.rejected += 1;
                    }
                }
            }
        }
    }

    fn tick_forwarder(&mut self, shard: usize) {
        for s in self.forwarders[shard].poll(&self.shards[shard], 4) {
            self.send(Msg::Submit {
                shard,
                idx: s.idx,
                msg: s.msg,
            });
        }
    }

    fn tick_settler(&mut self, shard: usize, src: u32) {
        match self.settlers[shard][src as usize].acked {
            None => self.send(Msg::HwQuery { shard, src }),
            Some(_) => {
                let pos = self.settlers[shard][src as usize].pos;
                let n = self.n;
                let events: Vec<(u64, Event)> = self
                    .stream(src)
                    .iter()
                    .enumerate()
                    .map(|(i, e)| (i as u64 + 1, *e))
                    .collect();
                let cmds = if src == SRC_MATCH {
                    settle_cmds(src, shard as u16, pos, events, 4)
                } else {
                    settle_cmds_routed(
                        src,
                        shard as u16,
                        pos,
                        events,
                        4,
                        move |a| (a % n as u64) as u16,
                        0,
                    )
                };
                for c in cmds {
                    if let Cmd::Settle { idx, .. } = c {
                        self.settlers[shard][src as usize].pos = idx;
                    }
                    self.send(Msg::Settle { shard, src, cmd: c });
                }
            }
        }
    }

    fn tick_gateway(&mut self, gw: usize) {
        if let Some((seq, body)) = self.gws[gw].pending.clone() {
            self.send(Msg::Req { gw, seq, body });
            return;
        }
        let account = 1 + self.rng.below(ACCOUNTS);
        let body = if self.rng.below(4) == 0 && !self.gws[gw].orders.is_empty() {
            let k = self.rng.below(self.gws[gw].orders.len() as u64) as usize;
            let (account, order) = self.gws[gw].orders[k];
            Body::Cancel { account, order }
        } else {
            let symbol = SYMS[self.rng.below(2) as usize];
            let sym = config(0).symbols[&symbol];
            self.demand.push((account, sym.quote));
            self.demand.push((account, sym.base));
            if self.demand.len() > 24 {
                self.demand.drain(..self.demand.len() - 24);
            }
            Body::Enter {
                account,
                symbol,
                side: if self.rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                price: 95 + self.rng.below(11),
                qty: 1 + self.rng.below(6),
            }
        };
        self.gws[gw].seq += 1;
        let seq = self.gws[gw].seq;
        self.gws[gw].pending = Some((seq, body.clone()));
        self.send(Msg::Req { gw, seq, body });
    }

    fn client_op(&mut self) {
        let account = 1 + self.rng.below(ACCOUNTS);
        let shard = self.shard_of(account);
        let asset = ASSETS[self.rng.below(3) as usize];
        match self.rng.below(16) {
            0..=3 => {
                let amount = 1 + self.rng.below(60_000) as u128;
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
            4..=7 => {
                let avail = self.shards[shard].balance(account, asset).available;
                let amount = 1 + self.rng.below(avail.min(40_000) as u64 + 1) as u128;
                self.ledger_apply(
                    shard,
                    Cmd::Lend {
                        group: 0,
                        account,
                        asset,
                        amount,
                    },
                );
            }
            8 => {
                let amount = 1 + self.rng.below(30_000) as u128;
                self.ledger_apply(
                    shard,
                    Cmd::Recall {
                        group: 0,
                        account,
                        asset,
                        amount,
                    },
                );
            }
            9..=10 => {
                let symbol = SYMS[self.rng.below(2) as usize];
                let side = if self.rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let price = 95 + self.rng.below(11);
                let qty = 1 + self.rng.below(6);
                let ioc = self.rng.below(4) == 0;
                if let Outcome::Reserved { order, .. } = self.ledger_apply(
                    shard,
                    maybe_ioc(
                        Cmd::Reserve {
                            account,
                            symbol,
                            side,
                            price,
                            qty,
                        },
                        ioc,
                    ),
                ) {
                    self.phase1.push((account, order));
                }
            }
            11 if !self.phase1.is_empty() => {
                let (owner, order) = self.phase1[self.rng.below(self.phase1.len() as u64) as usize];
                self.ledger_apply(
                    order_shard(order) as usize,
                    Cmd::Cancel {
                        account: owner,
                        order,
                    },
                );
            }
            _ => {
                // S4: a withdrawal never takes lent funds
                let amount = 1 + self.rng.below(5_000) as u128;
                let id = self.rng.below(30);
                let before = (
                    self.shards[shard].balance(account, asset).available,
                    self.shards[shard].lent(account, asset),
                );
                let o = self.ledger_apply(
                    shard,
                    Cmd::Withdraw {
                        account,
                        asset,
                        amount,
                        external_id: id,
                    },
                );
                if o == Outcome::WithdrawHeld {
                    assert!(amount <= before.0, "withdrew more than was available");
                }
                assert_eq!(
                    self.shards[shard].lent(account, asset),
                    before.1,
                    "a withdrawal touched lent funds"
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

    fn policy_cfg() -> PolicyConfig {
        PolicyConfig {
            group: 0,
            chunk: ASSETS.iter().map(|a| (*a, 20_000u128)).collect(),
            low_water_pct: 30,
            account_cap: ASSETS.iter().map(|a| (*a, 70_000u128)).collect(),
            group_cap: ASSETS.iter().map(|a| (*a, 200_000u128)).collect(),
            idle_ns: 120,
            max_actions: 6,
        }
    }

    /// One look of a shard's policy driver: demand is what its accounts' gateways
    /// have lately traded, credit is what the group says is free. Whatever it
    /// proposes goes through the ledger like any command; the ledger decides.
    fn policy_op(&mut self, shard: usize, everything_idle: bool) {
        let demand: Vec<(u64, u32)> = if everything_idle {
            Vec::new()
        } else {
            self.demand
                .iter()
                .copied()
                .filter(|(a, _)| self.shard_of(*a) == shard)
                .collect()
        };
        let slice = &self.group.slice;
        let credit = |a: u64, asset: u32| Some(slice.available(a, asset));
        let cmds = plan(
            &Self::policy_cfg(),
            &mut self.policies[shard],
            &self.shards[shard],
            &credit,
            &demand,
            self.tclock,
        );
        for c in cmds {
            match &c {
                Cmd::Lend { .. } => self.stats.policy_lends += 1,
                Cmd::Recall { .. } => self.stats.policy_recalls += 1,
                c => panic!("the policy proposed {c:?}"),
            }
            self.ledger_apply(shard, c);
        }
    }

    fn crash(&mut self) {
        let shard = self.rng.below(self.n as u64) as usize;
        match self.rng.below(8) {
            0 | 1 => {
                self.forwarders[shard] = Forwarder::new(shard as u16);
                self.policies[shard] = PolicyState::new();
                self.stats.crashes += 1;
            }
            2..=4 => {
                self.settlers[shard][self.rng.below(2) as usize] = SettlerSt::default();
                self.stats.crashes += 1;
            }
            5 => {
                let g = self.rng.below(GATEWAYS as u64) as usize;
                // a restarted gateway forgets its request in flight; the group
                // already has (or never got) it, either way nothing is owed
                self.gws[g].pending = None;
                self.stats.crashes += 1;
            }
            6 => {
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
            _ => {
                // group failover: the slice is restored from a snapshot
                let d = self.group.slice.digest();
                let snap = self.group.slice.snapshot();
                let mut fresh = Slice::new(config(0), 0);
                fresh.restore(&snap).expect("slice restore");
                assert_eq!(fresh.digest(), d);
                self.group.slice = fresh;
                let keep = self.net.len() / 2;
                self.net.truncate(keep);
                self.stats.group_failovers += 1;
            }
        }
    }

    fn step(&mut self) {
        match self.rng.below(100) {
            0..=17 => self.client_op(),
            18..=52 => self.deliver_one(),
            53..=59 => {
                let s = self.rng.below(self.n as u64) as usize;
                self.tick_forwarder(s)
            }
            60..=68 => {
                let s = self.rng.below(self.n as u64) as usize;
                let src = self.rng.below(2) as u32;
                self.tick_settler(s, src)
            }
            69..=82 => {
                let g = self.rng.below(GATEWAYS as u64) as usize;
                self.tick_gateway(g)
            }
            83..=85 => {
                let s = self.rng.below(self.n as u64) as usize;
                match self.rng.below(3) {
                    0 => self.forwarders[s].rewind(),
                    _ => {
                        let src = self.rng.below(2) as usize;
                        if let Some(a) = self.settlers[s][src].acked {
                            self.settlers[s][src].pos = a;
                        }
                    }
                }
            }
            86..=88 => self.crash(),
            89..=93 => {
                let s = self.rng.below(self.n as u64) as usize;
                self.policy_op(s, false);
            }
            _ => {}
        }
        self.tclock += 1;
        self.check();
        if self.tclock % 13 == 0 {
            for (i, l) in self.shards.iter().enumerate() {
                assert_eq!(l.digest(), l.digest_from_scratch(), "shard {i}: digest");
            }
            assert_eq!(
                self.group.slice.engine().digest(),
                self.group.slice.engine().digest_from_scratch(),
                "slice engine digest"
            );
        }
    }

    /// Money still to be delivered to the slice, per asset: grants not yet accepted.
    fn grants_unaccepted(&self, asset: u32) -> i128 {
        let mut t = 0i128;
        for (s, l) in self.shards.iter().enumerate() {
            let cursor = self.group.matcher.intake.cursor(s as u16);
            let (base, it) = l.outbox();
            for (i, m) in it.enumerate() {
                if let OutMsg::Grant {
                    asset: a, amount, ..
                } = *m
                {
                    if a == asset && base + i as u64 >= cursor {
                        t += amount as i128;
                    }
                }
            }
        }
        t
    }

    /// Returns emitted by the slice and not yet applied by their ledger shard.
    fn returns_pending(&self, asset: u32) -> i128 {
        let mut t = 0i128;
        for (i, e) in self.group.gstream.iter().enumerate() {
            if let Event::Returned {
                group: 0,
                account,
                asset: a,
                amount,
            } = *e
            {
                let s = self.shard_of(account);
                if a == asset && self.shards[s].high_water(SRC_GROUP).unwrap_or(0) < i as u64 + 1 {
                    t += amount as i128;
                }
            }
        }
        t
    }

    fn check(&self) {
        for (i, l) in self.shards.iter().enumerate() {
            if let Err(e) = l.check() {
                panic!("shard {i}: {e}");
            }
        }
        if let Err(e) = self.group.slice.check() {
            panic!("slice: {e}");
        }
        // The slice's money, less what it exchanged with phase 1 orders, equals
        // what the ledgers lent, less what is still in flight in either direction
        // (S1, exact at every step).
        for a in ASSETS {
            let eng = self.group.slice.engine();
            let (e_in, e_out) = eng.settled_flow(a);
            let lhs = eng.holdings(a) as i128 - e_in as i128 + e_out as i128;
            let (mut lent, mut s_in, mut s_out) = (0i128, 0i128, 0i128);
            for l in &self.shards {
                lent += l.lent_total(a) as i128;
                let (_, _, i, o) = l.slice_flow(a);
                s_in += i as i128;
                s_out += o as i128;
            }
            let rhs = lent - s_in + s_out - self.grants_unaccepted(a) - self.returns_pending(a);
            assert_eq!(
                lhs, rhs,
                "asset {a}: slice vs ledgers vs messages in flight"
            );
        }
    }

    fn drain(&mut self, what: &str) {
        self.faults = false;
        for round in 0..600 {
            self.group.epoch();
            for s in 0..self.n {
                if round % 5 == 0 {
                    self.forwarders[s].rewind();
                    for src in 0..2 {
                        if let Some(a) = self.settlers[s][src].acked {
                            self.settlers[s][src].pos = a;
                        }
                    }
                }
                self.tick_forwarder(s);
                self.tick_settler(s, SRC_MATCH);
                self.tick_settler(s, SRC_GROUP);
            }
            for g in 0..GATEWAYS {
                if self.gws[g].pending.is_some() {
                    self.tick_gateway(g);
                }
            }
            let mut guard = 0;
            while !self.net.is_empty() && guard < 200_000 {
                self.deliver_one();
                guard += 1;
            }
            self.check();
            if self.converged() {
                return;
            }
        }
        panic!("{what}: did not converge");
    }

    fn converged(&self) -> bool {
        self.group.cursor == self.group.matcher.events.len()
            && self.gws.iter().all(|g| g.pending.is_none())
            && (0..self.n).all(|s| {
                let last = |src| self.routed(s, src).last().map(|r| r.0).unwrap_or(0);
                self.group.matcher.intake.cursor(s as u16) == self.shards[s].outbox_next()
                    && self.shards[s].outbox().0 == self.shards[s].outbox_next()
                    && self.shards[s].high_water(SRC_MATCH).unwrap_or(0) == last(SRC_MATCH)
                    && self.shards[s].high_water(SRC_GROUP).unwrap_or(0) == last(SRC_GROUP)
            })
    }
}

fn run(n: usize, seed: u64, steps: usize) -> Stats {
    let mut s = Sys::new(n, seed);
    for _ in 0..steps {
        s.step();
    }
    s.drain("first drain");
    // cancel everything: phase 1 orders through their ledgers, slice orders at the group
    for (account, order) in s.phase1.clone() {
        s.ledger_apply(order_shard(order) as usize, Cmd::Cancel { account, order });
    }
    for (order, account, symbol) in s.group.slice.orders() {
        if s.group.slice.cancel(account, order).is_ok() {
            s.group.matcher.cancel(order, symbol);
        }
    }
    s.drain("second drain");
    // the policy alone returns every idle slice: two looks an idle period apart per
    // round (the first starts the clock), then the recalls travel and settle
    for _ in 0..3 {
        for step in 0..2 {
            s.tclock += 200;
            let _ = step;
            for sh in 0..n {
                s.policy_op(sh, true);
            }
        }
        s.drain("policy return drain");
    }
    for sh in 0..n {
        for a in ASSETS {
            assert_eq!(
                s.shards[sh].lent_total(a),
                0,
                "seed {seed}: the policy left funds lent"
            );
        }
    }
    // recall every lent unit: with nothing committed it all comes back
    for _ in 0..2 {
        for account in 1..=ACCOUNTS {
            for asset in ASSETS {
                let sh = s.shard_of(account);
                let lent = s.shards[sh].lent(account, asset);
                if lent > 0 {
                    s.ledger_apply(
                        sh,
                        Cmd::Recall {
                            group: 0,
                            account,
                            asset,
                            amount: lent,
                        },
                    );
                }
            }
        }
        s.drain("recall drain");
    }
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
    s.check();
    for (i, l) in s.shards.iter().enumerate() {
        l.check().unwrap();
        assert_eq!(l.anomalies(), 0, "seed {seed} shard {i}");
        assert_eq!(
            l.open_orders(),
            0,
            "seed {seed}: a phase 1 order is still open"
        );
        for a in ASSETS {
            assert_eq!(
                l.lent_total(a),
                0,
                "seed {seed}: lent funds were not all returned"
            );
        }
    }
    assert_eq!(s.group.slice.anomalies(), 0);
    assert_eq!(s.group.slice.open_orders(), 0);
    assert_eq!(
        s.group.matcher.resting(),
        0,
        "seed {seed}: orders still rest"
    );
    for a in ASSETS {
        assert_eq!(
            s.group.slice.accounts_total(a),
            0,
            "seed {seed}: slice still holds money"
        );
        let (mut have, mut wd, mut dep, mut inn, mut out) = (0u128, 0u128, 0u128, 0u128, 0u128);
        for l in &s.shards {
            have += l.holdings(a);
            let (d, w) = l.external_net(a);
            dep += d;
            wd += w;
            let (i, o) = l.settled_flow(a);
            inn += i;
            out += o;
        }
        assert_eq!(
            inn, out,
            "seed {seed} asset {a}: trades must move exactly what they take"
        );
        assert_eq!(have + wd, dep, "seed {seed} asset {a}: money is conserved");
    }
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
    for e in &s.group.matcher.events {
        if let Event::Trade { buy, sell, .. } = *e {
            let (a, b) = (is_slice_order(buy), is_slice_order(sell));
            if a || b {
                s.stats.slice_trades += 1;
            }
            if a != b {
                s.stats.mixed_trades += 1;
            }
        }
    }
    s.stats.returns = s
        .group
        .gstream
        .iter()
        .filter(|e| matches!(e, Event::Returned { .. }))
        .count() as u64;
    s.stats
}

#[test]
fn one_shard_under_faults() {
    for seed in 1..=30 {
        run(1, seed * 0x9E37_79B9, 3_000);
    }
}

#[test]
fn three_shards_under_faults() {
    for seed in 1..=30 {
        run(3, seed * 0x85EB_CA6B, 3_000);
    }
}

#[test]
fn the_faults_and_the_slice_traffic_actually_happen() {
    let mut t = Stats::default();
    for seed in 1..=20 {
        let s = run(3, seed * 0xC2B2_AE35, 3_000);
        t.dropped += s.dropped;
        t.duplicated += s.duplicated;
        t.crashes += s.crashes;
        t.failovers += s.failovers;
        t.group_failovers += s.group_failovers;
        t.entered += s.entered;
        t.rejected += s.rejected;
        t.slice_trades += s.slice_trades;
        t.mixed_trades += s.mixed_trades;
        t.returns += s.returns;
        t.gaps += s.gaps;
        t.policy_lends += s.policy_lends;
        t.policy_recalls += s.policy_recalls;
    }
    eprintln!("{t:?}");
    assert!(t.dropped > 100 && t.duplicated > 100 && t.crashes > 50);
    assert!(t.failovers > 5 && t.group_failovers > 5);
    assert!(t.entered > 100, "orders must enter through slices");
    assert!(t.rejected > 10, "the credit check must refuse some orders");
    assert!(t.slice_trades > 50, "slice orders must trade");
    assert!(
        t.mixed_trades > 5,
        "slice orders must trade against phase 1 orders"
    );
    assert!(t.returns > 20, "recalls must return funds");
    assert!(t.gaps > 10, "reordering must reach the ledger's gap check");
    assert!(t.policy_lends > 50, "the policy must lend");
    assert!(t.policy_recalls > 20, "the policy must return idle slices");
}
