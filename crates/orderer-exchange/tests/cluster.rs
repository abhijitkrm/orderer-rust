//! Three real replicas (TCP, files) running the matching tier, with the exchange
//! service in front of each: identical streams and digests, exactly-once intake
//! through retries and failovers, snapshot restore, and the ledger running
//! end to end against the real matching group.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::{Replica, ReplicaConfig, ReplicaHandle};
use orderer_consensus::{Config as RaftConfig, NodeId, Role};
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::*;
use orderer_ledger::{
    order_shard, settle_cmds, Cmd, Config, Event, Forwarder, Ledger, OutMsg, Outcome, Side, Symbol,
    SHARD_SHIFT,
};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const PARTITIONS: u32 = 4;
const BASE: u32 = 50;

fn free_addrs(n: usize) -> Vec<SocketAddr> {
    let ls: Vec<TcpListener> = (0..n)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    ls.iter().map(|l| l.local_addr().unwrap()).collect()
}

fn temp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("orderer-exchange-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

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

struct Node {
    replica: Option<Replica>,
    server: Option<ExchangeServer>,
    view: Option<TierView>,
    addr: Option<SocketAddr>,
}

struct World {
    dir: PathBuf,
    members: Vec<(NodeId, SocketAddr)>,
    nodes: Vec<Node>,
}

impl World {
    fn new(name: &str, n: usize, compact_every: u64) -> World {
        let members = free_addrs(n)
            .into_iter()
            .enumerate()
            .map(|(i, a)| (i as NodeId, a))
            .collect();
        let mut w = World {
            dir: temp(name),
            members,
            nodes: (0..n)
                .map(|_| Node {
                    replica: None,
                    server: None,
                    view: None,
                    addr: None,
                })
                .collect(),
        };
        for i in 0..n {
            w.start(i, compact_every);
        }
        w
    }

    fn start(&mut self, i: usize, compact_every: u64) {
        let mut cfg = ReplicaConfig::new(
            i as NodeId,
            self.members.clone(),
            self.dir.join(format!("n{i}")),
        );
        cfg.tick = Duration::from_millis(5);
        cfg.consensus = RaftConfig {
            election_ticks: 20,
            heartbeat_ticks: 2,
            max_batch: 256,
            ..RaftConfig::default()
        };
        cfg.fsync = false;
        cfg.compact_every = compact_every;
        cfg.digest_every = 20;
        cfg.propose_timeout = Duration::from_secs(5);
        let (sm, view) = MatchingTierSm::<FifoCore>::new(
            BookConfig::default(),
            PartitionMap::hash(PARTITIONS).unwrap(),
            TierConfig {
                source_base: BASE,
                retain: 1 << 20,
            },
            |init, plugs| {
                let mut b = Pipeline::<FifoCore>::builder()
                    .partitions(PARTITIONS)
                    .egress(plugs.digest)
                    .egress(plugs.archive);
                if let Some(i) = init {
                    b = b.initial(i);
                }
                b.build().unwrap()
            },
        );
        let replica = Replica::start(cfg, sm).unwrap();
        let server = ExchangeServer::start(
            "127.0.0.1:0".parse().unwrap(),
            replica.handle(),
            view.clone(),
        )
        .unwrap();
        let n = &mut self.nodes[i];
        n.addr = Some(server.addr);
        n.server = Some(server);
        n.view = Some(view);
        n.replica = Some(replica);
    }

    fn crash(&mut self, i: usize) {
        let n = &mut self.nodes[i];
        if let Some(s) = n.server.take() {
            s.shutdown();
        }
        n.replica.take().unwrap().crash();
        n.view = None;
        n.addr = None;
    }

    fn handles(&self) -> Vec<Option<ReplicaHandle>> {
        self.nodes
            .iter()
            .map(|n| n.replica.as_ref().map(|r| r.handle()))
            .collect()
    }

    fn wait_leader(&self) -> usize {
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(i) = self
                .handles()
                .iter()
                .position(|h| h.as_ref().is_some_and(|h| h.status().role == Role::Leader))
            {
                return i;
            }
            assert!(Instant::now() < end, "no leader");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn live(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|i| self.nodes[*i].replica.is_some())
            .collect()
    }

    /// Wait until every running node has applied the same index, then return their digests.
    fn settle(&self) -> Vec<[u8; 32]> {
        let end = Instant::now() + Duration::from_secs(40);
        loop {
            let hs = self.handles();
            let live: Vec<&ReplicaHandle> = hs.iter().flatten().collect();
            let st: Vec<_> = live.iter().map(|h| h.status()).collect();
            let max = st.iter().map(|s| s.commit).max().unwrap();
            if st.iter().all(|s| s.applied == max && s.halted.is_none()) {
                let hashes: Vec<_> = live.iter().map(|h| h.state_hash()).collect();
                if hashes.iter().all(|h| h.is_some()) {
                    let hashes: Vec<_> = hashes.into_iter().flatten().collect();
                    if hashes.iter().all(|h| h.0 == hashes[0].0) {
                        return hashes.into_iter().map(|h| h.1).collect();
                    }
                }
            }
            assert!(Instant::now() < end, "replicas did not settle");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Forward to whichever node is the leader, retrying until a definite answer.
    fn forward(&self, shard: u16, first: u64, msgs: &[OutMsg]) -> ForwardAck {
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            for i in self.live() {
                let Ok(mut c) = ExchangeClient::connect(self.nodes[i].addr.unwrap()) else {
                    continue;
                };
                if let Ok(Ok(ack)) = c.forward(shard, first, msgs) {
                    return ack;
                }
            }
            assert!(Instant::now() < end, "could not forward");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn stream(&self, i: usize, source: u32) -> Vec<(u64, Event)> {
        self.nodes[i]
            .view
            .as_ref()
            .unwrap()
            .read(source, 1, usize::MAX)
            .unwrap()
            .unwrap()
    }
}

impl Drop for World {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            if let Some(s) = n.server.take() {
                s.shutdown();
            }
            if let Some(r) = n.replica.take() {
                r.crash();
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn oid(shard: u16, n: u64) -> u64 {
    ((shard as u64) << SHARD_SHIFT) | n
}

/// Batches of forwarded messages with contiguous per-shard outbox indices.
fn script(shards: u16, steps: usize, seed: u64) -> Vec<(u16, u64, Vec<OutMsg>)> {
    let mut r = Rng(seed | 1);
    let mut cursor = vec![0u64; shards as usize];
    let mut counter = vec![0u64; shards as usize];
    let mut out = Vec::new();
    for _ in 0..steps {
        let s = r.below(shards as u64) as usize;
        let mut msgs = Vec::new();
        for _ in 0..1 + r.below(8) {
            if counter[s] > 0 && r.below(4) == 0 {
                let n = 1 + r.below(counter[s]);
                msgs.push(OutMsg::Cancel {
                    order: oid(s as u16, n),
                    symbol: r.below(8) as u32,
                });
            } else {
                counter[s] += 1;
                let side = if r.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                msgs.push(OutMsg::Submit {
                    order: oid(s as u16, counter[s]),
                    symbol: r.below(8) as u32,
                    side,
                    price: 95 + r.below(11),
                    qty: 1 + r.below(5),
                });
            }
        }
        out.push((s as u16, cursor[s], msgs.clone()));
        cursor[s] += msgs.len() as u64;
    }
    out
}

fn assert_same_streams(w: &World, sources: std::ops::Range<u32>) -> u64 {
    let live = w.live();
    let mut total = 0;
    for s in sources {
        let first = w.stream(live[0], s);
        total += first.len() as u64;
        for &i in &live[1..] {
            assert_eq!(w.stream(i, s), first, "stream {s} differs on node {i}");
        }
        assert!(
            first.iter().enumerate().all(|(k, e)| e.0 == k as u64 + 1),
            "stream {s} not contiguous from 1"
        );
    }
    total
}

#[test]
fn three_replicas_serve_identical_streams_under_retries_and_reordering() {
    let w = World::new("agree", 3, 1_000_000);
    w.wait_leader();
    let sc = script(3, 150, 17);
    let mut r = Rng(5);
    let mut acked = [0u64; 3];
    for (k, (shard, first, msgs)) in sc.iter().enumerate() {
        let ack = w.forward(*shard, *first, msgs);
        acked[*shard as usize] = ack.cursor;
        assert!(!ack.gap);
        assert_eq!(ack.cursor, first + msgs.len() as u64);
        // resends of this and an earlier batch, and a batch from the future
        if r.below(3) == 0 {
            let a = w.forward(*shard, *first, msgs);
            assert_eq!(
                a.cursor,
                first + msgs.len() as u64,
                "a duplicate does not move the cursor"
            );
            assert!(!a.gap);
        }
        if r.below(4) == 0 {
            let skip = first + msgs.len() as u64 + 5;
            let a = w.forward(*shard, skip, &msgs[..1]);
            assert!(a.gap, "a skipped batch is refused (batch {k})");
            assert_eq!(a.cursor, first + msgs.len() as u64);
        }
    }
    let digests = w.settle();
    assert!(
        digests.iter().all(|d| *d == digests[0]),
        "replica digests differ"
    );
    let total = assert_same_streams(&w, BASE..BASE + PARTITIONS);
    assert!(total > 200, "the workload should trade and cancel: {total}");
    for &i in &w.live() {
        let v = w.nodes[i].view.as_ref().unwrap();
        for s in 0..3 {
            assert_eq!(v.cursor(s), acked[s as usize]);
        }
        assert_eq!(v.stats().0, 0);
    }
    // the service reads the same thing a view does, from a follower too
    let follower = (0..3)
        .find(|&i| w.handles()[i].as_ref().unwrap().status().role != Role::Leader)
        .unwrap();
    let mut c = ExchangeClient::connect(w.nodes[follower].addr.unwrap()).unwrap();
    let mut got = Vec::new();
    let mut from = 1;
    loop {
        let (evs, end) = c.read(BASE, from, 7).unwrap().unwrap();
        if evs.is_empty() {
            assert_eq!(end, got.len() as u64);
            break;
        }
        from = evs.last().unwrap().0 + 1;
        got.extend(evs);
    }
    assert_eq!(got, w.stream(follower, BASE));
    // a linearizable cursor read works on the leader and is refused elsewhere
    let leader = w.wait_leader();
    let mut lc = ExchangeClient::connect(w.nodes[leader].addr.unwrap()).unwrap();
    assert_eq!(lc.cursor(1).unwrap().unwrap(), acked[1]);
    assert!(matches!(c.cursor(1).unwrap(), Err(Refusal::NotLeader(_))));
    assert_eq!(
        c.read(BASE + 99, 1, 1).unwrap(),
        Err(Refusal::Error("unknown source".into()))
    );
}

#[test]
fn snapshot_install_follower_restart_and_leader_failover_keep_streams_identical() {
    let mut w = World::new("chaos", 3, 25);
    let l0 = w.wait_leader();
    let follower = (0..3).find(|&i| i != l0).unwrap();
    let sc = script(2, 360, 31);
    let go = |w: &World, part: &[(u16, u64, Vec<OutMsg>)]| {
        for (s, f, m) in part {
            assert_eq!(w.forward(*s, *f, m).cursor, f + m.len() as u64);
        }
    };
    go(&w, &sc[..90]);
    // a follower down for a long stretch must come back through a snapshot
    w.crash(follower);
    go(&w, &sc[90..200]);
    w.start(follower, 25);
    // then the leader dies in the middle of the stream
    go(&w, &sc[200..260]);
    w.settle();
    w.crash(l0);
    go(&w, &sc[260..]);
    w.start(l0, 25);
    let digests = w.settle();
    assert!(digests.iter().all(|d| *d == digests[0]));
    let total = assert_same_streams(&w, BASE..BASE + PARTITIONS);
    assert!(total > 100, "{total}");
    let compacted = w
        .handles()
        .iter()
        .flatten()
        .map(|h| h.status().base)
        .max()
        .unwrap();
    assert!(
        compacted > 0,
        "the log was compacted, so restarts went through snapshots"
    );
    // every message was applied exactly once: cursors equal the script's totals
    for s in 0..2u16 {
        let want: u64 = sc
            .iter()
            .filter(|b| b.0 == s)
            .map(|b| b.2.len() as u64)
            .sum();
        for &i in &w.live() {
            assert_eq!(w.nodes[i].view.as_ref().unwrap().cursor(s), want);
        }
    }
}

// ---- the ledger against the real matching group ----------------------------

const BTC: u32 = 1;
const USD: u32 = 2;
const ACCOUNTS: u64 = 6;

fn ledger_cfg(shard: u16) -> Config {
    let mut symbols = std::collections::BTreeMap::new();
    for s in 0..8u32 {
        symbols.insert(
            s,
            Symbol {
                base: BTC,
                quote: USD,
                lot: 1_000,
            },
        );
    }
    Config {
        shard,
        fee_ppm: 500,
        symbols,
        fees: Default::default(),
    }
}

/// The relays and the ledger shards. The forwarders and the settler's scan
/// positions are disposable; the ledgers are the durable side.
struct Exchange {
    ledgers: Vec<Ledger>,
    forwarders: Vec<Forwarder>,
    /// Next index to scan per (shard, stream).
    scan: std::collections::BTreeMap<(usize, u32), u64>,
    known: Vec<(u64, u64)>,
    next_dep: u64,
}

impl Exchange {
    fn new(shards: usize) -> Exchange {
        let mut ledgers: Vec<Ledger> = (0..shards)
            .map(|s| Ledger::new(ledger_cfg(s as u16)))
            .collect();
        let mut dep = 0;
        for a in 1..=ACCOUNTS {
            for asset in [BTC, USD] {
                dep += 1;
                ledgers[(a as usize) % shards].apply(&Cmd::Deposit {
                    account: a,
                    asset,
                    amount: 5_000_000,
                    external_id: dep,
                });
            }
        }
        Exchange {
            ledgers,
            forwarders: (0..shards).map(|s| Forwarder::new(s as u16)).collect(),
            scan: Default::default(),
            known: Vec::new(),
            next_dep: 1_000,
        }
    }

    /// A crashed relay process: everything but the ledgers is forgotten.
    fn restart_relays(&mut self) {
        self.forwarders = (0..self.ledgers.len())
            .map(|s| Forwarder::new(s as u16))
            .collect();
        self.scan.clear();
    }

    fn pump_forwarders(&mut self, w: &World) {
        for s in 0..self.ledgers.len() {
            let batch = self.forwarders[s].poll(&self.ledgers[s], 6);
            let Some(first) = batch.first().map(|b| b.idx) else {
                continue;
            };
            let msgs: Vec<OutMsg> = batch.iter().map(|b| b.msg).collect();
            let ack = w.forward(s as u16, first, &msgs);
            if let Some(cmd) = self.forwarders[s].on_reply(ack.cursor, ack.gap) {
                self.ledgers[s].apply(&cmd);
            }
        }
    }

    fn pump_settlers(&mut self, v: &TierView) {
        for s in 0..self.ledgers.len() {
            for p in 0..PARTITIONS {
                let source = BASE + p;
                let hw = self.ledgers[s].high_water(source).unwrap_or(0);
                let from = self.scan.get(&(s, source)).copied().unwrap_or(0).max(hw) + 1;
                let Some(Ok(evs)) = v.read(source, from, 64) else {
                    continue;
                };
                let Some(last) = evs.last().map(|e| e.0) else {
                    continue;
                };
                for cmd in settle_cmds(source, s as u16, hw, evs, 64) {
                    let o = self.ledgers[s].apply(&cmd);
                    assert!(matches!(o, Outcome::Settled), "{o:?}");
                }
                self.scan.insert((s, source), last);
            }
        }
    }

    fn client_op(&mut self, r: &mut Rng) {
        let account = 1 + r.below(ACCOUNTS);
        let shard = (account % self.ledgers.len() as u64) as usize;
        match r.below(10) {
            0..=1 => {
                for asset in [BTC, USD] {
                    self.next_dep += 1;
                    self.ledgers[shard].apply(&Cmd::Deposit {
                        account,
                        asset,
                        amount: 1 + r.below(100_000) as u128,
                        external_id: self.next_dep,
                    });
                }
            }
            2..=7 => {
                let side = if r.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let c = Cmd::Reserve {
                    account,
                    symbol: r.below(8) as u32,
                    side,
                    price: 95 + r.below(11),
                    qty: 1 + r.below(5),
                };
                if let Outcome::Reserved { order, .. } = self.ledgers[shard].apply(&c) {
                    self.known.push((account, order));
                }
            }
            _ if !self.known.is_empty() => {
                let (account, order) = self.known[r.below(self.known.len() as u64) as usize];
                self.ledgers[order_shard(order) as usize].apply(&Cmd::Cancel { account, order });
            }
            _ => {}
        }
    }

    fn check(&self) {
        for (i, l) in self.ledgers.iter().enumerate() {
            if let Err(e) = l.check() {
                panic!("ledger shard {i}: {e}");
            }
        }
    }

    /// Pump until every outbox is accepted and every stream is settled.
    fn drain(&mut self, w: &World) {
        for _ in 0..400 {
            self.pump_forwarders(w);
            w.settle();
            let node = w.live()[0];
            let v = w.nodes[node].view.as_ref().unwrap().clone();
            self.pump_settlers(&v);
            let done = (0..self.ledgers.len()).all(|s| {
                let l = &self.ledgers[s];
                l.outbox().1.count() == 0
                    && (0..PARTITIONS).all(|p| {
                        let source = BASE + p;
                        let end = v.end(source).unwrap();
                        self.scan
                            .get(&(s, source))
                            .copied()
                            .unwrap_or(0)
                            .max(l.high_water(source).unwrap_or(0))
                            >= end
                    })
            });
            if done {
                return;
            }
            for f in &mut self.forwarders {
                f.rewind();
            }
        }
        panic!("exchange did not drain");
    }

    fn assert_conserved(&self) {
        for asset in [BTC, USD] {
            let (mut have, mut wd, mut dep, mut inn, mut out) = (0u128, 0u128, 0u128, 0u128, 0u128);
            for l in &self.ledgers {
                have += l.holdings(asset);
                let (d, x) = l.external_net(asset);
                dep += d;
                wd += x;
                let (i, o) = l.settled_flow(asset);
                inn += i;
                out += o;
            }
            assert_eq!(
                inn, out,
                "asset {asset}: trades move exactly what they take"
            );
            assert_eq!(have + wd, dep, "asset {asset}: money is conserved");
        }
    }
}

#[test]
fn the_ledger_runs_against_the_real_matching_group_through_a_leader_crash() {
    let mut w = World::new("e2e", 3, 40);
    w.wait_leader();
    let mut ex = Exchange::new(3);
    let mut r = Rng(123);
    let run = |ex: &mut Exchange, w: &World, steps: usize, r: &mut Rng| {
        for step in 0..steps {
            ex.client_op(r);
            if step % 3 == 0 {
                ex.pump_forwarders(w);
            }
            if step % 5 == 0 {
                let v = w.nodes[w.live()[0]].view.as_ref().unwrap().clone();
                ex.pump_settlers(&v);
            }
            ex.check();
        }
    };
    run(&mut ex, &w, 400, &mut r);
    // the leader dies with orders in flight and the relay processes restart
    let l = w.wait_leader();
    w.crash(l);
    ex.restart_relays();
    run(&mut ex, &w, 300, &mut r);
    w.start(l, 40);
    ex.restart_relays();
    run(&mut ex, &w, 200, &mut r);
    ex.drain(&w);
    // cancel everything still open and drain again
    let open: Vec<_> = ex.known.clone();
    for (account, order) in open {
        ex.ledgers[order_shard(order) as usize].apply(&Cmd::Cancel { account, order });
    }
    ex.drain(&w);
    ex.check();
    for l in &ex.ledgers {
        assert_eq!(l.anomalies(), 0);
    }
    ex.assert_conserved();
    w.settle();
    assert_same_streams(&w, BASE..BASE + PARTITIONS);
    let v = w.nodes[w.live()[0]].view.as_ref().unwrap();
    let open_in_ledgers: usize = ex.ledgers.iter().map(|l| l.open_orders()).sum();
    assert_eq!(
        v.stats(),
        (0, open_in_ledgers),
        "tracked orders are the ledgers' open orders"
    );
    let trades: usize = (0..PARTITIONS)
        .map(|p| {
            w.stream(w.live()[0], BASE + p)
                .iter()
                .filter(|e| matches!(e.1, Event::Trade { .. }))
                .count()
        })
        .sum();
    assert!(trades > 50, "the run must actually trade: {trades}");
}
