//! Two independent replicated groups behind a sharded client: per-symbol
//! order is preserved, groups hold only their own symbols, and an outage in
//! one group does not stop the other.

mod common;

use common::*;
use orderer_cluster::client::{Client, ClientServer};
use orderer_cluster::codec::decode_batch;
use orderer_cluster::shard::{ShardMap, ShardedClient};
use orderer_core::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

struct Group {
    c: Cluster,
    addrs: Vec<SocketAddr>,
    servers: Vec<Option<ClientServer>>,
}

impl Group {
    fn new(name: &str) -> Group {
        let c = Cluster::new(name, 3, |_| {});
        c.wait_leader(Duration::from_secs(5));
        let addrs = free_addrs(3);
        let servers = (0..3)
            .map(|i| Some(ClientServer::start(addrs[i], c.handle(i).unwrap()).unwrap()))
            .collect();
        Group { c, addrs, servers }
    }

    fn client(&self) -> Client {
        Client::new(
            self.addrs
                .iter()
                .copied()
                .enumerate()
                .map(|(i, a)| (i as u8, a))
                .collect(),
        )
    }

    fn crash(&mut self, i: usize) {
        self.servers[i].take().unwrap().shutdown();
        self.c.crash(i);
    }

    fn restart(&mut self, i: usize) {
        self.c.start(i);
        self.servers[i] =
            Some(ClientServer::start(self.addrs[i], self.c.handle(i).unwrap()).unwrap());
    }

    /// Everything node `i` applied, in order, as commands.
    fn applied(&self, i: usize) -> Vec<(Symbol, Command)> {
        self.c.recorders[i]
            .payloads()
            .iter()
            .flat_map(|p| decode_batch(p).unwrap())
            .collect()
    }
}

fn order_id(c: &Command) -> u64 {
    match c {
        Command::New { order_id, .. }
        | Command::Cancel { order_id }
        | Command::Replace { order_id, .. } => *order_id,
    }
}

struct Feed {
    next: u64,
}

impl Feed {
    /// `n` commands over the given symbols, ids strictly increasing.
    fn batch(&mut self, symbols: &[u32], n: usize) -> Vec<(Symbol, Command)> {
        (0..n)
            .map(|k| {
                self.next += 1;
                (
                    symbols[k % symbols.len()],
                    Command::new(self.next, Side::Bid, 10_000, 1, Tif::Gtc),
                )
            })
            .collect()
    }
}

fn assert_per_symbol_order(applied: &[(Symbol, Command)]) {
    let mut last: HashMap<Symbol, u64> = HashMap::new();
    for (sym, c) in applied {
        let id = order_id(c);
        if let Some(prev) = last.insert(*sym, id) {
            assert!(id > prev, "symbol {sym}: order {id} applied after {prev}");
        }
    }
}

#[test]
fn groups_hold_only_their_symbols_in_per_symbol_order() {
    let g0 = Group::new("shard-a0");
    let g1 = Group::new("shard-a1");
    let map = ShardMap::new(2);
    let mut client = ShardedClient::new(vec![g0.client(), g1.client()]);
    let symbols: Vec<u32> = (0..24).collect();
    let mut feed = Feed { next: 0 };
    let mut submitted: [Vec<u64>; 2] = [Vec::new(), Vec::new()];
    for _ in 0..60 {
        let batch = feed.batch(&symbols, 40);
        for (g, r) in client.propose(&batch).into_iter().enumerate() {
            if let Some(r) = r {
                r.unwrap_or_else(|e| panic!("group {g}: {e}"));
            }
        }
        for (sym, c) in &batch {
            submitted[map.group_of(*sym) as usize].push(order_id(c));
        }
    }
    g0.c.wait_converged(Duration::from_secs(10));
    g1.c.wait_converged(Duration::from_secs(10));
    for (g, grp) in [&g0, &g1].into_iter().enumerate() {
        let applied = grp.applied(0);
        assert!(
            applied.iter().all(|(s, _)| map.group_of(*s) as usize == g),
            "group {g} holds a foreign symbol"
        );
        assert_per_symbol_order(&applied);
        let mut ids: Vec<u64> = applied.iter().map(|(_, c)| order_id(c)).collect();
        ids.sort_unstable();
        let mut want = submitted[g].clone();
        want.sort_unstable();
        assert_eq!(
            ids, want,
            "group {g}: committed set differs from what was submitted"
        );
    }
    // Both groups did real work.
    assert!(g0.applied(0).len() > 500 && g1.applied(0).len() > 500);
}

#[test]
fn an_outage_in_one_group_does_not_stop_the_other() {
    let mut g0 = Group::new("shard-b0");
    let g1 = Group::new("shard-b1");
    let map = ShardMap::new(2);
    let symbols0: Vec<u32> = (0..200).filter(|&s| map.group_of(s) == 0).take(8).collect();
    let symbols1: Vec<u32> = (0..200).filter(|&s| map.group_of(s) == 1).take(8).collect();
    let mut c0 = g0.client();
    let mut c1 = g1.client();
    let mut feed = Feed { next: 0 };
    let enc = |cmds: &[(Symbol, Command)]| {
        let mut p = Vec::new();
        orderer_cluster::codec::encode_batch(cmds, &mut p);
        p
    };
    // Warm both.
    for _ in 0..5 {
        c0.propose(&enc(&feed.batch(&symbols0, 10))).unwrap();
        c1.propose(&enc(&feed.batch(&symbols1, 10))).unwrap();
    }

    // Group 0 loses its leader.
    let l0 = g0.c.leader().unwrap();
    g0.crash(l0);
    let start = Instant::now();
    let mut g1_acked = 0;
    let mut slowest = Duration::ZERO;
    while start.elapsed() < Duration::from_millis(600) {
        let t = Instant::now();
        c1.propose(&enc(&feed.batch(&symbols1, 10)))
            .expect("group 1 must keep committing during group 0's outage");
        slowest = slowest.max(t.elapsed());
        g1_acked += 1;
    }
    assert!(
        g1_acked >= 10,
        "group 1 only managed {g1_acked} batches during the outage"
    );
    assert!(
        slowest < Duration::from_millis(500),
        "group 1 stalled for {slowest:?}"
    );

    // Group 0 recovers and carries on.
    g0.restart(l0);
    for _ in 0..10 {
        // Retried across the election; ids are fresh so duplicates cannot hide.
        c0.propose(&enc(&feed.batch(&symbols0, 10))).ok();
    }
    g0.c.wait_converged(Duration::from_secs(20));
    g1.c.wait_converged(Duration::from_secs(10));
    for grp in [&g0, &g1] {
        assert_per_symbol_order(&grp.applied(0));
    }
    assert!(
        g0.applied(0).len() >= 50 + 10,
        "group 0 lost its warm-up batches"
    );
}
