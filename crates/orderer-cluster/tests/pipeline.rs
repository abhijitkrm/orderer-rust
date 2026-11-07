//! The real matching pipeline as the replicated state machine, over real
//! TCP and files: replicas must emit byte-identical events and reach
//! identical state, and match a plain, unreplicated pipeline fed the same
//! commands.

mod common;

use common::{free_addrs, temp};
use orderer::{Collect, CollectHandle, EventDigest, Initial, PartitionMap, Pipeline};
use orderer_cluster::pipeline_sm::{PipelineSm, Tap};
use orderer_cluster::sha256::sha256;
use orderer_cluster::{ProposeError, Replica, ReplicaConfig, ReplicaHandle};
use orderer_consensus::{Config, NodeId, Role};
use orderer_core::*;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const PARTITIONS: u32 = 2;

struct Gen {
    x: u64,
    next_id: u64,
    live: Vec<(u32, u64)>,
}

impl Gen {
    fn new(seed: u64) -> Gen {
        Gen {
            x: seed | 1,
            next_id: 1,
            live: Vec::new(),
        }
    }
    fn rnd(&mut self, n: u64) -> u64 {
        self.x ^= self.x << 13;
        self.x ^= self.x >> 7;
        self.x ^= self.x << 17;
        self.x % n
    }
    fn batch(&mut self, n: usize) -> Vec<(Symbol, Command)> {
        (0..n)
            .map(|_| {
                let r = self.rnd(100);
                if r < 70 || self.live.is_empty() {
                    let sym = self.rnd(8) as u32;
                    let id = self.next_id;
                    self.next_id += 1;
                    self.live.push((sym, id));
                    let side = if self.rnd(2) == 0 {
                        Side::Bid
                    } else {
                        Side::Ask
                    };
                    let tif = if self.rnd(5) == 0 { Tif::Ioc } else { Tif::Gtc };
                    (
                        sym,
                        Command::new(
                            id,
                            side,
                            10_000 + self.rnd(40) as i64 - 20,
                            1 + self.rnd(20),
                            tif,
                        ),
                    )
                } else if r < 90 {
                    let k = self.rnd(self.live.len() as u64) as usize;
                    let (sym, id) = self.live.swap_remove(k);
                    (sym, Command::Cancel { order_id: id })
                } else {
                    let k = self.rnd(self.live.len() as u64) as usize;
                    let (sym, id) = self.live[k];
                    (
                        sym,
                        Command::Replace {
                            order_id: id,
                            price: 10_000 + self.rnd(40) as i64 - 20,
                            qty: 1 + self.rnd(20),
                        },
                    )
                }
            })
            .collect()
    }
}

fn build(
    collect: Collect,
    digest: EventDigest,
    init: Option<Initial<FifoCore>>,
) -> Pipeline<FifoCore> {
    let mut b = Pipeline::<FifoCore>::builder()
        .partitions(PARTITIONS)
        .egress(collect)
        .egress(digest);
    if let Some(i) = init {
        b = b.initial(i);
    }
    b.build().unwrap()
}

/// What a plain pipeline produces for `cmds`: (event listing, state digest).
fn reference(cmds: &[(Symbol, Command)]) -> (Vec<u8>, [u8; 32]) {
    let (c, h) = Collect::new(false);
    let (d, _) = EventDigest::new(PARTITIONS, &[]);
    let p = build(c, d, None);
    p.publish_batch(cmds).unwrap();
    p.drain().unwrap();
    let snap = p.snapshot().unwrap();
    let mut enc = snap.iseq.to_le_bytes().to_vec();
    enc.extend_from_slice(&snap.partitions.to_le_bytes());
    enc.extend_from_slice(snap.body.as_bytes());
    (h.listing(), sha256(&enc))
}

struct Node {
    replica: Option<Replica>,
    tap: Tap,
    collect: Arc<Mutex<Option<CollectHandle>>>,
}

struct World {
    dir: std::path::PathBuf,
    members: Vec<(NodeId, std::net::SocketAddr)>,
    nodes: Vec<Node>,
}

impl World {
    fn new(name: &str, n: usize) -> World {
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
                    tap: Arc::default(),
                    collect: Arc::default(),
                })
                .collect(),
        };
        for i in 0..n {
            w.start(i);
        }
        w
    }

    fn start(&mut self, i: usize) {
        let mut cfg = ReplicaConfig::new(
            i as NodeId,
            self.members.clone(),
            self.dir.join(format!("n{i}")),
        );
        cfg.tick = Duration::from_millis(5);
        cfg.consensus = Config {
            election_ticks: 20,
            heartbeat_ticks: 2,
            max_batch: 256,
            ..Config::default()
        };
        cfg.fsync = false;
        cfg.compact_every = 25;
        cfg.digest_every = 20;
        cfg.propose_timeout = Duration::from_secs(5);
        let node = &mut self.nodes[i];
        node.tap = Arc::default();
        let collect_slot = node.collect.clone();
        let sm = PipelineSm::<FifoCore>::new(
            BookConfig::default(),
            PartitionMap::hash(PARTITIONS).unwrap(),
            move |init, digest| {
                let (c, h) = Collect::new(false);
                *collect_slot.lock().unwrap() = Some(h);
                build(c, digest, init)
            },
        )
        .with_tap(node.tap.clone());
        node.replica = Some(Replica::start(cfg, sm).unwrap());
    }

    fn handles(&self) -> Vec<Option<ReplicaHandle>> {
        self.nodes
            .iter()
            .map(|n| n.replica.as_ref().map(|r| r.handle()))
            .collect()
    }

    fn leader(&self) -> Option<usize> {
        self.handles().iter().enumerate().find_map(|(i, h)| {
            h.as_ref()
                .filter(|h| h.status().role == Role::Leader)
                .map(|_| i)
        })
    }

    fn wait_leader(&self) -> usize {
        let end = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(l) = self.leader() {
                return l;
            }
            assert!(Instant::now() < end, "no leader");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Commit one batch; returns whether it was acknowledged.
    fn submit(&self, cmds: &[(Symbol, Command)]) -> bool {
        let end = Instant::now() + Duration::from_secs(15);
        loop {
            let l = self.wait_leader();
            match self.handles()[l].as_ref().unwrap().propose_commands(cmds) {
                Ok(_) => return true,
                Err(ProposeError::NotLeader(_))
                | Err(ProposeError::Busy)
                | Err(ProposeError::Stopped) => {
                    assert!(Instant::now() < end, "could not submit");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => return false, // unknown: not retried
            }
        }
    }

    fn crash(&mut self, i: usize) {
        self.nodes[i].replica.take().unwrap().crash();
    }

    /// Wait until every running node has applied the same index, then return
    /// each node's `(applied, digest)`.
    fn settle(&self) -> Vec<(usize, u64, [u8; 32])> {
        let end = Instant::now() + Duration::from_secs(30);
        loop {
            let hs = self.handles();
            let live: Vec<(usize, &ReplicaHandle)> = hs
                .iter()
                .enumerate()
                .filter_map(|(i, h)| h.as_ref().map(|h| (i, h)))
                .collect();
            let st: Vec<_> = live.iter().map(|(_, h)| h.status()).collect();
            let max = st.iter().map(|s| s.commit).max().unwrap();
            if st.iter().all(|s| s.applied == max && s.halted.is_none()) {
                let hashes: Vec<_> = live
                    .iter()
                    .map(|(i, h)| h.state_hash().map(|(a, d)| (*i, a, d)))
                    .collect();
                if hashes.iter().all(|h| h.is_some()) {
                    let hashes: Vec<_> = hashes.into_iter().flatten().collect();
                    if hashes.iter().all(|h| h.1 == hashes[0].1) {
                        return hashes;
                    }
                }
            }
            assert!(Instant::now() < end, "replicas did not settle");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for World {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            if let Some(r) = n.replica.take() {
                r.crash();
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn flatten(tap: &Tap) -> Vec<(Symbol, Command)> {
    tap.lock()
        .unwrap()
        .iter()
        .flat_map(|(_, c)| c.clone())
        .collect()
}

#[test]
fn replicas_match_each_other_and_a_plain_pipeline() {
    let w = World::new("pipe-basic", 3);
    w.wait_leader();
    let mut g = Gen::new(7);
    for _ in 0..120 {
        assert!(w.submit(&g.batch(100)));
    }
    let hashes = w.settle();
    assert!(
        hashes.iter().all(|h| h.2 == hashes[0].2),
        "replica state differs"
    );

    let cmds = flatten(&w.nodes[0].tap);
    assert_eq!(cmds.len(), 12_000);
    let (listing, digest) = reference(&cmds);
    assert_eq!(
        hashes[0].2, digest,
        "replicated state differs from a plain pipeline"
    );
    let full = w.nodes[0].tap.lock().unwrap().len();
    let mut compared = 0;
    for n in &w.nodes {
        // A node that restored a snapshot only saw later events; compare the
        // streams of nodes that applied the whole history.
        let applied = n.tap.lock().unwrap().len();
        if applied == full {
            let got = n.collect.lock().unwrap().as_ref().unwrap().listing();
            assert_eq!(got, listing, "event stream differs from a plain pipeline");
            compared += 1;
        }
    }
    assert!(compared >= 2, "too few full event streams to compare");
}

#[test]
fn follower_restore_and_leader_failover_preserve_state_and_events() {
    let mut w = World::new("pipe-chaos", 3);
    let l0 = w.wait_leader();
    let follower = (0..3).find(|&i| i != l0).unwrap();
    let steady = (0..3).find(|&i| i != l0 && i != follower).unwrap();
    let mut g = Gen::new(99);
    let mut acked_first_ids = Vec::new();
    let mut go = |w: &World, n: usize| {
        for _ in 0..n {
            let b = g.batch(50);
            let first = match b[0].1 {
                Command::New { order_id, .. }
                | Command::Cancel { order_id }
                | Command::Replace { order_id, .. } => order_id,
            };
            if w.submit(&b) {
                acked_first_ids.push((b[0].0, first, b.len()));
            }
        }
    };

    go(&w, 30);
    // A follower dies; the rest move on far enough to compact their logs.
    w.crash(follower);
    go(&w, 80);
    w.start(follower); // rejoins via snapshot + log
    go(&w, 30);
    // The leader dies; a new one takes over under continued traffic.
    w.crash(l0);
    go(&w, 40);
    w.start(l0); // recovers from its WAL and snapshot
    go(&w, 20);

    let hashes = w.settle();
    assert_eq!(hashes.len(), 3);
    assert!(
        hashes.iter().all(|h| h.2 == hashes[0].2),
        "replica state diverged: {hashes:?}"
    );

    // `steady` applied everything in order and never restarted: its tap is
    // the committed history. Every acknowledged batch is in it.
    let cmds = flatten(&w.nodes[steady].tap);
    for (sym, first, len) in &acked_first_ids {
        let pos = cmds.iter().position(|(s, c)| {
            s == sym
                && matches!(c, Command::New { order_id, .. } | Command::Cancel { order_id } | Command::Replace { order_id, .. } if order_id == first)
        });
        assert!(
            pos.is_some(),
            "acknowledged batch starting at order {first} (len {len}) is missing"
        );
    }
    let (listing, digest) = reference(&cmds);
    assert_eq!(
        hashes[0].2, digest,
        "state differs from a plain pipeline fed the committed history"
    );
    let got = w.nodes[steady]
        .collect
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .listing();
    assert_eq!(
        got, listing,
        "steady node's events differ from a plain pipeline"
    );
    for i in 0..3 {
        assert_eq!(w.handles()[i].as_ref().unwrap().status().halted, None);
    }
}
