//! clusterbench — throughput of the replicated path, in one process over
//! loopback TCP.
//!
//!   clusterbench [--nodes N] [--groups G] [--sm noop|pipeline] [--partitions P]
//!                [--batch CMDS] [--producers K] [--window W]
//!                [--cmds TOTAL] [--fsync on|off] [--dir PATH]
//!
//! G independent replicated groups of N nodes each (symbols sharded between
//! them, `shard.rs`). For every group, K producers keep W batches of CMDS
//! commands in flight against its leader. Reports commands
//! committed-and-applied per second in total, the log payload per second,
//! and mean batch ack latency.
//!
//! Everything runs in one process on one machine, so the groups share its
//! cores, its disk and its loopback: use the numbers as a floor for what
//! dedicated hardware per replica would do, and read the per-group figure
//! next to the aggregate.

use orderer::{Metrics, PartitionMap, Pipeline};
use orderer_cluster::codec::{encode_batch, CMD_BYTES};
use orderer_cluster::pipeline_sm::PipelineSm;
use orderer_cluster::shard::ShardMap;
use orderer_cluster::{ProposeError, Replica, ReplicaConfig, StateMachine};
use orderer_consensus::{Config, Entry, Index, NodeId, Role};
use orderer_core::*;
use std::net::TcpListener;
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Noop;
impl StateMachine for Noop {
    fn apply(&mut self, _: Index, _: &Entry) {}
    fn snapshot(&mut self, _: Index) -> Vec<u8> {
        Vec::new()
    }
    fn restore(&mut self, _: Index, _: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

/// Where this run keeps its WALs; removed on every exit path.
static WORKDIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

fn die(msg: &str) -> ! {
    eprintln!("{msg}");
    if let Some(d) = WORKDIR.get() {
        let _ = std::fs::remove_dir_all(d);
    }
    exit(2)
}

fn main() {
    let (mut nodes, mut groups, mut sm, mut partitions) =
        (3usize, 1usize, "noop".to_string(), 2u32);
    let (mut batch, mut producers, mut window) = (1000usize, 2usize, 8usize);
    let (mut total, mut fsync, mut dir) = (20_000_000u64, true, String::new());
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        let num = |s: String, n: &str| {
            s.parse::<u64>()
                .unwrap_or_else(|_| die(&format!("{n} wants a number")))
        };
        match a.as_str() {
            "--nodes" => nodes = num(v("--nodes"), "--nodes") as usize,
            "--groups" => groups = num(v("--groups"), "--groups") as usize,
            "--sm" => sm = v("--sm"),
            "--partitions" => partitions = num(v("--partitions"), "--partitions") as u32,
            "--batch" => batch = num(v("--batch"), "--batch") as usize,
            "--producers" => producers = num(v("--producers"), "--producers") as usize,
            "--window" => window = num(v("--window"), "--window") as usize,
            "--cmds" => total = num(v("--cmds"), "--cmds"),
            "--fsync" => fsync = v("--fsync") == "on",
            "--dir" => dir = v("--dir"),
            other => die(&format!("unknown argument {other}")),
        }
    }
    let dir = if dir.is_empty() {
        std::env::temp_dir().join(format!("clusterbench-{}", std::process::id()))
    } else {
        dir.into()
    };
    let _ = std::fs::remove_dir_all(&dir);
    let _ = WORKDIR.set(dir.clone());

    // Start every group.
    let mut all = Vec::new();
    for g in 0..groups {
        let members: Vec<(NodeId, _)> = {
            let ls: Vec<_> = (0..nodes)
                .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
                .collect();
            ls.iter()
                .enumerate()
                .map(|(i, l)| (i as NodeId, l.local_addr().unwrap()))
                .collect()
        };
        let mut replicas = Vec::new();
        for i in 0..nodes {
            let mut cfg =
                ReplicaConfig::new(i as NodeId, members.clone(), dir.join(format!("g{g}n{i}")));
            cfg.cluster_id = 1000 + g as u64;
            cfg.consensus = Config {
                election_ticks: 30,
                heartbeat_ticks: 3,
                max_batch: 512,
                ..Config::default()
            };
            cfg.fsync = fsync;
            cfg.max_pending = 1 << 20;
            cfg.propose_timeout = Duration::from_secs(60);
            let r = match sm.as_str() {
                "noop" => Replica::start(cfg, Noop),
                "pipeline" => Replica::start(
                    cfg,
                    PipelineSm::<FifoCore>::new(
                        BookConfig::default(),
                        PartitionMap::hash(partitions).unwrap(),
                        move |init| {
                            // Counting egress: nothing accumulates in memory.
                            let (c, _h) = Metrics::new(0);
                            let mut b = Pipeline::<FifoCore>::builder()
                                .partitions(partitions)
                                .egress(c)
                                .timestamps(false);
                            if let Some(i) = init {
                                b = b.initial(i);
                            }
                            b.build().unwrap()
                        },
                    ),
                ),
                other => die(&format!("--sm {other}")),
            };
            replicas.push(r.unwrap_or_else(|e| die(&format!("start: {e}"))));
        }
        all.push(replicas);
    }
    let end = Instant::now() + Duration::from_secs(20);
    let leaders: Vec<usize> = all
        .iter()
        .map(|rs| loop {
            if let Some(i) = rs
                .iter()
                .position(|r| r.handle().status().role == Role::Leader)
            {
                break i;
            }
            if Instant::now() > end {
                die("no leader");
            }
            std::thread::sleep(Duration::from_millis(10));
        })
        .collect();

    let map = ShardMap::new(groups as u32);
    let per_producer = total / (groups * producers) as u64;
    let acked = Arc::new(AtomicU64::new(0));
    let lat_ns = Arc::new(AtomicU64::new(0));
    let batches = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let mut threads = Vec::new();
    for g in 0..groups {
        let lh = all[g][leaders[g]].handle();
        // Symbols this group owns.
        let syms: Arc<Vec<u32>> = Arc::new(
            (0..4096u32)
                .filter(|&s| map.group_of(s) as usize == g)
                .take(64)
                .collect(),
        );
        for p in 0..producers {
            let (lh, acked, lat_ns, batches, syms) = (
                lh.clone(),
                acked.clone(),
                lat_ns.clone(),
                batches.clone(),
                syms.clone(),
            );
            threads.push(std::thread::spawn(move || {
                let mut next_id = ((g * 64 + p + 1) as u64) << 36;
                let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ (g * 64 + p) as u64;
                let mut sent = 0u64;
                let mut inflight = std::collections::VecDeque::new();
                let mut payload = Vec::new();
                let mut cmds = Vec::with_capacity(batch);
                while sent < per_producer || !inflight.is_empty() {
                    while sent < per_producer && inflight.len() < window {
                        cmds.clear();
                        for _ in 0..batch {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            let sym = syms[(x % syms.len() as u64) as usize];
                            next_id += 1;
                            let side = if x & 1 == 0 { Side::Bid } else { Side::Ask };
                            let price = 10_000 + ((x >> 8) % 8) as i64 - 4;
                            cmds.push((
                                sym,
                                Command::new(next_id, side, price, 1 + (x >> 20) % 10, Tif::Gtc),
                            ));
                        }
                        payload.clear();
                        encode_batch(&cmds, &mut payload);
                        inflight.push_back((Instant::now(), lh.propose_async(payload.clone())));
                        sent += batch as u64;
                    }
                    let Some((t, rx)) = inflight.pop_front() else {
                        break;
                    };
                    match rx.recv() {
                        Ok(Ok(_)) => {
                            acked.fetch_add(batch as u64, Ordering::Relaxed);
                            lat_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            batches.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(Err(ProposeError::Busy)) => std::thread::sleep(Duration::from_millis(1)),
                        other => die(&format!("proposal failed: {other:?}")),
                    }
                }
            }));
        }
    }
    for t in threads {
        t.join().unwrap();
    }
    let secs = t0.elapsed().as_secs_f64();
    let done = acked.load(Ordering::Relaxed);
    let nb = batches.load(Ordering::Relaxed).max(1);
    // Every node of every group must apply what its group committed.
    let end = Instant::now() + Duration::from_secs(180);
    for (g, rs) in all.iter().enumerate() {
        let want = rs[leaders[g]].handle().status().commit;
        while rs.iter().any(|r| r.handle().status().applied < want) {
            if Instant::now() > end {
                die("followers did not catch up");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    let applied_secs = t0.elapsed().as_secs_f64();
    println!(
        "groups={groups} nodes={nodes} sm={sm} batch={batch} producers={producers}/group window={window} fsync={}: \
         {:.2}M cmds/s acked ({:.2}M per group), {:.2}M applied everywhere, {:.0} MB/s log payload, \
         mean batch latency {:.2} ms, {} cmds in {:.2}s",
        if fsync { "on" } else { "off" },
        done as f64 / secs / 1e6,
        done as f64 / secs / 1e6 / groups as f64,
        done as f64 / applied_secs / 1e6,
        done as f64 * CMD_BYTES as f64 / 1e6 / secs,
        lat_ns.load(Ordering::Relaxed) as f64 / nb as f64 / 1e6,
        done,
        secs
    );
    drop(all);
    let _ = std::fs::remove_dir_all(&dir);
}
