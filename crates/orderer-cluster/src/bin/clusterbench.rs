//! clusterbench — throughput of the replicated path, in one process over
//! loopback TCP.
//!
//!   clusterbench [--nodes N] [--groups G] [--sm noop|pipeline] [--partitions P]
//!                [--batch CMDS] [--producers K] [--window W]
//!                [--cmds TOTAL] [--fsync on|off|relaxed[:MS]] [--dir PATH]
//!                [--rate CMDS_PER_SEC | --sweep R1,R2,...] [--secs S] [--warmup S]
//!
//! G independent replicated groups of N nodes each (symbols sharded between
//! them, `shard.rs`). For every group, K producers keep W batches of CMDS
//! commands in flight against its leader. Reports commands
//! committed-and-applied per second in total, the log payload per second,
//! and mean batch ack latency.
//!
//! **Open-loop mode** (`--rate` or `--sweep`) measures latency against offered
//! load. Closed-loop mode (the default) keeps W batches in flight and reports
//! how fast the system drains them, which hides queueing: a slow system just
//! slows the producers down. Open-loop mode sends on a fixed schedule whether
//! or not earlier batches were acknowledged, and measures every batch from the
//! time it was *due* (not from when the generator got round to sending it), so
//! a stall shows up as latency for everything that should have been sent during
//! it (no coordinated omission). One row per offered rate: achieved throughput
//! and ack-latency percentiles (leader-side commit ack, in microseconds). Use a
//! small `--batch` (1-10) for latency work; `--sweep` walks up through the knee
//! where latency leaves the flat part of the curve.
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
    let mut fsync_interval: Option<Duration> = None;
    let (mut rates, mut secs, mut warmup) = (Vec::<f64>::new(), 5.0f64, 1.0f64);
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
        let fnum = |s: String, n: &str| {
            s.parse::<f64>()
                .ok()
                .filter(|x| *x > 0.0)
                .unwrap_or_else(|| die(&format!("{n} wants a positive number")))
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
            "--fsync" => {
                let m = v("--fsync");
                match m.as_str() {
                    "on" | "off" => fsync = m == "on",
                    _ => match m.strip_prefix("relaxed") {
                        Some("") => {
                            fsync = true;
                            fsync_interval = Some(Duration::from_millis(5));
                        }
                        Some(ms) => {
                            let ms = ms.strip_prefix(':').and_then(|x| x.parse::<u64>().ok());
                            fsync = true;
                            fsync_interval =
                                Some(Duration::from_millis(ms.unwrap_or_else(|| {
                                    die("--fsync relaxed:MS wants milliseconds")
                                })));
                        }
                        None => die("--fsync wants on, off or relaxed[:MS]"),
                    },
                }
            }
            "--dir" => dir = v("--dir"),
            "--rate" => rates = vec![fnum(v("--rate"), "--rate")],
            "--sweep" => {
                rates = v("--sweep")
                    .split(',')
                    .map(|r| fnum(r.to_string(), "--sweep"))
                    .collect()
            }
            "--secs" => secs = fnum(v("--secs"), "--secs"),
            "--warmup" => warmup = fnum(v("--warmup"), "--warmup"),
            other => die(&format!("unknown argument {other}")),
        }
    }
    let fsync_label = match (fsync, fsync_interval) {
        (true, Some(d)) => format!("relaxed({}ms)", d.as_millis()),
        (true, None) => "on".to_string(),
        (false, _) => "off".to_string(),
    };
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
            cfg.fsync_interval = fsync_interval;
            cfg.max_pending = 1 << 20;
            cfg.propose_timeout = Duration::from_secs(60);
            let r = match sm.as_str() {
                "noop" => Replica::start(cfg, Noop),
                "pipeline" => Replica::start(
                    cfg,
                    PipelineSm::<FifoCore>::new(
                        BookConfig::default(),
                        PartitionMap::hash(partitions).unwrap(),
                        move |init, digest| {
                            // Counting egress: nothing accumulates in memory.
                            let (c, _h) = Metrics::new(0);
                            let mut b = Pipeline::<FifoCore>::builder()
                                .partitions(partitions)
                                .egress(c)
                                .egress(digest)
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

    if !rates.is_empty() {
        open_loop(
            &all,
            &leaders,
            groups,
            producers,
            batch,
            &sm,
            &fsync_label,
            &rates,
            secs,
            warmup,
        );
        drop(all);
        let _ = std::fs::remove_dir_all(&dir);
        return;
    }

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
                let mut inflight: std::collections::VecDeque<(
                    Instant,
                    std::sync::mpsc::Receiver<Result<_, ProposeError>>,
                )> = std::collections::VecDeque::new();
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
        fsync_label,
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

/// Exact percentile (microseconds) of sorted nanosecond samples.
fn pct_us(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let i = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[i] as f64 / 1e3
}

struct Measured {
    lats: Vec<u64>,
    /// (due offset from the start of the measured window, latency) of acks slower than STALL_NS.
    slow: Vec<(u64, u64)>,
    acked: u64,
    busy: u64,
    other: u64,
}

/// Group slow acks into stalls. A stall delays every batch due while it lasts,
/// so it shows up as a run of slow acks whose due times are close together.
/// Returns (first due, last due, worst latency, count) per stall; all in ns.
fn group_stalls(slow: &mut [(u64, u64)]) -> Vec<(u64, u64, u64, usize)> {
    slow.sort_unstable();
    let mut stalls: Vec<(u64, u64, u64, usize)> = Vec::new();
    for &(at, lat) in slow.iter() {
        match stalls.last_mut() {
            Some(st) if at <= st.1 + 5_000_000 => {
                st.1 = st.1.max(at);
                st.2 = st.2.max(lat);
                st.3 += 1;
            }
            _ => stalls.push((at, at, lat, 1)),
        }
    }
    stalls
}

/// An ack slower than this is part of a "stall" in the report below the table row.
const STALL_NS: u64 = 10_000_000;

/// Offer `rate` commands/s in total, split evenly over every producer of every
/// group, on a fixed schedule, and report the ack-latency distribution.
#[allow(clippy::too_many_arguments)]
fn open_loop(
    all: &[Vec<Replica>],
    leaders: &[usize],
    groups: usize,
    producers: usize,
    batch: usize,
    sm: &str,
    fsync: &str,
    rates: &[f64],
    secs: f64,
    warmup: f64,
) {
    let map = ShardMap::new(groups as u32);
    println!(
        "open loop: groups={groups} nodes={} sm={sm} batch={batch} producers={producers}/group fsync={} \
         warmup={warmup}s measured={secs}s; latency = ack time minus time the batch was due",
        all[0].len(),
        fsync,
    );
    println!(
        "{:>12} {:>12} {:>9} {:>9} {:>9} {:>9} {:>9} {:>10} {:>10} {:>7} {:>7} {:>9}",
        "offered/s",
        "achieved/s",
        "p50 us",
        "p90 us",
        "p99 us",
        "p99.9 us",
        "p99.99 us",
        "max us",
        "batches",
        "busy",
        "other",
        "genlag us"
    );
    for (ri, &rate) in rates.iter().enumerate() {
        let per_producer_batches = rate / batch as f64 / (groups * producers) as f64;
        let interval_ns = 1e9 / per_producer_batches;
        let t0 = Instant::now() + Duration::from_millis(100);
        let meas_start = t0 + Duration::from_secs_f64(warmup);
        let meas_end = meas_start + Duration::from_secs_f64(secs);
        let total_n = (per_producer_batches * (warmup + secs)).ceil() as u64;
        let mut producers_h = Vec::new();
        for g in 0..groups {
            let lh = all[g][leaders[g]].handle();
            let syms: Arc<Vec<u32>> = Arc::new(
                (0..4096u32)
                    .filter(|&s| map.group_of(s) as usize == g)
                    .take(64)
                    .collect(),
            );
            for p in 0..producers {
                let (lh, syms) = (lh.clone(), syms.clone());
                let uid = (ri * 1000 + g * 64 + p + 1) as u64;
                // One thread per producer does both jobs and never blocks: it
                // sends when a batch is due and polls the oldest in-flight ack.
                // A sleeping sender plus a blocking collector would add two
                // park/unpark wake-ups to every measured latency.
                producers_h.push(std::thread::spawn(move || {
                    let mut next_id = uid << 36;
                    let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ uid;
                    let mut cmds = Vec::with_capacity(batch);
                    let mut payload = Vec::new();
                    let mut inflight: std::collections::VecDeque<(
                        Instant,
                        std::sync::mpsc::Receiver<Result<_, ProposeError>>,
                    )> = std::collections::VecDeque::new();
                    let mut m = Measured {
                        lats: Vec::new(),
                        slow: Vec::new(),
                        acked: 0,
                        busy: 0,
                        other: 0,
                    };
                    let mut max_lag = 0u64;
                    let mut n = 0u64;
                    let mut give_up = None;
                    loop {
                        // Acks come back in commit order, which is this producer's send order.
                        while let Some((due, rx)) = inflight.front() {
                            let due: Instant = *due;
                            let res = match rx.try_recv() {
                                Ok(r) => r,
                                Err(std::sync::mpsc::TryRecvError::Empty) => break,
                                Err(_) => Err(ProposeError::Stopped),
                            };
                            let now = Instant::now();
                            let in_window = due >= meas_start && due < meas_end;
                            match res {
                                Ok(_) if in_window => {
                                    let lat = now.saturating_duration_since(due).as_nanos() as u64;
                                    m.lats.push(lat);
                                    m.acked += 1;
                                    if lat > STALL_NS {
                                        m.slow.push((
                                            due.saturating_duration_since(meas_start).as_nanos()
                                                as u64,
                                            lat,
                                        ));
                                    }
                                }
                                Ok(_) => {}
                                Err(ProposeError::Busy) => m.busy += in_window as u64,
                                Err(_) => m.other += in_window as u64,
                            }
                            inflight.pop_front();
                        }
                        if n < total_n {
                            let due = t0 + Duration::from_nanos((n as f64 * interval_ns) as u64);
                            let now = Instant::now();
                            if now >= due {
                                max_lag = max_lag
                                    .max(now.saturating_duration_since(due).as_nanos() as u64);
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
                                        Command::new(
                                            next_id,
                                            side,
                                            price,
                                            1 + (x >> 20) % 10,
                                            Tif::Gtc,
                                        ),
                                    ));
                                }
                                payload.clear();
                                encode_batch(&cmds, &mut payload);
                                inflight.push_back((due, lh.propose_async(payload.clone())));
                                n += 1;
                                continue;
                            }
                        } else if inflight.is_empty() {
                            break;
                        } else {
                            // Everything is sent; wait (bounded) for the backlog to drain.
                            let end = *give_up
                                .get_or_insert_with(|| Instant::now() + Duration::from_secs(30));
                            if Instant::now() > end {
                                m.other += inflight.len() as u64;
                                break;
                            }
                        }
                        std::hint::spin_loop();
                    }
                    (m, max_lag)
                }));
            }
        }
        let mut lats = Vec::new();
        let mut slow: Vec<(u64, u64)> = Vec::new();
        let (mut acked, mut busy, mut other, mut max_lag) = (0u64, 0u64, 0u64, 0u64);
        for h in producers_h {
            let (m, lag) = h.join().unwrap();
            lats.extend(m.lats);
            slow.extend(m.slow);
            acked += m.acked;
            busy += m.busy;
            other += m.other;
            max_lag = max_lag.max(lag);
        }
        lats.sort_unstable();
        let dropped: u64 = all
            .iter()
            .flatten()
            .map(|r| r.handle().status().net_dropped)
            .sum();
        let bad: u64 = all
            .iter()
            .flatten()
            .map(|r| r.handle().status().net_bad_frames)
            .sum();
        println!(
            "{:>12.0} {:>12.0} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>10.1} {:>10} {:>7} {:>7} {:>9.1}",
            rate,
            acked as f64 * batch as f64 / secs,
            pct_us(&lats, 0.50),
            pct_us(&lats, 0.90),
            pct_us(&lats, 0.99),
            pct_us(&lats, 0.999),
            pct_us(&lats, 0.9999),
            pct_us(&lats, 1.0),
            acked,
            busy,
            other,
            max_lag as f64 / 1e3,
        );
        // Group slow acks that were due close together into stalls: a stall
        // delays every batch due during it, so it shows up as a run of them.
        if max_lag > 2_000_000 {
            println!(
                "             note: the load generator itself was descheduled for up to {:.1} ms; \
                 tail percentiles at or below that include scheduling noise, not only the system under test",
                max_lag as f64 / 1e6
            );
        }
        println!("             transport so far: {dropped} messages dropped, {bad} bad frames");
        let stalls = group_stalls(&mut slow);
        if !stalls.is_empty() {
            let shown: Vec<String> = stalls
                .iter()
                .take(8)
                .map(|(a, b, w, c)| {
                    format!(
                        "+{:.2}s for {:.0}ms (worst {:.0}ms, {c} batches)",
                        *a as f64 / 1e9,
                        (*b - *a) as f64 / 1e6 + *w as f64 / 1e6,
                        *w as f64 / 1e6
                    )
                })
                .collect();
            println!(
                "             {} stalls over {}ms: {}",
                stalls.len(),
                STALL_NS / 1_000_000,
                shown.join("; ")
            );
        }
        // Let every node apply what was committed so the next rate starts clean.
        let end = Instant::now() + Duration::from_secs(120);
        for (g, rs) in all.iter().enumerate() {
            let want = rs[leaders[g]].handle().status().commit;
            while rs.iter().any(|r| r.handle().status().applied < want) && Instant::now() < end {
                std::thread::sleep(Duration::from_millis(5));
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_exact_and_in_microseconds() {
        let v: Vec<u64> = (1..=1000).map(|i| i * 1000).collect(); // 1..1000 us
        assert_eq!(pct_us(&v, 0.0), 1.0);
        assert_eq!(pct_us(&v, 0.5), 501.0);
        assert_eq!(pct_us(&v, 0.99), 990.0);
        assert_eq!(pct_us(&v, 1.0), 1000.0);
        assert!(pct_us(&[], 0.5).is_nan());
    }

    #[test]
    fn slow_acks_due_close_together_are_one_stall() {
        let ms = 1_000_000;
        let mut slow = vec![
            (300 * ms, 30 * ms),
            (10 * ms, 90 * ms),
            (12 * ms, 120 * ms),
            (14 * ms, 100 * ms),
            (303 * ms, 12 * ms),
        ];
        let stalls = group_stalls(&mut slow);
        assert_eq!(
            stalls,
            vec![
                (10 * ms, 14 * ms, 120 * ms, 3),
                (300 * ms, 303 * ms, 30 * ms, 2)
            ]
        );
        assert!(group_stalls(&mut []).is_empty());
    }
}
