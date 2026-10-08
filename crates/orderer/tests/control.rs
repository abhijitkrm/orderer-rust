//! Control operations, durability gating and backpressure (plan §5.3
//! `control.rs`, `durability.rs`, `backpressure.rs`).

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use common::*;
use orderer::*;
use orderer_core::*;

/// Abort the whole test binary if something deadlocks.
fn watchdog(secs: u64) {
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(secs));
        eprintln!("watchdog: test exceeded {secs}s — deadlock?");
        std::process::abort();
    });
}

#[test]
fn snapshots_under_load_are_clean_cuts() {
    watchdog(120);
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(31, 30_000, 8);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(3)
        .ring_sizes(256, 64, 64)
        .build()
        .unwrap();
    // one producer thread ⇒ ingress order == list order, so cut N means
    // exactly cmds[..N]
    let h = p.handle();
    let producer_cmds = cmds.clone();
    let producer = thread::spawn(move || {
        for chunk in producer_cmds.chunks(50) {
            h.publish_batch(chunk).unwrap();
        }
    });
    let mut snaps = Vec::new();
    while snaps.len() < 6 {
        snaps.push(p.snapshot().unwrap());
        thread::sleep(Duration::from_millis(2));
    }
    producer.join().unwrap();
    snaps.push(p.snapshot().unwrap());
    p.shutdown().unwrap();
    let mut cuts = Vec::new();
    for s in &snaps {
        let n = s.iseq as usize;
        assert_eq!(s.body, reference_snapshot(cfg, &cmds[..n]), "cut at {n}");
        cuts.push(n);
    }
    assert_eq!(*cuts.last().unwrap(), cmds.len());
    assert!(cuts.windows(2).all(|w| w[0] <= w[1]));
}

#[test]
fn shutdown_is_idempotent_and_closes_publishing() {
    let mut p = Pipeline::<FifoCore>::builder()
        .partitions(2)
        .build()
        .unwrap();
    let h = p.handle();
    p.publish(1, Command::new(1, Side::Bid, 10, 1, Tif::Gtc))
        .unwrap();
    p.shutdown().unwrap();
    p.shutdown().unwrap();
    let c = Command::cancel(1);
    assert_eq!(p.publish(1, c), Err(Error::Closed));
    assert_eq!(h.publish(1, c), Err(Error::Closed));
    assert_eq!(h.try_publish(1, c), Err(Error::Closed));
    assert_eq!(h.publish_batch(&[(1, c)]), Err(Error::Closed));
    assert_eq!(p.drain(), Err(Error::Closed));
    assert!(p.snapshot().is_err());
}

#[test]
fn every_ok_publish_racing_shutdown_is_applied() {
    watchdog(120);
    for _round in 0..5 {
        let (collect, events) = Collect::new(true);
        let mut p = Pipeline::<NoopCore>::builder()
            .partitions(2)
            .ring_sizes(64, 16, 16)
            .egress(collect)
            .build()
            .unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..3u32)
            .map(|k| {
                let h = p.handle();
                let acc = accepted.clone();
                thread::spawn(move || {
                    let mut i = 0u64;
                    while h.publish(k, Command::cancel(i)).is_ok() {
                        acc.fetch_add(1, Ordering::SeqCst);
                        i += 1;
                    }
                })
            })
            .collect();
        thread::sleep(Duration::from_millis(5));
        p.shutdown().unwrap();
        for t in threads {
            t.join().unwrap();
        }
        let delivered: usize = events.take().iter().map(|b| lines(b).len()).sum();
        assert_eq!(
            delivered,
            accepted.load(Ordering::SeqCst),
            "Ok ⇒ applied, no more, no less"
        );
    }
}

fn ack_pipeline(
    fsync: FsyncPolicy,
    acks: Arc<Mutex<Vec<u64>>>,
) -> (Pipeline<FifoCore>, std::path::PathBuf) {
    let dir = scratch(&format!("acks-{fsync:?}").replace(['{', '}', ' ', ':', ','], ""));
    let p = Pipeline::<FifoCore>::builder()
        .book_config(fuzz_cfg())
        .partitions(2)
        .journal(JournalConfig {
            dir: dir.clone(),
            format: JournalFormat::Binary,
            fsync,
            events: false,
            append: false,
        })
        .egress(Acks::new(
            move |_p: u32, m: &EvtMsg| acks.lock().unwrap().push(m.iseq),
            1024,
        ))
        .build()
        .unwrap();
    (p, dir)
}

#[test]
fn acks_wait_for_fsync() {
    watchdog(120);
    let cmds = fuzz_corpus(4, 2_000, 6);
    let total_events = reference_lines(fuzz_cfg(), &cmds).len();

    // fsync effectively never (until shutdown): nothing may be acked
    let acks = Arc::new(Mutex::new(Vec::new()));
    let (mut p, dir) = ack_pipeline(FsyncPolicy::Every(Duration::from_secs(3600)), acks.clone());
    p.publish_batch(&cmds).unwrap();
    p.drain().unwrap();
    thread::sleep(Duration::from_millis(100));
    assert!(acks.lock().unwrap().is_empty(), "acked before any fsync");
    assert_eq!((p.durable_iseq(0), p.durable_iseq(1)), (0, 0));
    p.shutdown().unwrap(); // final group commit releases everything
    assert_eq!(acks.lock().unwrap().len(), total_events);
    let _ = std::fs::remove_dir_all(dir);

    // group commit on idle: acks follow the durable watermark
    let acks = Arc::new(Mutex::new(Vec::new()));
    let (mut p, dir) = ack_pipeline(
        FsyncPolicy::EveryN {
            n: 1 << 40,
            idle: Duration::from_millis(20),
        },
        acks.clone(),
    );
    p.publish_batch(&cmds).unwrap();
    p.drain().unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while acks.lock().unwrap().len() < total_events {
        assert!(Instant::now() < deadline, "acks never released");
        thread::sleep(Duration::from_millis(5));
    }
    let durable = p.durable_iseq(0).max(p.durable_iseq(1));
    assert_eq!(durable, cmds.len() as u64);
    assert!(acks.lock().unwrap().iter().all(|&i| i <= durable));
    p.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

/// Sleeps per event — the slowest plausible consumer.
struct Slow;
impl Egress for Slow {
    fn on_event(&mut self, _: &EvtMsg) {
        thread::sleep(Duration::from_micros(20));
    }
}

#[test]
fn tiny_rings_and_slow_egress_block_without_loss() {
    watchdog(120);
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(17, 3_000, 4);
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(2)
        .ring_sizes(2, 2, 2)
        .egress(|_: &EgressCtx| -> Box<dyn Egress> { Box::new(Slow) })
        .egress(collect)
        .build()
        .unwrap();
    for &(s, c) in &cmds {
        p.publish(s, c).unwrap(); // Block policy: waits, never drops
    }
    p.drain().unwrap();
    p.shutdown().unwrap();
    let got: Vec<String> = events.take().iter().flat_map(|b| lines(b)).collect();
    let reference = reference_lines(cfg, &cmds);
    assert_eq!(
        by_symbol(got.iter().map(String::as_str)),
        by_symbol(reference.iter().map(String::as_str))
    );
}

#[test]
fn try_publish_sheds_at_the_edge_only() {
    watchdog(120);
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<NoopCore>::builder()
        .partitions(1)
        .ring_sizes(4, 2, 2)
        .egress(|_: &EgressCtx| -> Box<dyn Egress> { Box::new(Slow) })
        .egress(collect)
        .build()
        .unwrap();
    let (mut ok, mut full) = (0usize, 0usize);
    for i in 0..2_000u64 {
        match p.try_publish(1, Command::cancel(i)) {
            Ok(()) => ok += 1,
            Err(Error::Full) => full += 1,
            Err(e) => panic!("{e}"),
        }
    }
    p.drain().unwrap();
    p.shutdown().unwrap();
    assert!(full > 0, "a 4-slot ingress behind a slow egress must fill");
    let delivered: Vec<String> = events.take().iter().flat_map(|b| lines(b)).collect();
    assert_eq!(
        delivered.len(),
        ok,
        "everything accepted was applied; nothing else"
    );
    assert_dense(&delivered);
}
