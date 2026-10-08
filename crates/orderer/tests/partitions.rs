//! Partitioning and fuzz determinism (plan §5.3 `partitions.rs`,
//! `fuzz_pipeline.rs`): for adversarial multi-symbol streams, every P and
//! every partition table yields the reference per-symbol streams; routing
//! is honored; `seq` stays dense; runs are deterministic.

mod common;

use common::*;
use orderer::*;
use orderer_core::jsonflat::get_u64;
use orderer_core::*;

#[test]
fn every_partition_count_matches_reference_per_symbol() {
    let cfg = fuzz_cfg();
    for seed in 1..=8u64 {
        let cmds = fuzz_corpus(seed, 4_000, 8);
        let reference = reference_lines(cfg, &cmds);
        let want = by_symbol(reference.iter().map(String::as_str));
        for p in [1u32, 2, 3, 4, 7] {
            let parts = run_pipeline(cfg, &cmds, p, true);
            if p == 1 {
                assert_eq!(
                    parts[0], reference,
                    "seed {seed}: P=1 is the plain engine stream"
                );
            }
            let all: Vec<String> = parts.concat();
            assert_dense(&all);
            assert_eq!(
                by_symbol(all.iter().map(String::as_str)),
                want,
                "seed {seed} P={p}: per-symbol streams"
            );
            for (part, lines) in parts.iter().enumerate() {
                for l in lines {
                    let sym = get_u64(l, "symbol").unwrap() as Symbol;
                    assert_eq!(hash_partition(sym, p), part as u32, "routing honored");
                }
            }
        }
    }
}

#[test]
fn fuzz_runs_are_deterministic_per_partition() {
    let cfg = fuzz_cfg();
    for seed in 1..=8u64 {
        let cmds = fuzz_corpus(seed, 3_000, 8);
        let a = run_pipeline(cfg, &cmds, 4, true);
        let b = run_pipeline(cfg, &cmds, 4, true);
        assert_eq!(a, b, "seed {seed}: same input, same per-partition streams");
    }
}

#[test]
fn partition_table_routes_symbols() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(11, 3_000, 8);
    // pin every symbol to partition 2 except symbol 5 → partition 0
    let table: Vec<(Symbol, u32)> = (0..8).map(|s| (s, if s == 5 { 0 } else { 2 })).collect();
    let map = PartitionMap::with_table(3, &table).unwrap();
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partition_map(map)
        .egress(collect)
        .build()
        .unwrap();
    p.publish_batch(&cmds).unwrap();
    p.drain().unwrap();
    p.shutdown().unwrap();
    let parts: Vec<Vec<String>> = events.take().iter().map(|b| lines(b)).collect();
    assert!(parts[1].is_empty(), "nothing routed to partition 1");
    assert!(parts[0].iter().all(|l| get_u64(l, "symbol") == Some(5)));
    assert!(parts[2].iter().all(|l| get_u64(l, "symbol") != Some(5)));
    let reference = reference_lines(cfg, &cmds);
    assert_eq!(
        by_symbol(parts.concat().iter().map(String::as_str)),
        by_symbol(reference.iter().map(String::as_str))
    );
}

#[test]
fn many_producers_preserve_per_symbol_order() {
    // producers own disjoint symbols, so each symbol's order is its
    // producer's order whatever the interleaving (spec/PIPELINE.md §2)
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(3, 20_000, 16);
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(4)
        .ring_sizes(256, 64, 64)
        .egress(collect)
        .build()
        .unwrap();
    let threads: Vec<_> = (0..4u32)
        .map(|k| {
            let h = p.handle();
            let mine: Vec<_> = cmds.iter().copied().filter(|(s, _)| s % 4 == k).collect();
            std::thread::spawn(move || {
                for chunk in mine.chunks(7) {
                    if chunk.len() % 2 == 0 {
                        h.publish_batch(chunk).unwrap();
                    } else {
                        for &(s, c) in chunk {
                            h.publish(s, c).unwrap();
                        }
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
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
