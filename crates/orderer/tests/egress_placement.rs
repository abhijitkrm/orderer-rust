//! `EgressPlacement::Inline` (plugs run on the engine thread) delivers exactly
//! the events the egress threads do, in the same per-partition order, for every
//! partition count, and refuses to run with journals.

mod common;

use common::*;
use orderer::*;
use orderer_core::*;

fn run(
    cfg: BookConfig,
    cmds: &[(Symbol, Command)],
    partitions: u32,
    placement: EgressPlacement,
) -> Vec<Vec<String>> {
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(partitions)
        .ring_sizes(1 << 10, 1 << 8, 1 << 8)
        .egress_placement(placement)
        .egress(collect)
        .check_invariants(cfg!(debug_assertions))
        .build()
        .unwrap();
    p.publish_batch(cmds).unwrap();
    p.drain().unwrap();
    p.shutdown().unwrap();
    events.take().iter().map(|b| lines(b)).collect()
}

#[test]
fn inline_egress_matches_thread_egress() {
    let cfg = fuzz_cfg();
    for seed in 1..=6u64 {
        let cmds = fuzz_corpus(seed, 4_000, 8);
        for p in [1u32, 2, 3, 4, 7] {
            let threaded = run(cfg, &cmds, p, EgressPlacement::Thread);
            let inline = run(cfg, &cmds, p, EgressPlacement::Inline);
            assert_eq!(
                inline, threaded,
                "seed {seed}, P={p}: inline egress changed the per-partition streams"
            );
            assert_dense(&inline.concat());
        }
    }
}

#[test]
fn inline_egress_drain_sees_every_event() {
    // drain() must not return before the plugs have seen every event of the
    // commands published so far, even though no egress thread exists to ack it
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(9, 6_000, 8);
    let (metrics, handle) = Metrics::new(cmds.len() + 16);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(3)
        .egress_placement(EgressPlacement::Inline)
        .egress(metrics)
        .build()
        .unwrap();
    p.publish_batch(&cmds).unwrap();
    p.drain().unwrap();
    p.shutdown().unwrap();
    let commands: u64 = handle.results().iter().map(|m| m.commands).sum();
    assert!(commands > 0);
    let reference = reference_lines(cfg, &cmds);
    let events: u64 = handle.results().iter().map(|m| m.events).sum();
    assert_eq!(
        events as usize,
        reference.len(),
        "every event reached the plug"
    );
}

#[test]
fn inline_egress_requires_journals_off() {
    let dir = scratch("inline-egress-journal");
    let res = Pipeline::<FifoCore>::builder()
        .book_config(fuzz_cfg())
        .partitions(2)
        .journal(JournalConfig::new(&dir, JournalFormat::Binary))
        .egress_placement(EgressPlacement::Inline)
        .build();
    match res {
        Err(Error::Config(m)) => assert!(m.contains("inline egress"), "{m}"),
        Err(e) => panic!("wrong error: {e}"),
        Ok(_) => panic!("inline egress with journals must be refused"),
    }
}

#[test]
fn inline_egress_has_stats_without_an_outbox() {
    // stats() reads each partition's ring depths; inline egress has no outbox
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(5, 2_000, 8);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(3)
        .egress_placement(EgressPlacement::Inline)
        .build()
        .unwrap();
    p.publish_batch(&cmds).unwrap();
    p.drain().unwrap();
    let st = p.stats();
    assert_eq!(st.partitions.len(), 3);
    assert!(st.partitions.iter().all(|s| s.outbox_depth == 0));
    assert_eq!(st.partitions.iter().map(|s| s.commands).sum::<u64>(), cmds.len() as u64);
    p.shutdown().unwrap();
}
