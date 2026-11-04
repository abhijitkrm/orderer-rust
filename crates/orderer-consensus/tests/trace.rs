use orderer_consensus::sim::{run, SimConfig};
use orderer_consensus::trace::replay;

fn recorded(seed: u64, nodes: usize) -> Vec<String> {
    let cfg = SimConfig {
        nodes,
        chaos_ticks: 600,
        heal_ticks: 900,
        record: true,
        ..SimConfig::default()
    };
    run(seed, &cfg).expect("sim run").traces
}

#[test]
fn every_recorded_node_trace_replays_exactly() {
    for seed in 1..=12 {
        for (i, t) in recorded(seed, 3).iter().enumerate() {
            let steps = replay(t).unwrap_or_else(|e| panic!("seed {seed} node {i}: {e}"));
            assert!(steps > 100, "seed {seed} node {i}: only {steps} steps");
        }
    }
}

#[test]
fn replay_reports_the_first_difference() {
    let t = recorded(3, 3).remove(0);
    // Corrupt one expected output line.
    let bad = t.replacen("out send", "out send 9", 1);
    let err = replay(&bad).unwrap_err();
    assert!(
        err.contains("differs") || err.contains("unknown") || err.contains("line"),
        "{err}"
    );
    // A trace missing its node line is rejected, not silently accepted.
    assert!(replay("in tick\n").is_err());
}

#[test]
fn recording_does_not_change_the_run() {
    let mut cfg = SimConfig {
        chaos_ticks: 500,
        heal_ticks: 900,
        ..SimConfig::default()
    };
    let a = run(5, &cfg).unwrap();
    cfg.record = true;
    let b = run(5, &cfg).unwrap();
    assert_eq!(
        (a.committed, a.elections, a.crashes),
        (b.committed, b.elections, b.crashes)
    );
}
