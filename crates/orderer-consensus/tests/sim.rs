use orderer_consensus::sim::{run, SimConfig};

#[test]
fn seeds_pass_three_nodes() {
    let cfg = SimConfig::default();
    for seed in 1..=60 {
        run(seed, &cfg).unwrap_or_else(|e| panic!("{e}"));
    }
}

#[test]
fn seeds_pass_five_nodes() {
    let cfg = SimConfig {
        nodes: 5,
        ..SimConfig::default()
    };
    for seed in 1..=30 {
        run(seed, &cfg).unwrap_or_else(|e| panic!("{e}"));
    }
}

#[test]
fn seeds_pass_without_pre_vote() {
    let mut cfg = SimConfig::default();
    cfg.node_cfg.pre_vote = false;
    for seed in 1..=30 {
        run(seed, &cfg).unwrap_or_else(|e| panic!("{e}"));
    }
}

#[test]
fn single_node_cluster_commits() {
    let cfg = SimConfig {
        nodes: 1,
        ..SimConfig::default()
    };
    for seed in 1..=20 {
        let r = run(seed, &cfg).unwrap_or_else(|e| panic!("{e}"));
        assert!(r.committed > 0);
    }
}

#[test]
fn same_seed_same_run() {
    let cfg = SimConfig::default();
    let a = run(7, &cfg).unwrap();
    let b = run(7, &cfg).unwrap();
    assert_eq!(
        (a.committed, a.elections, a.crashes, a.max_term),
        (b.committed, b.elections, b.crashes, b.max_term)
    );
}
