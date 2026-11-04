//! simsweep — run the deterministic simulator over a range of seeds.
//!
//!   simsweep [--seeds N] [--start S] [--nodes N] [--chaos TICKS] [--no-pre-vote]
//!
//! Exit status 1 and the first failing seed on a violation. Re-run with
//! `--start <seed> --seeds 1` to reproduce it.

use orderer_consensus::sim::{run, SimConfig};
use std::process::exit;

fn main() {
    let mut seeds = 1000u64;
    let mut start = 1u64;
    let mut cfg = SimConfig::default();
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut num = |name: &str| -> u64 {
            args.next().and_then(|v| v.parse().ok()).unwrap_or_else(|| {
                eprintln!("{name} needs a number");
                exit(2)
            })
        };
        match a.as_str() {
            "--seeds" => seeds = num("--seeds"),
            "--start" => start = num("--start"),
            "--nodes" => cfg.nodes = num("--nodes") as usize,
            "--chaos" => cfg.chaos_ticks = num("--chaos"),
            "--no-pre-vote" => cfg.node_cfg.pre_vote = false,
            other => {
                eprintln!("unknown argument {other}\nusage: simsweep [--seeds N] [--start S] [--nodes N] [--chaos TICKS] [--no-pre-vote]");
                exit(2)
            }
        }
    }
    let (mut committed, mut elections, mut crashes) = (0usize, 0u64, 0u64);
    for seed in start..start + seeds {
        match run(seed, &cfg) {
            Ok(r) => {
                committed += r.committed;
                elections += r.elections;
                crashes += r.crashes;
            }
            Err(e) => {
                eprintln!("FAIL {e}");
                exit(1);
            }
        }
    }
    println!(
        "ok: {seeds} seeds ({} nodes), {committed} entries committed, {elections} elections, {crashes} crashes",
        cfg.nodes
    );
}
