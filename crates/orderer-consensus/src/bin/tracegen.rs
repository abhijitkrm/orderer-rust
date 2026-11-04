//! tracegen — write protocol conformance vectors from the simulator.
//!
//!   tracegen OUT_DIR [--seeds a,b,c] [--nodes N] [--steps N]
//!
//! One file per (seed, node), each cut after at most `--steps` recorded
//! drain points. Ports replay these and must reproduce every effect and
//! state line exactly. Prints how much of the protocol the corpus exercises.

use orderer_consensus::sim::{run, SimConfig};
use orderer_consensus::trace::replay;
use std::collections::BTreeMap;
use std::process::exit;

fn main() {
    let mut args = std::env::args().skip(1);
    let out = args.next().unwrap_or_else(|| {
        eprintln!("usage: tracegen OUT_DIR [--seeds a,b,c] [--nodes N] [--steps N]");
        exit(2)
    });
    let (mut seeds, mut nodes, mut steps): (Vec<u64>, usize, usize) = ((1..=6).collect(), 3, 1500);
    while let Some(a) = args.next() {
        let v = args.next().unwrap_or_else(|| exit(2));
        match a.as_str() {
            "--seeds" => seeds = v.split(',').map(|s| s.parse().expect("seed")).collect(),
            "--nodes" => nodes = v.parse().expect("nodes"),
            "--steps" => steps = v.parse().expect("steps"),
            other => {
                eprintln!("unknown argument {other}");
                exit(2)
            }
        }
    }
    std::fs::create_dir_all(&out).expect("create output dir");
    let mut coverage: BTreeMap<&str, usize> = BTreeMap::new();
    let (mut files, mut bytes) = (0, 0);
    for seed in seeds {
        let cfg = SimConfig {
            nodes,
            chaos_ticks: 1200,
            heal_ticks: 900,
            record: true,
            ..SimConfig::default()
        };
        let rep = run(seed, &cfg).unwrap_or_else(|e| {
            eprintln!("simulation failed: {e}");
            exit(1)
        });
        for (i, trace) in rep.traces.iter().enumerate() {
            // Cut after the `steps`-th drain point.
            let mut kept = String::new();
            let mut seen = 0;
            for line in trace.lines() {
                kept.push_str(line);
                kept.push('\n');
                if line.starts_with("state ") {
                    seen += 1;
                    if seen == steps {
                        break;
                    }
                }
            }
            replay(&kept).unwrap_or_else(|e| {
                eprintln!("seed {seed} node {i} does not replay in Rust: {e}");
                exit(1)
            });
            for (k, pat) in [
                ("elections won (role=Leader)", "role=Leader"),
                ("pre-vote requests", " vote "),
                ("votes granted", "votereply "),
                ("entries replicated (append)", "out send"),
                ("entries applied", "out apply"),
                ("compactions", "in compact"),
                ("snapshot requests (needsnap)", "needsnap"),
                ("snapshot installs", "in install"),
                ("restores", "out restore"),
                ("restarts (extra stanzas)", "node id="),
                ("proposals", "in propose"),
            ] {
                *coverage.entry(k).or_default() += kept.lines().filter(|l| l.contains(pat)).count();
            }
            let path = format!("{out}/seed{seed}-node{i}.trace");
            std::fs::write(&path, &kept).expect("write trace");
            files += 1;
            bytes += kept.len();
        }
    }
    println!("wrote {files} traces, {} KiB, into {out}", bytes / 1024);
    for (k, v) in coverage {
        println!("  {k:34} {v}");
    }
}
