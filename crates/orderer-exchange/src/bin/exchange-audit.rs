//! exchange-audit: checks the whole exchange from outside.
//!
//!   exchange-audit --ledgers A,B,C;D,E,F --tier A,B,C --partitions P --source-base B
//!                  --ledger-clients A,B,C;D,E,F --tier-clients A,B,C
//!                  [--interval-ms 1000]                     continuous
//!                  [--once --wait SECS [--expect-idle] [--acked FILE]]
//!                                                           wait for convergence, check, exit
//!
//! Continuous mode prints a line per round and exits 1 at the first violation.
//! `--once` waits up to SECS for every shard to settle everything, then requires
//! a clean round with the conservation checks run, equal replica digests in every
//! group and, with `--acked` (the load generator's file), that every acknowledged
//! order is filled or closed and the deposit and payout totals match.
//! `--ledgers`/`--tier` are gateway/exchange addresses, `--*-clients` the client
//! ports (for state digests), each in node-id order, one `;`-separated list per shard.

use orderer_exchange::audit::{compare_digests, Audit};
use orderer_exchange::{demo_config, GatewayClient, TierClient};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::exit;
use std::time::{Duration, Instant};

fn die(msg: &str) -> ! {
    eprintln!("exchange-audit: {msg}");
    exit(2)
}

fn addrs(s: &str) -> Vec<SocketAddr> {
    s.split(',')
        .map(|a| {
            a.parse()
                .unwrap_or_else(|_| die(&format!("bad address {a}")))
        })
        .collect()
}

fn main() {
    let (mut ledgers, mut tier, mut lclients, mut tclients) =
        (String::new(), String::new(), String::new(), String::new());
    let (mut partitions, mut source_base, mut interval, mut once, mut wait) =
        (4u32, 0u32, 1000u64, false, 60u64);
    let mut acked = None::<String>;
    let mut expect_idle = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--ledgers" => ledgers = v("--ledgers"),
            "--tier" => tier = v("--tier"),
            "--ledger-clients" => lclients = v("--ledger-clients"),
            "--tier-clients" => tclients = v("--tier-clients"),
            "--partitions" => {
                partitions = v("--partitions")
                    .parse()
                    .unwrap_or_else(|_| die("--partitions"))
            }
            "--source-base" => {
                source_base = v("--source-base")
                    .parse()
                    .unwrap_or_else(|_| die("--source-base"))
            }
            "--interval-ms" => {
                interval = v("--interval-ms")
                    .parse()
                    .unwrap_or_else(|_| die("--interval-ms"))
            }
            "--once" => once = true,
            "--wait" => wait = v("--wait").parse().unwrap_or_else(|_| die("--wait")),
            "--acked" => acked = Some(v("--acked")),
            "--expect-idle" => expect_idle = true,
            other => die(&format!("unknown argument {other}")),
        }
    }
    if ledgers.is_empty() || tier.is_empty() {
        die("--ledgers and --tier are required");
    }
    let key = std::env::var("ORDERER_KEY")
        .ok()
        .filter(|k| !k.is_empty())
        .map(String::into_bytes);
    let gws: Vec<GatewayClient> = ledgers
        .split(';')
        .map(|g| GatewayClient::new(addrs(g)))
        .collect();
    let mut audit = Audit::new(
        gws,
        TierClient::new(addrs(&tier)),
        (source_base..source_base + partitions).collect(),
        demo_config,
    );
    let groups: Vec<Vec<SocketAddr>> = lclients
        .split(';')
        .chain(std::iter::once(tclients.as_str()))
        .filter(|g| !g.is_empty())
        .map(addrs)
        .collect();

    let start = Instant::now();
    let fail = |what: &str| -> ! {
        eprintln!("AUDIT VIOLATION: {what}");
        exit(1)
    };
    let mut round = 0u64;
    loop {
        round += 1;
        let rep = match audit.round() {
            Ok(r) => r,
            Err(e) => {
                if once && start.elapsed() < Duration::from_secs(wait) {
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                }
                eprintln!("audit round {round}: could not read the system: {e}");
                if once {
                    exit(1);
                }
                std::thread::sleep(Duration::from_millis(interval));
                continue;
            }
        };
        if let Some(v) = rep.violations.first() {
            fail(v);
        }
        let mut digests_checked = true;
        for g in &groups {
            let (v, checked) = compare_digests(g, key.as_deref(), if once { 20 } else { 3 });
            if let Some(v) = v {
                fail(&v);
            }
            digests_checked &= checked;
        }
        println!(
            "round {round}: ok; {}; digests_checked={digests_checked}",
            rep.summary
        );
        if once {
            if rep.caught_up && digests_checked && (!expect_idle || rep.open_orders == 0) {
                break;
            }
            if start.elapsed() > Duration::from_secs(wait) {
                eprintln!("audit: did not converge within {wait}s: caught_up={} digests_checked={digests_checked}", rep.caught_up);
                exit(1);
            }
            std::thread::sleep(Duration::from_millis(500));
        } else {
            std::thread::sleep(Duration::from_millis(interval));
        }
    }
    if let Some(path) = acked {
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| die(&format!("{path}: {e}")));
        let (mut orders, mut deposits, mut paid) = (Vec::new(), BTreeMap::new(), BTreeMap::new());
        for line in text.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            match f.as_slice() {
                ["R", id, qty] => orders.push((
                    id.parse::<u64>().unwrap_or(0),
                    qty.parse::<u64>().unwrap_or(0),
                )),
                ["D", asset, amt] => {
                    *deposits
                        .entry(asset.parse::<u32>().unwrap_or(0))
                        .or_insert(0u128) += amt.parse::<u128>().unwrap_or(0)
                }
                ["P", asset, amt] => {
                    *paid
                        .entry(asset.parse::<u32>().unwrap_or(0))
                        .or_insert(0u128) += amt.parse::<u128>().unwrap_or(0)
                }
                _ => {}
            }
        }
        let mut bad = audit
            .verify_orders(&orders)
            .unwrap_or_else(|e| vec![format!("order check failed: {e}")]);
        bad.extend(
            audit
                .verify_money(&deposits, &paid)
                .unwrap_or_else(|e| vec![format!("money check failed: {e}")]),
        );
        if !bad.is_empty() {
            eprintln!("AUDIT VIOLATION: {} problems, first: {}", bad.len(), bad[0]);
            exit(1);
        }
        println!(
            "verified {} acknowledged orders and the deposit/payout totals",
            orders.len()
        );
    }
    println!("audit clean");
}
