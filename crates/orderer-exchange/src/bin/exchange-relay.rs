//! exchange-relay: the memory-less relays between ledger shards and the
//! matching groups. Kill and restart any of them at any time.
//!
//!   exchange-relay forwarder --config FILE --shard S --ledger A,B,C --tier "A,B,C;D,E,F"
//!   exchange-relay settler   --config FILE --ledgers "A,B,C;D,E,F" --tier "A,B,C;D,E,F"
//!   (forwarder and settler also take --chaos N: fault injection, see relay.rs)
//!   exchange-relay tick      --config FILE --ledgers "A,B,C;D,E,F" [--interval-ms 200]
//!                            [--sweep-every-ms 2000] [--max-age-ms 5000] [--retry-ms 5000]
//!
//! `--ledger` lists one shard's gateway addresses and `--ledgers` one list per
//! shard (separated by `;`, in shard order); `--tier` lists the matching nodes'
//! exchange addresses, one list per group (separated by `;`, in group order), each in
//! node-id order (a leader hint is a node id). The forwarder serves one shard and
//! every group; the settler serves every shard and reads every stream once.
//! The key, if any, is read from `ORDERER_KEY`.

use orderer_exchange::cli::{parse_addrs, parse_groups, Setup};
use orderer_exchange::relay::{ForwarderRelay, SettlerRelay, TickRelay};
use orderer_exchange::{GatewayClient, TierClient};
use std::process::exit;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn die(msg: &str) -> ! {
    eprintln!("exchange-relay: {msg}");
    exit(2)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .unwrap_or_else(|| die("mode: forwarder|settler|tick"));
    let (mut config, mut shard, mut ledger, mut tier, mut ledgers) = (
        String::new(),
        0u16,
        String::new(),
        String::new(),
        String::new(),
    );
    let (mut interval, mut sweep_every, mut max_age, mut retry) =
        (200u64, 2000u64, 5000u64, 5000u64);
    let mut chaos = 0u32;
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--config" => config = v("--config"),
            "--shard" => shard = v("--shard").parse().unwrap_or_else(|_| die("--shard")),
            "--ledger" => ledger = v("--ledger"),
            "--tier" => tier = v("--tier"),
            "--ledgers" => ledgers = v("--ledgers"),
            "--interval-ms" => {
                interval = v("--interval-ms")
                    .parse()
                    .unwrap_or_else(|_| die("--interval-ms"))
            }
            "--sweep-every-ms" => {
                sweep_every = v("--sweep-every-ms")
                    .parse()
                    .unwrap_or_else(|_| die("--sweep-every-ms"))
            }
            "--max-age-ms" => {
                max_age = v("--max-age-ms")
                    .parse()
                    .unwrap_or_else(|_| die("--max-age-ms"))
            }
            "--chaos" => chaos = v("--chaos").parse().unwrap_or_else(|_| die("--chaos")),
            "--retry-ms" => {
                retry = v("--retry-ms")
                    .parse()
                    .unwrap_or_else(|_| die("--retry-ms"))
            }
            other => die(&format!("unknown argument {other}")),
        }
    }
    if config.is_empty() {
        die("--config is required");
    }
    let setup = Setup::load(&config).unwrap_or_else(|e| die(&e));
    let (cfg, auth) = (&setup.cfg, &setup.auth);
    let tier_clients = |tier: &str| -> Vec<TierClient> {
        let g = parse_groups(tier).unwrap_or_else(|e| die(&e));
        if g.len() != cfg.matching_groups as usize {
            die(&format!(
                "--tier has {} groups but the config has {}",
                g.len(),
                cfg.matching_groups
            ));
        }
        g.into_iter()
            .map(|a| TierClient::with_auth(a, auth.clone()))
            .collect()
    };
    let ledger_clients = |ledgers: &str| -> Vec<GatewayClient> {
        let g = parse_groups(ledgers).unwrap_or_else(|e| die(&e));
        if g.len() != cfg.ledger_shards as usize {
            die(&format!(
                "--ledgers has {} shards but the config has {}",
                g.len(),
                cfg.ledger_shards
            ));
        }
        g.into_iter()
            .map(|a| GatewayClient::with_auth(a, auth.clone()))
            .collect()
    };
    let mut last_log = Instant::now();
    match mode.as_str() {
        "forwarder" => {
            if shard >= cfg.ledger_shards {
                die("--shard is not in the config");
            }
            let mut gw = GatewayClient::with_auth(
                parse_addrs(&ledger).unwrap_or_else(|e| die(&e)),
                auth.clone(),
            );
            let mut t = tier_clients(&tier);
            let mut r = ForwarderRelay::new(shard, t.len());
            r.chaos = chaos;
            let (mut moved, mut gaps, mut errs) = (0usize, 0usize, 0usize);
            println!("forwarder shard {shard} running ({} groups)", t.len());
            loop {
                match r.step(&mut gw, &mut t) {
                    Ok(s) => {
                        moved += s.moved;
                        gaps += s.gaps;
                        if s.moved == 0 {
                            std::thread::sleep(Duration::from_millis(2));
                        }
                    }
                    Err(e) => {
                        errs += 1;
                        eprintln!("forwarder shard {shard}: {e}");
                        std::thread::sleep(Duration::from_millis(200));
                    }
                }
                if last_log.elapsed() > Duration::from_secs(5) {
                    println!(
                        "forwarder shard {shard}: forwarded {moved}, gaps {gaps}, errors {errs}"
                    );
                    last_log = Instant::now();
                }
            }
        }
        "settler" => {
            let mut gws = ledger_clients(&ledgers);
            let mut t = tier_clients(&tier);
            let mut r = SettlerRelay::new((0..cfg.ledger_shards).collect(), cfg.sources());
            r.chaos = chaos;
            let (mut moved, mut gaps, mut errs) = (0usize, 0usize, 0usize);
            println!(
                "settler running ({} shards, {} streams)",
                gws.len(),
                cfg.sources().len()
            );
            loop {
                match r.step(&mut gws, &mut t) {
                    Ok(s) => {
                        moved += s.moved;
                        gaps += s.gaps;
                        if s.moved == 0 {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                    }
                    Err(e) => {
                        errs += 1;
                        eprintln!("settler: {e}");
                        std::thread::sleep(Duration::from_millis(200));
                    }
                }
                if last_log.elapsed() > Duration::from_secs(5) {
                    println!("settler: settled {moved}, gaps {gaps}, errors {errs}");
                    last_log = Instant::now();
                }
            }
        }
        "tick" => {
            let mut gws = ledger_clients(&ledgers);
            let t = TickRelay {
                max_age_ns: max_age * 1_000_000,
                retry_ns: retry * 1_000_000,
                max: 1000,
            };
            let mut last_sweep = Instant::now();
            println!("tick over {} shards running", gws.len());
            loop {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                let sweep = last_sweep.elapsed() >= Duration::from_millis(sweep_every);
                for (s, gw) in gws.iter_mut().enumerate() {
                    if let Err(e) = t.clock(gw, now) {
                        eprintln!("tick shard {s}: {e}");
                        continue;
                    }
                    if sweep {
                        match t.sweep(gw) {
                            Ok(q) if q > 0 => println!("sweep shard {s}: queued {q} cancels"),
                            Ok(_) => {}
                            Err(e) => eprintln!("sweep shard {s}: {e}"),
                        }
                    }
                }
                if sweep {
                    last_sweep = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(interval));
            }
        }
        other => die(&format!("unknown mode {other}")),
    }
}
