//! exchange-relay: the memory-less relays between ledger shards and the
//! matching groups. Kill and restart any of them at any time.
//!
//!   exchange-relay forwarder --config FILE --shard S --ledger A,B,C --tier "A,B,C;D,E,F"
//!   exchange-relay settler   --config FILE --ledgers "A,B,C;D,E,F" --tier "A,B,C;D,E,F"
//!   (forwarder and settler also take --chaos N: fault injection, see relay.rs)
//!   exchange-relay tick      --config FILE --ledgers "A,B,C;D,E,F" [--interval-ms 200]
//!                            [--sweep-every-ms 2000] [--max-age-ms 5000] [--retry-ms 5000]
//!   exchange-relay slice-driver --config FILE --ledgers "A,B,C;D,E,F" --tier "A,B,C;D,E,F"
//!                            [--interval-ms 250] [--chunk N] [--low-water-pct 25]
//!                            [--idle-ms 0] [--account-cap N] [--max-actions 64]
//!   (forwarder also takes --no-thin: send every group the whole outbox)
//!
//! `--ledger` lists one shard's gateway addresses and `--ledgers` one list per
//! shard (separated by `;`, in shard order); `--tier` lists the matching nodes'
//! exchange addresses, one list per group (separated by `;`, in group order), each in
//! node-id order (a leader hint is a node id). The forwarder serves one shard and
//! every group; the settler serves every shard and reads every stream once.
//! The key, if any, is read from `ORDERER_KEY`.

use orderer_exchange::cli::{parse_addrs, parse_groups, Setup};
use orderer_exchange::relay::{ForwarderRelay, SettlerRelay, SliceDriver, Thinning, TickRelay};
use orderer_exchange::{GatewayClient, TierClient};
use orderer_ledger::policy::PolicyConfig;
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
    let mut no_thin = false;
    let (mut chunk, mut low_water, mut idle_ms, mut account_cap, mut max_actions) =
        (10_000_000u128, 25u32, 0u64, None::<u128>, 64usize);
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
            "--no-thin" => no_thin = true,
            "--chunk" => chunk = v("--chunk").parse().unwrap_or_else(|_| die("--chunk")),
            "--low-water-pct" => {
                low_water = v("--low-water-pct")
                    .parse()
                    .unwrap_or_else(|_| die("--low-water-pct"))
            }
            "--idle-ms" => idle_ms = v("--idle-ms").parse().unwrap_or_else(|_| die("--idle-ms")),
            "--account-cap" => {
                account_cap = Some(
                    v("--account-cap")
                        .parse()
                        .unwrap_or_else(|_| die("--account-cap")),
                )
            }
            "--max-actions" => {
                max_actions = v("--max-actions")
                    .parse()
                    .unwrap_or_else(|_| die("--max-actions"))
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
            if !no_thin && cfg.matching_groups > 1 {
                r = r.thinned(Thinning {
                    map: cfg.shard_map(),
                    credit_mask: cfg.credit_mask(),
                });
            }
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
            let mut r = SettlerRelay::new((0..cfg.ledger_shards).collect(), cfg.settle_sources());
            r.chaos = chaos;
            let (mut moved, mut gaps, mut errs) = (0usize, 0usize, 0usize);
            println!(
                "settler running ({} shards, {} streams)",
                gws.len(),
                cfg.settle_sources().len()
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
        "slice-driver" => {
            if cfg.credit.is_none() {
                die("the config has no credit_group: nothing to drive");
            }
            let mut gws = ledger_clients(&ledgers);
            let mut ts = tier_clients(&tier);
            let mut assets: Vec<u32> = cfg.symbols.iter().flat_map(|s| [s.base, s.quote]).collect();
            assets.sort_unstable();
            assets.dedup();
            let pol = PolicyConfig {
                group: 0,
                chunk: assets.iter().map(|a| (*a, chunk)).collect(),
                low_water_pct: low_water,
                account_cap: account_cap
                    .map(|c| assets.iter().map(|a| (*a, c)).collect())
                    .unwrap_or_default(),
                group_cap: Default::default(),
                idle_ns: idle_ms * 1_000_000,
                max_actions,
            };
            // one driver per credit group, all in this process
            let mut ds: Vec<SliceDriver> = cfg
                .credit_groups()
                .iter()
                .map(|&g| SliceDriver::new(pol.clone(), gws.len(), g as u16))
                .collect();
            let c2 = cfg.clone();
            let ledger_cfg = move |s: u16| c2.ledger_config(s);
            let (mut lends, mut recalls, mut refused, mut errs) = (0usize, 0usize, 0usize, 0usize);
            let t0 = Instant::now();
            println!(
                "slice-driver for credit groups {:?} running",
                cfg.credit_groups()
            );
            loop {
                let now = t0.elapsed().as_nanos() as u64;
                for d in ds.iter_mut() {
                    match d.step(&mut gws, &mut ts[d.group as usize], &ledger_cfg, now) {
                        Ok(s) => {
                            lends += s.lends;
                            recalls += s.recalls;
                            refused += s.refused;
                        }
                        Err(e) => {
                            errs += 1;
                            eprintln!("slice-driver (group {}): {e}", d.group);
                            std::thread::sleep(Duration::from_millis(200));
                        }
                    }
                }
                if last_log.elapsed() > Duration::from_secs(5) {
                    println!(
                        "slice-driver: lent {lends}, recalled {recalls}, refused {refused}, errors {errs}"
                    );
                    last_log = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(interval));
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
