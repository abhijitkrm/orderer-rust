//! exchange-relay: the memory-less relays between ledger shards and the
//! matching group. Kill and restart any of them at any time.
//!
//!   exchange-relay forwarder --shard S --ledger A,B,C --tier A,B,C
//!   exchange-relay settler   --shard S --ledger A,B,C --tier A,B,C
//!                            --source-base B --partitions P
//!   (forwarder and settler also take --chaos N: fault injection, see relay.rs)
//!   exchange-relay tick      --ledgers A,B,C;D,E,F [--interval-ms 200]
//!                            [--sweep-every-ms 2000] [--max-age-ms 5000] [--retry-ms 5000]
//!
//! `--ledger` lists a shard's gateway addresses and `--tier` the matching nodes'
//! exchange addresses, each in node-id order (a leader hint is a node id).
//! `tick` takes one such list per shard, separated by `;`.

use orderer_exchange::relay::{ForwarderRelay, SettlerRelay, TickRelay};
use orderer_exchange::{GatewayClient, TierClient};
use std::net::SocketAddr;
use std::process::exit;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn die(msg: &str) -> ! {
    eprintln!("exchange-relay: {msg}");
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
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .unwrap_or_else(|| die("mode: forwarder|settler|tick"));
    let (mut shard, mut ledger, mut tier, mut ledgers) =
        (0u16, String::new(), String::new(), String::new());
    let (mut source_base, mut partitions) = (0u32, 4u32);
    let (mut interval, mut sweep_every, mut max_age, mut retry) =
        (200u64, 2000u64, 5000u64, 5000u64);
    let mut chaos = 0u32;
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--shard" => shard = v("--shard").parse().unwrap_or_else(|_| die("--shard")),
            "--ledger" => ledger = v("--ledger"),
            "--tier" => tier = v("--tier"),
            "--ledgers" => ledgers = v("--ledgers"),
            "--source-base" => {
                source_base = v("--source-base")
                    .parse()
                    .unwrap_or_else(|_| die("--source-base"))
            }
            "--partitions" => {
                partitions = v("--partitions")
                    .parse()
                    .unwrap_or_else(|_| die("--partitions"))
            }
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
    let mut last_log = Instant::now();
    match mode.as_str() {
        "forwarder" => {
            let mut gw = GatewayClient::new(addrs(&ledger));
            let mut t = TierClient::new(addrs(&tier));
            let mut r = ForwarderRelay::new(shard);
            r.chaos = chaos;
            let (mut moved, mut gaps, mut errs) = (0usize, 0usize, 0usize);
            println!("forwarder shard {shard} running");
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
            let mut gw = GatewayClient::new(addrs(&ledger));
            let mut t = TierClient::new(addrs(&tier));
            let mut r = SettlerRelay::new(shard, (source_base..source_base + partitions).collect());
            r.chaos = chaos;
            let (mut moved, mut gaps, mut errs) = (0usize, 0usize, 0usize);
            println!("settler shard {shard} running");
            loop {
                match r.step(&mut gw, &mut t) {
                    Ok(s) => {
                        moved += s.moved;
                        gaps += s.gaps;
                        if s.moved == 0 {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                    }
                    Err(e) => {
                        errs += 1;
                        eprintln!("settler shard {shard}: {e}");
                        std::thread::sleep(Duration::from_millis(200));
                    }
                }
                if last_log.elapsed() > Duration::from_secs(5) {
                    println!("settler shard {shard}: settled {moved}, gaps {gaps}, errors {errs}");
                    last_log = Instant::now();
                }
            }
        }
        "tick" => {
            let mut gws: Vec<GatewayClient> = ledgers
                .split(';')
                .map(|g| GatewayClient::new(addrs(g)))
                .collect();
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
