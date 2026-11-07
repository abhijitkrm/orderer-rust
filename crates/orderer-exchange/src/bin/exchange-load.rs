//! exchange-load: a load generator for the exchange, through the ledger gateways.
//!
//!   exchange-load deposit --ledgers A,B,C;D,E,F --accounts N --acked FILE
//!   exchange-load run     --ledgers ... --accounts N --secs S --threads T --acked FILE [--seed X]
//!   exchange-load drain   --ledgers ... --acked FILE [--timeout SECS]
//!
//! Account `a` lives on shard `a % shards`. `deposit` funds every account
//! (idempotent: fixed external ids). `run` issues random reserves, cancels,
//! deposits and withdrawals, each as a session command that is retried until it
//! has a definite answer, and appends what was acknowledged to FILE:
//! `R order qty` (a reserve), `D asset amount` (a deposit), `P asset amount` (a
//! withdrawal that was paid out). `drain` cancels every acknowledged order that is
//! still open and waits until none is.

use orderer_exchange::{GatewayClient, Reply};
use orderer_ledger::{Cmd, Side};
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn die(msg: &str) -> ! {
    eprintln!("exchange-load: {msg}");
    exit(2)
}

fn groups(s: &str) -> Vec<Vec<SocketAddr>> {
    s.split(';')
        .map(|g| {
            g.split(',')
                .map(|a| {
                    a.parse()
                        .unwrap_or_else(|_| die(&format!("bad address {a}")))
                })
                .collect()
        })
        .collect()
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

type Out = Arc<Mutex<std::fs::File>>;

fn note(out: &Out, line: String) {
    let mut f = out.lock().unwrap();
    let _ = writeln!(f, "{line}");
}

const FUND: [(u32, u128); 3] = [(1, 10_000_000), (2, 10_000_000), (3, 1_000_000)];

fn deposit_all(addrs: &[Vec<SocketAddr>], accounts: u64, out: &Out, client: u64) {
    let n = addrs.len() as u64;
    let mut gws: Vec<GatewayClient> = addrs
        .iter()
        .map(|a| GatewayClient::new(a.clone()))
        .collect();
    let mut seq = vec![0u64; addrs.len()];
    for account in 1..=accounts {
        let s = (account % n) as usize;
        for (asset, amount) in FUND {
            seq[s] += 1;
            let ext = 1_000_000 + account * 10 + asset as u64;
            let r = gws[s].cmd(
                client + s as u64,
                seq[s],
                &Cmd::Deposit {
                    account,
                    asset,
                    amount,
                    external_id: ext,
                },
            );
            match r {
                Ok(Reply::Other(ref t)) if t == "Deposited" => {
                    note(out, format!("D {asset} {amount}"))
                }
                // A replay after a restart of this tool: the ledger already has it.
                Ok(Reply::Other(ref t)) if t == "Duplicate" => {
                    note(out, format!("D {asset} {amount}"))
                }
                other => die(&format!("deposit for account {account}: {other:?}")),
            }
        }
    }
    println!("deposited {accounts} accounts");
}

#[derive(Default)]
struct Counts {
    reserves: u64,
    rejected: u64,
    cancels: u64,
    withdraws: u64,
    deposits: u64,
}

#[allow(clippy::too_many_arguments)]
fn worker(
    addrs: Vec<Vec<SocketAddr>>,
    accounts: u64,
    secs: u64,
    seed: u64,
    client: u64,
    out: Out,
) -> Counts {
    let n = addrs.len() as u64;
    let mut gws: Vec<GatewayClient> = addrs.into_iter().map(GatewayClient::new).collect();
    let mut seq = vec![0u64; gws.len()];
    let mut rng = Rng(seed | 1);
    let mut open: Vec<(u64, u64)> = Vec::new(); // (account, order)
    let mut c = Counts::default();
    let mut ext = client << 24;
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        let account = 1 + rng.below(accounts);
        let s = (account % n) as usize;
        let cid = client + s as u64;
        match rng.below(100) {
            0..=59 => {
                let symbol = if rng.below(2) == 0 { 10 } else { 11 };
                let side = if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let price = 95 + rng.below(11);
                let qty = 1 + rng.below(5);
                seq[s] += 1;
                match gws[s].cmd(
                    cid,
                    seq[s],
                    &Cmd::Reserve {
                        account,
                        symbol,
                        side,
                        price,
                        qty,
                    },
                ) {
                    Ok(Reply::Reserved { order, .. }) => {
                        note(&out, format!("R {order} {qty}"));
                        open.push((account, order));
                        c.reserves += 1;
                    }
                    Ok(Reply::Rejected(_)) => c.rejected += 1,
                    other => die(&format!("reserve: {other:?}")),
                }
            }
            60..=84 if !open.is_empty() => {
                let i = rng.below(open.len() as u64) as usize;
                let (acct, order) = open.swap_remove(i);
                let sh = ((order >> 48) as usize) % gws.len();
                seq[sh] += 1;
                let r = gws[sh].cmd(
                    client + sh as u64,
                    seq[sh],
                    &Cmd::Cancel {
                        account: acct,
                        order,
                    },
                );
                match r {
                    Ok(Reply::CancelQueued { .. }) | Ok(Reply::Rejected(_)) => c.cancels += 1,
                    other => die(&format!("cancel: {other:?}")),
                }
            }
            85..=92 => {
                let (asset, _) = FUND[rng.below(3) as usize];
                let amount = 1 + rng.below(500) as u128;
                ext += 1;
                seq[s] += 1;
                match gws[s].cmd(
                    cid,
                    seq[s],
                    &Cmd::Withdraw {
                        account,
                        asset,
                        amount,
                        external_id: ext,
                    },
                ) {
                    Ok(Reply::Other(ref t)) if t == "WithdrawHeld" => {
                        let paid = rng.below(2) == 0;
                        seq[s] += 1;
                        match gws[s].cmd(
                            cid,
                            seq[s],
                            &Cmd::Finalize {
                                external_id: ext,
                                paid,
                            },
                        ) {
                            Ok(Reply::Other(ref t)) if t == "WithdrawDone" => {
                                if paid {
                                    note(&out, format!("P {asset} {amount}"));
                                }
                                c.withdraws += 1;
                            }
                            other => die(&format!("finalize: {other:?}")),
                        }
                    }
                    Ok(Reply::Rejected(_)) => {}
                    other => die(&format!("withdraw: {other:?}")),
                }
            }
            _ => {
                let (asset, _) = FUND[rng.below(3) as usize];
                let amount = 1 + rng.below(5_000) as u128;
                ext += 1;
                seq[s] += 1;
                match gws[s].cmd(
                    cid,
                    seq[s],
                    &Cmd::Deposit {
                        account,
                        asset,
                        amount,
                        external_id: ext,
                    },
                ) {
                    Ok(Reply::Other(ref t)) if t == "Deposited" => {
                        note(&out, format!("D {asset} {amount}"));
                        c.deposits += 1;
                    }
                    other => die(&format!("deposit: {other:?}")),
                }
            }
        }
    }
    c
}

fn drain(addrs: Vec<Vec<SocketAddr>>, orders: Vec<u64>, timeout: u64, client: u64) {
    let n = addrs.len();
    let mut gws: Vec<GatewayClient> = addrs.into_iter().map(GatewayClient::new).collect();
    let mut seq = vec![0u64; n];
    let end = Instant::now() + Duration::from_secs(timeout);
    loop {
        let mut open = 0;
        for &o in &orders {
            let s = ((o >> 48) as usize) % n;
            match gws[s].order(true, o) {
                Ok(Some(info)) => {
                    open += 1;
                    if !info.cancelling {
                        seq[s] += 1;
                        let _ = gws[s].cmd(
                            client + s as u64,
                            seq[s],
                            &Cmd::Cancel {
                                account: info.account,
                                order: o,
                            },
                        );
                    }
                }
                Ok(None) => {}
                Err(e) => die(&format!("order query: {e}")),
            }
        }
        if open == 0 {
            println!("drained: no acknowledged order is open");
            return;
        }
        if Instant::now() > end {
            eprintln!("drain: {open} orders still open after {timeout}s");
            exit(1);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .unwrap_or_else(|| die("mode: deposit|run|drain"));
    let (mut ledgers, mut accounts, mut secs, mut threads, mut acked) =
        (String::new(), 50u64, 20u64, 4u64, String::new());
    let (mut seed, mut timeout) = (0x9E37_79B9_7F4A_7C15u64, 120u64);
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--ledgers" => ledgers = v("--ledgers"),
            "--accounts" => {
                accounts = v("--accounts")
                    .parse()
                    .unwrap_or_else(|_| die("--accounts"))
            }
            "--secs" => secs = v("--secs").parse().unwrap_or_else(|_| die("--secs")),
            "--threads" => threads = v("--threads").parse().unwrap_or_else(|_| die("--threads")),
            "--acked" => acked = v("--acked"),
            "--seed" => seed = v("--seed").parse().unwrap_or_else(|_| die("--seed")),
            "--timeout" => timeout = v("--timeout").parse().unwrap_or_else(|_| die("--timeout")),
            other => die(&format!("unknown argument {other}")),
        }
    }
    if ledgers.is_empty() || acked.is_empty() {
        die("--ledgers and --acked are required");
    }
    let addrs = groups(&ledgers);
    // Session ids must be unique per run: a restarted tool must not reuse the sequence numbers.
    let base = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
        << 8)
        ^ ((std::process::id() as u64) << 40);
    let open = |append: bool| -> Out {
        Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(append)
                .write(true)
                .truncate(!append)
                .open(&acked)
                .unwrap_or_else(|e| die(&format!("{acked}: {e}"))),
        ))
    };
    match mode.as_str() {
        "deposit" => deposit_all(&addrs, accounts, &open(true), base & !0xff),
        "run" => {
            let out = open(true);
            let hs: Vec<_> = (0..threads)
                .map(|t| {
                    let (a, o) = (addrs.clone(), out.clone());
                    let client = (base.wrapping_add(t << 20)) & !0xff;
                    std::thread::spawn(move || {
                        worker(a, accounts, secs, seed.wrapping_add(t * 7919), client, o)
                    })
                })
                .collect();
            let mut tot = Counts::default();
            for h in hs {
                let c = h.join().unwrap_or_else(|_| die("worker panicked"));
                tot.reserves += c.reserves;
                tot.rejected += c.rejected;
                tot.cancels += c.cancels;
                tot.withdraws += c.withdraws;
                tot.deposits += c.deposits;
            }
            println!(
                "load done: {} reserves acked ({} rejected), {} cancels, {} withdrawals, {} deposits",
                tot.reserves, tot.rejected, tot.cancels, tot.withdraws, tot.deposits
            );
        }
        "drain" => {
            let text =
                std::fs::read_to_string(&acked).unwrap_or_else(|e| die(&format!("{acked}: {e}")));
            let orders: Vec<u64> = text
                .lines()
                .filter_map(|l| {
                    let f: Vec<&str> = l.split_whitespace().collect();
                    (f.first() == Some(&"R"))
                        .then(|| f.get(1).and_then(|x| x.parse().ok()))
                        .flatten()
                })
                .collect();
            drain(addrs, orders, timeout, base & !0xff);
        }
        other => die(&format!("unknown mode {other}")),
    }
}
