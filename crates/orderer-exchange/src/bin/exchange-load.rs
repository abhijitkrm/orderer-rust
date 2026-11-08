//! exchange-load: a load generator for the exchange, through the ledger gateways.
//!
//!   exchange-load deposit --config FILE --ledgers A,B,C;D,E,F --accounts N --acked FILE
//!   exchange-load run     --config FILE --ledgers ... --accounts N --secs S --threads T --acked FILE [--seed X]
//!   exchange-load drain   --ledgers ... --acked FILE [--timeout SECS] [--tier ...]
//!   exchange-load lend    --ledgers ... --tier ... --accounts N --lend AMOUNT [--timeout SECS]
//!                         lend AMOUNT of every asset of every account to the credit
//!                         group and wait until the group's slices show it (phase 2
//!                         benchmarks start from funded slices)
//!
//! Phase 2: with `--tier "A,B,C;D,E,F" --slice-pct P` (and `credit_group` in the
//! config), P percent of the orders for symbols a credit group owns are entered at
//! that group directly, against the credit slices the slice driver lends it; an order
//! the group refuses for lack of credit goes through the ledger (phase 1) instead, and
//! the refusal is what makes the driver lend. `--chaos N` sends every Nth slice request
//! twice and fails if the two answers differ (exactly-once). `--withdraw-max N` raises the largest
//! withdrawal (default 500) so withdrawals compete with lent funds.
//!
//! Account `a` lives on shard `a % shards`. `deposit` funds every account
//! (idempotent: fixed external ids). `run` issues random reserves, cancels,
//! deposits and withdrawals, each as a session command that is retried until it
//! has a definite answer, and appends what was acknowledged to FILE:
//! `R order qty` (a reserve), `D asset amount` (a deposit), `P asset amount` (a
//! withdrawal that was paid out), `S order qty account` (an order entered at the credit
//! group). `drain` cancels every acknowledged order that is still open and waits until
//! none is.

use orderer_cluster::shard::ShardMap;
use orderer_exchange::cli::{parse_groups, Setup};
use orderer_exchange::{Auth, GatewayClient, Reply, SliceClient, TierClient};
use orderer_ledger::Slice;
use orderer_ledger::{Cmd, Side};
use std::fs::OpenOptions;
use std::io::Write;
use std::net::SocketAddr;
use std::process::exit;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn die(msg: &str) -> ! {
    eprintln!("exchange-load: {msg}");
    exit(2)
}

/// The configuration this run uses: symbols to trade, assets to fund, credentials.
struct Ctx {
    symbols: Vec<u32>,
    assets: Vec<u32>,
    auth: Auth,
    /// The credit groups, the number of groups (for `group_of`) and the share of
    /// their orders sent through the slice path.
    credit: Vec<usize>,
    groups: u32,
    slice_pct: u64,
    withdraw_max: u64,
    chaos: u32,
    config: orderer_exchange::ExchangeConfig,
}

static CTX: OnceLock<Ctx> = OnceLock::new();

fn ctx() -> &'static Ctx {
    CTX.get().expect("context is set in main")
}

fn gateway(addrs: Vec<SocketAddr>) -> GatewayClient {
    GatewayClient::with_auth(addrs, ctx().auth.clone())
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

/// What every account is funded with: 10M of each asset.
fn fund() -> Vec<(u32, u128)> {
    ctx().assets.iter().map(|&a| (a, 10_000_000)).collect()
}

fn deposit_all(addrs: &[Vec<SocketAddr>], accounts: u64, out: &Out, client: u64) {
    let n = addrs.len() as u64;
    let mut gws: Vec<GatewayClient> = addrs.iter().map(|a| gateway(a.clone())).collect();
    let mut seq = vec![0u64; addrs.len()];
    for account in 1..=accounts {
        let s = (account % n) as usize;
        for (asset, amount) in fund() {
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
    slice_orders: u64,
    slice_refused: u64,
}

#[allow(clippy::too_many_arguments)]
fn worker(
    addrs: Vec<Vec<SocketAddr>>,
    tiers: Vec<Vec<SocketAddr>>,
    accounts: u64,
    secs: u64,
    seed: u64,
    client: u64,
    out: Out,
) -> Counts {
    let n = addrs.len() as u64;
    let mut gws: Vec<GatewayClient> = addrs.into_iter().map(gateway).collect();
    let mut seq = vec![0u64; gws.len()];
    let mut rng = Rng(seed | 1);
    // (account, order, entered at the credit group)
    let mut open: Vec<(u64, u64, bool)> = Vec::new();
    let mut c = Counts::default();
    // one client per credit group (indexed by group)
    let mut slices: Vec<Option<SliceClient>> = (0..tiers.len())
        .map(|g| {
            if ctx().credit.contains(&g) && ctx().slice_pct > 0 {
                let mut sc = SliceClient::new(
                    TierClient::with_auth(tiers[g].clone(), ctx().auth.clone()),
                    SliceClient::fresh_id(),
                );
                sc.chaos = ctx().chaos;
                Some(sc)
            } else {
                None
            }
        })
        .collect();
    let map = ShardMap::new(ctx().groups);
    let mut ext = client << 24;
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        let account = 1 + rng.below(accounts);
        let s = (account % n) as usize;
        let cid = client + s as u64;
        match rng.below(100) {
            0..=59 => {
                let symbol = ctx().symbols[rng.below(ctx().symbols.len() as u64) as usize];
                let side = if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let price = 95 + rng.below(11);
                let qty = 1 + rng.below(5);
                // phase 2: straight to the credit group, if it owns the symbol
                let g = map.group_of(symbol) as usize;
                if let Some(sc) = slices.get_mut(g).and_then(|c| c.as_mut()) {
                    if rng.below(100) < ctx().slice_pct {
                        let ioc = rng.below(10) == 0;
                        let mut r = sc.enter(account, symbol, side, price, qty, ioc);
                        // no definite answer (leaders being killed): repeat the same request
                        while r.is_err() {
                            match sc.resolve_pending() {
                                Ok(Some((_, res))) => r = Ok(res),
                                Ok(None) => break,
                                Err(_) => std::thread::sleep(Duration::from_millis(200)),
                            }
                        }
                        match r {
                            Ok(Ok(order)) => {
                                note(&out, format!("S {order} {qty} {account}"));
                                if !ioc {
                                    open.push((account, order, true));
                                }
                                c.slice_orders += 1;
                                continue;
                            }
                            // no credit: fall through to phase 1 (the refusal is the
                            // driver's cue to lend)
                            Ok(Err(orderer_ledger::Reject::Insufficient)) => c.slice_refused += 1,
                            Ok(Err(_)) => {
                                c.slice_refused += 1;
                                continue;
                            }
                            Err(e) => die(&format!("slice order: {e}")),
                        }
                    }
                }
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
                        open.push((account, order, false));
                        c.reserves += 1;
                    }
                    Ok(Reply::Rejected(_)) => c.rejected += 1,
                    other => die(&format!("reserve: {other:?}")),
                }
            }
            60..=84 if !open.is_empty() => {
                let i = rng.below(open.len() as u64) as usize;
                let (acct, order, at_group) = open.swap_remove(i);
                if at_group {
                    // cancel at the group; a refusal (already filled) is fine
                    // a slice order's id carries its credit group in the shard field
                    let g = ((order >> 48) & 0x7fff) as usize;
                    let sc = slices[g]
                        .as_mut()
                        .expect("slice orders need a slice client");
                    let mut r = sc.cancel(acct, order);
                    while r.is_err() {
                        match sc.resolve_pending() {
                            Ok(Some((_, res))) => r = Ok(res),
                            Ok(None) => break,
                            Err(_) => std::thread::sleep(Duration::from_millis(200)),
                        }
                    }
                    if let Err(e) = r {
                        die(&format!("slice cancel: {e}"));
                    }
                    c.cancels += 1;
                    continue;
                }
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
                let asset = ctx().assets[rng.below(ctx().assets.len() as u64) as usize];
                let amount = 1 + rng.below(ctx().withdraw_max) as u128;
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
                let asset = ctx().assets[rng.below(ctx().assets.len() as u64) as usize];
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

/// Cancel every acknowledged order that is still open at the credit group and wait
/// until none is.
fn drain_slice(tier: Vec<SocketAddr>, orders: Vec<(u64, u64)>, timeout: u64, group: u16) {
    let mut sc = SliceClient::new(
        TierClient::with_auth(tier.clone(), ctx().auth.clone()),
        SliceClient::fresh_id(),
    );
    let mut reader = TierClient::with_auth(tier, ctx().auth.clone());
    let end = Instant::now() + Duration::from_secs(timeout);
    loop {
        let st = match reader.slice_state() {
            Ok(Ok(st)) => st,
            other => die(&format!("slice state: {other:?}")),
        };
        let mut sl = Slice::new(ctx().config.ledger_config(0), group);
        sl.restore(&st.snapshot)
            .unwrap_or_else(|e| die(&format!("slice snapshot: {e}")));
        let mut open = 0;
        for &(order, account) in &orders {
            if sl.order_symbol(order).is_some() {
                open += 1;
                let mut r = sc.cancel(account, order);
                while r.is_err() {
                    match sc.resolve_pending() {
                        Ok(Some((_, res))) => r = Ok(res),
                        Ok(None) => break,
                        Err(_) => std::thread::sleep(Duration::from_millis(200)),
                    }
                }
            }
        }
        if open == 0 {
            println!("drained: no acknowledged slice order is open");
            return;
        }
        if Instant::now() > end {
            eprintln!("drain: {open} slice orders still open after {timeout}s");
            exit(1);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn drain(addrs: Vec<Vec<SocketAddr>>, orders: Vec<u64>, timeout: u64, client: u64) {
    let n = addrs.len();
    let mut gws: Vec<GatewayClient> = addrs.into_iter().map(gateway).collect();
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
    let mut config = String::new();
    let (mut seed, mut timeout) = (0x9E37_79B9_7F4A_7C15u64, 120u64);
    let (mut tier, mut slice_pct, mut withdraw_max) = (String::new(), 0u64, 500u64);
    let mut lend = 0u128;
    let mut chaos = 0u32;
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--config" => config = v("--config"),
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
            "--tier" => tier = v("--tier"),
            "--chaos" => chaos = v("--chaos").parse().unwrap_or_else(|_| die("--chaos")),
            "--lend" => lend = v("--lend").parse().unwrap_or_else(|_| die("--lend")),
            "--slice-pct" => {
                slice_pct = v("--slice-pct")
                    .parse()
                    .unwrap_or_else(|_| die("--slice-pct"))
            }
            "--withdraw-max" => {
                withdraw_max = v("--withdraw-max")
                    .parse()
                    .unwrap_or_else(|_| die("--withdraw-max"))
            }
            "--timeout" => timeout = v("--timeout").parse().unwrap_or_else(|_| die("--timeout")),
            other => die(&format!("unknown argument {other}")),
        }
    }
    if ledgers.is_empty() || acked.is_empty() || config.is_empty() {
        die("--config, --ledgers and --acked are required");
    }
    let setup = Setup::load(&config).unwrap_or_else(|e| die(&e));
    let mut assets: Vec<u32> = setup
        .cfg
        .symbols
        .iter()
        .flat_map(|s| [s.base, s.quote])
        .collect();
    assets.sort_unstable();
    assets.dedup();
    let _ = CTX.set(Ctx {
        symbols: setup.cfg.symbols.iter().map(|s| s.id).collect(),
        assets,
        auth: setup.auth.clone(),
        credit: setup
            .cfg
            .credit_groups()
            .iter()
            .map(|&g| g as usize)
            .collect(),
        groups: setup.cfg.matching_groups,
        slice_pct,
        withdraw_max,
        chaos,
        config: setup.cfg.clone(),
    });
    let tiers = if tier.is_empty() {
        Vec::new()
    } else {
        parse_groups(&tier).unwrap_or_else(|e| die(&e))
    };
    if slice_pct > 0
        && (tiers.len() != setup.cfg.matching_groups as usize || ctx().credit.is_empty())
    {
        die("--slice-pct needs --tier for every group and a credit_group in the config");
    }
    let addrs = parse_groups(&ledgers).unwrap_or_else(|e| die(&e));
    if addrs.len() != setup.cfg.ledger_shards as usize {
        die("--ledgers must list every shard of the config");
    }
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
                    let (a, o, ti) = (addrs.clone(), out.clone(), tiers.clone());
                    let client = (base.wrapping_add(t << 20)) & !0xff;
                    std::thread::spawn(move || {
                        worker(
                            a,
                            ti,
                            accounts,
                            secs,
                            seed.wrapping_add(t * 7919),
                            client,
                            o,
                        )
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
                tot.slice_orders += c.slice_orders;
                tot.slice_refused += c.slice_refused;
            }
            println!(
                "load done: {} reserves acked ({} rejected), {} cancels, {} withdrawals, {} deposits, {} slice orders ({} refused at the group)",
                tot.reserves, tot.rejected, tot.cancels, tot.withdraws, tot.deposits,
                tot.slice_orders, tot.slice_refused
            );
        }
        "lend" => {
            if ctx().credit.is_empty() {
                die("lend needs credit_group in the config");
            }
            let mut gws: Vec<GatewayClient> = addrs.iter().map(|a| gateway(a.clone())).collect();
            let n = gws.len() as u64;
            let mut seq = vec![0u64; gws.len()];
            let client = base & !0xff;
            for &g in &ctx().credit {
                for account in 1..=accounts {
                    let s = (account % n) as usize;
                    for asset in ctx().assets.clone() {
                        seq[s] += 1;
                        match gws[s].cmd(
                            client + s as u64,
                            seq[s],
                            &Cmd::Lend {
                                group: g as u16,
                                account,
                                asset,
                                amount: lend,
                            },
                        ) {
                            Ok(Reply::Other(_)) => {}
                            other => die(&format!("lend for account {account}: {other:?}")),
                        }
                    }
                }
            }
            for &g in &ctx().credit {
                let t = tiers
                    .get(g)
                    .cloned()
                    .unwrap_or_else(|| die("lend needs --tier"));
                let mut reader = TierClient::with_auth(t, ctx().auth.clone());
                let end = Instant::now() + Duration::from_secs(timeout);
                loop {
                    let st = match reader.slice_state() {
                        Ok(Ok(st)) => st,
                        other => die(&format!("slice state: {other:?}")),
                    };
                    let mut sl = Slice::new(ctx().config.ledger_config(0), g as u16);
                    sl.restore(&st.snapshot)
                        .unwrap_or_else(|e| die(&format!("slice snapshot: {e}")));
                    let short = (1..=accounts)
                        .flat_map(|a| ctx().assets.iter().map(move |&x| (a, x)))
                        .filter(|&(a, x)| sl.total(a, x) < lend)
                        .count();
                    if short == 0 {
                        println!(
                            "lent {lend} of every asset to {accounts} accounts; group {g} has it"
                        );
                        break;
                    }
                    if Instant::now() > end {
                        die(&format!(
                            "{short} slices of group {g} still short after {timeout}s: is the forwarder running?"
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(300));
                }
            }
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
            let slice_orders: Vec<(u64, u64)> = text
                .lines()
                .filter_map(|l| {
                    let f: Vec<&str> = l.split_whitespace().collect();
                    match f.as_slice() {
                        ["S", id, _, acct] => Some((id.parse().ok()?, acct.parse().ok()?)),
                        _ => None,
                    }
                })
                .collect();
            drain(addrs, orders, timeout, base & !0xff);
            for &g in &ctx().credit {
                let mine: Vec<(u64, u64)> = slice_orders
                    .iter()
                    .copied()
                    .filter(|(id, _)| ((id >> 48) & 0x7fff) as usize == g)
                    .collect();
                if !mine.is_empty() {
                    let t = tiers
                        .get(g)
                        .cloned()
                        .unwrap_or_else(|| die("drain with slice orders needs --tier"));
                    drain_slice(t, mine, timeout, g as u16);
                }
            }
        }
        other => die(&format!("unknown mode {other}")),
    }
}
