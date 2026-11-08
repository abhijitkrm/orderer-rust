//! exchange-bench: an open-loop (coordinated-omission-free) latency harness for the
//! order path through the ledger gateways.
//!
//!   exchange-bench --config FILE --ledgers A,B,C;D,E,F --tier "A,B,C;D,E,F"
//!                  --rate R [--secs 10] [--warmup 2] [--threads 32] [--accounts 40]
//!                  [--poll-us 500] [--seed X] [--mode phase1] [--label TEXT] [--csv]
//!
//! Requests are due on a fixed schedule, `R` per second. One dispatcher thread
//! releases each request at its due time to a pool of `--threads` workers, each with
//! its own connections; a request's latency runs from its **due time**, not from when a
//! worker got round to it, so a slow system shows its queueing instead of hiding it
//! (coordinated omission). At most `--threads` requests are in flight; if the system
//! cannot keep up, the queue grows and the latency says so ("late starts" counts the
//! requests a worker picked up more than 2 ms after their due time; "dispatcher lag"
//! is the dispatcher's own lateness, which must stay small or the offered rate is wrong).
//! Two things are measured for every `Reserve`:
//!
//! - **order-to-ack**: due time to the ledger's reply (the order is reserved,
//!   replicated and has an id);
//! - **ack-to-resting** (an upper bound): from the reply to the first moment an observer
//!   saw the intake cursor of the matching group that owns the symbol past the order's
//!   outbox index, i.e. the order was committed in that group's log and is being
//!   entered into the book. One observer per (shard, group) reads the cursor from the
//!   applied state (`cursor_local`: no round trip through the log), back to back with
//!   `--poll-us` between reads, so the figure carries one read round trip plus the
//!   pause as slack.
//!
//! The relays (forwarder, settler) and the tick driver must be running; the accounts
//! must be funded (`exchange-load deposit`). Orders are limit orders near one price,
//! so they cross and trade.
//!
//! `--mode phase2` enters the orders at the credit group directly (`SliceClient`,
//! one replicated hop, against slices the accounts must already have: run
//! `exchange-load lend` first) and measures **order-to-ack**, which here is already
//! "committed in the matching group's log" (the group answers after applying the
//! entry). The like-for-like figure in phase 1 is `order-to-in-group`: order-to-ack plus
//! ack-to-resting, per order, an upper bound on the time to be committed at the group.
//! In phase 2 only symbols the credit group owns are traded and ack-to-resting is not
//! reported (it is zero by construction).

use orderer_exchange::cli::{parse_groups, Setup};
use orderer_exchange::{GatewayClient, Reply, SliceClient, TierClient};
use orderer_ledger::{Cmd, Side};
use std::collections::VecDeque;
use std::process::exit;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn die(msg: &str) -> ! {
    eprintln!("exchange-bench: {msg}");
    exit(2)
}

/// The exact percentile `p` (0..=1) of sorted microsecond samples: the smallest
/// value with at least `p` of the samples at or below it.
pub fn pct_us(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
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

/// One acknowledged reserve.
#[derive(Clone, Copy)]
struct Acked {
    shard: usize,
    group: usize,
    outbox: u64,
    at: Instant,
    /// Due time to the ledger's reply.
    ack_us: u64,
}

#[derive(Default)]
struct Out {
    ack_us: Vec<u64>,
    acked: Vec<Acked>,
    sent: u64,
    rejected: u64,
    errors: u64,
    late: u64,
    max_queue_us: u64,
}

/// A request released by the dispatcher at its due time.
struct Job {
    due: Instant,
    n: u64,
}

type Queue = Arc<(Mutex<VecDeque<Job>>, Condvar)>;

fn sleep_until_precise(t: Instant) {
    loop {
        let now = Instant::now();
        if now >= t {
            return;
        }
        let left = t - now;
        if left > Duration::from_millis(3) {
            std::thread::sleep(left - Duration::from_millis(2));
        } else {
            std::hint::spin_loop();
        }
    }
}

/// Releases request `i` at `start + i / rate`. Returns its own worst lateness.
fn dispatcher(q: Queue, rate: f64, start: Instant, end: Instant, done: Arc<AtomicBool>) -> u64 {
    let period = Duration::from_secs_f64(1.0 / rate);
    let mut worst = 0u64;
    let mut i = 0u64;
    loop {
        let due = start + period.mul_f64(i as f64);
        if due >= end {
            break;
        }
        sleep_until_precise(due);
        worst = worst.max(Instant::now().saturating_duration_since(due).as_micros() as u64);
        q.0.lock().unwrap().push_back(Job { due, n: i });
        q.1.notify_one();
        i += 1;
    }
    done.store(true, Ordering::SeqCst);
    q.1.notify_all();
    worst
}

#[allow(clippy::too_many_arguments)]
fn worker(
    q: Queue,
    done: Arc<AtomicBool>,
    warm_until: Instant,
    ledgers: Vec<Vec<std::net::SocketAddr>>,
    auth: orderer_exchange::Auth,
    symbols: Vec<(u32, usize)>,
    accounts: u64,
    seed: u64,
    session: u64,
    slice_tier: Option<Vec<std::net::SocketAddr>>,
) -> Out {
    let mut slice: Option<SliceClient> = slice_tier.map(|a| {
        SliceClient::new(
            TierClient::with_auth(a, auth.clone()),
            SliceClient::fresh_id(),
        )
    });
    let n = ledgers.len() as u64;
    let mut gws: Vec<GatewayClient> = ledgers
        .into_iter()
        .map(|a| GatewayClient::with_auth(a, auth.clone()))
        .collect();
    let mut seq = vec![0u64; gws.len()];
    let mut out = Out::default();
    loop {
        let job = {
            let mut g = q.0.lock().unwrap();
            loop {
                if let Some(j) = g.pop_front() {
                    break Some(j);
                }
                if done.load(Ordering::SeqCst) {
                    break None;
                }
                g = q.1.wait(g).unwrap();
            }
        };
        let Some(job) = job else { return out };
        let began = Instant::now();
        // the request's content is a function of its number, so runs are repeatable
        let mut rng = Rng(seed.wrapping_add(job.n.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1);
        let account = 1 + rng.below(accounts);
        let s = (account % n) as usize;
        let (symbol, group) = symbols[rng.below(symbols.len() as u64) as usize];
        let side = if rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let price = 95 + rng.below(11);
        let qty = 1 + rng.below(5);
        if let Some(sc) = slice.as_mut() {
            let r = sc.enter(account, symbol, side, price, qty, false);
            let finished = Instant::now();
            if job.due < warm_until {
                continue;
            }
            out.sent += 1;
            let queued = began.saturating_duration_since(job.due);
            if queued > Duration::from_millis(2) {
                out.late += 1;
            }
            out.max_queue_us = out.max_queue_us.max(queued.as_micros() as u64);
            match r {
                Ok(Ok(_)) => out
                    .ack_us
                    .push(finished.saturating_duration_since(job.due).as_micros() as u64),
                Ok(Err(_)) => out.rejected += 1,
                Err(_) => {
                    out.errors += 1;
                    let _ = sc.resolve_pending();
                }
            }
            continue;
        }
        seq[s] += 1;
        let r = gws[s].cmd(
            session + s as u64,
            seq[s],
            &Cmd::Reserve {
                account,
                symbol,
                side,
                price,
                qty,
            },
        );
        let finished = Instant::now();
        if job.due < warm_until {
            continue;
        }
        out.sent += 1;
        let queued = began.saturating_duration_since(job.due);
        if queued > Duration::from_millis(2) {
            out.late += 1;
        }
        out.max_queue_us = out.max_queue_us.max(queued.as_micros() as u64);
        match r {
            Ok(Reply::Reserved { outbox, .. }) => {
                let ack = finished.saturating_duration_since(job.due).as_micros() as u64;
                out.ack_us.push(ack);
                out.acked.push(Acked {
                    shard: s,
                    group,
                    outbox,
                    at: finished,
                    ack_us: ack,
                });
            }
            Ok(Reply::Rejected(_)) => out.rejected += 1,
            _ => out.errors += 1,
        }
    }
}

fn report(label: &str, what: &str, mut us: Vec<u64>, csv: bool) {
    us.sort_unstable();
    let row = [
        pct_us(&us, 0.50),
        pct_us(&us, 0.90),
        pct_us(&us, 0.99),
        pct_us(&us, 0.999),
        us.last().copied().unwrap_or(0),
    ];
    if csv {
        println!(
            "{label},{what},{},{},{},{},{},{}",
            us.len(),
            row[0],
            row[1],
            row[2],
            row[3],
            row[4]
        );
    } else {
        println!(
            "  {what:<16} n={:<7} p50 {:>8.2} ms  p90 {:>8.2}  p99 {:>8.2}  p99.9 {:>8.2}  max {:>8.2}",
            us.len(),
            row[0] as f64 / 1e3,
            row[1] as f64 / 1e3,
            row[2] as f64 / 1e3,
            row[3] as f64 / 1e3,
            row[4] as f64 / 1e3
        );
    }
}

fn main() {
    let (mut config, mut ledgers, mut tier) = (String::new(), String::new(), String::new());
    let (mut rate, mut secs, mut warm, mut threads, mut accounts) =
        (0f64, 10f64, 2f64, 32u64, 40u64);
    let (mut poll_us, mut seed, mut mode, mut label, mut csv) = (
        500u64,
        0x9E37_79B9_7F4A_7C15u64,
        "phase1".to_string(),
        String::new(),
        false,
    );
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--config" => config = v("--config"),
            "--ledgers" => ledgers = v("--ledgers"),
            "--tier" => tier = v("--tier"),
            "--rate" => rate = v("--rate").parse().unwrap_or_else(|_| die("--rate")),
            "--secs" => secs = v("--secs").parse().unwrap_or_else(|_| die("--secs")),
            "--warmup" => warm = v("--warmup").parse().unwrap_or_else(|_| die("--warmup")),
            "--threads" => threads = v("--threads").parse().unwrap_or_else(|_| die("--threads")),
            "--accounts" => {
                accounts = v("--accounts")
                    .parse()
                    .unwrap_or_else(|_| die("--accounts"))
            }
            "--poll-us" => poll_us = v("--poll-us").parse().unwrap_or_else(|_| die("--poll-us")),
            "--seed" => seed = v("--seed").parse().unwrap_or_else(|_| die("--seed")),
            "--mode" => mode = v("--mode"),
            "--label" => label = v("--label"),
            "--csv" => csv = true,
            other => die(&format!("unknown argument {other}")),
        }
    }
    if mode != "phase1" && mode != "phase2" {
        die("--mode is phase1 or phase2");
    }
    if config.is_empty() || ledgers.is_empty() || tier.is_empty() || rate <= 0.0 {
        die("--config, --ledgers, --tier and --rate are required");
    }
    if warm >= secs {
        die("--warmup must be shorter than --secs");
    }
    let setup = Setup::load(&config).unwrap_or_else(|e| die(&e));
    let (cfg, auth) = (setup.cfg, setup.auth);
    let ledger_addrs = parse_groups(&ledgers).unwrap_or_else(|e| die(&e));
    let tier_addrs = parse_groups(&tier).unwrap_or_else(|e| die(&e));
    if ledger_addrs.len() != cfg.ledger_shards as usize
        || tier_addrs.len() != cfg.matching_groups as usize
    {
        die("--ledgers and --tier must list every shard and every group of the config");
    }
    let phase2 = mode == "phase2";
    let credit = cfg.credit.map(|c| c.group as usize);
    if phase2 && credit.is_none() {
        die("--mode phase2 needs credit_group in the config");
    }
    let symbols: Vec<(u32, usize)> = cfg
        .symbols
        .iter()
        .map(|s| (s.id, cfg.group_of(s.id) as usize))
        .filter(|(_, g)| !phase2 || Some(*g) == credit)
        .collect();
    if symbols.is_empty() {
        die("the credit group owns no symbol");
    }
    let slice_tier = if phase2 {
        Some(tier_addrs[credit.expect("checked")].clone())
    } else {
        None
    };

    let session = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
        << 8)
        ^ ((std::process::id() as u64) << 40);
    let start = Instant::now() + Duration::from_millis(300);
    let warm_until = start + Duration::from_secs_f64(warm);
    let end = start + Duration::from_secs_f64(secs);

    // observers: one per (ledger shard, matching group), each reading that group's intake
    // cursor for that shard from its applied state, recording when it moves
    let stop = Arc::new(AtomicBool::new(false));
    let shards = cfg.ledger_shards as usize;
    let groups = tier_addrs.len();
    let latest: Arc<Vec<AtomicU64>> =
        Arc::new((0..shards * groups).map(|_| AtomicU64::new(0)).collect());
    let observers: Vec<_> = (0..shards * groups)
        .map(|o| {
            let (stop, latest, auth) = (stop.clone(), latest.clone(), auth.clone());
            let addrs = tier_addrs[o % groups].clone();
            let shard = o / groups;
            std::thread::spawn(move || {
                let mut t = TierClient::with_auth(addrs, auth);
                t.budget = Duration::from_secs(5);
                let mut seen: Vec<(Instant, u64)> = Vec::new();
                let mut last = u64::MAX;
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(Ok(c)) = t.cursor_local(shard as u16) {
                        if c != last {
                            seen.push((Instant::now(), c));
                            last = c;
                            latest[o].store(c, Ordering::SeqCst);
                        }
                    }
                    if poll_us > 0 {
                        std::thread::sleep(Duration::from_micros(poll_us));
                    }
                }
                seen
            })
        })
        .collect();

    let q: Queue = Arc::new((Mutex::new(VecDeque::new()), Condvar::new()));
    let done = Arc::new(AtomicBool::new(false));
    let disp = {
        let (q, done) = (q.clone(), done.clone());
        std::thread::spawn(move || dispatcher(q, rate, start, end, done))
    };
    let hs: Vec<_> = (0..threads)
        .map(|k| {
            let (q, done) = (q.clone(), done.clone());
            let (l, a, sy) = (ledger_addrs.clone(), auth.clone(), symbols.clone());
            let st = slice_tier.clone();
            let sess = (session.wrapping_add(k << 20)) & !0xff;
            std::thread::spawn(move || {
                worker(q, done, warm_until, l, a, sy, accounts, seed, sess, st)
            })
        })
        .collect();
    let dispatcher_lag_us = disp.join().unwrap_or_else(|_| die("dispatcher panicked"));
    let mut tot = Out::default();
    for h in hs {
        let o = h.join().unwrap_or_else(|_| die("worker panicked"));
        tot.ack_us.extend(o.ack_us);
        tot.acked.extend(o.acked);
        tot.sent += o.sent;
        tot.rejected += o.rejected;
        tot.errors += o.errors;
        tot.late += o.late;
        tot.max_queue_us = tot.max_queue_us.max(o.max_queue_us);
    }

    // let the observers see the tail: wait until every group has passed every
    // acknowledged order (or give up)
    let mut need = vec![0u64; shards * groups];
    for a in &tot.acked {
        let o = a.shard * groups + a.group;
        need[o] = need[o].max(a.outbox + 1);
    }
    let give_up = Instant::now() + Duration::from_secs(15);
    while need
        .iter()
        .enumerate()
        .any(|(o, &n)| latest[o].load(Ordering::SeqCst) < n)
        && Instant::now() < give_up
    {
        std::thread::sleep(Duration::from_millis(20));
    }
    stop.store(true, Ordering::Relaxed);
    let seen: Vec<Vec<(Instant, u64)>> = observers
        .into_iter()
        .map(|h| h.join().unwrap_or_else(|_| die("observer panicked")))
        .collect();
    let polls: usize = seen.iter().map(|v| v.len()).sum();

    // resting: the first observation that shows the group's cursor past the order
    let mut rest_us = Vec::with_capacity(tot.acked.len());
    // due time to "committed in the matching group's log" (ack + ack-to-resting)
    let mut in_group_us = Vec::with_capacity(tot.acked.len());
    let mut unresolved = 0u64;
    for a in &tot.acked {
        let v = &seen[a.shard * groups + a.group];
        let first = v.partition_point(|&(_, c)| c <= a.outbox);
        match v.get(first) {
            Some((t, _)) => {
                let rest = t.saturating_duration_since(a.at).as_micros() as u64;
                rest_us.push(rest);
                in_group_us.push(a.ack_us + rest);
            }
            None => unresolved += 1,
        }
    }
    let window = (secs - warm).max(f64::EPSILON);
    let achieved = tot.ack_us.len() as f64 / window;
    if csv {
        println!("label,what,n,p50_us,p90_us,p99_us,p999_us,max_us");
    } else {
        println!(
            "{}offered {:.0}/s, {} workers, {:.0}s (+{:.0}s warm-up): sent {}, acked {} ({:.0}/s), rejected {}, errors {}, late starts {} (max queue wait {:.2} ms), dispatcher lag {:.2} ms, {} cursor moves seen",
            if label.is_empty() { String::new() } else { format!("[{label}] ") },
            rate,
            threads,
            secs - warm,
            warm,
            tot.sent,
            tot.ack_us.len(),
            achieved,
            tot.rejected,
            tot.errors,
            tot.late,
            tot.max_queue_us as f64 / 1e3,
            dispatcher_lag_us as f64 / 1e3,
            polls
        );
    }
    report(&label, "order-to-ack", tot.ack_us, csv);
    if !phase2 {
        report(&label, "ack-to-resting", rest_us, csv);
        report(&label, "order-to-in-group", in_group_us, csv);
    }
    if unresolved > 0 {
        eprintln!("exchange-bench: {unresolved} acknowledged orders were never seen accepted by matching within 15 s of the end");
    }
    if dispatcher_lag_us > 2_000 {
        eprintln!("exchange-bench: WARNING: the dispatcher was up to {:.2} ms late: the offered schedule is not what was asked for", dispatcher_lag_us as f64 / 1e3);
    }
    if tot.late * 100 > tot.sent.max(1) {
        eprintln!(
            "exchange-bench: NOTE: {} of {} requests waited more than 2 ms for a free worker: the system (or --threads) is saturated at this rate, so the latencies include that queueing, which is the point",
            tot.late, tot.sent
        );
    }
}

#[cfg(test)]
mod tests {
    use super::pct_us;

    #[test]
    fn percentiles_are_exact() {
        let v: Vec<u64> = (1..=100).collect();
        assert_eq!(pct_us(&v, 0.50), 50);
        assert_eq!(pct_us(&v, 0.99), 99);
        assert_eq!(pct_us(&v, 0.999), 100);
        assert_eq!(pct_us(&v, 1.0), 100);
        assert_eq!(pct_us(&[7], 0.5), 7);
        assert_eq!(pct_us(&[], 0.5), 0);
        // 1000 samples: p99.9 is the 999th value
        let w: Vec<u64> = (1..=1000).collect();
        assert_eq!(pct_us(&w, 0.999), 999);
    }
}
