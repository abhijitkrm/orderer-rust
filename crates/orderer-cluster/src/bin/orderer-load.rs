//! orderer-load — load generator, cluster inspector and failover verifier.
//!
//!   orderer-load run    --clients A,B,C --secs S --acked FILE [--maybe FILE] [--batch N] [--threads T]
//!   orderer-load status --clients A,B,C
//!   orderer-load settle --clients A,B,C [--timeout S]
//!   orderer-load verify --acked FILE [--maybe FILE] --applied F1,F2,...
//!
//! `run` writes the id of every acknowledged batch to the acked file as soon
//! as it is acknowledged, and ids with an unknown outcome to the maybe file.
//! `verify` then proves that nothing acknowledged was lost, nothing was
//! applied twice or invented, and every replica applied the same sequence.

use orderer_cluster::client::{Client, ClientError, Conn};
use orderer_core::*;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::process::exit;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn die(msg: &str) -> ! {
    eprintln!("orderer-load: {msg}");
    exit(2)
}

/// The pre-shared cluster key, if `ORDERER_KEY` is set.
fn key_from_env() -> Option<Vec<u8>> {
    std::env::var("ORDERER_KEY")
        .ok()
        .filter(|k| !k.is_empty())
        .map(String::into_bytes)
}

fn addrs(s: &str) -> Vec<SocketAddr> {
    s.split(',')
        .map(|a| {
            a.parse()
                .unwrap_or_else(|_| die(&format!("bad address {a}")))
        })
        .collect()
}

fn read_ids(path: &str) -> Vec<u64> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| l.trim().parse().ok())
        .collect()
}

fn read_applied(path: &str) -> Vec<(u64, u64)> {
    let b = std::fs::read(path).unwrap_or_else(|e| die(&format!("{path}: {e}")));
    b.chunks_exact(16)
        .map(|c| {
            (
                u64::from_le_bytes(c[..8].try_into().expect("8")),
                u64::from_le_bytes(c[8..].try_into().expect("8")),
            )
        })
        .collect()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .unwrap_or_else(|| die("mode: run|status|settle|verify"));
    let (mut clients, mut secs, mut acked, mut maybe, mut applied) = (
        String::new(),
        10u64,
        String::new(),
        String::new(),
        String::new(),
    );
    let (mut batch, mut threads, mut timeout) = (50usize, 2usize, 60u64);
    while let Some(a) = args.next() {
        let mut v = |n: &str| {
            args.next()
                .unwrap_or_else(|| die(&format!("{n} needs a value")))
        };
        match a.as_str() {
            "--clients" => clients = v("--clients"),
            "--secs" => secs = v("--secs").parse().unwrap_or_else(|_| die("--secs")),
            "--acked" => acked = v("--acked"),
            "--maybe" => maybe = v("--maybe"),
            "--applied" => applied = v("--applied"),
            "--batch" => batch = v("--batch").parse().unwrap_or_else(|_| die("--batch")),
            "--threads" => threads = v("--threads").parse().unwrap_or_else(|_| die("--threads")),
            "--timeout" => timeout = v("--timeout").parse().unwrap_or_else(|_| die("--timeout")),
            other => die(&format!("unknown argument {other}")),
        }
    }
    match mode.as_str() {
        "run" => run(addrs(&clients), secs, &acked, &maybe, batch, threads),
        "status" => status(&addrs(&clients)),
        "settle" => settle(&addrs(&clients), timeout),
        "verify" => verify(&acked, &maybe, &applied),
        other => die(&format!("unknown mode {other}")),
    }
}

fn run(
    clients: Vec<SocketAddr>,
    secs: u64,
    acked: &str,
    maybe: &str,
    batch: usize,
    threads: usize,
) {
    if acked.is_empty() {
        die("--acked is required");
    }
    let open = |p: &str| -> Option<Arc<Mutex<File>>> {
        (!p.is_empty()).then(|| {
            Arc::new(Mutex::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(p)
                    .unwrap_or_else(|e| die(&format!("{p}: {e}"))),
            ))
        })
    };
    let (acked_f, maybe_f) = (open(acked).unwrap(), open(maybe));
    let end = Instant::now() + Duration::from_secs(secs);
    let counts = Arc::new(Mutex::new((0u64, 0u64, 0u64)));
    let ts: Vec<_> = (0..threads)
        .map(|t| {
            let (clients, acked_f, maybe_f, counts) = (
                clients.clone(),
                acked_f.clone(),
                maybe_f.clone(),
                counts.clone(),
            );
            std::thread::spawn(move || {
                let mut c = Client::new(
                    clients
                        .iter()
                        .copied()
                        .enumerate()
                        .map(|(i, a)| (i as u8, a))
                        .collect(),
                )
                // One session per thread: retries are exactly-once.
                .with_session(t as u64 + 1);
                if let Some(k) = key_from_env() {
                    c = c.with_key(k);
                }
                let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ (t as u64 + 1);
                let mut seq = 0u64;
                while Instant::now() < end {
                    seq += 1;
                    // Batch id doubles as its first order's id; the rest are
                    // derived from it so every order id is unique.
                    let id = (((t as u64 + 1) << 40) | seq) << 12;
                    let cmds: Vec<(Symbol, Command)> = (0..batch)
                        .map(|k| {
                            x ^= x << 13;
                            x ^= x >> 7;
                            x ^= x << 17;
                            let side = if x & 1 == 0 { Side::Bid } else { Side::Ask };
                            let price = 10_000 + ((x >> 8) % 8) as i64 - 4;
                            (
                                (x % 16) as u32,
                                Command::new(
                                    id | k as u64,
                                    side,
                                    price,
                                    1 + (x >> 20) % 10,
                                    Tif::Gtc,
                                ),
                            )
                        })
                        .collect();
                    match c.propose_session(&cmds) {
                        Ok(_) => {
                            let _ = writeln!(acked_f.lock().unwrap(), "{id}");
                            counts.lock().unwrap().0 += 1;
                        }
                        Err(ClientError::Unknown(_)) => {
                            if let Some(m) = &maybe_f {
                                let _ = writeln!(m.lock().unwrap(), "{id}");
                            }
                            counts.lock().unwrap().1 += 1;
                        }
                        Err(_) => counts.lock().unwrap().2 += 1,
                    }
                }
            })
        })
        .collect();
    for t in ts {
        t.join().unwrap();
    }
    let (ok, unknown, failed) = *counts.lock().unwrap();
    println!("load finished: {ok} acknowledged, {unknown} unknown, {failed} not submitted ({batch} commands each)");
}

fn status(clients: &[SocketAddr]) {
    for a in clients {
        match Conn::connect_with_key(*a, key_from_env().as_deref()).and_then(|mut c| c.status()) {
            Ok(s) => print!("{a}: {s}"),
            Err(e) => println!("{a}: unreachable ({e})"),
        }
    }
}

fn field(s: &str, k: &str) -> Option<String> {
    s.split_whitespace()
        .find_map(|kv| kv.strip_prefix(&format!("{k}=")).map(str::to_string))
}

/// Wait until every node has applied everything committed and all agree.
fn settle(clients: &[SocketAddr], timeout: u64) {
    let end = Instant::now() + Duration::from_secs(timeout);
    let mut last = String::from("no poll completed (a node was unreachable)");
    loop {
        let mut rows = Vec::new();
        let mut ok = true;
        for a in clients {
            match Conn::connect_with_key(*a, key_from_env().as_deref())
                .and_then(|mut c| c.status().and_then(|s| c.hash().map(|h| (s, h))))
            {
                Ok((s, h)) => rows.push((s, h)),
                Err(e) => {
                    ok = false;
                    last = format!("last poll: {a} failed: {e}");
                }
            }
        }
        if ok && rows.len() == clients.len() {
            let applied: Vec<_> = rows.iter().map(|(s, _)| field(s, "applied")).collect();
            let commit: Vec<_> = rows.iter().map(|(s, _)| field(s, "commit")).collect();
            let hashes: Vec<_> = rows
                .iter()
                .map(|(_, h)| h.as_ref().map(|(i, d)| (*i, *d)))
                .collect();
            let halted = rows
                .iter()
                .any(|(s, _)| field(s, "halted").is_some_and(|h| h != "-"));
            if halted {
                println!("FAIL: a node halted");
                exit(1);
            }
            last = format!(
                "last poll: applied {applied:?}, commit {commit:?}, state index {:?}, state digests {}",
                hashes.iter().map(|h| h.map(|x| x.0)).collect::<Vec<_>>(),
                if hashes.iter().all(|h| *h == hashes[0]) { "equal" } else { "DIFFER" }
            );
            if applied.iter().all(|x| *x == applied[0])
                && commit.iter().all(|x| *x == commit[0])
                && applied == commit
                && hashes.iter().all(|h| *h == hashes[0])
                && hashes[0].is_some()
            {
                let (i, d) = hashes[0].unwrap();
                println!(
                    "settled: all {} nodes at index {i}, state digest {}",
                    clients.len(),
                    orderer_cluster::sha256::hex(&d)
                );
                return;
            }
        }
        if Instant::now() > end {
            println!("FAIL: nodes did not settle within {timeout}s ({last})");
            status(clients);
            // And what each one says its state is, to tell "still catching up"
            // from "disagree about the same index".
            for a in clients {
                match Conn::connect_with_key(*a, key_from_env().as_deref())
                    .and_then(|mut c| c.hash())
                {
                    Ok(Some((i, d))) => println!(
                        "{a}: state at index {i} = {}",
                        orderer_cluster::sha256::hex(&d)
                    ),
                    Ok(None) => println!("{a}: no state hash"),
                    Err(e) => println!("{a}: hash unavailable ({e})"),
                }
            }
            exit(1);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn verify(acked: &str, maybe: &str, applied: &str) {
    let acked_ids = read_ids(acked);
    let maybe_ids: HashSet<u64> = read_ids(maybe).into_iter().collect();
    let acked_set: HashSet<u64> = acked_ids.iter().copied().collect();
    if acked_set.len() != acked_ids.len() {
        println!("FAIL: the load generator acknowledged an id twice");
        exit(1);
    }
    let mut first: Option<Vec<(u64, u64)>> = None;
    let mut failures = 0;
    for path in applied.split(',') {
        let log = read_applied(path);
        let ids: Vec<u64> = log.iter().map(|l| l.1).collect();
        let set: HashSet<u64> = ids.iter().copied().collect();
        let mut bad = Vec::new();
        if set.len() != ids.len() {
            bad.push("a batch was applied twice".to_string());
        }
        if !log.windows(2).all(|w| w[0].0 < w[1].0) {
            bad.push("log indexes are not increasing".into());
        }
        let missing = acked_set.iter().filter(|a| !set.contains(a)).count();
        if missing > 0 {
            bad.push(format!("{missing} acknowledged batches are missing"));
        }
        let invented = set
            .iter()
            .filter(|i| !acked_set.contains(i) && !maybe_ids.contains(i))
            .count();
        if invented > 0 {
            bad.push(format!("{invented} applied batches were never submitted"));
        }
        match &first {
            None => first = Some(log.clone()),
            Some(f) if *f != log => {
                bad.push("applied sequence differs from the first node's".into())
            }
            _ => {}
        }
        if bad.is_empty() {
            println!(
                "{path}: ok ({} batches applied, {} acknowledged all present)",
                log.len(),
                acked_set.len()
            );
        } else {
            failures += 1;
            println!("{path}: FAIL: {}", bad.join("; "));
        }
    }
    if failures > 0 {
        exit(1);
    }
    println!(
        "PASS: every acknowledged batch survived on every replica, in the same order, exactly once"
    );
}
