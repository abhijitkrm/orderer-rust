//! orderbench — spec/BENCH.md benchmark protocol.
//!
//! ```text
//! orderbench <prefix> --mode core [--tag NAME]
//! orderbench <prefix> --mode pipe --partitions P [--producers N]
//!            [--journal binary|jsonl|off] [--journal-dir DIR] [--fsync N] [--tag NAME]
//! ```
//!
//! orderer-rust tuning flags (listed in the row's config column when set):
//! ```text
//! --core fifo|noop        matching core (noop: the pipeline alone)
//! --waits relaxed|low     wait strategies (low: router + engines busy-spin)
//! --batch N               producer claim batch (default 64)
//! --ingress N --inbox N --outbox N   ring sizes
//! --baseline OPS          core *untimed* ops/s for the eff column (the core
//!                         row reports it as `untimed=` in its config column)
//! --events on|off         also write event journals (default off — spec/BENCH.md §2.2)
//! --stage-threads J,E     journal / egress thread counts (default 1,1)
//! --placement inline|stage   where command records are encoded (default inline)
//! ```
//!
//! Prints one RESULTS.md row to stdout; environment to stderr.

use std::sync::{Arc, Barrier};
use std::time::Instant;

use orderer::harness::{die, load_corpus, Args, Corpus};
use orderer::*;
use orderer_core::*;

const USAGE: &str = "orderbench <prefix> --mode core|pipe [--partitions P] [--producers N] [--journal binary|jsonl|off] [--journal-dir DIR] [--fsync N] [--tag NAME] [--core fifo|noop] [--waits relaxed|low] [--batch N] [--ingress N] [--inbox N] [--outbox N] [--baseline OPS] [--stats]";

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).ceil() as usize;
    sorted[i.min(sorted.len() - 1)]
}

struct Row {
    ops: usize,
    wall_ns: u64,
    lat: Vec<u64>,
    /// Core mode: ops/s of the same run without per-op clock reads — the
    /// scaling gate's denominator (spec/BENCH.md §5).
    untimed: Option<f64>,
}

fn core_mode(setup: &Corpus, run: &Corpus) -> Row {
    let cfg = setup.book;
    if setup.engine {
        let go = |cmds: &[(Symbol, Command)], eng: &mut Engine, sink: &mut NullSink| {
            for &(s, c) in cmds {
                eng.submit(s, c, sink);
            }
        };
        {
            let (mut eng, mut sink) = (Engine::new(cfg), NullSink::new());
            go(&setup.cmds, &mut eng, &mut sink);
            go(&run.cmds[..run.cmds.len() / 10], &mut eng, &mut sink);
            std::hint::black_box(sink.acc);
        }
        let (mut eng, mut sink) = (Engine::new(cfg), NullSink::new());
        go(&setup.cmds, &mut eng, &mut sink);
        let mut lat = Vec::with_capacity(run.cmds.len());
        let wall = Instant::now();
        for &(s, c) in &run.cmds {
            let t0 = Instant::now();
            eng.submit(s, c, &mut sink);
            lat.push(t0.elapsed().as_nanos() as u64);
        }
        let wall_ns = wall.elapsed().as_nanos() as u64;
        std::hint::black_box(sink.acc);
        // Transparency: the protocol's per-op clock reads cost real time.
        let (mut eng, mut sink) = (Engine::new(cfg), NullSink::new());
        go(&setup.cmds, &mut eng, &mut sink);
        let t = Instant::now();
        go(&run.cmds, &mut eng, &mut sink);
        let untimed = run.cmds.len() as f64 / t.elapsed().as_secs_f64();
        std::hint::black_box(sink.acc);
        Row {
            ops: run.cmds.len(),
            wall_ns,
            lat,
            untimed: Some(untimed),
        }
    } else {
        {
            let (mut book, mut sink) = (OrderBook::new(cfg), NullSink::new());
            for &(_, c) in setup.cmds.iter().chain(&run.cmds[..run.cmds.len() / 10]) {
                book.apply(c, &mut sink);
            }
            std::hint::black_box(sink.acc);
        }
        let (mut book, mut sink) = (OrderBook::new(cfg), NullSink::new());
        for &(_, c) in &setup.cmds {
            book.apply(c, &mut sink);
        }
        let mut lat = Vec::with_capacity(run.cmds.len());
        let wall = Instant::now();
        for &(_, c) in &run.cmds {
            let t0 = Instant::now();
            book.apply(c, &mut sink);
            lat.push(t0.elapsed().as_nanos() as u64);
        }
        let wall_ns = wall.elapsed().as_nanos() as u64;
        std::hint::black_box(sink.acc);
        let (mut book, mut sink) = (OrderBook::new(cfg), NullSink::new());
        for &(_, c) in &setup.cmds {
            book.apply(c, &mut sink);
        }
        let t = Instant::now();
        for &(_, c) in &run.cmds {
            book.apply(c, &mut sink);
        }
        let untimed = run.cmds.len() as f64 / t.elapsed().as_secs_f64();
        std::hint::black_box(sink.acc);
        Row {
            ops: run.cmds.len(),
            wall_ns,
            lat,
            untimed: Some(untimed),
        }
    }
}

struct PipeOpts {
    partitions: u32,
    producers: usize,
    journal: Option<JournalConfig>,
    waits: Waits,
    batch: usize,
    rings: (usize, usize, usize),
    stage_threads: Option<(usize, usize)>,
    placement: JournalPlacement,
    stats: bool,
}

fn build<C: MatchingCore>(book: BookConfig, o: &PipeOpts, metrics: Option<Metrics>) -> Pipeline<C> {
    let mut b = Pipeline::<C>::builder()
        .book_config(book)
        .partitions(o.partitions)
        .waits(o.waits)
        .ring_sizes(o.rings.0, o.rings.1, o.rings.2);
    if let Some((j, e)) = o.stage_threads {
        b = b.stage_threads(j, e);
    }
    b = b.journal_placement(o.placement);
    if let Some(j) = &o.journal {
        b = b.journal(j.clone());
    }
    if let Some(m) = metrics {
        b = b.egress(m);
    }
    b.build().unwrap_or_else(|e| die(e))
}

fn pipe_mode<C: MatchingCore>(setup: &Corpus, run: &Corpus, o: &PipeOpts) -> Row {
    let book = setup.book;
    // 2. warmup on a throwaway pipeline
    {
        let mut p = build::<C>(book, o, None);
        p.publish_batch(&setup.cmds).unwrap();
        p.publish_batch(&run.cmds[..run.cmds.len() / 10]).unwrap();
        p.drain().unwrap();
        p.shutdown().unwrap();
    }
    // 3. fresh pipeline, untimed setup
    let (metrics, results) = Metrics::new(run.cmds.len() + 1024);
    let mut p = build::<C>(book, o, Some(metrics));
    p.publish_batch(&setup.cmds).unwrap();
    p.drain().unwrap();

    // 4. producer k publishes the run commands with symbol % N == k
    let n = o.producers;
    let mut streams: Vec<Vec<(Symbol, Command)>> = vec![Vec::new(); n];
    for &(s, c) in &run.cmds {
        streams[s as usize % n].push((s, c));
    }
    p.set_timestamps(true);
    let start = Arc::new(Barrier::new(n + 1));
    let batch = o.batch;
    let producers: Vec<_> = streams
        .into_iter()
        .map(|stream| {
            let h = p.handle();
            let start = start.clone();
            std::thread::Builder::new()
                .name("orderbench-producer".into())
                .spawn(move || {
                    orderer::affinity::set_current(orderer::affinity::Role::Hot);
                    start.wait();
                    for chunk in stream.chunks(batch) {
                        h.publish_batch(chunk).unwrap();
                    }
                })
                .unwrap()
        })
        .collect();
    start.wait();
    let wall = Instant::now();
    for t in producers {
        t.join().unwrap();
    }
    p.drain().unwrap();
    let wall_ns = wall.elapsed().as_nanos() as u64;
    p.set_timestamps(false);
    if o.stats {
        // fsync behaviour of the measured pipeline (spec/BENCH.md doesn't
        // gate on it; it explains durable rows)
        let st = p.stats();
        let (n, tot, max) = st.partitions.iter().fold((0, 0, 0), |a, s| {
            (
                a.0 + s.fsyncs,
                a.1 + s.fsync_ns_total,
                a.2.max(s.fsync_ns_max),
            )
        });
        eprintln!(
            "stats: fsyncs={n} fsync_mean_us={:.1} fsync_max_us={:.1}",
            if n > 0 {
                tot as f64 / n as f64 / 1e3
            } else {
                0.0
            },
            max as f64 / 1e3
        );
    }
    p.shutdown().unwrap();

    let mut lat: Vec<u64> = results
        .results()
        .into_iter()
        .flat_map(|m| m.latencies)
        .collect();
    lat.sort_unstable();
    Row {
        ops: run.cmds.len(),
        wall_ns,
        lat,
        untimed: None,
    }
}

fn cpu() -> String {
    std::process::Command::new("sysctl")
        .args(["-n", "machdep.cpu.brand_string"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| std::env::consts::ARCH.to_string())
}

fn main() {
    let args = Args::parse(
        USAGE,
        &[
            "--mode",
            "--partitions",
            "--producers",
            "--journal",
            "--journal-dir",
            "--fsync",
            "--tag",
            "--core",
            "--waits",
            "--batch",
            "--ingress",
            "--inbox",
            "--outbox",
            "--baseline",
            "--events",
            "--stage-threads",
            "--placement",
        ],
        &["--stats"],
    );
    let prefix = args
        .positional
        .first()
        .unwrap_or_else(|| die(USAGE))
        .clone();
    let tag = args
        .get("--tag")
        .map(str::to_string)
        .unwrap_or_else(|| prefix.rsplit('/').next().unwrap().to_string());
    let setup = load_corpus(&format!("{prefix}.setup.cmd.jsonl"));
    let run = load_corpus(&format!("{prefix}.run.cmd.jsonl"));
    let mode = args.get("--mode").unwrap_or("core");
    let mut config = Vec::new();

    let (row, p, prod) = match mode {
        "core" => (core_mode(&setup, &run), "-".to_string(), "-".to_string()),
        "pipe" => {
            let partitions: u32 = args.num("--partitions", 1);
            let producers: usize = args.num("--producers", 1);
            let fsync: u64 = args.num("--fsync", 1024);
            let jmode = args.get("--journal").unwrap_or("binary");
            let tmp = std::env::temp_dir().join(format!("orderbench-{}", std::process::id()));
            let dir = args
                .get("--journal-dir")
                .map(Into::into)
                .unwrap_or(tmp.clone());
            let journal = match jmode {
                "off" => None,
                "binary" | "jsonl" => Some(JournalConfig {
                    dir,
                    format: if jmode == "binary" {
                        JournalFormat::Binary
                    } else {
                        JournalFormat::Jsonl
                    },
                    fsync: if fsync == 0 {
                        FsyncPolicy::Never
                    } else {
                        FsyncPolicy::every_n(fsync)
                    },
                    events: args.get("--events").unwrap_or("off") == "on",
                    append: false,
                }),
                j => die(format!("--journal: unknown mode {j}")),
            };
            let waits = match args.get("--waits").unwrap_or("low") {
                "low" => Waits::low_latency(),
                "relaxed" => Waits::relaxed(),
                "yield" => Waits {
                    journal: WaitStrategy::Yield,
                    egress: WaitStrategy::Yield,
                    ..Waits::low_latency()
                },
                "spin" => Waits {
                    journal: WaitStrategy::BusySpin,
                    ..Waits::low_latency()
                },
                "short" => Waits {
                    journal: WaitStrategy::Backoff {
                        spin: 1024,
                        yields: 256,
                        park_min: std::time::Duration::from_micros(5),
                        park_max: std::time::Duration::from_micros(50),
                    },
                    egress: WaitStrategy::Backoff {
                        spin: 256,
                        yields: 64,
                        park_min: std::time::Duration::from_micros(10),
                        park_max: std::time::Duration::from_micros(200),
                    },
                    ..Waits::low_latency()
                },
                w => die(format!("--waits: unknown {w}")),
            };
            for k in [
                "--core",
                "--waits",
                "--batch",
                "--ingress",
                "--inbox",
                "--outbox",
                "--events",
                "--stage-threads",
                "--placement",
            ] {
                if let Some(v) = args.get(k) {
                    config.push(format!("{}={v}", &k[2..]));
                }
            }
            let o = PipeOpts {
                partitions,
                producers: producers.max(1),
                journal,
                waits,
                batch: args.num("--batch", 64usize).max(1),
                rings: (
                    args.num("--ingress", 1usize << 14),
                    args.num("--inbox", 1usize << 12),
                    args.num("--outbox", 1usize << 13),
                ),
                placement: match args.get("--placement").unwrap_or("inline") {
                    "inline" => JournalPlacement::Inline,
                    "stage" => JournalPlacement::Stage,
                    v => die(format!("--placement: unknown {v}")),
                },
                stage_threads: args.get("--stage-threads").map(|v| {
                    let (j, e) = v
                        .split_once(',')
                        .unwrap_or_else(|| die("--stage-threads J,E"));
                    (
                        j.parse().unwrap_or_else(|_| die("--stage-threads J,E")),
                        e.parse().unwrap_or_else(|_| die("--stage-threads J,E")),
                    )
                }),
                stats: args.flag("--stats"),
            };
            let row = match args.get("--core").unwrap_or("fifo") {
                "fifo" => pipe_mode::<FifoCore>(&setup, &run, &o),
                "noop" => pipe_mode::<NoopCore>(&setup, &run, &o),
                c => die(format!("--core: unknown {c}")),
            };
            let _ = std::fs::remove_dir_all(&tmp);
            config.insert(0, format!("journal={jmode} fsync={fsync}"));
            (row, partitions.to_string(), producers.to_string())
        }
        m => die(format!("--mode: unknown {m}")),
    };

    if let Some(u) = row.untimed {
        config.push(format!("untimed={u:.0}"));
    }
    let mut lat = row.lat;
    lat.sort_unstable();
    let ops_s = row.ops as f64 / (row.wall_ns as f64 / 1e9);
    let mean = if lat.is_empty() {
        0
    } else {
        (lat.iter().map(|&v| v as u128).sum::<u128>() / lat.len() as u128) as u64
    };
    let eff = match (mode, args.get("--baseline")) {
        ("pipe", Some(b)) => {
            let b: f64 = b
                .parse()
                .unwrap_or_else(|_| die("--baseline: not a number"));
            let p: f64 = p.parse().unwrap();
            format!("{:.2}", ops_s / (p * b))
        }
        _ => String::new(),
    };
    println!(
        "| {tag} | {mode} | {p} | {prod} | {} | {ops_s:.0} | {eff} | {mean} | {} | {} | {} | {} | {} | {} |",
        row.ops,
        percentile(&lat, 0.50),
        percentile(&lat, 0.90),
        percentile(&lat, 0.99),
        percentile(&lat, 0.999),
        lat.last().copied().unwrap_or(0),
        config.join(" "),
    );
    eprintln!(
        "env: {} / {} {} / orderer-rust {}",
        cpu(),
        std::env::consts::OS,
        std::env::consts::ARCH,
        env!("CARGO_PKG_VERSION")
    );
}
