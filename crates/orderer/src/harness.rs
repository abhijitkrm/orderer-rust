//! Shared plumbing for the spec/HARNESS.md tools (`src/bin/*`): corpus
//! loading, option parsing, and "run a file through a pipeline".

use std::path::PathBuf;
use std::process::exit;

use orderer_core::jsonflat::{get_str, get_u64, parse_command, parse_header};
use orderer_core::{BookConfig, Command, FifoCore, MatchingCore, Symbol};

use crate::egress::Collect;
use crate::journal::{FsyncPolicy, JournalConfig, JournalFormat};
use crate::pipeline::{Pipeline, PipelineBuilder, Snapshot};
use crate::routing::PartitionMap;

/// Exit with spec/HARNESS.md §5 code 2 (usage / input / config error).
pub fn die(msg: impl std::fmt::Display) -> ! {
    eprintln!("{msg}");
    exit(2)
}

/// A parsed command file (spec/HARNESS.md §1).
pub struct Corpus {
    pub book: BookConfig,
    pub engine: bool,
    pub cmds: Vec<(Symbol, Command)>,
}

pub fn parse_corpus(text: &str, path: &str) -> Result<Corpus, String> {
    let mut lines = text.lines();
    let header = lines.next().ok_or_else(|| format!("{path}: empty file"))?;
    let (pmin, pmax, max_orders, index) = parse_header(header);
    let engine = get_str(header, "engine") == Some("true");
    let mut cmds = Vec::new();
    for (i, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let cmd = parse_command(line)
            .ok_or_else(|| format!("{path}:{}: malformed command: {line}", i + 2))?;
        let sym = if engine {
            get_u64(line, "symbol")
                .filter(|&s| s <= u32::MAX as u64)
                .ok_or_else(|| format!("{path}:{}: missing symbol: {line}", i + 2))?
                as Symbol
        } else {
            0
        };
        cmds.push((sym, cmd));
    }
    Ok(Corpus {
        book: BookConfig {
            price_min: pmin,
            price_max: pmax,
            max_orders,
            index,
        },
        engine,
        cmds,
    })
}

pub fn load_corpus(path: &str) -> Corpus {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| die(format!("{path}: {e}")));
    parse_corpus(&text, path).unwrap_or_else(|e| die(e))
}

/// Minimal argv parser: positionals plus `--flag` / `--opt value`.
pub struct Args {
    pub positional: Vec<String>,
    opts: Vec<(String, Option<String>)>,
}

impl Args {
    /// `valued`: options that take a value; `flags`: options that don't.
    pub fn parse(usage: &str, valued: &[&str], flags: &[&str]) -> Args {
        let mut positional = Vec::new();
        let mut opts = Vec::new();
        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            if a.starts_with("--") {
                if valued.contains(&a.as_str()) {
                    let v = it
                        .next()
                        .unwrap_or_else(|| die(format!("{a} needs a value\nusage: {usage}")));
                    opts.push((a, Some(v)));
                } else if flags.contains(&a.as_str()) {
                    opts.push((a, None));
                } else {
                    die(format!("unknown option {a}\nusage: {usage}"));
                }
            } else {
                positional.push(a);
            }
        }
        Args { positional, opts }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.opts
            .iter()
            .rev()
            .find(|(k, _)| k == name)
            .and_then(|(_, v)| v.as_deref())
    }

    pub fn flag(&self, name: &str) -> bool {
        self.opts.iter().any(|(k, _)| k == name)
    }

    pub fn num<T: std::str::FromStr>(&self, name: &str, default: T) -> T {
        match self.get(name) {
            None => default,
            Some(v) => v
                .parse()
                .unwrap_or_else(|_| die(format!("{name}: not a number: {v}"))),
        }
    }
}

/// The common options of spec/HARNESS.md §2.
pub const COMMON_VALUED: &[&str] = &["--partitions", "--partition-map", "--journal-dir"];
pub const COMMON_FLAGS: &[&str] = &["--binary"];

pub struct Common {
    pub map: PartitionMap,
    pub journal: Option<JournalConfig>,
}

pub fn common(args: &Args) -> Common {
    let p: u32 = args.num("--partitions", 1);
    let map = match args.get("--partition-map") {
        Some(path) => {
            let text =
                std::fs::read_to_string(path).unwrap_or_else(|e| die(format!("{path}: {e}")));
            PartitionMap::parse_table(&text, p).unwrap_or_else(|e| die(format!("{path}: {e}")))
        }
        None => PartitionMap::hash(p).unwrap_or_else(|e| die(e)),
    };
    let format = if args.flag("--binary") {
        JournalFormat::Binary
    } else {
        JournalFormat::Jsonl
    };
    if args.flag("--binary") && args.get("--journal-dir").is_none() {
        die("--binary requires --journal-dir");
    }
    let journal = args.get("--journal-dir").map(|d| JournalConfig {
        dir: PathBuf::from(d),
        format,
        // harness runs need complete files, not power-loss safety
        fsync: FsyncPolicy::Never,
        events: true,
        append: false,
    });
    Common { map, journal }
}

/// Run `corpus` through a fresh pipeline (one producer, file order), drain
/// and shut down. Returns the per-partition listing (spec/HARNESS.md §3)
/// and, if asked, a snapshot taken after the last command.
pub fn run_corpus<C: MatchingCore>(
    corpus: &Corpus,
    common: &Common,
    tagged: bool,
    check_invariants: bool,
    snapshot: bool,
) -> (Vec<u8>, Option<Snapshot>) {
    let (collect, events) = Collect::new(tagged);
    let mut b: PipelineBuilder<C> = Pipeline::builder()
        .book_config(corpus.book)
        .partition_map(common.map.clone())
        .egress(collect)
        .check_invariants(check_invariants);
    if let Some(j) = &common.journal {
        b = b.journal(j.clone());
    }
    let mut p = b.build().unwrap_or_else(|e| die(e));
    let fail = |e: crate::pipeline::Error| -> ! {
        eprintln!("{e}");
        exit(1)
    };
    p.publish_batch(&corpus.cmds).unwrap_or_else(|e| fail(e));
    p.drain().unwrap_or_else(|e| fail(e));
    let snap = if snapshot {
        Some(p.snapshot().unwrap_or_else(|e| fail(e)))
    } else {
        None
    };
    p.shutdown().unwrap_or_else(|e| fail(e));
    (events.listing(), snap)
}

/// `run_corpus` with the spec-proven core.
pub fn run_fifo(
    corpus: &Corpus,
    common: &Common,
    tagged: bool,
    check_invariants: bool,
    snapshot: bool,
) -> (Vec<u8>, Option<Snapshot>) {
    run_corpus::<FifoCore>(corpus, common, tagged, check_invariants, snapshot)
}

/// Write bytes to stdout, exiting quietly on a closed pipe.
pub fn print_bytes(bytes: &[u8]) {
    use std::io::Write as _;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if out.write_all(bytes).and_then(|_| out.flush()).is_err() {
        exit(0);
    }
}
