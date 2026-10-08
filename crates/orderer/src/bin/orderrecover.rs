//! orderrecover — spec/HARNESS.md §4.3 (mirrors matcherrecover).
//!
//!   orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]
//!   orderrecover --journal-dir DIR [--snap PATH] [--binary] [--partitions P] [--partition-map F]
//!
//! Tail form: restore the snapshot, submit every tail line that has no
//! "format" key through a pipeline, print the replayed events (tagged,
//! grouped by partition). A malformed line exits 2.
//!
//! Journal form: recover per spec/JOURNAL.md §5 from the journal directory
//! (after the optional snapshot's cut) and print the replayed events.
//! Corrupt or torn journals exit 2.

use orderer::harness::*;
use orderer::recover::{read_snapshot, recover, restore};
use orderer::*;
use orderer_core::jsonflat::{get_u64, parse_command};
use orderer_core::*;

const USAGE: &str = "orderrecover <snapshot> <tail-file> [--partitions P] [--partition-map F]\n       orderrecover --journal-dir DIR [--snap PATH] [--binary] [--partitions P] [--partition-map F]";

fn tail_form(snap_path: &str, tail_path: &str, map: PartitionMap) {
    let snap = read_snapshot(snap_path.as_ref()).unwrap_or_else(|e| die(e));
    let (book, cores) =
        restore::<FifoCore>(BookConfig::default(), &map, Some(&snap)).unwrap_or_else(|e| die(e));
    let text =
        std::fs::read_to_string(tail_path).unwrap_or_else(|e| die(format!("{tail_path}: {e}")));
    let mut cmds = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.is_empty() || line.contains("\"format\"") {
            continue;
        }
        let cmd = parse_command(line).unwrap_or_else(|| {
            die(format!(
                "{tail_path}:{}: malformed journal line: {line}",
                i + 1
            ))
        });
        let sym = get_u64(line, "symbol").unwrap_or(0);
        if sym > u32::MAX as u64 {
            die(format!(
                "{tail_path}:{}: malformed journal line: {line}",
                i + 1
            ));
        }
        cmds.push((sym as Symbol, cmd));
    }
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(book)
        .partition_map(map)
        .egress(collect)
        .initial(Initial {
            cores,
            next_iseq: snap.iseq + 1,
        })
        .build()
        .unwrap_or_else(|e| die(e));
    let fail = |e: Error| -> ! {
        eprintln!("{e}");
        std::process::exit(1)
    };
    p.publish_batch(&cmds).unwrap_or_else(|e| fail(e));
    p.drain().unwrap_or_else(|e| fail(e));
    p.shutdown().unwrap_or_else(|e| fail(e));
    print_bytes(&events.listing());
}

fn journal_form(dir: &str, snap_path: Option<&str>, format: JournalFormat, map: PartitionMap) {
    let snap = snap_path.map(|p| read_snapshot(p.as_ref()).unwrap_or_else(|e| die(e)));
    let mut parts: Vec<Vec<u8>> = vec![Vec::new(); map.partitions() as usize];
    let mut line = String::new();
    recover::<FifoCore>(
        BookConfig::default(),
        &map,
        snap.as_ref(),
        Some((dir.as_ref(), format)),
        |p, sym, seq, ev| {
            line.clear();
            Event::write_canonical_sym(seq, sym, ev, &mut line);
            line.push('\n');
            parts[p as usize].extend_from_slice(line.as_bytes());
        },
    )
    .unwrap_or_else(|e| die(e));
    print_bytes(&parts.concat());
}

fn main() {
    let valued = ["--partitions", "--partition-map", "--journal-dir", "--snap"];
    let args = Args::parse(USAGE, &valued, &["--binary"]);
    let p: u32 = args.num("--partitions", 1);
    let map = match args.get("--partition-map") {
        Some(path) => {
            let text =
                std::fs::read_to_string(path).unwrap_or_else(|e| die(format!("{path}: {e}")));
            PartitionMap::parse_table(&text, p).unwrap_or_else(|e| die(format!("{path}: {e}")))
        }
        None => PartitionMap::hash(p).unwrap_or_else(|e| die(e)),
    };
    match (args.get("--journal-dir"), args.positional.as_slice()) {
        (Some(dir), []) => {
            let format = if args.flag("--binary") {
                JournalFormat::Binary
            } else {
                JournalFormat::Jsonl
            };
            journal_form(dir, args.get("--snap"), format, map)
        }
        (None, [snap, tail]) if args.get("--snap").is_none() && !args.flag("--binary") => {
            tail_form(snap, tail, map)
        }
        _ => die(USAGE),
    }
}
