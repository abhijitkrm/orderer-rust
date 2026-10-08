//! orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//!
//! ```text
//! orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
//! ```
//!
//! Runs the file through a pipeline (one producer, file order), drains, and
//! prints every event as a symbol-tagged canonical line, grouped by
//! partition. `--snap` then writes the merged snapshot to PATH and its cut to
//! PATH.meta.

use orderer::harness::*;

const USAGE: &str = "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]";

fn main() {
    let mut valued = COMMON_VALUED.to_vec();
    valued.push("--snap");
    let args = Args::parse(USAGE, &valued, COMMON_FLAGS);
    let [path] = args.positional.as_slice() else {
        die(USAGE)
    };
    let corpus = load_corpus(path);
    let common = common(&args);
    let snap_path = args.get("--snap");
    let (listing, snap) = run_fifo(&corpus, &common, true, false, snap_path.is_some());
    print_bytes(&listing);
    if let (Some(p), Some(s)) = (snap_path, snap) {
        s.write(p).unwrap_or_else(|e| die(format!("{p}: {e}")));
    }
}
