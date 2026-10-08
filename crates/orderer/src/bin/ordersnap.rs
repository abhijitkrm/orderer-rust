//! ordersnap — spec/HARNESS.md §4.4 (mirrors matchersnap).
//!
//! ```text
//! ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
//! ```
//!
//! Runs the file, drains, prints the merged `matcher-snap/1` snapshot —
//! byte-identical to matchersnap's for any P.

use orderer::harness::*;

const USAGE: &str =
    "ordersnap <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]";

fn main() {
    let args = Args::parse(USAGE, COMMON_VALUED, COMMON_FLAGS);
    let [path] = args.positional.as_slice() else {
        die(USAGE)
    };
    let corpus = load_corpus(path);
    let common = common(&args);
    let (_, snap) = run_fifo(&corpus, &common, true, false, true);
    print_bytes(snap.expect("snapshot requested").body.as_bytes());
}
