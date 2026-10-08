//! ordererfuzz — spec/HARNESS.md §4.2 (mirrors matcherfuzz).
//!
//!   ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]
//!
//! Like orderrun, but lines are symbol-tagged only for engine files, exactly
//! as matcherfuzz prints them. Builds with debug assertions (`dev`, `fuzz`
//! profiles) check book invariants after every command.

use orderer::harness::*;

const USAGE: &str =
    "ordererfuzz <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary]";

fn main() {
    let args = Args::parse(USAGE, COMMON_VALUED, COMMON_FLAGS);
    let [path] = args.positional.as_slice() else {
        die(USAGE)
    };
    let corpus = load_corpus(path);
    let common = common(&args);
    let (listing, _) = run_fifo(
        &corpus,
        &common,
        corpus.engine,
        cfg!(debug_assertions),
        false,
    );
    print_bytes(&listing);
}
