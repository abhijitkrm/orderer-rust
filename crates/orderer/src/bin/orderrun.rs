//! orderrun — spec/HARNESS.md §4.1 (mirrors matcherrun).
//!
//! ```text
//! orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH]
//!          [--checkpoint-every K] [--durable]
//! ```
//!
//! Runs the file through a pipeline (one producer, file order), drains, and
//! prints every event as a symbol-tagged canonical line, grouped by
//! partition. `--snap` then writes the merged snapshot to PATH and its cut to
//! PATH.meta.

use orderer::harness::*;

const USAGE: &str = "orderrun <cmd-file> [--partitions P] [--partition-map F] [--journal-dir D] [--binary] [--snap PATH] [--checkpoint-every K] [--durable]";

fn main() {
    let mut valued = COMMON_VALUED.to_vec();
    valued.extend(["--snap", "--checkpoint-every"]);
    let mut flags = COMMON_FLAGS.to_vec();
    flags.push("--durable");
    let args = Args::parse(USAGE, &valued, &flags);
    let [path] = args.positional.as_slice() else {
        die(USAGE)
    };
    let corpus = load_corpus(path);
    let common = common(&args);
    let snap_path = args.get("--snap");
    let opts = RunOpts {
        snapshot: snap_path.is_some(),
        checkpoint_every: args
            .get("--checkpoint-every")
            .map(|_| args.num("--checkpoint-every", 0usize)),
        durable: args.flag("--durable"),
    };
    let (listing, snap) =
        run_corpus_opts::<orderer_core::FifoCore>(&corpus, &common, true, false, opts);
    print_bytes(&listing);
    if let (Some(p), Some(s)) = (snap_path, snap) {
        s.write(p).unwrap_or_else(|e| die(format!("{p}: {e}")));
    }
}
