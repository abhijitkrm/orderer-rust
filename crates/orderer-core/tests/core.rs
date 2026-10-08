//! The MatchingCore seam: FifoCore is byte-identical to matcher's Engine,
//! snapshot blocks round-trip through restore_book, bad snapshots are
//! errors (never panics), NoopCore echoes every command.

use orderer_core::jsonflat::{get_u64, parse_command, parse_header};
use orderer_core::snapshot;
use orderer_core::*;

const CORPUS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../vectors/matcher/engine/001_multisymbol.cmd.jsonl"
);

fn load() -> (BookConfig, Vec<(Symbol, Command)>) {
    let text = std::fs::read_to_string(CORPUS).unwrap();
    let mut lines = text.lines();
    let (pmin, pmax, max_orders, index) = parse_header(lines.next().unwrap());
    let cmds = lines
        .filter(|l| !l.is_empty())
        .map(|l| {
            (
                get_u64(l, "symbol").unwrap_or(0) as Symbol,
                parse_command(l).unwrap(),
            )
        })
        .collect();
    let cfg = BookConfig {
        price_min: pmin,
        price_max: pmax,
        max_orders,
        index,
    };
    (cfg, cmds)
}

fn run_core<C: MatchingCore>(core: &mut C, cmds: &[(Symbol, Command)]) -> Vec<String> {
    let mut out = Vec::new();
    for &(sym, cmd) in cmds {
        core.apply(sym, cmd, &mut |s, seq, ev| {
            out.push(Event::canonical_sym(seq, s, ev))
        });
    }
    out
}

fn merged_snapshot<C: MatchingCore>(cfg: BookConfig, core: &C) -> String {
    let mut blocks = Vec::new();
    core.snapshot_blocks(&mut blocks);
    blocks.sort_by_key(|(s, _)| *s);
    let mut out = String::new();
    snapshot::write_header(cfg, &mut out);
    for (_, b) in blocks {
        out.push_str(&b);
    }
    out
}

#[test]
fn fifo_core_matches_engine() {
    let (cfg, cmds) = load();
    let mut eng = Engine::new(cfg);
    let mut expected = Vec::new();
    for &(sym, cmd) in &cmds {
        eng.submit_tagged(sym, cmd, &mut |s, seq, ev| {
            expected.push(Event::canonical_sym(seq, s, ev))
        });
    }
    let mut core = FifoCore::new(cfg);
    assert_eq!(run_core(&mut core, &cmds), expected);

    let mut snap = String::new();
    snapshot::write_engine(&eng, &mut snap);
    assert_eq!(
        merged_snapshot(cfg, &core),
        snap,
        "merged blocks == engine snapshot"
    );
}

#[test]
fn restore_book_round_trip_and_continuation() {
    let (cfg, cmds) = load();
    let split = cmds.len() / 2;
    let mut full = FifoCore::new(cfg);
    let all = run_core(&mut full, &cmds);

    let mut first = FifoCore::new(cfg);
    let prefix_events = run_core(&mut first, &cmds[..split]);
    let snap = merged_snapshot(cfg, &first);

    let parsed = snapshot::try_parse(&snap).unwrap();
    let mut restored = FifoCore::new(parsed.cfg);
    for b in &parsed.books {
        restored.restore_book(b.symbol, b.seq, &b.orders).unwrap();
    }
    assert_eq!(
        merged_snapshot(cfg, &restored),
        snap,
        "snap → restore → snap"
    );
    let tail_events = run_core(&mut restored, &cmds[split..]);
    assert_eq!([prefix_events, tail_events].concat(), all);
}

#[test]
fn bad_snapshots_are_errors() {
    let cfg = BookConfig::default();
    let o = |order_id, side, price, qty| RestingOrder {
        order_id,
        side,
        tif: Tif::Gtc,
        price,
        qty,
    };
    let mut core = FifoCore::new(cfg);
    let dup = [o(1, Side::Bid, 10, 1), o(1, Side::Bid, 11, 1)];
    assert!(core.restore_book(1, 0, &dup).is_err());
    let crossed = [o(1, Side::Bid, 20, 1), o(2, Side::Ask, 20, 1)];
    assert!(core.restore_book(1, 0, &crossed).is_err());
    let out_of_range = [o(1, Side::Bid, cfg.price_max + 1, 1)];
    assert!(core.restore_book(1, 0, &out_of_range).is_err());
    let zero = [o(1, Side::Ask, 5, 0)];
    assert!(core.restore_book(1, 0, &zero).is_err());
    let small = BookConfig {
        max_orders: 1,
        ..cfg
    };
    let mut core = FifoCore::new(small);
    let two = [o(1, Side::Bid, 5, 1), o(2, Side::Bid, 6, 1)];
    assert!(core.restore_book(1, 0, &two).is_err());

    assert!(snapshot::try_parse("").is_err());
    assert!(snapshot::try_parse("{\"format\":\"nope\"}\n").is_err());
    let orphan = "{\"format\":\"matcher-snap/1\"}\n{\"rec\":\"order\",\"order_id\":1,\"side\":\"bid\",\"otype\":\"limit\",\"tif\":\"gtc\",\"price\":1,\"qty\":1}\n";
    assert!(snapshot::try_parse(orphan).is_err());
    let truncated = "{\"format\":\"matcher-snap/1\"}\n{\"rec\":\"book\",\"symbol\":1,\"seq\":3}\n{\"rec\":\"order\",\"order_id\":1,\"side\":\"bid\"";
    assert!(snapshot::try_parse(truncated).is_err());
}

#[test]
fn noop_core_echoes_every_command() {
    let (cfg, cmds) = load();
    let mut core = NoopCore::new(cfg);
    let out = run_core(&mut core, &cmds);
    assert_eq!(out.len(), cmds.len());
    let mut seqs = std::collections::HashMap::new();
    for (line, &(sym, _)) in out.iter().zip(&cmds) {
        let next = seqs.entry(sym).or_insert(0u64);
        *next += 1;
        assert_eq!(get_u64(line, "seq"), Some(*next));
        assert_eq!(get_u64(line, "symbol"), Some(sym as u64));
    }
}
