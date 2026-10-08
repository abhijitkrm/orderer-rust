//! Snapshot/journal round-trip tests (spec/JOURNAL.md):
//! 1. snap → restore → snap is byte-identical
//! 2. restore → continue emits byte-identical events vs uninterrupted run
//! 3. cmd journal replay reproduces the event journal byte-for-byte

use orderer_core::jsonflat::{get_u64, parse_command, parse_header};
use orderer_core::snapshot;
use orderer_core::*;

const CORPUS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../vectors/matcher/engine/001_multisymbol.cmd.jsonl"
);

fn load_engine_corpus(path: &str) -> (BookConfig, Vec<(Symbol, Command)>) {
    let text = std::fs::read_to_string(path).unwrap();
    let mut lines = text.lines();
    let (pmin, pmax, mo, idx) = parse_header(lines.next().unwrap());
    let cmds = lines
        .filter(|l| !l.is_empty())
        .map(|l| {
            (
                get_u64(l, "symbol").unwrap_or(0) as Symbol,
                parse_command(l).unwrap(),
            )
        })
        .collect();
    (
        BookConfig {
            price_min: pmin,
            price_max: pmax,
            max_orders: mo,
            index: idx,
        },
        cmds,
    )
}

fn run_tagged(eng: &mut Engine, cmds: &[(Symbol, Command)]) -> Vec<String> {
    let mut out = Vec::new();
    for (sym, cmd) in cmds {
        eng.submit_tagged(*sym, *cmd, &mut |s, seq, ev| {
            out.push(Event::canonical_sym(seq, s, ev));
        });
    }
    out
}

#[test]
fn snapshot_continuation_is_byte_identical() {
    let (cfg, cmds) = load_engine_corpus(CORPUS);
    let split = cmds.len() / 2;

    // Uninterrupted reference run.
    let mut ref_eng = Engine::new(cfg);
    let reference = run_tagged(&mut ref_eng, &cmds);

    // Split run: snapshot at midpoint, restore, continue.
    let mut eng = Engine::new(cfg);
    let mut out = run_tagged(&mut eng, &cmds[..split]);
    let mut snap = String::new();
    snapshot::write_engine(&eng, &mut snap);
    let mut eng2 = snapshot::restore_engine(&snapshot::parse(&snap));

    // snap → restore → snap must be byte-identical (before continuing).
    let mut snap2 = String::new();
    snapshot::write_engine(&eng2, &mut snap2);
    assert_eq!(snap, snap2, "re-snapshot not byte-identical");

    out.extend(run_tagged(&mut eng2, &cmds[split..]));
    assert_eq!(out, reference, "continuation after restore diverged");
}

#[test]
fn journal_replay_reproduces_events() {
    let (cfg, cmds) = load_engine_corpus(CORPUS);
    let mut eng = Engine::new(cfg);

    // Record cmd journal + event journal.
    let mut cmd_journal = Vec::new();
    let mut evt_journal = Vec::new();
    let mut j = journal::CmdJournal::new(&mut cmd_journal);
    let mut scratch = String::new();
    for (sym, cmd) in &cmds {
        j.record_sym(*sym, cmd);
        eng.submit_tagged(*sym, *cmd, &mut |s, seq, ev| {
            journal::journal_event(s, seq, ev, &mut evt_journal, &mut scratch);
        });
    }
    j.flush();

    // Replay the cmd journal into a fresh engine; compare event streams.
    let mut eng2 = Engine::new(cfg);
    let journal_text = String::from_utf8(cmd_journal).unwrap();
    let cmds2: Vec<(Symbol, Command)> = journal_text
        .lines()
        .map(|l| {
            (
                get_u64(l, "symbol").unwrap() as Symbol,
                parse_command(l).unwrap(),
            )
        })
        .collect();
    let replayed = run_tagged(&mut eng2, &cmds2);

    let evt_text = String::from_utf8(evt_journal).unwrap();
    let expected: Vec<&str> = evt_text.lines().collect();
    assert_eq!(
        replayed.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        expected
    );
}

/// xorshift64* — same generator as fuzzgen/vectorgen, duplicated locally so
/// the test needs no external corpus.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[test]
fn snapshot_mid_fuzz_stream() {
    let cfg = BookConfig {
        price_min: 0,
        price_max: 1000,
        max_orders: 4096,
        index: IndexKind::Ladder,
    };
    let mut rng = Rng(0xC0FFEE);
    let mut cmds = Vec::new();
    for _ in 0..4000 {
        let sym = rng.below(6) as Symbol;
        let id = rng.below(256);
        let cmd = match rng.below(3) {
            0 => Command::new(
                id,
                if rng.below(2) == 0 {
                    Side::Bid
                } else {
                    Side::Ask
                },
                rng.below(999) as i64 + 1,
                rng.below(200) + 1,
                match rng.below(4) {
                    0 => Tif::Ioc,
                    1 => Tif::Fok,
                    2 => Tif::PostOnly,
                    _ => Tif::Gtc,
                },
            ),
            1 => Command::cancel(id),
            _ => Command::replace(id, rng.below(999) as i64 + 1, rng.below(200) + 1),
        };
        cmds.push((sym, cmd));
    }

    let split = 2000;
    let mut reference = Engine::new(cfg);
    let expected = run_tagged(&mut reference, &cmds);

    let mut eng = Engine::new(cfg);
    let mut out = run_tagged(&mut eng, &cmds[..split]);
    let mut snap = String::new();
    snapshot::write_engine(&eng, &mut snap);
    let mut eng2 = snapshot::restore_engine(&snapshot::parse(&snap));
    out.extend(run_tagged(&mut eng2, &cmds[split..]));
    assert_eq!(out, expected, "mid-fuzz restore diverged");
}

#[test]
fn snapshot_empty_engine() {
    let mut snap = String::new();
    snapshot::write_engine(&Engine::new(BookConfig::default()), &mut snap);
    let parsed = snapshot::parse(&snap);
    assert!(parsed.books.is_empty());
    let _ = snapshot::restore_engine(&parsed);
}
