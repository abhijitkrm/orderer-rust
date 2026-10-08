//! Shared test helpers: vendored vector access, a seeded adversarial
//! multi-symbol generator, and reference (single-Engine) runs.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use orderer::*;
use orderer_core::jsonflat::get_u64;
use orderer_core::*;

pub fn vectors() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vectors")
}

/// Every vendored matcher vector: (name, cmd path, evt path, index mode).
pub fn matcher_vectors() -> Vec<(String, PathBuf, PathBuf, String)> {
    let dir = vectors().join("matcher");
    let mf: String = std::fs::read_to_string(dir.join("manifest.json")).unwrap();
    // flat scan of `"file": "x"` entries — no serde dependency here
    let mut out = Vec::new();
    for part in mf.split("\"file\"").skip(1) {
        let name = part.split('"').nth(1).unwrap().to_string();
        let cmd = dir.join(format!("{name}.cmd.jsonl"));
        let evt = dir.join(format!("{name}.evt.jsonl"));
        let hdr = std::fs::read_to_string(&cmd).unwrap();
        let hdr = hdr.lines().next().unwrap();
        let index = orderer_core::jsonflat::get_str(hdr, "index")
            .unwrap_or("ladder")
            .to_string();
        out.push((name, cmd, evt, index));
    }
    out
}

/// xorshift64* (same generator family as the spec repo's tools).
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

pub fn fuzz_cfg() -> BookConfig {
    BookConfig {
        price_min: 1,
        price_max: 200,
        max_orders: 4096,
        index: IndexKind::Ladder,
    }
}

/// Adversarial engine stream: crossing prices, small id space (collisions,
/// unknown ids), every TIF, markets, ~5% malformed (qty 0, bad prices).
pub fn fuzz_corpus(seed: u64, n: usize, symbols: u32) -> Vec<(Symbol, Command)> {
    let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let sym = r.below(symbols as u64) as Symbol;
        let id = r.below(256);
        let price = if r.below(20) == 0 {
            [0, 201, -5][r.below(3) as usize]
        } else {
            90 + r.below(21) as i64
        };
        let qty = if r.below(25) == 0 { 0 } else { r.below(50) + 1 };
        let cmd = match r.below(10) {
            0..=4 => {
                let side = if r.below(2) == 0 {
                    Side::Bid
                } else {
                    Side::Ask
                };
                if r.below(8) == 0 {
                    Command::market(id, side, qty)
                } else {
                    let tif = [Tif::Gtc, Tif::Gtc, Tif::Ioc, Tif::Fok, Tif::PostOnly]
                        [r.below(5) as usize];
                    Command::new(id, side, price, qty, tif)
                }
            }
            5..=7 => Command::cancel(id),
            _ => Command::replace(id, price, qty),
        };
        out.push((sym, cmd));
    }
    out
}

/// Reference: one matcher `Engine`, symbol-tagged canonical lines in order.
pub fn reference_lines(cfg: BookConfig, cmds: &[(Symbol, Command)]) -> Vec<String> {
    let mut eng = Engine::new(cfg);
    let mut out = Vec::new();
    for &(s, c) in cmds {
        eng.submit_tagged(s, c, &mut |sym, seq, ev| {
            out.push(Event::canonical_sym(seq, sym, ev))
        });
    }
    out
}

/// Reference snapshot after `cmds`.
pub fn reference_snapshot(cfg: BookConfig, cmds: &[(Symbol, Command)]) -> String {
    let mut eng = Engine::new(cfg);
    for &(s, c) in cmds {
        eng.submit(s, c, &mut NullSink::new());
    }
    let mut out = String::new();
    snapshot::write_engine(&eng, &mut out);
    out
}

/// Split symbol-tagged lines into per-symbol streams.
pub fn by_symbol<'a>(lines: impl IntoIterator<Item = &'a str>) -> BTreeMap<u64, Vec<String>> {
    let mut m: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for l in lines {
        m.entry(get_u64(l, "symbol").unwrap_or(0))
            .or_default()
            .push(l.to_string());
    }
    m
}

pub fn lines(bytes: &[u8]) -> Vec<String> {
    String::from_utf8(bytes.to_vec())
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}

/// Per-symbol `seq` must run 1, 2, 3, … with no gaps.
pub fn assert_dense(lines: &[String]) {
    let mut next: BTreeMap<u64, u64> = BTreeMap::new();
    for l in lines {
        let sym = get_u64(l, "symbol").unwrap_or(0);
        let seq = get_u64(l, "seq").unwrap();
        let n = next.entry(sym).or_insert(1);
        assert_eq!(seq, *n, "seq gap on symbol {sym}: {l}");
        *n += 1;
    }
}

/// Run commands through a pipeline; return per-partition canonical lines.
pub fn run_pipeline(
    cfg: BookConfig,
    cmds: &[(Symbol, Command)],
    partitions: u32,
    tagged: bool,
) -> Vec<Vec<String>> {
    let (collect, events) = Collect::new(tagged);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(partitions)
        .ring_sizes(1 << 10, 1 << 8, 1 << 8)
        .egress(collect)
        .check_invariants(cfg!(debug_assertions))
        .build()
        .unwrap();
    p.publish_batch(cmds).unwrap();
    p.drain().unwrap();
    p.shutdown().unwrap();
    events.take().iter().map(|b| lines(b)).collect()
}

/// A fresh scratch directory under the target dir.
pub fn scratch(name: &str) -> PathBuf {
    let d = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "{name}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}
