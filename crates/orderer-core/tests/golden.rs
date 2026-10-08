//! Golden vector runner: feeds every `vectors/**/*.cmd.jsonl` through
//! `OrderBook` and byte-compares the canonical event stream to the matching
//! `.evt.jsonl`. Run with `REGEN=1` to (re)generate expected files — then
//! AUDIT them against spec/SPEC.md before committing.

use std::fs;
use std::path::{Path, PathBuf};

use orderer_core::*;

fn vectors_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vectors/matcher")
}

fn parse_side(v: &serde_json::Value) -> Side {
    match v.as_str().unwrap() {
        "bid" => Side::Bid,
        "ask" => Side::Ask,
        s => panic!("bad side {s}"),
    }
}

fn parse_cmd(v: &serde_json::Value) -> (Symbol, Command) {
    let sym = v["symbol"].as_u64().unwrap_or(0) as Symbol;
    let cmd = match v["cmd"].as_str().unwrap() {
        "new" => Command::New {
            order_id: v["order_id"].as_u64().unwrap(),
            side: parse_side(&v["side"]),
            otype: match v["otype"].as_str().unwrap() {
                "limit" => OType::Limit,
                "market" => OType::Market,
                s => panic!("bad otype {s}"),
            },
            price: v["price"].as_i64().unwrap(),
            qty: v["qty"].as_u64().unwrap(),
            tif: match v["tif"].as_str().unwrap() {
                "gtc" => Tif::Gtc,
                "ioc" => Tif::Ioc,
                "fok" => Tif::Fok,
                "post_only" => Tif::PostOnly,
                s => panic!("bad tif {s}"),
            },
        },
        "cancel" => Command::Cancel {
            order_id: v["order_id"].as_u64().unwrap(),
        },
        "replace" => Command::Replace {
            order_id: v["order_id"].as_u64().unwrap(),
            price: v["price"].as_i64().unwrap(),
            qty: v["qty"].as_u64().unwrap(),
        },
        c => panic!("bad cmd {c}"),
    };
    (sym, cmd)
}

struct Header {
    pmin: i64,
    pmax: i64,
    max_orders: usize,
    index: String,
    engine: bool,
}

fn parse_header(line: &str) -> Header {
    let v: serde_json::Value = serde_json::from_str(line).unwrap();
    Header {
        pmin: v["pmin"].as_i64().unwrap(),
        pmax: v["pmax"].as_i64().unwrap(),
        max_orders: v["max_orders"].as_u64().unwrap_or(65_536) as usize,
        index: v["index"].as_str().unwrap_or("ladder").to_string(),
        engine: v["engine"].as_bool().unwrap_or(false),
    }
}

/// Run one vector file in one index mode; returns produced event lines.
fn run(cmd_path: &Path, kind: IndexKind) -> (Vec<String>, String) {
    let text = fs::read_to_string(cmd_path).unwrap();
    let mut lines = text.lines();
    let header = parse_header(lines.next().unwrap());
    let cfg = BookConfig {
        price_min: header.pmin,
        price_max: header.pmax,
        max_orders: header.max_orders,
        index: kind,
    };
    if header.engine {
        let mut eng = Engine::new(cfg);
        let mut out: Vec<String> = Vec::new();
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            let (sym, cmd) = parse_cmd(&v);
            eng.submit_tagged(sym, cmd, &mut |s, seq, ev| {
                out.push(Event::canonical_sym(seq, s, ev));
            });
        }
        #[cfg(debug_assertions)]
        for (_, book) in eng.books_iter() {
            book.check_invariants();
        }
        return (out, header.index);
    }
    let mut book = OrderBook::new(cfg);
    let mut sink = LinesSink::new();
    for line in lines {
        if line.trim().is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        book.apply(parse_cmd(&v).1, &mut sink);
    }
    #[cfg(debug_assertions)]
    book.check_invariants();
    (sink.lines, header.index)
}

#[test]
fn golden_vectors() {
    let dir = vectors_dir();
    let manifest: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(dir.join("manifest.json")).expect("manifest.json"),
    )
    .unwrap();
    let regen = std::env::var("REGEN").is_ok();
    let mut failures = 0;
    let mut checked = 0;

    for v in manifest["vectors"].as_array().unwrap() {
        let name = v["file"].as_str().unwrap();
        let cmd_path = dir.join(format!("{name}.cmd.jsonl"));
        let evt_path = dir.join(format!("{name}.evt.jsonl"));

        // Index modes to run: "ladder" | "tree" | "both". The header's primary
        // mode produces the canonical stream; "both" requires the other mode
        // to match it exactly.
        let header_line = fs::read_to_string(&cmd_path).unwrap();
        let hdr_index = parse_header(header_line.lines().next().unwrap()).index;
        let (primary, modes): (IndexKind, Vec<IndexKind>) = match hdr_index.as_str() {
            "both" => (IndexKind::Ladder, vec![IndexKind::Ladder, IndexKind::Tree]),
            "tree" => (IndexKind::Tree, vec![IndexKind::Tree]),
            _ => (IndexKind::Ladder, vec![IndexKind::Ladder]),
        };
        let (expected_lines, _) = run(&cmd_path, primary);
        for kind in modes {
            let (produced, _) = run(&cmd_path, kind);
            if produced != expected_lines {
                eprintln!(
                    "FAIL {name} [{kind:?}]: tree/ladder divergence\n  expected: {expected_lines:?}\n  got:      {produced:?}"
                );
                failures += 1;
            }
        }

        if regen {
            let mut out = String::new();
            let first = fs::read_to_string(&evt_path).unwrap_or_default();
            let hdr = first.lines().next().unwrap_or("").to_string();
            // Keep existing evt header if present, else synthesize.
            if hdr.starts_with("{\"format\"") {
                out.push_str(&hdr);
            } else {
                let name_only = name.rsplit('/').next().unwrap();
                out.push_str(&format!(
                    "{{\"format\":\"matcher-vector/1\",\"name\":\"{name_only}\"}}"
                ));
            }
            out.push('\n');
            for l in &expected_lines {
                out.push_str(l);
                out.push('\n');
            }
            fs::write(&evt_path, out).unwrap();
            continue;
        }

        let evt_text = fs::read_to_string(&evt_path)
            .unwrap_or_else(|_| panic!("missing {evt_path:?} (run REGEN=1 cargo test)"));
        let exp: Vec<&str> = evt_text.lines().skip(1).collect();
        checked += 1;
        if exp
            != expected_lines
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
        {
            eprintln!("FAIL {name}: golden mismatch");
            let n = exp.len().max(expected_lines.len());
            for i in 0..n {
                let e = exp.get(i).copied().unwrap_or("<none>");
                let a = expected_lines
                    .get(i)
                    .map(|s| s.as_str())
                    .unwrap_or("<none>");
                if e != a {
                    eprintln!("  line {}:\n    expected {e}\n    actual   {a}", i + 2);
                }
            }
            failures += 1;
        }
    }

    if !regen {
        assert_eq!(failures, 0, "{failures} golden vector(s) failed");
        eprintln!("golden: {checked} vectors passed");
    }
}
