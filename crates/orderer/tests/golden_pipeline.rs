//! A1 — spec parity through the live pipeline. Every vendored matcher
//! vector, in every index mode it declares:
//! - P=1: the full event stream is byte-identical to the `.evt` file.
//! - P=4: per-symbol substreams are byte-identical (engine vectors), and
//!   single-book vectors (all symbol 0) are unchanged.

mod common;

use common::*;
use orderer::harness::parse_corpus;
use orderer_core::IndexKind;

#[test]
fn every_vector_byte_identical_through_pipeline() {
    let mut checked = 0;
    for (name, cmd, evt, index) in matcher_vectors() {
        let path = cmd.to_string_lossy().to_string();
        let corpus = parse_corpus(&std::fs::read_to_string(&cmd).unwrap(), &path).unwrap();
        let expected: Vec<String> = std::fs::read_to_string(&evt)
            .unwrap()
            .lines()
            .skip(1)
            .map(str::to_string)
            .collect();
        let modes = match index.as_str() {
            "both" => vec![IndexKind::Ladder, IndexKind::Tree],
            "tree" => vec![IndexKind::Tree],
            _ => vec![IndexKind::Ladder],
        };
        for kind in modes {
            let cfg = orderer_core::BookConfig {
                index: kind,
                ..corpus.book
            };
            // P=1: whole stream, exactly the golden file
            let got: Vec<String> = run_pipeline(cfg, &corpus.cmds, 1, corpus.engine).concat();
            assert_eq!(got, expected, "{name} [{kind:?}] P=1");

            // P=4: grouped by partition; per-symbol streams unchanged
            let parts = run_pipeline(cfg, &corpus.cmds, 4, corpus.engine);
            if corpus.engine {
                let got = by_symbol(parts.iter().flatten().map(String::as_str));
                let want = by_symbol(expected.iter().map(String::as_str));
                assert_eq!(got, want, "{name} [{kind:?}] P=4 per-symbol");
                for (p, lines) in parts.iter().enumerate() {
                    for l in lines {
                        let sym = orderer_core::jsonflat::get_u64(l, "symbol").unwrap() as u32;
                        assert_eq!(
                            orderer::hash_partition(sym, 4),
                            p as u32,
                            "{name}: symbol {sym} listed under partition {p}"
                        );
                    }
                }
            } else {
                assert_eq!(
                    parts.concat(),
                    expected,
                    "{name} [{kind:?}] P=4 single-book"
                );
            }
            checked += 1;
        }
    }
    assert!(checked >= 42, "only {checked} vector runs");
    eprintln!("golden_pipeline: {checked} vector runs byte-identical at P=1 and P=4");
}
