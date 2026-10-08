//! Conformance with the vendored orderer vectors (`vectors/manifest.json`),
//! driven through the harness binaries so the spec/HARNESS.md CLI contract
//! is under test too:
//! - routing-hash / routing-table: spec/ROUTING.md
//! - golden: matcher-format vectors through the pipeline (A1)
//! - pipeline: `orderrun` listings and per-partition journals, both
//!   encodings, byte-exact at every P
//! - recovery: `orderrun --snap` then `orderrecover` at other P's

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::*;
use orderer::harness::parse_corpus;
use orderer::*;
use orderer_core::jsonflat::{get_str, get_u64};
use orderer_core::IndexKind;

/// The vendored manifest's orderer vectors, as (kind, name, json text).
fn manifest() -> Vec<(String, String, String)> {
    let text = std::fs::read_to_string(vectors().join("manifest.json")).unwrap();
    let body = &text[text.find("\"vectors\"").unwrap()..];
    let mut out = Vec::new();
    for entry in body.split("\"name\": ").skip(1) {
        let name = entry.split('"').nth(1).unwrap().to_string();
        let kind = entry
            .split("\"kind\": \"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap()
            .to_string();
        out.push((kind, name, entry.to_string()));
    }
    out
}

fn bin(name: &str) -> PathBuf {
    match name {
        "orderrun" => env!("CARGO_BIN_EXE_orderrun").into(),
        "ordererfuzz" => env!("CARGO_BIN_EXE_ordererfuzz").into(),
        "orderrecover" => env!("CARGO_BIN_EXE_orderrecover").into(),
        "ordersnap" => env!("CARGO_BIN_EXE_ordersnap").into(),
        _ => unreachable!(),
    }
}

fn run(name: &str, args: &[&str]) -> Vec<u8> {
    let out = Command::new(bin(name)).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "{name} {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

#[test]
fn routing_vectors() {
    let v = vectors();
    let hash = std::fs::read_to_string(v.join("routing/hash.jsonl")).unwrap();
    let mut n = 0;
    for l in hash.lines().skip(1) {
        let (sym, p, want) = (
            get_u64(l, "symbol").unwrap() as u32,
            get_u64(l, "partitions").unwrap() as u32,
            get_u64(l, "partition").unwrap() as u32,
        );
        assert_eq!(hash_partition(sym, p), want, "{l}");
        assert_eq!(PartitionMap::hash(p).unwrap().partition(sym), want, "{l}");
        n += 1;
    }
    assert!(n > 500);
    let map = std::fs::read_to_string(v.join("routing/table.map.jsonl")).unwrap();
    let m = PartitionMap::parse_table(&map, 4).unwrap();
    let expect = std::fs::read_to_string(v.join("routing/table.expect.jsonl")).unwrap();
    for l in expect.lines().skip(1) {
        let sym = get_u64(l, "symbol").unwrap() as u32;
        assert_eq!(
            m.partition(sym),
            get_u64(l, "partition").unwrap() as u32,
            "{l}"
        );
    }
}

#[test]
fn every_manifest_vector() {
    let v = vectors();
    let mut seen = std::collections::BTreeMap::<String, usize>::new();
    for (kind, name, entry) in manifest() {
        *seen.entry(kind.clone()).or_default() += 1;
        match kind.as_str() {
            "routing-hash" | "routing-table" => {} // routing_vectors()
            "golden" => golden(&v, &name),
            "pipeline" => pipeline(&v, &name, &entry),
            "recovery" => recovery(&v, &name),
            k => panic!("unknown vector kind {k} — re-vendored spec newer than this test?"),
        }
    }
    for k in [
        "golden",
        "pipeline",
        "recovery",
        "routing-hash",
        "routing-table",
    ] {
        assert!(seen.contains_key(k), "no {k} vectors vendored");
    }
}

fn golden(v: &Path, name: &str) {
    let cmd = v.join(format!("{name}.cmd.jsonl"));
    let text = std::fs::read_to_string(&cmd).unwrap();
    let corpus = parse_corpus(&text, s(&cmd)).unwrap();
    let expected: Vec<String> = std::fs::read_to_string(v.join(format!("{name}.evt.jsonl")))
        .unwrap()
        .lines()
        .skip(1)
        .map(str::to_string)
        .collect();
    let hdr = text.lines().next().unwrap();
    let modes = match get_str(hdr, "index") {
        Some("both") => vec![IndexKind::Ladder, IndexKind::Tree],
        Some("tree") => vec![IndexKind::Tree],
        _ => vec![IndexKind::Ladder],
    };
    for kind in modes {
        let cfg = orderer_core::BookConfig {
            index: kind,
            ..corpus.book
        };
        for p in [1, 3] {
            let got = run_pipeline(cfg, &corpus.cmds, p, corpus.engine).concat();
            if corpus.engine {
                assert_eq!(
                    by_symbol(got.iter().map(String::as_str)),
                    by_symbol(expected.iter().map(String::as_str)),
                    "{name} [{kind:?}] P={p}"
                );
            } else {
                assert_eq!(got, expected, "{name} [{kind:?}] P={p}");
            }
        }
    }
    // and through the harness, byte-exact
    let out = run("ordererfuzz", &[s(&cmd)]);
    assert_eq!(lines(&out), expected, "{name} via ordererfuzz");
}

fn pipeline(v: &Path, name: &str, entry: &str) {
    let input = v.join(format!("{name}.cmd.jsonl"));
    assert!(entry.contains("\"partitions\""));
    for p in [1, 2, 4] {
        let want = v.join(name).join(format!("P{p}"));
        for (flag, sub) in [(None, "jsonl"), (Some("--binary"), "binary")] {
            let dir = scratch(&format!("vec-{}-{p}-{sub}", name.replace('/', "_")));
            let ps = p.to_string();
            let mut args = vec![s(&input), "--partitions", &ps, "--journal-dir", s(&dir)];
            args.extend(flag);
            let listing = run("orderrun", &args);
            assert_eq!(
                listing,
                std::fs::read(want.join("listing.evt")).unwrap(),
                "{name} P={p} {sub}: listing"
            );
            let mut files: Vec<_> = std::fs::read_dir(want.join(sub))
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect();
            files.sort();
            assert_eq!(files.len(), 2 * p as usize);
            for f in files {
                assert_eq!(
                    std::fs::read(dir.join(&f)).unwrap(),
                    std::fs::read(want.join(sub).join(&f)).unwrap(),
                    "{name} P={p}: {} differs byte-for-byte",
                    f.to_string_lossy()
                );
            }
            // journal-only recovery reproduces the listing per symbol
            let mut rargs = vec!["--journal-dir", s(&dir), "--partitions", &ps];
            rargs.extend(flag);
            let rec = run("orderrecover", &rargs);
            assert_eq!(
                by_symbol(lines(&rec).iter().map(String::as_str)),
                by_symbol(lines(&listing).iter().map(String::as_str)),
                "{name} P={p} {sub}: journal-form recovery"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

fn recovery(v: &Path, name: &str) {
    let d = v.join(name);
    let dir = scratch(&format!("vec-{}", name.replace('/', "_")));
    let snap = dir.join("prefix.snap");
    run(
        "orderrun",
        &[
            s(&d.join("prefix.cmd.jsonl")),
            "--partitions",
            "3",
            "--snap",
            s(&snap),
        ],
    );
    assert_eq!(
        std::fs::read(&snap).unwrap(),
        std::fs::read(d.join("prefix.snap")).unwrap(),
        "{name}: snapshot body"
    );
    assert_eq!(
        std::fs::read(meta_path(&snap)).unwrap(),
        std::fs::read(d.join("prefix.snap.meta")).unwrap(),
        "{name}: snapshot sidecar"
    );
    for p in ["1", "3"] {
        let out = run(
            "orderrecover",
            &[
                s(&d.join("prefix.snap")),
                s(&d.join("tail.cmd.jsonl")),
                "--partitions",
                p,
            ],
        );
        assert_eq!(
            out,
            std::fs::read(d.join(format!("recov.P{p}.evt"))).unwrap(),
            "{name}: recovery at P={p}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn harness_exit_codes() {
    let bad = |name: &str, args: &[&str]| {
        let st = Command::new(bin(name)).args(args).output().unwrap().status;
        assert_eq!(st.code(), Some(2), "{name} {args:?}");
    };
    let v = vectors();
    let input = v.join("pipeline/multisymbol.cmd.jsonl");
    bad("orderrun", &[]);
    bad("orderrun", &[s(&input), "--bogus"]);
    bad("orderrun", &[s(&input), "--partitions", "0"]);
    bad(
        "orderrun",
        &[
            s(&input),
            "--partitions",
            "4",
            "--partition-map",
            s(&v.join("routing/table.map.jsonl"))
                .replace("map", "nope")
                .as_str(),
        ],
    );
    bad(
        "orderrun",
        &[
            s(&input),
            "--partitions",
            "3",
            "--partition-map",
            s(&v.join("routing/table.map.jsonl")),
        ],
    );
    bad("ordererfuzz", &[s(&input), "--binary"]);
    bad("orderrun", &["/nonexistent/file"]);
    let dir = scratch("exitcodes");
    let tail = dir.join("trunc.cmd");
    std::fs::write(
        &tail,
        "{\"cmd\":\"cancel\",\"symbol\":1,\"order_id\":5}\n{\"cmd\":\"new\",\"sym",
    )
    .unwrap();
    bad(
        "orderrecover",
        &[s(&v.join("recovery/fuzz_s11/prefix.snap")), s(&tail)],
    );
    bad("orderrecover", &["--journal-dir", s(&dir)]);
    let _ = std::fs::remove_dir_all(&dir);
}
