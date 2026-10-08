//! A3 — journals, snapshots and recovery (spec/JOURNAL.md): journal
//! contents in both encodings, snapshot cuts, recovery at the same or a
//! different P, resumption with append, and strict rejection of torn or
//! corrupt journals.

mod common;

use std::path::Path;

use common::*;
use orderer::journal::{self, journal_path, read_cmd_dir, read_evt_journal, JournalFormat, Kind};
use orderer::recover::{read_snapshot, recover};
use orderer::*;
use orderer_core::*;

fn journal_cfg(dir: &Path, format: JournalFormat) -> JournalConfig {
    JournalConfig {
        dir: dir.to_path_buf(),
        format,
        fsync: FsyncPolicy::every_n(64),
        events: true,
        append: false,
    }
}

/// Run `cmds` with journals, taking a snapshot after `cut` commands.
fn run_with_snapshot(
    cfg: BookConfig,
    cmds: &[(Symbol, Command)],
    cut: usize,
    p: u32,
    jc: &JournalConfig,
) -> (Snapshot, Vec<Vec<String>>) {
    let (collect, events) = Collect::new(true);
    let mut pl = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(p)
        .ring_sizes(512, 128, 128)
        .journal(jc.clone())
        .egress(collect)
        .build()
        .unwrap();
    pl.publish_batch(&cmds[..cut]).unwrap();
    let snap = pl.snapshot().unwrap();
    pl.publish_batch(&cmds[cut..]).unwrap();
    pl.shutdown().unwrap();
    (snap, events.take().iter().map(|b| lines(b)).collect())
}

#[test]
fn journals_snapshot_and_recovery_round_trip() {
    let cfg = fuzz_cfg();
    for format in [JournalFormat::Jsonl, JournalFormat::Binary] {
        for p in [1u32, 3] {
            let dir = scratch(&format!("jr-{format:?}-{p}"));
            let jc = journal_cfg(&dir, format);
            let cmds = fuzz_corpus(21 + p as u64, 5_000, 8);
            let cut = 2_000;
            let (snap, collected) = run_with_snapshot(cfg, &cmds, cut, p, &jc);
            let all_ref = reference_lines(cfg, &cmds);
            let prefix_len = reference_lines(cfg, &cmds[..cut]).len();

            // snapshot: exact cut, body identical to matcher's
            assert_eq!(snap.iseq, cut as u64);
            assert_eq!(
                snap.body,
                reference_snapshot(cfg, &cmds[..cut]),
                "{format:?} P={p}"
            );

            // command journals: every command once, iseq = file position,
            // each in its routed partition
            let (hdr, recs) = read_cmd_dir(&dir, format).unwrap();
            assert_eq!(hdr.partitions, p);
            assert!(
                journal::same_book(hdr.book, cfg),
                "journal header carries the book config"
            );
            let mut merged: Vec<_> = Vec::new();
            for (q, rs) in recs.iter().enumerate() {
                for &(iseq, sym, cmd) in rs {
                    assert_eq!(hash_partition(sym, p), q as u32);
                    merged.push((iseq, sym, cmd));
                }
            }
            merged.sort_by_key(|r| r.0);
            assert_eq!(merged.len(), cmds.len());
            for (i, &(iseq, sym, cmd)) in merged.iter().enumerate() {
                assert_eq!(iseq, i as u64 + 1);
                assert_eq!((sym, cmd), cmds[i]);
            }

            // event journals == what egress delivered, partition by partition
            for (q, delivered) in collected.iter().enumerate() {
                let path = journal_path(&dir, Kind::Evt, q as u32, format);
                let (_, evts) = read_evt_journal(&path, format).unwrap();
                assert_eq!(&evts, delivered, "{format:?} P={p} evt-{q}");
            }

            // recovery at the same P and at a different P
            for rp in [p, 2] {
                let map = PartitionMap::hash(rp).unwrap();
                let mut replayed = Vec::new();
                let rec = recover::<FifoCore>(
                    cfg,
                    &map,
                    Some(&snap),
                    Some((&dir, format)),
                    |_, s, q, e| replayed.push(Event::canonical_sym(q, s, e)),
                )
                .unwrap();
                assert_eq!(rec.snapshot_iseq, cut as u64);
                assert_eq!(rec.last_iseq, cmds.len() as u64);
                assert_eq!(rec.replayed, (cmds.len() - cut) as u64);
                assert_eq!(replayed, all_ref[prefix_len..], "{format:?} P={p} → {rp}");
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

#[test]
fn recovered_pipeline_resumes_and_appends() {
    let cfg = fuzz_cfg();
    let dir = scratch("resume");
    let mut jc = journal_cfg(&dir, JournalFormat::Binary);
    let cmds = fuzz_corpus(77, 4_000, 6);
    let (first, second) = cmds.split_at(2_500);

    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(2)
        .journal(jc.clone())
        .build()
        .unwrap();
    p.publish_batch(first).unwrap();
    p.shutdown().unwrap(); // a clean stop; journals hold all 2,500

    // restart from journals alone, appending to the same files
    let map = PartitionMap::hash(2).unwrap();
    let rec = recover::<FifoCore>(
        cfg,
        &map,
        None,
        Some((&dir, JournalFormat::Binary)),
        |_, _, _, _| {},
    )
    .unwrap();
    assert_eq!(rec.last_iseq, 2_500);
    jc.append = true;
    let (collect, events) = Collect::new(true);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(rec.book)
        .partition_map(map.clone())
        .journal(jc)
        .egress(collect)
        .initial(rec.into_initial())
        .build()
        .unwrap();
    p.publish_batch(second).unwrap();
    p.drain().unwrap();
    let snap = p.snapshot().unwrap();
    p.shutdown().unwrap();

    // continuation events == the reference's tail; state == reference
    let all_ref = reference_lines(cfg, &cmds);
    let prefix_len = reference_lines(cfg, first).len();
    let got: Vec<String> = events.take().iter().flat_map(|b| lines(b)).collect();
    assert_eq!(
        by_symbol(got.iter().map(String::as_str)),
        by_symbol(all_ref[prefix_len..].iter().map(String::as_str))
    );
    assert_eq!(
        snap.iseq, 4_000,
        "iseq resumed after the recovered high-water mark"
    );
    assert_eq!(snap.body, reference_snapshot(cfg, &cmds));

    // the appended journals replay the whole history
    let (_, recs) = read_cmd_dir(&dir, JournalFormat::Binary).unwrap();
    let mut iseqs: Vec<u64> = recs.iter().flatten().map(|r| r.0).collect();
    iseqs.sort_unstable();
    assert_eq!(iseqs, (1..=4_000).collect::<Vec<_>>());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn seq0_snapshot_and_empty_tail() {
    let cfg = fuzz_cfg();
    let dir = scratch("edges");
    let jc = journal_cfg(&dir, JournalFormat::Jsonl);
    let cmds = fuzz_corpus(5, 1_500, 4);
    // snapshot before any command: cut 0, empty books
    let (snap0, _) = run_with_snapshot(cfg, &cmds, 0, 2, &jc);
    assert_eq!(snap0.iseq, 0);
    assert_eq!(snap0.body, reference_snapshot(cfg, &[]));
    let map = PartitionMap::hash(2).unwrap();
    let mut n = 0;
    recover::<FifoCore>(
        cfg,
        &map,
        Some(&snap0),
        Some((&dir, JournalFormat::Jsonl)),
        |_, _, _, _| n += 1,
    )
    .unwrap();
    assert_eq!(
        n,
        reference_lines(cfg, &cmds).len(),
        "seq-0 snapshot + full replay"
    );

    // snapshot after the last command: nothing to replay
    let (snap_end, _) = run_with_snapshot(cfg, &cmds, cmds.len(), 2, &jc);
    let mut n = 0;
    let rec = recover::<FifoCore>(
        cfg,
        &map,
        Some(&snap_end),
        Some((&dir, JournalFormat::Jsonl)),
        |_, _, _, _| n += 1,
    )
    .unwrap();
    assert_eq!((n, rec.replayed), (0, 0));

    // snapshot files round-trip with their sidecar
    let path = dir.join("s.snap");
    snap_end.write(&path).unwrap();
    assert_eq!(read_snapshot(&path).unwrap(), snap_end);
    let _ = std::fs::remove_dir_all(&dir);
}

fn first_journal(dir: &Path, f: JournalFormat) -> std::path::PathBuf {
    journal_path(dir, Kind::Cmd, 0, f)
}

#[test]
fn torn_and_corrupt_journals_are_errors() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(8, 500, 1);
    for format in [JournalFormat::Jsonl, JournalFormat::Binary] {
        let dir = scratch(&format!("torn-{format:?}"));
        let jc = journal_cfg(&dir, format);
        let mut p = Pipeline::<FifoCore>::builder()
            .book_config(cfg)
            .journal(jc)
            .build()
            .unwrap();
        p.publish_batch(&cmds).unwrap();
        p.shutdown().unwrap();
        let path = first_journal(&dir, format);
        let good = std::fs::read(&path).unwrap();
        assert!(read_cmd_dir(&dir, format).is_ok());

        // torn tail: last record cut short
        std::fs::write(&path, &good[..good.len() - 7]).unwrap();
        let e = read_cmd_dir(&dir, format).unwrap_err();
        assert!(e.detail.contains("torn"), "{format:?}: {e}");

        // a record whose iseq goes backwards
        let mut bad = good.clone();
        match format {
            JournalFormat::Binary => {
                let last = bad.len() - journal::CMD_RECORD;
                bad[last..last + 8].copy_from_slice(&1u64.to_le_bytes());
            }
            JournalFormat::Jsonl => {
                bad.extend_from_slice(
                    b"{\"cmd\":\"cancel\",\"symbol\":0,\"order_id\":1,\"iseq\":3}\n",
                );
            }
        }
        std::fs::write(&path, &bad).unwrap();
        let e = read_cmd_dir(&dir, format).unwrap_err();
        assert!(e.detail.contains("iseq"), "{format:?}: {e}");

        // garbage header
        let mut bad = good.clone();
        bad[2] = b'#';
        std::fs::write(&path, &bad).unwrap();
        assert!(read_cmd_dir(&dir, format).is_err());

        // a JSONL record that doesn't parse
        if format == JournalFormat::Jsonl {
            let mut bad = good.clone();
            bad.extend_from_slice(b"{\"cmd\":\"new\",\"symbol\":0,\"iseq\":999}\n");
            std::fs::write(&path, &bad).unwrap();
            assert!(read_cmd_dir(&dir, format)
                .unwrap_err()
                .detail
                .contains("malformed"));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn binary_and_jsonl_encode_the_same_records() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(13, 2_000, 5);
    let (a, b) = (scratch("enc-j"), scratch("enc-b"));
    for (dir, f) in [(&a, JournalFormat::Jsonl), (&b, JournalFormat::Binary)] {
        let mut p = Pipeline::<FifoCore>::builder()
            .book_config(cfg)
            .partitions(3)
            .journal(journal_cfg(dir, f))
            .build()
            .unwrap();
        p.publish_batch(&cmds).unwrap();
        p.shutdown().unwrap();
    }
    assert_eq!(
        read_cmd_dir(&a, JournalFormat::Jsonl).unwrap(),
        read_cmd_dir(&b, JournalFormat::Binary).unwrap()
    );
    for q in 0..3 {
        assert_eq!(
            read_evt_journal(
                &journal_path(&a, Kind::Evt, q, JournalFormat::Jsonl),
                JournalFormat::Jsonl
            )
            .unwrap()
            .1,
            read_evt_journal(
                &journal_path(&b, Kind::Evt, q, JournalFormat::Binary),
                JournalFormat::Binary
            )
            .unwrap()
            .1
        );
    }
    let _ = std::fs::remove_dir_all(&a);
    let _ = std::fs::remove_dir_all(&b);
}
