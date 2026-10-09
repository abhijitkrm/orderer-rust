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
    run_placed(cfg, cmds, cut, p, jc, JournalPlacement::Inline)
}

fn run_placed(
    cfg: BookConfig,
    cmds: &[(Symbol, Command)],
    cut: usize,
    p: u32,
    jc: &JournalConfig,
    placement: JournalPlacement,
) -> (Snapshot, Vec<Vec<String>>) {
    let (collect, events) = Collect::new(true);
    let mut pl = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(p)
        .ring_sizes(512, 128, 128)
        .journal_placement(placement)
        .stage_threads(2, 1)
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
    for (format, placement) in [
        (JournalFormat::Jsonl, JournalPlacement::Inline),
        (JournalFormat::Binary, JournalPlacement::Inline),
        (JournalFormat::Jsonl, JournalPlacement::Stage),
        (JournalFormat::Binary, JournalPlacement::Stage),
    ] {
        for p in [1u32, 3] {
            let dir = scratch(&format!("jr-{format:?}-{placement:?}-{p}"));
            let jc = journal_cfg(&dir, format);
            let cmds = fuzz_corpus(21 + p as u64, 5_000, 8);
            let cut = 2_000;
            let (snap, collected) = run_placed(cfg, &cmds, cut, p, &jc, placement);
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
                // a well-formed (resealed) record, just out of order
                let last = bad.len() - journal::CMD_RECORD;
                bad[last..last + 8].copy_from_slice(&1u64.to_le_bytes());
                let crc = journal::crc32c(&bad[last..last + journal::CMD_RECORD_V1]);
                bad[last + 40..last + 44].copy_from_slice(&crc.to_le_bytes());
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

#[test]
fn crc32c_matches_the_spec_check_value() {
    assert_eq!(journal::crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn checksums_catch_flipped_bits_anywhere() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(9, 300, 2);
    let dir = scratch("crc");
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .journal(journal_cfg(&dir, JournalFormat::Binary))
        .build()
        .unwrap();
    p.publish_batch(&cmds).unwrap();
    p.shutdown().unwrap();
    let path = first_journal(&dir, JournalFormat::Binary);
    let good = std::fs::read(&path).unwrap();
    // a flipped bit in a middle record: corrupt in strict and repair mode
    let mut bad = good.clone();
    let mid = journal::HEADER + 100 * journal::CMD_RECORD + 20;
    bad[mid] ^= 0x10;
    std::fs::write(&path, &bad).unwrap();
    let e = read_cmd_dir(&dir, JournalFormat::Binary).unwrap_err();
    assert!(e.detail.contains("checksum"), "{e}");
    assert!(journal::repair_dir(&dir, JournalFormat::Binary).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn repair_cuts_only_a_torn_tail() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(10, 400, 3);
    for format in [JournalFormat::Jsonl, JournalFormat::Binary] {
        let dir = scratch(&format!("repair-{format:?}"));
        let mut p = Pipeline::<FifoCore>::builder()
            .book_config(cfg)
            .journal(journal_cfg(&dir, format))
            .build()
            .unwrap();
        p.publish_batch(&cmds).unwrap();
        p.shutdown().unwrap();
        let path = first_journal(&dir, format);
        let good = std::fs::read(&path).unwrap();
        let (_, full) = read_cmd_dir(&dir, format).unwrap();
        let n = full[0].len();
        // a partial final record
        std::fs::write(&path, &good[..good.len() - 5]).unwrap();
        assert!(read_cmd_dir(&dir, format).is_err(), "strict rejects it");
        let fixed = journal::repair_dir(&dir, format).unwrap();
        assert_eq!(fixed.len(), 1, "{format:?}");
        let (_, recs) = read_cmd_dir(&dir, format).unwrap();
        assert_eq!(recs[0], full[0][..n - 1], "a prefix survives");
        if format == JournalFormat::Binary {
            // a complete final record whose bytes never reached the disk
            let mut zeroed = good.clone();
            let last = zeroed.len() - journal::CMD_RECORD;
            zeroed[last..].fill(0);
            std::fs::write(&path, &zeroed).unwrap();
            assert!(read_cmd_dir(&dir, format).is_err());
            assert_eq!(journal::repair_dir(&dir, format).unwrap().len(), 1);
            assert_eq!(read_cmd_dir(&dir, format).unwrap().1[0], full[0][..n - 1]);
        }
        // a clean file is left alone
        assert!(journal::repair_dir(&dir, format).unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn checkpoints_rotate_segments_and_bound_recovery() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(12, 3_000, 6);
    for format in [JournalFormat::Jsonl, JournalFormat::Binary] {
        let dir = scratch(&format!("ckpt-{format:?}"));
        let (collect, events) = Collect::new(true);
        let mut p = Pipeline::<FifoCore>::builder()
            .book_config(cfg)
            .partitions(3)
            .journal(journal_cfg(&dir, format))
            .egress(collect)
            .build()
            .unwrap();
        p.publish_batch(&cmds[..1000]).unwrap();
        let c1 = p.checkpoint().unwrap();
        p.publish_batch(&cmds[1000..2200]).unwrap();
        let c2 = p.checkpoint().unwrap();
        p.publish_batch(&cmds[2200..]).unwrap();
        p.shutdown().unwrap();
        assert_eq!((c1.iseq, c2.iseq), (1000, 2200));
        // only the last checkpoint and its segments remain
        let cps = journal::list_checkpoints(&dir);
        assert_eq!(cps.iter().map(|c| c.0).collect::<Vec<_>>(), vec![2200]);
        for kind in [Kind::Cmd, Kind::Evt] {
            let segs = journal::list_segments(&dir, kind, format);
            assert!(segs.iter().all(|s| s.1 == 2200), "{kind:?}: {segs:?}");
            assert_eq!(segs.len(), 3);
        }
        assert_eq!(c2.body, reference_snapshot(cfg, &cmds[..2200]));
        // recovery from the checkpoint replays exactly the tail
        let snap = read_snapshot(&cps[0].1).unwrap();
        let mut replayed = Vec::new();
        let rec = recover::<FifoCore>(
            cfg,
            &PartitionMap::hash(3).unwrap(),
            Some(&snap),
            Some((&dir, format)),
            |_, s, q, e| replayed.push(Event::canonical_sym(q, s, e)),
        )
        .unwrap();
        assert_eq!(rec.replayed, (cmds.len() - 2200) as u64);
        let all = reference_lines(cfg, &cmds);
        let prefix = reference_lines(cfg, &cmds[..2200]).len();
        assert_eq!(replayed, all[prefix..].to_vec());
        // event journal segments hold the tail's events
        let mut evts = Vec::new();
        for q in 0..3 {
            evts.extend(journal::read_evt_partition(&dir, format, q).unwrap());
        }
        evts.sort();
        let mut want = all[prefix..].to_vec();
        want.sort();
        assert_eq!(evts, want);
        assert_eq!(lines(&events.listing()).len(), all.len());
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn append_continues_the_last_segment_after_a_checkpoint() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(14, 2_000, 4);
    let dir = scratch("ckpt-append");
    let jc = journal_cfg(&dir, JournalFormat::Binary);
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(2)
        .journal(jc.clone())
        .build()
        .unwrap();
    p.publish_batch(&cmds[..800]).unwrap();
    p.checkpoint().unwrap();
    p.publish_batch(&cmds[800..1200]).unwrap();
    p.shutdown().unwrap();
    let map = PartitionMap::hash(2).unwrap();
    let snap = read_snapshot(&journal::list_checkpoints(&dir)[0].1).unwrap();
    let rec = recover::<FifoCore>(
        cfg,
        &map,
        Some(&snap),
        Some((&dir, JournalFormat::Binary)),
        |_, _, _, _| {},
    )
    .unwrap();
    assert_eq!(rec.last_iseq, 1200);
    let mut jc2 = jc.clone();
    jc2.append = true;
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(rec.book)
        .partition_map(map.clone())
        .journal(jc2)
        .initial(rec.into_initial())
        .build()
        .unwrap();
    p.publish_batch(&cmds[1200..]).unwrap();
    let s = p.snapshot().unwrap();
    p.shutdown().unwrap();
    assert_eq!(s.iseq, 2000);
    assert_eq!(s.body, reference_snapshot(cfg, &cmds));
    let (_, recs) = read_cmd_dir(&dir, JournalFormat::Binary).unwrap();
    assert_eq!(
        recs.iter().map(Vec::len).sum::<usize>(),
        1200,
        "800 checkpointed away"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn automatic_checkpoints_keep_the_directory_recoverable() {
    let cfg = fuzz_cfg();
    let cmds = fuzz_corpus(15, 20_000, 8);
    let dir = scratch("ckpt-auto");
    let mut p = Pipeline::<FifoCore>::builder()
        .book_config(cfg)
        .partitions(3)
        .journal(journal_cfg(&dir, JournalFormat::Binary))
        .checkpoint_every(std::time::Duration::from_millis(20))
        .build()
        .unwrap();
    for chunk in cmds.chunks(500) {
        p.publish_batch(chunk).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(3));
    }
    let last = p.snapshot().unwrap();
    p.shutdown().unwrap();
    let cps = journal::list_checkpoints(&dir);
    assert_eq!(cps.len(), 1, "older checkpoints are removed: {cps:?}");
    assert!(cps[0].0 > 0, "at least one automatic checkpoint ran");
    // the directory alone (newest checkpoint + its segments) recovers the final state
    let snap = read_snapshot(&cps[0].1).unwrap();
    let map = PartitionMap::hash(3).unwrap();
    let rec = recover::<FifoCore>(
        cfg,
        &map,
        Some(&snap),
        Some((&dir, JournalFormat::Binary)),
        |_, _, _, _| {},
    )
    .unwrap();
    assert_eq!(rec.last_iseq, cmds.len() as u64);
    let mut p2 = Pipeline::<FifoCore>::builder()
        .book_config(rec.book)
        .partition_map(map)
        .initial(rec.into_initial())
        .build()
        .unwrap();
    assert_eq!(p2.snapshot().unwrap().body, last.body);
    p2.shutdown().unwrap();
    assert!(
        Pipeline::<FifoCore>::builder()
            .checkpoint_every(std::time::Duration::from_millis(5))
            .build()
            .is_err(),
        "needs journals"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
