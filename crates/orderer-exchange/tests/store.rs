//! The durable archive store: round trips, segment rolls, recovery from torn and
//! corrupt files, reconciliation with the replica's window, retention, and the
//! tier reading from disk and recovering after a restart.

use orderer::{PartitionMap, Pipeline};
use orderer_cluster::StateMachine;
use orderer_consensus::Entry;
use orderer_core::{BookConfig, FifoCore};
use orderer_exchange::wire::*;
use orderer_exchange::*;
use orderer_ledger::{Event, OutMsg, Side, SHARD_SHIFT};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

struct Tmp(PathBuf);
impl Tmp {
    fn new(tag: &str) -> Tmp {
        static N: AtomicU32 = AtomicU32::new(0);
        let p = std::env::temp_dir().join(format!(
            "xstore-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&p);
        Tmp(p)
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn ev(i: u64) -> Event {
    match i % 3 {
        0 => Event::Closed { order: i },
        1 => Event::Trade {
            buy: i,
            sell: i + 1,
            price: 100 + i % 7,
            qty: 1 + i % 5,
            taker_is_buy: i % 2 == 0,
        },
        _ => Event::SliceIn {
            account: i,
            asset: 2,
            amount: i as u128 * 1_000_003,
        },
    }
}

fn cfg(dir: &Path, seg: u64) -> StoreConfig {
    let mut c = StoreConfig::new(dir);
    c.segment_events = seg;
    c.sync = SyncPolicy::Off;
    c
}

fn fill(st: &mut EventStore, stream: usize, from: u64, to: u64) {
    for i in from..to {
        st.append(stream, i, &ev(i)).unwrap();
    }
}

fn read_all(st: &mut EventStore, stream: usize, from: u64) -> Vec<(u64, Event)> {
    st.read(stream, from, usize::MAX).unwrap().unwrap()
}

fn expect(from: u64, to: u64) -> Vec<(u64, Event)> {
    (from..to).map(|i| (i, ev(i))).collect()
}

#[test]
fn events_round_trip_across_segment_rolls() {
    let d = Tmp::new("rt");
    let mut st = EventStore::open(cfg(&d.0, 64), 2).unwrap();
    fill(&mut st, 0, 1, 1001);
    fill(&mut st, 1, 1, 11);
    assert_eq!((st.base(0), st.next(0)), (1, 1001));
    assert_eq!(st.segment_paths(0).len(), 16);
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 1001));
    assert_eq!(read_all(&mut st, 0, 500), expect(500, 1001));
    assert_eq!(
        read_all(&mut st, 1, 1),
        expect(1, 11),
        "streams are separate"
    );
    assert_eq!(st.read(0, 10, 5).unwrap().unwrap(), expect(10, 15));
    assert_eq!(
        st.read(0, 63, 3).unwrap().unwrap(),
        expect(63, 66),
        "across a roll"
    );
    assert!(st.read(0, 1001, 10).unwrap().unwrap().is_empty());
    assert!(st.read(0, 5_000, 10).unwrap().unwrap().is_empty());
    assert!(st.read(0, 1, 0).unwrap().unwrap().is_empty());
}

#[test]
fn reopening_recovers_everything_and_appends_continue() {
    let d = Tmp::new("reopen");
    {
        let mut st = EventStore::open(cfg(&d.0, 50), 1).unwrap();
        fill(&mut st, 0, 1, 237);
        st.sync().unwrap();
    }
    let mut st = EventStore::open(cfg(&d.0, 50), 1).unwrap();
    assert_eq!((st.base(0), st.next(0)), (1, 237));
    assert_eq!(st.stats.truncated_bytes, 0);
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 237));
    // a replay of what is stored compares equal against the preloaded ring
    fill(&mut st, 0, 200, 237);
    assert_eq!((st.stats.mismatches, st.stats.unverified), (0, 0));
    fill(&mut st, 0, 237, 400);
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 400));
    drop(st);
    let mut st = EventStore::open(cfg(&d.0, 50), 1).unwrap();
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 400));
}

#[test]
fn a_replay_that_differs_is_counted() {
    let d = Tmp::new("mismatch");
    let mut st = EventStore::open(cfg(&d.0, 50), 1).unwrap();
    fill(&mut st, 0, 1, 20);
    st.append(0, 5, &ev(6)).unwrap();
    assert_eq!(st.stats.mismatches, 1);
    st.append(0, 5, &ev(5)).unwrap();
    assert_eq!(st.stats.mismatches, 1);
    assert_eq!(st.next(0), 20, "a replay never appends");
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 20));
}

/// Cut the newest segment at every byte offset inside its last records: recovery
/// keeps exactly the complete records and the store is writable afterwards.
#[test]
fn a_torn_tail_is_cut_at_every_offset() {
    let d = Tmp::new("torn");
    {
        let mut st = EventStore::open(cfg(&d.0, 1000), 1).unwrap();
        fill(&mut st, 0, 1, 31);
        st.sync().unwrap();
    }
    let path = std::fs::read_dir(&d.0)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let full = std::fs::read(&path).unwrap();
    // find where each record ends by reading the lengths
    let mut ends = vec![12usize];
    let mut at = 12;
    while at < full.len() {
        let plen = u32::from_le_bytes(full[at..at + 4].try_into().unwrap()) as usize;
        at += 8 + plen;
        ends.push(at);
    }
    assert_eq!(ends.len(), 31);
    let last3 = ends[ends.len() - 4];
    for cut in last3..=full.len() {
        std::fs::write(&path, &full[..cut]).unwrap();
        let mut st = EventStore::open(cfg(&d.0, 1000), 1).unwrap();
        let complete = ends.iter().filter(|e| **e <= cut).count() as u64 - 1;
        assert_eq!(st.next(0), 1 + complete, "cut at {cut}");
        assert_eq!(
            read_all(&mut st, 0, 1),
            expect(1, 1 + complete),
            "cut at {cut}"
        );
        let torn = (cut - ends[complete as usize]) as u64;
        assert_eq!(st.stats.truncated_bytes, torn, "cut at {cut}");
        // writable: a new event lands at the next index and survives a reopen
        st.append(0, 1 + complete, &ev(1 + complete)).unwrap();
        st.sync().unwrap();
        drop(st);
        let mut st = EventStore::open(cfg(&d.0, 1000), 1).unwrap();
        assert_eq!(
            read_all(&mut st, 0, 1),
            expect(1, 2 + complete),
            "cut at {cut}"
        );
    }
}

#[test]
fn a_torn_header_or_empty_segment_is_dropped() {
    let d = Tmp::new("hdr");
    {
        let mut st = EventStore::open(cfg(&d.0, 10), 1).unwrap();
        fill(&mut st, 0, 1, 11); // exactly one full segment
        st.sync().unwrap();
    }
    // a roll that crashed after creating the file but before its first record
    let stub = d.0.join("s0000-00000000000000000011.seg");
    std::fs::write(&stub, b"XEV1\x0b\0\0").unwrap(); // header cut short
    let mut st = EventStore::open(cfg(&d.0, 10), 1).unwrap();
    assert_eq!(st.next(0), 11);
    assert!(!stub.exists());
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 11));
    // and one with a whole header and no records
    let mut h = b"XEV1".to_vec();
    h.extend_from_slice(&11u64.to_le_bytes());
    std::fs::write(&stub, &h).unwrap();
    drop(st);
    let mut st = EventStore::open(cfg(&d.0, 10), 1).unwrap();
    assert_eq!(st.next(0), 11);
    fill(&mut st, 0, 11, 15);
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 15));
}

#[test]
fn a_corrupt_record_ends_the_stream_there_and_later_segments_go() {
    let d = Tmp::new("corrupt");
    {
        let mut st = EventStore::open(cfg(&d.0, 20), 1).unwrap();
        fill(&mut st, 0, 1, 101);
        st.sync().unwrap();
    }
    let seg2 = d.0.join("s0000-00000000000000000021.seg");
    let mut b = std::fs::read(&seg2).unwrap();
    // flip a payload bit in the 4th record of the second segment (index 24)
    let mut at = 12;
    for _ in 0..3 {
        at += 8 + u32::from_le_bytes(b[at..at + 4].try_into().unwrap()) as usize;
    }
    b[at + 8 + 9] ^= 0x40;
    std::fs::write(&seg2, &b).unwrap();
    let mut st = EventStore::open(cfg(&d.0, 20), 1).unwrap();
    assert_eq!(st.next(0), 24, "everything before the bad record is kept");
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 24));
    assert_eq!(st.segment_paths(0).len(), 2);
    assert!(st.stats.truncated_bytes > 0);
    // appends resume at 24 into the (reopened) second segment
    fill(&mut st, 0, 24, 60);
    drop(st);
    let mut st = EventStore::open(cfg(&d.0, 20), 1).unwrap();
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 60));
}

#[test]
fn a_missing_middle_segment_keeps_only_the_prefix() {
    let d = Tmp::new("hole");
    {
        let mut st = EventStore::open(cfg(&d.0, 20), 1).unwrap();
        fill(&mut st, 0, 1, 101);
        st.sync().unwrap();
    }
    std::fs::remove_file(d.0.join("s0000-00000000000000000041.seg")).unwrap();
    let mut st = EventStore::open(cfg(&d.0, 20), 1).unwrap();
    assert_eq!(st.next(0), 41);
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 41));
}

#[test]
fn the_window_is_authoritative_and_a_lagging_store_is_backfilled() {
    let d = Tmp::new("attach");
    let mut st = EventStore::open(cfg(&d.0, 16), 1).unwrap();
    fill(&mut st, 0, 1, 11);
    // the replica's window is [5, 31): the store lacks 11..31
    let window: VecDeque<Event> = (5..31).map(ev).collect();
    st.attach(0, 5, &window).unwrap();
    assert_eq!((st.base(0), st.next(0)), (1, 31));
    assert_eq!(st.stats.gaps, 0);
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 31));
    // a store *behind the window's start* has lost events nothing else has: restart
    let d2 = Tmp::new("attach-gap");
    let mut st = EventStore::open(cfg(&d2.0, 16), 1).unwrap();
    fill(&mut st, 0, 1, 4);
    let window: VecDeque<Event> = (10..21).map(ev).collect();
    st.attach(0, 10, &window).unwrap();
    assert_eq!(st.stats.gaps, 1);
    assert_eq!((st.base(0), st.next(0)), (10, 21));
    assert_eq!(st.read(0, 5, 10).unwrap(), Err(10));
    assert_eq!(read_all(&mut st, 0, 10), expect(10, 21));
    // an append past the next index is the same situation
    st.append(0, 50, &ev(50)).unwrap();
    assert_eq!((st.base(0), st.next(0), st.stats.gaps), (50, 51, 2));
    // a store ahead of the window is left alone
    let d3 = Tmp::new("attach-ahead");
    let mut st = EventStore::open(cfg(&d3.0, 16), 1).unwrap();
    fill(&mut st, 0, 1, 40);
    let window: VecDeque<Event> = (10..20).map(ev).collect();
    st.attach(0, 10, &window).unwrap();
    assert_eq!((st.base(0), st.next(0), st.stats.gaps), (1, 40, 0));
}

#[test]
fn retention_drops_whole_old_segments() {
    let d = Tmp::new("retain");
    let mut c = cfg(&d.0, 16);
    c.retain_events = Some(40);
    let mut st = EventStore::open(c, 1).unwrap();
    fill(&mut st, 0, 1, 201);
    let base = st.base(0);
    assert!(base > 1 && base <= 201 - 40, "base {base}");
    assert_eq!((base - 1) % 16, 0, "whole segments only");
    assert!(st.stats.segments_dropped > 0);
    assert_eq!(st.read(0, base - 1, 5).unwrap(), Err(base));
    assert_eq!(read_all(&mut st, 0, base), expect(base, 201));
    // at least `retain` events are always kept
    assert!(201 - base >= 40);
    drop(st);
    let mut c = cfg(&d.0, 16);
    c.retain_events = Some(40);
    let st = EventStore::open(c, 1).unwrap();
    assert_eq!((st.base(0), st.next(0)), (base, 201));
}

#[test]
fn sync_policies() {
    let d = Tmp::new("sync-on");
    let mut c = cfg(&d.0, 1000);
    c.sync = SyncPolicy::On;
    let mut st = EventStore::open(c, 1).unwrap();
    fill(&mut st, 0, 1, 11);
    assert_eq!(st.stats.syncs, 10, "On syncs every append");
    let d = Tmp::new("sync-off");
    let mut st = EventStore::open(cfg(&d.0, 1000), 1).unwrap();
    fill(&mut st, 0, 1, 11);
    assert_eq!(st.stats.syncs, 0);
    st.sync().unwrap();
    assert_eq!(st.stats.syncs, 1);
    st.sync().unwrap();
    assert_eq!(st.stats.syncs, 1, "nothing dirty, nothing to sync");
    let d = Tmp::new("sync-relaxed");
    let mut c = cfg(&d.0, 1000);
    c.sync = SyncPolicy::Relaxed(Duration::from_secs(3600));
    let mut st = EventStore::open(c, 1).unwrap();
    fill(&mut st, 0, 1, 50);
    assert_eq!(st.stats.syncs, 0, "not before the interval");
    let d = Tmp::new("sync-relaxed2");
    let mut c = cfg(&d.0, 1000);
    c.sync = SyncPolicy::Relaxed(Duration::from_millis(30));
    let mut st = EventStore::open(c, 1).unwrap();
    fill(&mut st, 0, 1, 3);
    std::thread::sleep(Duration::from_millis(50));
    fill(&mut st, 0, 3, 5);
    assert!(st.stats.syncs >= 1, "after the interval an append syncs");
}

#[test]
fn garbage_files_never_panic_recovery() {
    let d = Tmp::new("garbage");
    {
        let mut st = EventStore::open(cfg(&d.0, 30), 1).unwrap();
        fill(&mut st, 0, 1, 100);
        st.sync().unwrap();
    }
    let paths: Vec<_> = std::fs::read_dir(&d.0)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    let mut x = 0x2545_f491_4f6c_dd1du64;
    for round in 0..60 {
        let p = &paths[round % paths.len()];
        let mut b = std::fs::read(p).unwrap();
        for _ in 0..3 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let at = (x as usize) % b.len();
            b[at] ^= 1 << (x % 8);
        }
        std::fs::write(p, &b).unwrap();
        // whatever survives is a contiguous run from 1 that reads back correctly
        let mut st = EventStore::open(cfg(&d.0, 30), 1).unwrap();
        let next = st.next(0);
        assert!((1..=100).contains(&next));
        let base = st.base(0);
        assert_eq!(read_all(&mut st, 0, base), expect(base, next));
        // restore the pristine data for the next round
        drop(st);
        let _ = std::fs::remove_dir_all(&d.0);
        let mut st = EventStore::open(cfg(&d.0, 30), 1).unwrap();
        fill(&mut st, 0, 1, 100);
        st.sync().unwrap();
    }
}

// ---- the tier on top of the store ------------------------------------------

fn tier(retain: usize, store: Option<StoreConfig>) -> (MatchingTierSm<FifoCore>, TierView) {
    let (sm, view) = MatchingTierSm::<FifoCore>::new(
        BookConfig::default(),
        PartitionMap::hash(2).unwrap(),
        TierConfig {
            source_base: 100,
            retain,
        },
        move |init, plugs| {
            let mut b = Pipeline::<FifoCore>::builder()
                .partitions(2)
                .egress(plugs.digest)
                .egress(plugs.archive);
            if let Some(i) = init {
                b = b.initial(i);
            }
            b.build().unwrap()
        },
    );
    let sm = match store {
        Some(c) => sm.with_archive_store(c).unwrap(),
        None => sm,
    };
    (sm, view)
}

fn oid(n: u64) -> u64 {
    n | (0u64 << SHARD_SHIFT)
}

/// Entry `k` of a scripted run: a batch of crossing orders and cancels, forwarded
/// as ledger shard 0's outbox messages `[k*4, k*4+4)`.
fn entry(k: u64) -> Vec<u8> {
    let sym = (k % 4) as u32;
    let mk = |i: u64| {
        let n = k * 4 + i;
        let side = if n % 2 == 0 { Side::Buy } else { Side::Sell };
        OutMsg::Submit {
            order: oid(n + 1),
            symbol: sym,
            side,
            price: 100 + (n % 3),
            qty: 1 + (n % 4),
        }
    };
    encode_forward(&Forward {
        shard: 0,
        first_idx: k * 4,
        msgs: (0..4).map(mk).collect(),
    })
}

fn apply(sm: &mut MatchingTierSm<FifoCore>, k: u64) {
    sm.apply(
        k + 1,
        &Entry {
            term: 1,
            data: entry(k),
        },
    );
}

fn drain(sm: &mut MatchingTierSm<FifoCore>, at: u64) {
    sm.digest(at);
}

fn whole(view: &TierView) -> Vec<Vec<(u64, Event)>> {
    (0..2)
        .map(|p| {
            let mut out = Vec::new();
            let mut from = 1;
            loop {
                let v = view.read(100 + p, from, 37).unwrap().unwrap();
                if v.is_empty() {
                    return out;
                }
                from = v.last().unwrap().0 + 1;
                out.extend(v);
            }
        })
        .collect()
}

#[test]
fn a_reader_behind_the_window_is_served_from_disk() {
    let d = Tmp::new("tier-read");
    let (mut reference, rview) = tier(1 << 20, None);
    let (mut sm, view) = tier(8, Some(cfg(&d.0, 32)));
    for k in 0..300 {
        apply(&mut reference, k);
        apply(&mut sm, k);
    }
    drain(&mut reference, 300);
    drain(&mut sm, 300);
    let want = whole(&rview);
    assert!(want.iter().map(|w| w.len()).sum::<usize>() > 200);
    // without a store the same window would be truncated
    let (mut plain, pview) = tier(8, None);
    for k in 0..300 {
        apply(&mut plain, k);
    }
    drain(&mut plain, 300);
    assert!(pview.read(100, 1, 10).unwrap().is_err());
    // with one, every event from index 1 reads back, identically
    assert_eq!(whole(&view), want);
    assert_eq!(view.store_stats().unwrap().mismatches, 0);
    assert!(view.store_error().is_none());
    // a snapshot syncs the store before it exists (policy Off never syncs on its own;
    // the syncs so far are segment rolls)
    let before = view.store_stats().unwrap().syncs;
    let _ = sm.snapshot(300);
    assert!(view.store_stats().unwrap().syncs > before);
}

#[test]
fn a_restart_replays_into_the_store_without_a_gap_or_a_mismatch() {
    let d = Tmp::new("tier-restart");
    let (mut reference, rview) = tier(1 << 20, None);
    for k in 0..400 {
        apply(&mut reference, k);
    }
    drain(&mut reference, 400);
    let want = whole(&rview);
    let snap;
    {
        let (mut sm, _view) = tier(16, Some(cfg(&d.0, 32)));
        for k in 0..200 {
            apply(&mut sm, k);
        }
        snap = sm.snapshot(200);
        for k in 200..330 {
            apply(&mut sm, k); // applied (and stored) after the snapshot
        }
        drain(&mut sm, 330);
        // the process dies here: nothing after this point is closed cleanly
    }
    let (mut sm, view) = tier(16, Some(cfg(&d.0, 32)));
    sm.restore(200, &snap).unwrap();
    for k in 200..400 {
        apply(&mut sm, k);
    }
    drain(&mut sm, 400);
    assert_eq!(whole(&view), want);
    let st = view.store_stats().unwrap();
    assert_eq!(st.mismatches, 0, "replayed events equal the stored ones");
    assert_eq!(st.gaps, 0);
    assert_eq!(st.unverified, 0, "the ring was preloaded from disk");
}

#[test]
fn a_crash_that_tears_the_store_is_repaired_by_snapshot_and_replay() {
    let d = Tmp::new("tier-crash");
    let (mut reference, rview) = tier(1 << 20, None);
    for k in 0..300 {
        apply(&mut reference, k);
    }
    drain(&mut reference, 300);
    let want = whole(&rview);
    let snap;
    {
        let (mut sm, _v) = tier(1 << 20, Some(cfg(&d.0, 32)));
        for k in 0..150 {
            apply(&mut sm, k);
        }
        snap = sm.snapshot(150); // syncs the store
        for k in 150..220 {
            apply(&mut sm, k);
        }
        drain(&mut sm, 220);
    }
    // power loss: the newest segment of each stream loses its unsynced tail, cut
    // mid-record, and one segment past the snapshot disappears altogether
    let mut cut = 0;
    for p in 0..2 {
        let mut segs: Vec<_> = std::fs::read_dir(&d.0)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p2| {
                p2.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(&format!("s{p:04}"))
            })
            .collect();
        segs.sort();
        let last = segs.last().unwrap();
        let len = std::fs::metadata(last).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(last).unwrap();
        f.set_len(len - len.min(40) / 2 - 3).unwrap();
        cut += 1;
    }
    assert_eq!(cut, 2);
    let (mut sm, view) = tier(1 << 20, Some(cfg(&d.0, 32)));
    sm.restore(150, &snap).unwrap();
    for k in 150..300 {
        apply(&mut sm, k);
    }
    drain(&mut sm, 300);
    assert_eq!(whole(&view), want);
    let st = view.store_stats().unwrap();
    assert_eq!(st.mismatches, 0);
    assert!(st.truncated_bytes > 0, "recovery cut the torn tails");
}

#[test]
fn a_store_that_lost_events_older_than_the_snapshot_window_restarts_cleanly() {
    let d = Tmp::new("tier-gap");
    let (mut reference, rview) = tier(1 << 20, None);
    for k in 0..200 {
        apply(&mut reference, k);
    }
    drain(&mut reference, 200);
    let want = whole(&rview);
    let snap;
    {
        let (mut sm, _v) = tier(1 << 20, None);
        for k in 0..120 {
            apply(&mut sm, k);
        }
        snap = sm.snapshot(120);
    }
    // a brand new (empty) store meets a snapshot whose window starts at 1: no gap
    let (mut sm, view) = tier(1 << 20, Some(cfg(&d.0, 32)));
    sm.restore(120, &snap).unwrap();
    for k in 120..200 {
        apply(&mut sm, k);
    }
    drain(&mut sm, 200);
    assert_eq!(whole(&view), want);
    assert_eq!(view.store_stats().unwrap().gaps, 0);
    // the same, but the snapshot window is shorter than the history: the store
    // restarts at the window's base and says so
    let d2 = Tmp::new("tier-gap2");
    let snap2;
    {
        let (mut sm, _v) = tier(10, None);
        for k in 0..120 {
            apply(&mut sm, k);
        }
        snap2 = sm.snapshot(120);
    }
    // an older run left only events 1..=3 of stream 0 in the store
    {
        let mut st = EventStore::open(cfg(&d2.0, 32), 2).unwrap();
        for i in 1..4 {
            st.append(0, i, &ev(i)).unwrap();
        }
        st.sync().unwrap();
    }
    let (mut sm, view) = tier(10, Some(cfg(&d2.0, 32)));
    sm.restore(120, &snap2).unwrap();
    assert!(view.store_stats().unwrap().gaps >= 1);
    // what was lost is reported as truncated, not served wrongly
    assert!(view.read(100, 1, 10).unwrap().is_err());
}

/// A record that is intact (its CRC is good) but at the wrong index ends the
/// stream just as a damaged one does.
#[test]
fn a_well_formed_record_at_the_wrong_index_is_not_trusted() {
    let d = Tmp::new("wrongidx");
    {
        let mut st = EventStore::open(cfg(&d.0, 1000), 1).unwrap();
        fill(&mut st, 0, 1, 31);
        st.sync().unwrap();
    }
    let path = std::fs::read_dir(&d.0)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut b = std::fs::read(&path).unwrap();
    // copy record 3 (valid on its own) to the end of the file
    let mut at = 12;
    let mut starts = vec![];
    while at < b.len() {
        starts.push(at);
        at += 8 + u32::from_le_bytes(b[at..at + 4].try_into().unwrap()) as usize;
    }
    let rec = b[starts[2]..starts[3]].to_vec();
    b.extend_from_slice(&rec);
    std::fs::write(&path, &b).unwrap();
    let mut st = EventStore::open(cfg(&d.0, 1000), 1).unwrap();
    assert_eq!(st.next(0), 31, "the stray record is cut");
    assert_eq!(st.stats.truncated_bytes, rec.len() as u64);
    assert_eq!(read_all(&mut st, 0, 1), expect(1, 31));
}

#[test]
fn an_empty_window_beyond_the_store_still_restarts_it() {
    let d = Tmp::new("emptywin");
    let mut st = EventStore::open(cfg(&d.0, 16), 1).unwrap();
    fill(&mut st, 0, 1, 4);
    // the replica has seen events 1..10 but its window is empty (retain pruned all)
    st.attach(0, 10, &VecDeque::new()).unwrap();
    assert_eq!(st.stats.gaps, 1);
    assert_eq!((st.base(0), st.next(0)), (10, 10));
    assert_eq!(st.read(0, 5, 10).unwrap(), Err(10));
    assert!(st.read(0, 10, 10).unwrap().unwrap().is_empty());
}
