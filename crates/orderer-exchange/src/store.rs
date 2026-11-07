//! A durable store behind the archive's in-memory window: append-only segment
//! files, one set per stream, so a settler that has fallen further behind than the
//! window reads from disk instead of getting `Truncated`.
//!
//! **Files.** `s{stream:04}-{first:020}.seg`, where `first` is the index of the
//! segment's first event. A segment starts with a 12-byte header (`XEV1`, `first`
//! u64) followed by records `len u32 | crc32c u32 | payload` with `payload = idx
//! u64 | event` (the event in the `wire::put_event` encoding, `len` = payload
//! length). Indices are contiguous across segments.
//!
//! **What it stores is derived data.** Every event is a function of the replicated
//! log, and a snapshot carries the memory window, so the store can always be
//! rebuilt from the replica's own state; durability here only decides how far back
//! a reader can go. That is why the default sync policy is relaxed and why a
//! crash that loses the unsynced tail loses nothing that matters: `snapshot()`
//! syncs the store before it returns, the log is compacted only after a snapshot,
//! and entries after the last snapshot are replayed, regenerating the same events
//! with the same indices.
//!
//! **Recovery** ([`EventStore::open`]) scans each stream's segments in order,
//! checking header, CRC and index contiguity. A torn or corrupt record ends the
//! stream there: the tail of that segment is truncated and any later segment is
//! deleted, so what remains is a contiguous prefix.
//!
//! **Reconciling with the replica** ([`EventStore::attach`], and every
//! [`EventStore::append`]): the replica's memory window `[base, next)` is
//! authoritative for the index space. An event at an index the store already
//! has is a replay and is skipped (and compared against the recent ring when it
//! is in it); one at the next index is appended; the window's events that the
//! store lacks are backfilled. If the store is *behind the window's start* (events
//! lost that no memory copy has), the stream's segments are discarded and the store
//! restarts at the window's base, and [`StoreStats::gaps`] says so.
//!
//! **Retention** is by whole segments: [`StoreConfig::retain_events`] keeps at least
//! that many of the newest events per stream; older complete segments are deleted
//! and [`EventStore::base`] moves up. Readers behind it get `Truncated`.

use crate::wire::{get_event, put_event, Rd};
use orderer::journal::crc32c;
use orderer_ledger::Event;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MAGIC: &[u8; 4] = b"XEV1";
const HEADER: u64 = 12;
/// The most recent events kept in memory to compare replays against.
const RING: usize = 4096;
/// A record's payload cannot exceed this (an event is a few dozen bytes).
const MAX_PAYLOAD: u32 = 1 << 12;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Never sync on its own (a snapshot and `sync()` still do).
    Off,
    /// Sync when this long has passed since the last, checked at each append.
    Relaxed(Duration),
    /// Sync after every append: the archive survives a power loss event by event.
    On,
}

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub dir: PathBuf,
    /// Events per segment file.
    pub segment_events: u64,
    pub sync: SyncPolicy,
    /// Keep at least this many of the newest events per stream (`None`: keep all).
    pub retain_events: Option<u64>,
}

impl StoreConfig {
    pub fn new(dir: impl Into<PathBuf>) -> StoreConfig {
        StoreConfig {
            dir: dir.into(),
            segment_events: 1 << 16,
            sync: SyncPolicy::Relaxed(Duration::from_millis(10)),
            retain_events: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreStats {
    /// Streams restarted because the store was behind the memory window.
    pub gaps: u64,
    /// Bytes cut off segment tails at recovery (torn writes).
    pub truncated_bytes: u64,
    /// Replayed events compared against the ring that did not match: must be 0.
    pub mismatches: u64,
    /// Replayed events older than the ring, skipped without comparison.
    pub unverified: u64,
    pub syncs: u64,
    pub segments_dropped: u64,
}

struct Seg {
    first: u64,
    count: u64,
    path: PathBuf,
}

struct Stream {
    id: usize,
    segs: Vec<Seg>,
    /// Index the next appended event gets.
    next: u64,
    w: Option<io::BufWriter<File>>,
    ring: VecDeque<(u64, Vec<u8>)>,
    dirty: bool,
}

pub struct EventStore {
    cfg: StoreConfig,
    streams: Vec<Stream>,
    last_sync: Instant,
    pub stats: StoreStats,
}

impl std::fmt::Debug for EventStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventStore({} streams, {:?})",
            self.streams.len(),
            self.stats
        )
    }
}

fn seg_name(stream: usize, first: u64) -> String {
    format!("s{stream:04}-{first:020}.seg")
}

fn parse_name(n: &str) -> Option<(usize, u64)> {
    let n = n.strip_suffix(".seg")?.strip_prefix('s')?;
    let (a, b) = n.split_once('-')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

fn encode_record(idx: u64, ev: &Event) -> Vec<u8> {
    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(&idx.to_le_bytes());
    put_event(&mut payload, ev);
    let mut rec = Vec::with_capacity(8 + payload.len());
    rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    rec.extend_from_slice(&crc32c(&payload).to_le_bytes());
    rec.extend_from_slice(&payload);
    rec
}

fn decode_payload(p: &[u8]) -> Option<(u64, Event)> {
    let idx = u64::from_le_bytes(p.get(..8)?.try_into().ok()?);
    let mut r = Rd::new(&p[8..]);
    let ev = get_event(&mut r).ok()?;
    r.done().ok()?;
    Some((idx, ev))
}

/// What a scan of one segment found.
struct Scan {
    count: u64,
    /// Offset just past the last good record.
    good_end: u64,
    /// The file ended cleanly (no torn or corrupt record).
    clean: bool,
}

/// Walk a segment's records; `f` sees each good `(idx, event)`.
fn scan_segment(
    path: &Path,
    first: u64,
    mut f: impl FnMut(u64, Event) -> bool,
) -> io::Result<Scan> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut hdr = [0u8; HEADER as usize];
    if len < HEADER || file.read_exact(&mut hdr).is_err() {
        return Ok(Scan {
            count: 0,
            good_end: 0,
            clean: false,
        });
    }
    if &hdr[..4] != MAGIC || u64::from_le_bytes(hdr[4..].try_into().unwrap()) != first {
        return Ok(Scan {
            count: 0,
            good_end: 0,
            clean: false,
        });
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let (mut at, mut count) = (0usize, 0u64);
    let mut clean = true;
    while at < buf.len() {
        let bad = |_: ()| Scan {
            count,
            good_end: HEADER + at as u64,
            clean: false,
        };
        if buf.len() - at < 8 {
            return Ok(bad(()));
        }
        let plen = u32::from_le_bytes(buf[at..at + 4].try_into().unwrap());
        let crc = u32::from_le_bytes(buf[at + 4..at + 8].try_into().unwrap());
        if plen > MAX_PAYLOAD || buf.len() - at - 8 < plen as usize {
            return Ok(bad(()));
        }
        let payload = &buf[at + 8..at + 8 + plen as usize];
        if crc32c(payload) != crc {
            return Ok(bad(()));
        }
        let Some((idx, ev)) = decode_payload(payload) else {
            return Ok(bad(()));
        };
        if idx != first + count {
            return Ok(bad(()));
        }
        if !f(idx, ev) {
            clean = false;
            break;
        }
        count += 1;
        at += 8 + plen as usize;
    }
    Ok(Scan {
        count,
        good_end: HEADER + at as u64,
        clean,
    })
}

impl Stream {
    fn base(&self) -> u64 {
        self.segs.first().map_or(self.next, |s| s.first)
    }
}

impl EventStore {
    /// Open (creating the directory) and recover `streams` streams.
    pub fn open(cfg: StoreConfig, streams: usize) -> io::Result<EventStore> {
        fs::create_dir_all(&cfg.dir)?;
        let mut st = EventStore {
            cfg,
            streams: Vec::new(),
            last_sync: Instant::now(),
            stats: StoreStats::default(),
        };
        for id in 0..streams {
            let s = st.recover_stream(id)?;
            st.streams.push(s);
        }
        Ok(st)
    }

    fn recover_stream(&mut self, id: usize) -> io::Result<Stream> {
        let mut files: Vec<(u64, PathBuf)> = Vec::new();
        for e in fs::read_dir(&self.cfg.dir)? {
            let e = e?;
            if let Some((s, first)) = e.file_name().to_str().and_then(parse_name) {
                if s == id {
                    files.push((first, e.path()));
                }
            }
        }
        files.sort();
        let mut segs: Vec<Seg> = Vec::new();
        let mut broken_at: Option<usize> = None;
        for (i, (first, path)) in files.iter().enumerate() {
            if let Some(prev) = segs.last() {
                if prev.first + prev.count != *first {
                    broken_at = Some(i);
                    break;
                }
            }
            let scan = scan_segment(path, *first, |_, _| true)?;
            if !scan.clean {
                let size = fs::metadata(path)?.len();
                if scan.good_end < HEADER {
                    // not even a header: drop the file
                    self.stats.truncated_bytes += size;
                    fs::remove_file(path)?;
                    broken_at = Some(i + 1);
                    break;
                }
                self.stats.truncated_bytes += size - scan.good_end;
                OpenOptions::new()
                    .write(true)
                    .open(path)?
                    .set_len(scan.good_end)?;
                segs.push(Seg {
                    first: *first,
                    count: scan.count,
                    path: path.clone(),
                });
                broken_at = Some(i + 1);
                break;
            }
            segs.push(Seg {
                first: *first,
                count: scan.count,
                path: path.clone(),
            });
        }
        if let Some(i) = broken_at {
            for (_, p) in &files[i..] {
                self.stats.truncated_bytes += fs::metadata(p).map(|m| m.len()).unwrap_or(0);
                fs::remove_file(p)?;
            }
        }
        // an empty last segment (a roll that crashed before its first record) is noise
        while segs.last().is_some_and(|s| s.count == 0) {
            let s = segs.pop().unwrap();
            fs::remove_file(&s.path)?;
        }
        let next = segs.last().map_or(1, |s| s.first + s.count);
        // preload the ring with the newest events, so replays after a restart (the
        // usual case: the replica re-applies the log past its snapshot) are compared
        let mut ring: VecDeque<(u64, Vec<u8>)> = VecDeque::new();
        let want_from = next.saturating_sub(RING as u64);
        for g in segs.iter().rev() {
            if g.first + g.count <= want_from {
                break;
            }
            let mut part = Vec::new();
            scan_segment(&g.path, g.first, |idx, ev| {
                if idx >= want_from {
                    let mut b = Vec::new();
                    put_event(&mut b, &ev);
                    part.push((idx, b));
                }
                true
            })?;
            for e in part.into_iter().rev() {
                ring.push_front(e);
            }
        }
        Ok(Stream {
            id,
            segs,
            next,
            w: None,
            ring,
            dirty: false,
        })
    }

    pub fn streams(&self) -> usize {
        self.streams.len()
    }

    /// Index of the first event still stored in `stream`.
    pub fn base(&self, stream: usize) -> u64 {
        self.streams[stream].base()
    }

    /// Index the next event of `stream` will get (everything below is stored).
    pub fn next(&self, stream: usize) -> u64 {
        self.streams[stream].next
    }

    /// The store brought into line with the replica's window `[base, next)` of
    /// `stream` (see the module docs). `events` are the window's events.
    pub fn attach(&mut self, stream: usize, base: u64, events: &VecDeque<Event>) -> io::Result<()> {
        let next = base + events.len() as u64;
        if self.streams[stream].next < base {
            self.reset_stream(stream, base)?;
            self.stats.gaps += 1;
        }
        let from = self.streams[stream].next;
        for idx in from.max(base)..next {
            let ev = events[(idx - base) as usize];
            self.append(stream, idx, &ev)?;
        }
        Ok(())
    }

    fn reset_stream(&mut self, stream: usize, at: u64) -> io::Result<()> {
        let s = &mut self.streams[stream];
        s.w = None;
        for seg in s.segs.drain(..) {
            fs::remove_file(&seg.path)?;
        }
        s.ring.clear();
        s.next = at;
        Ok(())
    }

    /// Record event `idx` of `stream`. At or below what is stored it is a replay:
    /// skipped, and compared with the recent ring when it is in it. Above the next
    /// index it means events were missed: the stream restarts here.
    pub fn append(&mut self, stream: usize, idx: u64, ev: &Event) -> io::Result<()> {
        let seg_events = self.cfg.segment_events.max(1);
        let s = &mut self.streams[stream];
        if idx < s.next {
            let mut p = Vec::new();
            put_event(&mut p, ev);
            match s.ring.iter().find(|(i, _)| *i == idx) {
                Some((_, stored)) if *stored != p => self.stats.mismatches += 1,
                Some(_) => {}
                None => self.stats.unverified += 1,
            }
            return Ok(());
        }
        if idx > s.next {
            self.reset_stream(stream, idx)?;
            self.stats.gaps += 1;
        }
        let s = &mut self.streams[stream];
        let full = s.segs.last().is_some_and(|g| g.count >= seg_events);
        if full {
            if let Some(w) = s.w.as_mut() {
                w.flush()?;
                w.get_ref().sync_data()?;
                self.stats.syncs += 1;
            }
            s.w = None;
        }
        if s.w.is_none() {
            if s.segs.is_empty() || full {
                let path = self.cfg.dir.join(seg_name(s.id, idx));
                let mut f = File::create(&path)?;
                let mut h = Vec::with_capacity(HEADER as usize);
                h.extend_from_slice(MAGIC);
                h.extend_from_slice(&idx.to_le_bytes());
                f.write_all(&h)?;
                s.segs.push(Seg {
                    first: idx,
                    count: 0,
                    path,
                });
                s.w = Some(io::BufWriter::new(f));
            } else {
                // reopen the unfinished last segment (after a restart) for append
                let g = s.segs.last().expect("a segment");
                let f = OpenOptions::new().append(true).open(&g.path)?;
                s.w = Some(io::BufWriter::new(f));
            }
        }
        let rec = encode_record(idx, ev);
        let w = s.w.as_mut().expect("writer");
        w.write_all(&rec)?;
        s.dirty = true;
        s.segs.last_mut().expect("segment").count += 1;
        s.next = idx + 1;
        let mut enc = Vec::new();
        put_event(&mut enc, ev);
        s.ring.push_back((idx, enc));
        if s.ring.len() > RING {
            s.ring.pop_front();
        }
        match self.cfg.sync {
            SyncPolicy::On => self.sync()?,
            SyncPolicy::Relaxed(d) if self.last_sync.elapsed() >= d => self.sync()?,
            _ => {}
        }
        if let Some(keep) = self.cfg.retain_events {
            self.compact(stream, keep)?;
        }
        Ok(())
    }

    /// Push buffered records of `stream` to the file (so a read sees them).
    pub fn flush(&mut self, stream: usize) -> io::Result<()> {
        if let Some(w) = self.streams[stream].w.as_mut() {
            w.flush()?;
        }
        Ok(())
    }

    /// Flush and fsync every stream with unsynced records.
    pub fn sync(&mut self) -> io::Result<()> {
        for s in &mut self.streams {
            if s.dirty {
                if let Some(w) = s.w.as_mut() {
                    w.flush()?;
                    w.get_ref().sync_data()?;
                }
                s.dirty = false;
                self.stats.syncs += 1;
            }
        }
        self.last_sync = Instant::now();
        Ok(())
    }

    /// Delete whole segments that lie entirely below `next - keep`.
    fn compact(&mut self, stream: usize, keep: u64) -> io::Result<()> {
        let s = &mut self.streams[stream];
        let floor = s.next.saturating_sub(keep);
        while s.segs.len() > 1 && s.segs[0].first + s.segs[0].count <= floor {
            let g = s.segs.remove(0);
            fs::remove_file(&g.path)?;
            self.stats.segments_dropped += 1;
        }
        Ok(())
    }

    /// Up to `max` events of `stream` from index `from`. `Err(base)` when `from`
    /// is older than the first stored event. Records still in a write buffer are
    /// flushed first.
    pub fn read(
        &mut self,
        stream: usize,
        from: u64,
        max: usize,
    ) -> io::Result<Result<Vec<(u64, Event)>, u64>> {
        self.flush(stream)?;
        let s = &self.streams[stream];
        let base = s.base();
        if from < base {
            return Ok(Err(base));
        }
        let mut out = Vec::new();
        if from >= s.next || max == 0 {
            return Ok(Ok(out));
        }
        // the segment holding `from`: the last one starting at or before it
        let first_seg = s
            .segs
            .partition_point(|g| g.first <= from)
            .saturating_sub(1);
        for g in &s.segs[first_seg..] {
            if out.len() >= max {
                break;
            }
            scan_segment(&g.path, g.first, |idx, ev| {
                if idx >= from && out.len() < max {
                    out.push((idx, ev));
                }
                out.len() < max
            })?;
        }
        Ok(Ok(out))
    }

    /// Where a segment of `stream` starts, for tests that damage files.
    pub fn segment_paths(&self, stream: usize) -> Vec<PathBuf> {
        self.streams[stream]
            .segs
            .iter()
            .map(|g| g.path.clone())
            .collect()
    }

    /// Offsets and sizes help tests cut a file mid-record.
    pub fn file_len(path: &Path) -> io::Result<u64> {
        let mut f = File::open(path)?;
        f.seek(SeekFrom::End(0))
    }
}
