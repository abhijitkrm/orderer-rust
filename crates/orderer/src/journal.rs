//! Per-partition journals (spec/JOURNAL.md): file naming, JSONL and binary
//! encodings, writers, and strict readers for recovery.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use orderer_core::jsonflat::{get_i64, get_str, get_u64, parse_command};
use orderer_core::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalFormat {
    Jsonl,
    Binary,
}

/// When the syncer fsyncs a partition's command journal. Never observable
/// (spec/PIPELINE.md §8) — only latency and power-loss exposure change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// No fsync. The durable watermark follows the written one: acks mean
    /// "handed to the OS", not "on disk".
    Never,
    /// Group commit: fsync once `n` records are pending, or whenever
    /// records have been pending for `idle` with no new ones arriving.
    EveryN { n: u64, idle: Duration },
    /// fsync at most every `interval` while records are pending.
    Every(Duration),
}

impl FsyncPolicy {
    /// spec/BENCH.md's gated configuration: every 1024 records.
    pub const fn every_n(n: u64) -> FsyncPolicy {
        FsyncPolicy::EveryN {
            n,
            idle: Duration::from_micros(200),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JournalConfig {
    pub dir: PathBuf,
    pub format: JournalFormat,
    pub fsync: FsyncPolicy,
    /// Also write `evt-{p}` event journals (an egress plug).
    pub events: bool,
    /// Append to existing journals (after recovery, same `P`) instead of
    /// truncating.
    pub append: bool,
}

impl JournalConfig {
    pub fn new(dir: impl Into<PathBuf>, format: JournalFormat) -> JournalConfig {
        JournalConfig {
            dir: dir.into(),
            format,
            fsync: FsyncPolicy::every_n(1024),
            events: true,
            append: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Cmd,
    Evt,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Cmd => "cmd",
            Kind::Evt => "evt",
        }
    }
    fn code(self) -> u8 {
        match self {
            Kind::Cmd => 1,
            Kind::Evt => 2,
        }
    }
    /// Record size in a binary journal of `version`.
    fn record_size(self, version: u16) -> usize {
        match (self, version) {
            (Kind::Cmd, 1) => CMD_RECORD_V1,
            (Kind::Evt, 1) => EVT_RECORD_V1,
            (Kind::Cmd, _) => CMD_RECORD,
            (Kind::Evt, _) => EVT_RECORD,
        }
    }
    /// Bytes the checksum covers (the version-1 record).
    fn payload(self) -> usize {
        match self {
            Kind::Cmd => CMD_RECORD_V1,
            Kind::Evt => EVT_RECORD_V1,
        }
    }
}

fn ext(format: JournalFormat) -> &'static str {
    match format {
        JournalFormat::Jsonl => "journal",
        JournalFormat::Binary => "bin",
    }
}

/// spec/JOURNAL.md §1: segment 0's file.
pub fn journal_path(dir: &Path, kind: Kind, partition: u32, format: JournalFormat) -> PathBuf {
    segment_path(dir, kind, partition, 0, format)
}

/// spec/JOURNAL.md §1: the segment starting after cut `start`.
pub fn segment_path(
    dir: &Path,
    kind: Kind,
    partition: u32,
    start: u64,
    format: JournalFormat,
) -> PathBuf {
    if start == 0 {
        dir.join(format!("{}-{partition}.{}", kind.name(), ext(format)))
    } else {
        dir.join(format!(
            "{}-{partition}.{start}.{}",
            kind.name(),
            ext(format)
        ))
    }
}

/// Every `kind` segment in `dir`: (partition, start, path), sorted.
pub fn list_segments(dir: &Path, kind: Kind, format: JournalFormat) -> Vec<(u32, u64, PathBuf)> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    let prefix = format!("{}-", kind.name());
    let suffix = format!(".{}", ext(format));
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(mid) = name
            .strip_prefix(&prefix)
            .and_then(|r| r.strip_suffix(&suffix))
        else {
            continue;
        };
        let mut it = mid.splitn(2, '.');
        let p = it.next().and_then(|x| x.parse::<u32>().ok());
        let start = match it.next() {
            None => Some(0),
            Some(x) => x.parse::<u64>().ok().filter(|&s| s > 0),
        };
        if let (Some(p), Some(start)) = (p, start) {
            out.push((p, start, e.path()));
        }
    }
    out.sort();
    out
}

/// spec/JOURNAL.md §6: checkpoint snapshot path for cut `n`.
pub fn checkpoint_path(dir: &Path, n: u64) -> PathBuf {
    dir.join(format!("checkpoint-{n}.snap"))
}

/// Complete checkpoints in `dir` (body and sidecar present), ascending cut.
pub fn list_checkpoints(dir: &Path) -> Vec<(u64, PathBuf)> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(n) = name
            .strip_prefix("checkpoint-")
            .and_then(|r| r.strip_suffix(".snap"))
            .and_then(|x| x.parse::<u64>().ok())
        else {
            continue;
        };
        let p = e.path();
        if crate::pipeline::meta_path(&p).exists() {
            out.push((n, p));
        }
    }
    out.sort();
    out
}

// ---- encodings -------------------------------------------------------------

pub const HEADER: usize = 64;
/// Version-2 record sizes (1.2): the version-1 record plus CRC-32C and 4
/// reserved bytes.
pub const CMD_RECORD: usize = 48;
pub const EVT_RECORD: usize = 56;
/// Version-1 record sizes (read only).
pub const CMD_RECORD_V1: usize = 40;
pub const EVT_RECORD_V1: usize = 48;
/// The binary journal version 1.2 writers produce.
pub const VERSION: u16 = 2;
const MAGIC: &[u8; 4] = b"ORDJ";

/// CRC-32C (Castagnoli, reflected 0x82F63B78), spec/JOURNAL.md §2.2.
pub fn crc32c(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut t = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut c = i as u32;
            let mut k = 0;
            while k < 8 {
                c = if c & 1 != 0 {
                    (c >> 1) ^ 0x82F6_3B78
                } else {
                    c >> 1
                };
                k += 1;
            }
            t[i] = c;
            i += 1;
        }
        t
    };
    let mut c = !0u32;
    for &b in data {
        c = TABLE[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
    }
    !c
}

/// Seal a version-2 record: CRC-32C of the payload, then zeros.
fn seal(r: &mut [u8], payload: usize) {
    let crc = crc32c(&r[..payload]);
    r[payload..payload + 4].copy_from_slice(&crc.to_le_bytes());
    r[payload + 4..payload + 8].fill(0);
}

fn sealed(r: &[u8], payload: usize) -> bool {
    u32_at(r, payload) == crc32c(&r[..payload])
}

fn index_name(i: IndexKind) -> &'static str {
    match i {
        IndexKind::Ladder => "ladder",
        IndexKind::Tree => "tree",
    }
}

/// spec/JOURNAL.md §2.1/§3.1 header line (with newline). Carries the
/// pipeline's default book config, so journals are self-describing.
pub fn jsonl_header(kind: Kind, partition: u32, partitions: u32, book: BookConfig) -> String {
    format!(
        "{{\"format\":\"orderer-journal/1\",\"kind\":\"{}\",\"partition\":{partition},\"partitions\":{partitions},\"pmin\":{},\"pmax\":{},\"max_orders\":{},\"index\":\"{}\"}}\n",
        kind.name(),
        book.price_min,
        book.price_max,
        book.max_orders,
        index_name(book.index)
    )
}

/// spec/JOURNAL.md §2.2 header (64 bytes).
pub fn binary_header(
    kind: Kind,
    partition: u32,
    partitions: u32,
    book: BookConfig,
) -> [u8; HEADER] {
    let mut h = [0u8; HEADER];
    h[0..4].copy_from_slice(MAGIC);
    h[4..6].copy_from_slice(&VERSION.to_le_bytes());
    h[6] = kind.code();
    h[7] = match book.index {
        IndexKind::Ladder => 0,
        IndexKind::Tree => 1,
    };
    h[8..12].copy_from_slice(&partition.to_le_bytes());
    h[12..16].copy_from_slice(&partitions.to_le_bytes());
    h[16..20].copy_from_slice(&(kind.record_size(VERSION) as u32).to_le_bytes());
    h[24..32].copy_from_slice(&book.price_min.to_le_bytes());
    h[32..40].copy_from_slice(&book.price_max.to_le_bytes());
    h[40..48].copy_from_slice(&(book.max_orders as u64).to_le_bytes());
    h
}

/// Two book configs are the same config.
pub fn same_book(a: BookConfig, b: BookConfig) -> bool {
    a.price_min == b.price_min
        && a.price_max == b.price_max
        && a.max_orders == b.max_orders
        && a.index == b.index
}

/// spec/JOURNAL.md §2.1 record line (no newline): matcher's canonical
/// engine command line with `"iseq"` appended as the last key.
pub fn write_cmd_line(iseq: u64, sym: Symbol, cmd: &Command, out: &mut String) {
    cmd.write_canonical_sym(sym, out);
    out.pop(); // the closing brace
    use std::fmt::Write as _;
    let _ = write!(out, ",\"iseq\":{iseq}}}");
}

fn side_code(s: Side) -> u8 {
    match s {
        Side::Bid => 0,
        Side::Ask => 1,
    }
}
fn otype_code(o: OType) -> u8 {
    match o {
        OType::Limit => 0,
        OType::Market => 1,
    }
}
fn tif_code(t: Tif) -> u8 {
    match t {
        Tif::Gtc => 0,
        Tif::Ioc => 1,
        Tif::Fok => 2,
        Tif::PostOnly => 3,
    }
}

/// spec/JOURNAL.md §2.2 command record (version 2, sealed).
pub fn encode_cmd(iseq: u64, sym: Symbol, cmd: &Command, r: &mut [u8; CMD_RECORD]) {
    *r = [0; CMD_RECORD];
    encode_cmd_payload(iseq, sym, cmd, r);
    seal(r, CMD_RECORD_V1);
}

fn encode_cmd_payload(iseq: u64, sym: Symbol, cmd: &Command, r: &mut [u8]) {
    r[0..8].copy_from_slice(&iseq.to_le_bytes());
    r[8..12].copy_from_slice(&sym.to_le_bytes());
    match *cmd {
        Command::New {
            order_id,
            side,
            otype,
            price,
            qty,
            tif,
        } => {
            r[12] = 1;
            r[13] = side_code(side);
            r[14] = otype_code(otype);
            r[15] = tif_code(tif);
            r[16..24].copy_from_slice(&order_id.to_le_bytes());
            r[24..32].copy_from_slice(&price.to_le_bytes());
            r[32..40].copy_from_slice(&qty.to_le_bytes());
        }
        Command::Cancel { order_id } => {
            r[12] = 2;
            r[16..24].copy_from_slice(&order_id.to_le_bytes());
        }
        Command::Replace {
            order_id,
            price,
            qty,
        } => {
            r[12] = 3;
            r[16..24].copy_from_slice(&order_id.to_le_bytes());
            r[24..32].copy_from_slice(&price.to_le_bytes());
            r[32..40].copy_from_slice(&qty.to_le_bytes());
        }
    }
}

fn u64_at(r: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(r[at..at + 8].try_into().unwrap())
}
fn i64_at(r: &[u8], at: usize) -> i64 {
    i64::from_le_bytes(r[at..at + 8].try_into().unwrap())
}
fn u32_at(r: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(r[at..at + 4].try_into().unwrap())
}

/// Inverse of [`encode_cmd`]; `None` for invalid codes.
pub fn decode_cmd(r: &[u8]) -> Option<(u64, Symbol, Command)> {
    let iseq = u64_at(r, 0);
    let sym = u32_at(r, 8);
    let order_id = u64_at(r, 16);
    let cmd = match r[12] {
        1 => Command::New {
            order_id,
            side: match r[13] {
                0 => Side::Bid,
                1 => Side::Ask,
                _ => return None,
            },
            otype: match r[14] {
                0 => OType::Limit,
                1 => OType::Market,
                _ => return None,
            },
            tif: match r[15] {
                0 => Tif::Gtc,
                1 => Tif::Ioc,
                2 => Tif::Fok,
                3 => Tif::PostOnly,
                _ => return None,
            },
            price: i64_at(r, 24),
            qty: u64_at(r, 32),
        },
        2 => Command::Cancel { order_id },
        3 => Command::Replace {
            order_id,
            price: i64_at(r, 24),
            qty: u64_at(r, 32),
        },
        _ => return None,
    };
    Some((iseq, sym, cmd))
}

fn reject_code(r: RejectReason) -> u8 {
    match r {
        RejectReason::InvalidQty => 1,
        RejectReason::InvalidPrice => 2,
        RejectReason::DuplicateOrderId => 3,
        RejectReason::UnknownOrderId => 4,
        RejectReason::PostOnlyWouldCross => 5,
        RejectReason::FokCannotFill => 6,
        RejectReason::BookFull => 7,
    }
}
fn close_code(r: CloseReason) -> u8 {
    match r {
        CloseReason::Filled => 1,
        CloseReason::Cancelled => 2,
        CloseReason::Expired => 3,
    }
}

/// spec/JOURNAL.md §3.2 event record (version 2, sealed).
pub fn encode_evt(seq: u64, sym: Symbol, ev: &Event, r: &mut [u8; EVT_RECORD]) {
    *r = [0; EVT_RECORD];
    encode_evt_payload(seq, sym, ev, r);
    seal(r, EVT_RECORD_V1);
}

fn encode_evt_payload(seq: u64, sym: Symbol, ev: &Event, r: &mut [u8]) {
    r[0..8].copy_from_slice(&seq.to_le_bytes());
    r[8..12].copy_from_slice(&sym.to_le_bytes());
    let (code, reason, a, b, c, d): (u8, u8, u64, u64, i64, u64) = match *ev {
        Event::Accepted {
            order_id,
            leaves_qty,
        } => (1, 0, order_id, 0, 0, leaves_qty),
        Event::Rejected { order_id, reason } => (2, reject_code(reason), order_id, 0, 0, 0),
        Event::Trade {
            maker,
            taker,
            price,
            qty,
        } => (3, 0, maker, taker, price, qty),
        Event::Closed { order_id, reason } => (4, close_code(reason), order_id, 0, 0, 0),
        Event::Replaced {
            order_id,
            price,
            qty,
        } => (5, 0, order_id, 0, price, qty),
    };
    r[12] = code;
    r[13] = reason;
    r[16..24].copy_from_slice(&a.to_le_bytes());
    r[24..32].copy_from_slice(&b.to_le_bytes());
    r[32..40].copy_from_slice(&c.to_le_bytes());
    r[40..48].copy_from_slice(&d.to_le_bytes());
}

/// Inverse of [`encode_evt`]; `None` for invalid codes.
pub fn decode_evt(r: &[u8]) -> Option<(u64, Symbol, Event)> {
    let (a, b, c, d) = (u64_at(r, 16), u64_at(r, 24), i64_at(r, 32), u64_at(r, 40));
    let ev = match (r[12], r[13]) {
        (1, 0) => Event::Accepted {
            order_id: a,
            leaves_qty: d,
        },
        (2, code) => Event::Rejected {
            order_id: a,
            reason: match code {
                1 => RejectReason::InvalidQty,
                2 => RejectReason::InvalidPrice,
                3 => RejectReason::DuplicateOrderId,
                4 => RejectReason::UnknownOrderId,
                5 => RejectReason::PostOnlyWouldCross,
                6 => RejectReason::FokCannotFill,
                7 => RejectReason::BookFull,
                _ => return None,
            },
        },
        (3, 0) => Event::Trade {
            maker: a,
            taker: b,
            price: c,
            qty: d,
        },
        (4, code) => Event::Closed {
            order_id: a,
            reason: match code {
                1 => CloseReason::Filled,
                2 => CloseReason::Cancelled,
                3 => CloseReason::Expired,
                _ => return None,
            },
        },
        (5, 0) => Event::Replaced {
            order_id: a,
            price: c,
            qty: d,
        },
        _ => return None,
    };
    Some((u64_at(r, 0), u32_at(r, 8), ev))
}

// ---- writing -------------------------------------------------------------------

/// Open `kind`'s journal for `partition`, ready for records:
///
/// - append mode: the partition's last existing segment, header checked
///   (segment 0 is created if the partition has none);
/// - otherwise: a fresh segment 0. Callers start fresh once per directory
///   with [`clear_journal_dir`].
pub fn open_journal(
    cfg: &JournalConfig,
    kind: Kind,
    partition: u32,
    partitions: u32,
    book: BookConfig,
) -> io::Result<File> {
    if cfg.append {
        let last = list_segments(&cfg.dir, kind, cfg.format)
            .into_iter()
            .rfind(|(p, _, _)| *p == partition);
        if let Some((_, _, path)) = last {
            check_header(&path, kind, partition, partitions, book, cfg.format)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            return OpenOptions::new().append(true).open(&path);
        }
    }
    open_segment(&cfg.dir, cfg.format, kind, partition, partitions, book, 0)
}

/// Create segment `start` (truncating any old file) with its header.
pub fn open_segment(
    dir: &Path,
    format: JournalFormat,
    kind: Kind,
    partition: u32,
    partitions: u32,
    book: BookConfig,
    start: u64,
) -> io::Result<File> {
    let mut f = File::create(segment_path(dir, kind, partition, start, format))?;
    match format {
        JournalFormat::Jsonl => {
            f.write_all(jsonl_header(kind, partition, partitions, book).as_bytes())?
        }
        JournalFormat::Binary => f.write_all(&binary_header(kind, partition, partitions, book))?,
    }
    Ok(f)
}

/// A fresh (non-append) pipeline owns its directory's journals: remove
/// every segment of `format` and every checkpoint, so stale files can't
/// join the new journal.
pub fn clear_journal_dir(dir: &Path, format: JournalFormat) -> io::Result<()> {
    for kind in [Kind::Cmd, Kind::Evt] {
        for (_, _, path) in list_segments(dir, kind, format) {
            std::fs::remove_file(path)?;
        }
    }
    remove_checkpoints_below(dir, u64::MAX)
}

/// Remove checkpoints with cut below `n` (body first, so a half-removed
/// pair is never a "complete" checkpoint).
pub fn remove_checkpoints_below(dir: &Path, n: u64) -> io::Result<()> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Ok(());
    };
    let mut bodies = Vec::new();
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if let Some(cut) = name
            .strip_prefix("checkpoint-")
            .and_then(|r| r.strip_suffix(".snap"))
            .and_then(|x| x.parse::<u64>().ok())
        {
            if cut < n {
                bodies.push(e.path());
            }
        }
    }
    for b in bodies {
        std::fs::remove_file(&b)?;
        let m = crate::pipeline::meta_path(&b);
        if m.exists() {
            std::fs::remove_file(m)?;
        }
    }
    Ok(())
}

/// spec/JOURNAL.md §6 step 4: remove segments that start below `n`.
pub fn remove_segments_below(dir: &Path, format: JournalFormat, n: u64) -> io::Result<()> {
    for kind in [Kind::Cmd, Kind::Evt] {
        for (_, start, path) in list_segments(dir, kind, format) {
            if start < n {
                std::fs::remove_file(path)?;
            }
        }
    }
    Ok(())
}

/// Write `contents` to `path` durably: temporary name, sync, rename, sync
/// the directory (spec/JOURNAL.md §6 step 3).
pub fn write_durably(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(contents)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(d) = path.parent() {
        File::open(d)?.sync_all()?;
    }
    Ok(())
}

/// Longest encoded record (a JSONL command line with every field maxed).
pub const MAX_RECORD: usize = 256;

/// Append one command record (spec/JOURNAL.md §2) to `out`.
#[inline]
pub fn push_cmd(
    out: &mut Vec<u8>,
    format: JournalFormat,
    iseq: u64,
    sym: Symbol,
    cmd: &Command,
    scratch: &mut String,
) {
    match format {
        JournalFormat::Binary => {
            let mut r = [0u8; CMD_RECORD];
            encode_cmd(iseq, sym, cmd, &mut r);
            out.extend_from_slice(&r);
        }
        JournalFormat::Jsonl => {
            scratch.clear();
            write_cmd_line(iseq, sym, cmd, scratch);
            scratch.push('\n');
            out.extend_from_slice(scratch.as_bytes());
        }
    }
}

/// Append one event record (spec/JOURNAL.md §3) to `out`.
#[inline]
pub fn push_evt(
    out: &mut Vec<u8>,
    format: JournalFormat,
    seq: u64,
    sym: Symbol,
    ev: &Event,
    scratch: &mut String,
) {
    match format {
        JournalFormat::Binary => {
            let mut r = [0u8; EVT_RECORD];
            encode_evt(seq, sym, ev, &mut r);
            out.extend_from_slice(&r);
        }
        JournalFormat::Jsonl => {
            scratch.clear();
            Event::write_canonical_sym(seq, sym, ev, scratch);
            scratch.push('\n');
            out.extend_from_slice(scratch.as_bytes());
        }
    }
}

// ---- readers (recovery) --------------------------------------------------------

/// A journal that recovery must not trust (spec/JOURNAL.md §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorruptJournal {
    pub path: PathBuf,
    pub detail: String,
}

impl fmt::Display for CorruptJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.path.display(), self.detail)
    }
}

impl std::error::Error for CorruptJournal {}

fn corrupt(path: &Path, detail: impl Into<String>) -> CorruptJournal {
    CorruptJournal {
        path: path.to_path_buf(),
        detail: detail.into(),
    }
}

/// Header facts of a journal file.
#[derive(Debug, Clone, Copy)]
pub struct JournalHeader {
    pub kind: Kind,
    pub partition: u32,
    pub partitions: u32,
    /// The writing pipeline's default book config.
    pub book: BookConfig,
    /// Binary journal version (1 or 2); JSONL journals report 2.
    pub version: u16,
}

impl PartialEq for JournalHeader {
    fn eq(&self, o: &Self) -> bool {
        self.kind == o.kind
            && self.partition == o.partition
            && self.partitions == o.partitions
            && same_book(self.book, o.book)
    }
}

impl Eq for JournalHeader {}

fn check_header(
    path: &Path,
    kind: Kind,
    partition: u32,
    partitions: u32,
    book: BookConfig,
    format: JournalFormat,
) -> Result<(), CorruptJournal> {
    let h = read_header(path, format)?;
    let want = JournalHeader {
        kind,
        partition,
        partitions,
        book,
        version: VERSION,
    };
    if h != want {
        return Err(corrupt(
            path,
            format!("header {h:?} does not match {want:?}"),
        ));
    }
    if format == JournalFormat::Binary && h.version != VERSION {
        return Err(corrupt(path, "cannot append to a version-1 journal"));
    }
    Ok(())
}

fn parse_jsonl_header(path: &Path, line: &str) -> Result<JournalHeader, CorruptJournal> {
    if get_str(line, "format") != Some("orderer-journal/1") {
        return Err(corrupt(path, "not an orderer-journal/1 header"));
    }
    let kind = match get_str(line, "kind") {
        Some("cmd") => Kind::Cmd,
        Some("evt") => Kind::Evt,
        _ => return Err(corrupt(path, "bad kind")),
    };
    let num = |k: &str| {
        get_u64(line, k)
            .filter(|&v| v <= u32::MAX as u64)
            .map(|v| v as u32)
            .ok_or_else(|| corrupt(path, format!("bad {k}")))
    };
    let int = |k: &str| get_i64(line, k).ok_or_else(|| corrupt(path, format!("bad {k}")));
    let book = BookConfig {
        price_min: int("pmin")?,
        price_max: int("pmax")?,
        max_orders: get_u64(line, "max_orders").ok_or_else(|| corrupt(path, "bad max_orders"))?
            as usize,
        index: match get_str(line, "index") {
            Some("ladder") => IndexKind::Ladder,
            Some("tree") => IndexKind::Tree,
            _ => return Err(corrupt(path, "bad index")),
        },
    };
    Ok(JournalHeader {
        kind,
        partition: num("partition")?,
        partitions: num("partitions")?,
        book,
        version: VERSION,
    })
}

fn parse_binary_header(path: &Path, h: &[u8]) -> Result<JournalHeader, CorruptJournal> {
    if h.len() < HEADER || &h[0..4] != MAGIC {
        return Err(corrupt(path, "bad magic"));
    }
    let version = u16::from_le_bytes([h[4], h[5]]);
    if version != 1 && version != 2 {
        return Err(corrupt(path, "unsupported version"));
    }
    let kind = match h[6] {
        1 => Kind::Cmd,
        2 => Kind::Evt,
        _ => return Err(corrupt(path, "bad kind")),
    };
    if u32_at(h, 16) as usize != kind.record_size(version) {
        return Err(corrupt(path, "bad record_size"));
    }
    let index = match h[7] {
        0 => IndexKind::Ladder,
        1 => IndexKind::Tree,
        _ => return Err(corrupt(path, "bad index")),
    };
    Ok(JournalHeader {
        kind,
        partition: u32_at(h, 8),
        partitions: u32_at(h, 12),
        book: BookConfig {
            price_min: i64_at(h, 24),
            price_max: i64_at(h, 32),
            max_orders: u64_at(h, 40) as usize,
            index,
        },
        version,
    })
}

pub fn read_header(path: &Path, format: JournalFormat) -> Result<JournalHeader, CorruptJournal> {
    let mut f = File::open(path).map_err(|e| corrupt(path, e.to_string()))?;
    match format {
        JournalFormat::Binary => {
            let mut h = [0u8; HEADER];
            f.read_exact(&mut h)
                .map_err(|_| corrupt(path, "short header"))?;
            parse_binary_header(path, &h)
        }
        JournalFormat::Jsonl => {
            let mut buf = Vec::new();
            f.seek(SeekFrom::Start(0)).ok();
            f.take(4096)
                .read_to_end(&mut buf)
                .map_err(|e| corrupt(path, e.to_string()))?;
            let text = String::from_utf8_lossy(&buf);
            let line = text.lines().next().unwrap_or("");
            parse_jsonl_header(path, line)
        }
    }
}

/// Strict (default) or repair reading (spec/JOURNAL.md §5, §5.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadMode {
    Strict,
    /// Report the length of a valid prefix instead of failing on a torn
    /// tail (the file's bytes are not changed by reading).
    Repair,
}

/// One file's records as raw byte ranges, after header and tail checks.
struct Body {
    header: JournalHeader,
    /// Record payloads: binary records (sealed already checked) or JSONL lines.
    records: Vec<std::ops::Range<usize>>,
    /// Bytes of the file a repair keeps.
    valid_len: usize,
}

fn split_body(
    path: &Path,
    bytes: &[u8],
    format: JournalFormat,
    mode: ReadMode,
) -> Result<Body, CorruptJournal> {
    match format {
        JournalFormat::Binary => {
            let header = parse_binary_header(path, bytes)?;
            let size = header.kind.record_size(header.version);
            let body = bytes.len() - HEADER;
            let mut n = body / size;
            if body % size != 0 && mode == ReadMode::Strict {
                return Err(corrupt(path, "torn tail (partial record)"));
            }
            let at = |i: usize| HEADER + i * size;
            if mode == ReadMode::Repair {
                // 1.3: zero records an interrupted write left (§5.1)
                while n > 0 && bytes[at(n - 1)..at(n)].iter().all(|&b| b == 0) {
                    n -= 1;
                }
            }
            if header.version >= 2 {
                for i in 0..n {
                    let r = &bytes[at(i)..at(i) + size];
                    if !sealed(r, header.kind.payload()) {
                        if mode == ReadMode::Repair && i + 1 == n {
                            n -= 1; // a torn final record (§5.1)
                            break;
                        }
                        return Err(corrupt(path, format!("record {i}: checksum mismatch")));
                    }
                }
            }
            Ok(Body {
                header,
                records: (0..n).map(|i| at(i)..at(i) + size).collect(),
                valid_len: at(n),
            })
        }
        JournalFormat::Jsonl => {
            let mut end = bytes.len();
            if !bytes.is_empty() && bytes[end - 1] != b'\n' {
                if mode == ReadMode::Strict {
                    return Err(corrupt(path, "torn tail (final line has no newline)"));
                }
                end = bytes[..end]
                    .iter()
                    .rposition(|&b| b == b'\n')
                    .map_or(0, |i| i + 1);
            }
            let text =
                std::str::from_utf8(&bytes[..end]).map_err(|_| corrupt(path, "not UTF-8"))?;
            let first = text.find('\n').unwrap_or(text.len());
            let header = parse_jsonl_header(path, &text[..first])?;
            let mut records = Vec::new();
            let mut pos = (first + 1).min(end);
            while pos < end {
                let e = pos + text[pos..].find('\n').unwrap();
                records.push(pos..e);
                pos = e + 1;
            }
            Ok(Body {
                header,
                records,
                valid_len: end,
            })
        }
    }
}

/// One decoded command record.
pub type CmdRecord = (u64, Symbol, Command);

fn decode_cmds(
    path: &Path,
    bytes: &[u8],
    format: JournalFormat,
    body: &Body,
) -> Result<Vec<CmdRecord>, CorruptJournal> {
    let mut recs = Vec::with_capacity(body.records.len());
    for (i, r) in body.records.iter().enumerate() {
        let rec = match format {
            JournalFormat::Binary => decode_cmd(&bytes[r.clone()])
                .ok_or_else(|| corrupt(path, format!("record {i}: bad codes")))?,
            JournalFormat::Jsonl => {
                let l = std::str::from_utf8(&bytes[r.clone()]).unwrap();
                let bad = || corrupt(path, format!("line {}: malformed record: {l}", i + 2));
                let iseq = get_u64(l, "iseq").ok_or_else(bad)?;
                let sym = get_u64(l, "symbol")
                    .filter(|&s| s <= u32::MAX as u64)
                    .ok_or_else(bad)?;
                (iseq, sym as Symbol, parse_command(l).ok_or_else(bad)?)
            }
        };
        recs.push(rec);
    }
    Ok(recs)
}

fn check_increasing(
    path: &Path,
    recs: &[CmdRecord],
    after: Option<u64>,
) -> Result<(), CorruptJournal> {
    let mut prev = after;
    for r in recs {
        if let Some(p) = prev {
            if r.0 <= p {
                return Err(corrupt(
                    path,
                    format!("iseq not increasing ({p} then {})", r.0),
                ));
            }
        }
        prev = Some(r.0);
    }
    Ok(())
}

/// Read one command journal file strictly: torn tails, bad records, bad
/// checksums and non-increasing `iseq` are errors.
pub fn read_cmd_journal(
    path: &Path,
    format: JournalFormat,
) -> Result<(JournalHeader, Vec<CmdRecord>), CorruptJournal> {
    let bytes = std::fs::read(path).map_err(|e| corrupt(path, e.to_string()))?;
    let body = split_body(path, &bytes, format, ReadMode::Strict)?;
    if body.header.kind != Kind::Cmd {
        return Err(corrupt(path, "not a command journal"));
    }
    let recs = decode_cmds(path, &bytes, format, &body)?;
    check_increasing(path, &recs, None)?;
    Ok((body.header, recs))
}

/// Read one event journal file into canonical symbol-tagged lines.
pub fn read_evt_journal(
    path: &Path,
    format: JournalFormat,
) -> Result<(JournalHeader, Vec<String>), CorruptJournal> {
    let bytes = std::fs::read(path).map_err(|e| corrupt(path, e.to_string()))?;
    let body = split_body(path, &bytes, format, ReadMode::Strict)?;
    let mut out = Vec::with_capacity(body.records.len());
    for (i, r) in body.records.iter().enumerate() {
        match format {
            JournalFormat::Binary => {
                let (seq, sym, ev) = decode_evt(&bytes[r.clone()])
                    .ok_or_else(|| corrupt(path, format!("record {i}: bad codes")))?;
                out.push(Event::canonical_sym(seq, sym, &ev));
            }
            JournalFormat::Jsonl => {
                out.push(std::str::from_utf8(&bytes[r.clone()]).unwrap().to_string())
            }
        }
    }
    Ok((body.header, out))
}

/// A partition's whole event journal (all segments, in order).
pub fn read_evt_partition(
    dir: &Path,
    format: JournalFormat,
    partition: u32,
) -> Result<Vec<String>, CorruptJournal> {
    let mut out = Vec::new();
    for (p, _, path) in list_segments(dir, Kind::Evt, format) {
        if p == partition {
            out.extend(read_evt_journal(&path, format)?.1);
        }
    }
    Ok(out)
}

/// Read every partition's command journal in `dir`, all segments in order
/// (spec/JOURNAL.md §1, §5 step 3).
pub fn read_cmd_dir(
    dir: &Path,
    format: JournalFormat,
) -> Result<(JournalHeader, Vec<Vec<CmdRecord>>), CorruptJournal> {
    let segs = list_segments(dir, Kind::Cmd, format);
    let Some((_, _, first)) = segs.first() else {
        return Err(corrupt(
            &journal_path(dir, Kind::Cmd, 0, format),
            "no command journal",
        ));
    };
    let h0 = read_header(first, format)?;
    let mut all: Vec<Vec<CmdRecord>> = (0..h0.partitions).map(|_| Vec::new()).collect();
    let mut seen = vec![false; h0.partitions as usize];
    for (p, _, path) in &segs {
        let (h, recs) = read_cmd_journal(path, format)?;
        if h.partition != *p
            || *p >= h0.partitions
            || h.partitions != h0.partitions
            || !same_book(h.book, h0.book)
        {
            return Err(corrupt(
                path,
                "header does not match its file name, partition count or book config",
            ));
        }
        let part = &mut all[*p as usize];
        check_increasing(path, &recs, part.last().map(|r| r.0))?;
        part.extend(recs);
        seen[*p as usize] = true;
    }
    if let Some(p) = seen.iter().position(|s| !s) {
        return Err(corrupt(
            &journal_path(dir, Kind::Cmd, p as u32, format),
            "partition has no journal",
        ));
    }
    Ok((h0, all))
}

/// spec/JOURNAL.md §5.1: truncate a torn tail off each journal family's
/// last segment, in place. Returns (file, bytes removed) per truncation.
pub fn repair_dir(
    dir: &Path,
    format: JournalFormat,
) -> Result<Vec<(PathBuf, u64)>, CorruptJournal> {
    let mut out = Vec::new();
    for kind in [Kind::Cmd, Kind::Evt] {
        let mut by_part: std::collections::BTreeMap<u32, Vec<(u64, PathBuf)>> = Default::default();
        for (p, start, path) in list_segments(dir, kind, format) {
            by_part.entry(p).or_default().push((start, path));
        }
        for segs in by_part.values_mut() {
            segs.sort();
            // 1.3: drop trailing segments a crash left without a usable
            // header; the segment before becomes the last
            while let Some((start, path)) = segs.last() {
                let bytes = std::fs::read(path).map_err(|e| corrupt(path, e.to_string()))?;
                if *start == 0 || !headerless(path, &bytes, format) {
                    break;
                }
                std::fs::remove_file(path)
                    .and_then(|_| File::open(dir)?.sync_all())
                    .map_err(|e| corrupt(path, e.to_string()))?;
                out.push((path.clone(), bytes.len() as u64));
                segs.pop();
            }
            // repair the last segment; while it holds no records, the one
            // before it too (its writer may still have been finishing it)
            for (_, path) in segs.iter().rev() {
                let bytes = std::fs::read(path).map_err(|e| corrupt(path, e.to_string()))?;
                let body = split_body(path, &bytes, format, ReadMode::Repair)?;
                if body.valid_len < bytes.len() {
                    let f = OpenOptions::new()
                        .write(true)
                        .open(path)
                        .map_err(|e| corrupt(path, e.to_string()))?;
                    f.set_len(body.valid_len as u64)
                        .and_then(|_| f.sync_all())
                        .map_err(|e| corrupt(path, e.to_string()))?;
                    out.push((path.clone(), (bytes.len() - body.valid_len) as u64));
                }
                if !body.records.is_empty() {
                    break;
                }
            }
        }
    }
    Ok(out)
}

/// A segment that cannot hold a record (spec/JOURNAL.md 1.3 §5.1): JSONL
/// with no newline at all, or binary with an invalid header and nothing but
/// zeros after it.
fn headerless(path: &Path, bytes: &[u8], format: JournalFormat) -> bool {
    match format {
        JournalFormat::Jsonl => !bytes.contains(&b'\n'),
        JournalFormat::Binary => {
            parse_binary_header(path, bytes).is_err() && bytes.iter().skip(HEADER).all(|&b| b == 0)
        }
    }
}
