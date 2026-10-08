//! Per-partition journals (spec/JOURNAL.md): file naming, JSONL and binary
//! encodings, writers, and strict readers for recovery.

use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use orderer_core::jsonflat::{get_str, get_u64, parse_command};
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
    fn record_size(self) -> usize {
        match self {
            Kind::Cmd => CMD_RECORD,
            Kind::Evt => EVT_RECORD,
        }
    }
}

/// spec/JOURNAL.md §1.
pub fn journal_path(dir: &Path, kind: Kind, partition: u32, format: JournalFormat) -> PathBuf {
    let ext = match format {
        JournalFormat::Jsonl => "journal",
        JournalFormat::Binary => "bin",
    };
    dir.join(format!("{}-{partition}.{ext}", kind.name()))
}

// ---- encodings -------------------------------------------------------------

pub const HEADER: usize = 32;
pub const CMD_RECORD: usize = 40;
pub const EVT_RECORD: usize = 48;
const MAGIC: &[u8; 4] = b"ORDJ";

/// spec/JOURNAL.md §2.1/§3.1 header line (with newline).
pub fn jsonl_header(kind: Kind, partition: u32, partitions: u32) -> String {
    format!(
        "{{\"format\":\"orderer-journal/1\",\"kind\":\"{}\",\"partition\":{partition},\"partitions\":{partitions}}}\n",
        kind.name()
    )
}

/// spec/JOURNAL.md §2.2 header.
pub fn binary_header(kind: Kind, partition: u32, partitions: u32) -> [u8; HEADER] {
    let mut h = [0u8; HEADER];
    h[0..4].copy_from_slice(MAGIC);
    h[4..6].copy_from_slice(&1u16.to_le_bytes());
    h[6] = kind.code();
    h[8..12].copy_from_slice(&partition.to_le_bytes());
    h[12..16].copy_from_slice(&partitions.to_le_bytes());
    h[16..20].copy_from_slice(&(kind.record_size() as u32).to_le_bytes());
    h
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

/// spec/JOURNAL.md §2.2 command record.
pub fn encode_cmd(iseq: u64, sym: Symbol, cmd: &Command, r: &mut [u8; CMD_RECORD]) {
    *r = [0; CMD_RECORD];
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

/// spec/JOURNAL.md §3.2 event record.
pub fn encode_evt(seq: u64, sym: Symbol, ev: &Event, r: &mut [u8; EVT_RECORD]) {
    *r = [0; EVT_RECORD];
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

/// Create (truncate, writing the header) or open for append (checking the
/// header) `kind`'s journal for `partition`. The file is positioned at its
/// end, ready for records.
pub fn open_journal(
    cfg: &JournalConfig,
    kind: Kind,
    partition: u32,
    partitions: u32,
) -> io::Result<File> {
    let path = journal_path(&cfg.dir, kind, partition, cfg.format);
    if cfg.append && path.exists() {
        check_header(&path, kind, partition, partitions, cfg.format)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        return OpenOptions::new().append(true).open(&path);
    }
    let mut f = File::create(&path)?;
    match cfg.format {
        JournalFormat::Jsonl => {
            f.write_all(jsonl_header(kind, partition, partitions).as_bytes())?
        }
        JournalFormat::Binary => f.write_all(&binary_header(kind, partition, partitions))?,
    }
    Ok(f)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalHeader {
    pub kind: Kind,
    pub partition: u32,
    pub partitions: u32,
}

fn check_header(
    path: &Path,
    kind: Kind,
    partition: u32,
    partitions: u32,
    format: JournalFormat,
) -> Result<(), CorruptJournal> {
    let h = read_header(path, format)?;
    if h.kind != kind || h.partition != partition || h.partitions != partitions {
        return Err(corrupt(
            path,
            format!("header {h:?} does not match {kind:?} partition {partition}/{partitions}"),
        ));
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
    Ok(JournalHeader {
        kind,
        partition: num("partition")?,
        partitions: num("partitions")?,
    })
}

fn parse_binary_header(path: &Path, h: &[u8]) -> Result<JournalHeader, CorruptJournal> {
    if h.len() < HEADER || &h[0..4] != MAGIC {
        return Err(corrupt(path, "bad magic"));
    }
    if u16::from_le_bytes([h[4], h[5]]) != 1 {
        return Err(corrupt(path, "unsupported version"));
    }
    let kind = match h[6] {
        1 => Kind::Cmd,
        2 => Kind::Evt,
        _ => return Err(corrupt(path, "bad kind")),
    };
    if u32_at(h, 16) as usize != kind.record_size() {
        return Err(corrupt(path, "bad record_size"));
    }
    Ok(JournalHeader {
        kind,
        partition: u32_at(h, 8),
        partitions: u32_at(h, 12),
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

/// One decoded command record.
pub type CmdRecord = (u64, Symbol, Command);

/// Read a whole command journal, strictly: torn tails, bad records and
/// non-increasing `iseq` are errors.
pub fn read_cmd_journal(
    path: &Path,
    format: JournalFormat,
) -> Result<(JournalHeader, Vec<CmdRecord>), CorruptJournal> {
    let bytes = std::fs::read(path).map_err(|e| corrupt(path, e.to_string()))?;
    let (hdr, recs) = match format {
        JournalFormat::Binary => {
            let hdr = parse_binary_header(path, &bytes)?;
            let body = &bytes[HEADER..];
            if body.len() % CMD_RECORD != 0 {
                return Err(corrupt(path, "torn tail (partial record)"));
            }
            let mut recs = Vec::with_capacity(body.len() / CMD_RECORD);
            for (i, r) in body.chunks_exact(CMD_RECORD).enumerate() {
                recs.push(
                    decode_cmd(r).ok_or_else(|| corrupt(path, format!("record {i}: bad codes")))?,
                );
            }
            (hdr, recs)
        }
        JournalFormat::Jsonl => {
            let text = std::str::from_utf8(&bytes).map_err(|_| corrupt(path, "not UTF-8"))?;
            if !text.is_empty() && !text.ends_with('\n') {
                return Err(corrupt(path, "torn tail (final line has no newline)"));
            }
            let mut lines = text.lines();
            let hdr = parse_jsonl_header(path, lines.next().unwrap_or(""))?;
            let mut recs = Vec::new();
            for (i, l) in lines.enumerate() {
                let bad = || corrupt(path, format!("line {}: malformed record: {l}", i + 2));
                let iseq = get_u64(l, "iseq").ok_or_else(bad)?;
                let sym = get_u64(l, "symbol")
                    .filter(|&s| s <= u32::MAX as u64)
                    .ok_or_else(bad)?;
                let cmd = parse_command(l).ok_or_else(bad)?;
                recs.push((iseq, sym as Symbol, cmd));
            }
            (hdr, recs)
        }
    };
    if hdr.kind != Kind::Cmd {
        return Err(corrupt(path, "not a command journal"));
    }
    if let Some(w) = recs.windows(2).find(|w| w[1].0 <= w[0].0) {
        return Err(corrupt(
            path,
            format!("iseq not increasing ({} then {})", w[0].0, w[1].0),
        ));
    }
    Ok((hdr, recs))
}

/// Read a whole event journal into canonical symbol-tagged lines.
pub fn read_evt_journal(
    path: &Path,
    format: JournalFormat,
) -> Result<(JournalHeader, Vec<String>), CorruptJournal> {
    let bytes = std::fs::read(path).map_err(|e| corrupt(path, e.to_string()))?;
    match format {
        JournalFormat::Binary => {
            let hdr = parse_binary_header(path, &bytes)?;
            let body = &bytes[HEADER..];
            if body.len() % EVT_RECORD != 0 {
                return Err(corrupt(path, "torn tail (partial record)"));
            }
            let mut out = Vec::with_capacity(body.len() / EVT_RECORD);
            for (i, r) in body.chunks_exact(EVT_RECORD).enumerate() {
                let (seq, sym, ev) =
                    decode_evt(r).ok_or_else(|| corrupt(path, format!("record {i}: bad codes")))?;
                out.push(Event::canonical_sym(seq, sym, &ev));
            }
            Ok((hdr, out))
        }
        JournalFormat::Jsonl => {
            let text = std::str::from_utf8(&bytes).map_err(|_| corrupt(path, "not UTF-8"))?;
            if !text.is_empty() && !text.ends_with('\n') {
                return Err(corrupt(path, "torn tail (final line has no newline)"));
            }
            let mut lines = text.lines();
            let hdr = parse_jsonl_header(path, lines.next().unwrap_or(""))?;
            Ok((hdr, lines.map(str::to_string).collect()))
        }
    }
}

/// Read every partition's command journal in `dir` (partitions found by
/// probing `cmd-0`, `cmd-1`, … against the first header's `partitions`).
pub fn read_cmd_dir(
    dir: &Path,
    format: JournalFormat,
) -> Result<(u32, Vec<Vec<CmdRecord>>), CorruptJournal> {
    let first = journal_path(dir, Kind::Cmd, 0, format);
    let (h0, r0) = read_cmd_journal(&first, format)?;
    if h0.partition != 0 {
        return Err(corrupt(&first, "header partition is not 0"));
    }
    let mut all = vec![r0];
    for p in 1..h0.partitions {
        let path = journal_path(dir, Kind::Cmd, p, format);
        let (h, r) = read_cmd_journal(&path, format)?;
        if h.partition != p || h.partitions != h0.partitions {
            return Err(corrupt(
                &path,
                "header does not match its file name / partition count",
            ));
        }
        all.push(r);
    }
    Ok((h0.partitions, all))
}
