//! Recovery (spec/JOURNAL.md §4–5): restore a snapshot into per-partition
//! cores under the *restoring* pipeline's routing, then replay command
//! journals merged by `iseq`. Determinism makes the replayed events
//! byte-identical to the originals — `orderrecover` proves it.

use std::fmt;
use std::path::Path;

use orderer_core::jsonflat::{get_str, get_u64};
use orderer_core::{snapshot, BookConfig, Command, Event, MatchingCore, Symbol};

use crate::journal::{read_cmd_dir, CmdRecord, CorruptJournal, JournalFormat};
use crate::pipeline::{meta_path, Initial, Snapshot};
use crate::routing::PartitionMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverError {
    /// Unreadable or inconsistent snapshot / sidecar.
    Snapshot(String),
    /// Torn or corrupt journal.
    Journal(CorruptJournal),
}

impl fmt::Display for RecoverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoverError::Snapshot(m) => write!(f, "snapshot: {m}"),
            RecoverError::Journal(j) => write!(f, "journal: {j}"),
        }
    }
}

impl std::error::Error for RecoverError {}

impl From<CorruptJournal> for RecoverError {
    fn from(e: CorruptJournal) -> Self {
        RecoverError::Journal(e)
    }
}

/// Read a snapshot body and its `.meta` sidecar. A missing sidecar (e.g. a
/// matcher snapshot) means cut `iseq = 0`.
pub fn read_snapshot(path: &Path) -> Result<Snapshot, RecoverError> {
    let body = std::fs::read_to_string(path)
        .map_err(|e| RecoverError::Snapshot(format!("{}: {e}", path.display())))?;
    let meta = meta_path(path);
    let (iseq, partitions) = match std::fs::read_to_string(&meta) {
        Ok(line) => {
            let bad = || RecoverError::Snapshot(format!("{}: bad sidecar", meta.display()));
            if get_str(&line, "format") != Some("orderer-meta/1") {
                return Err(bad());
            }
            let iseq = get_u64(&line, "iseq").ok_or_else(bad)?;
            let p = get_u64(&line, "partitions").unwrap_or(1) as u32;
            (iseq, p)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (0, 1),
        Err(e) => return Err(RecoverError::Snapshot(format!("{}: {e}", meta.display()))),
    };
    Ok(Snapshot {
        body,
        iseq,
        partitions,
    })
}

/// Empty cores, or cores restored from `snapshot` (whose header then
/// overrides `book`). Books are routed by `map`, so a snapshot taken at any
/// `P` restores at any `P`.
pub fn restore<C: MatchingCore>(
    book: BookConfig,
    map: &PartitionMap,
    snapshot: Option<&Snapshot>,
) -> Result<(BookConfig, Vec<C>), RecoverError> {
    let Some(snap) = snapshot else {
        return Ok((book, (0..map.partitions()).map(|_| C::new(book)).collect()));
    };
    let parsed = snapshot::try_parse(&snap.body).map_err(|e| RecoverError::Snapshot(e.0))?;
    let book = parsed.cfg;
    let mut cores: Vec<C> = (0..map.partitions()).map(|_| C::new(book)).collect();
    let mut seen = std::collections::HashSet::new();
    for b in &parsed.books {
        if !seen.insert(b.symbol) {
            return Err(RecoverError::Snapshot(format!(
                "book {} appears twice",
                b.symbol
            )));
        }
        cores[map.partition(b.symbol) as usize]
            .restore_book(b.symbol, b.seq, &b.orders)
            .map_err(|e| RecoverError::Snapshot(e.0))?;
    }
    Ok((book, cores))
}

/// Merge per-partition record lists into one `iseq`-ordered stream,
/// dropping records at or before `after`. `iseq`s must be disjoint.
pub fn merge_journals(
    partitions: Vec<Vec<CmdRecord>>,
    after: u64,
) -> Result<Vec<CmdRecord>, RecoverError> {
    let mut all: Vec<CmdRecord> = partitions
        .into_iter()
        .flatten()
        .filter(|r| r.0 > after)
        .collect();
    all.sort_by_key(|r| r.0);
    if let Some(w) = all.windows(2).find(|w| w[0].0 == w[1].0) {
        return Err(RecoverError::Journal(CorruptJournal {
            path: Default::default(),
            detail: format!("iseq {} appears in two partitions", w[0].0),
        }));
    }
    Ok(all)
}

/// Apply commands directly to the cores (no threads), in order:
/// `emit(partition, symbol, seq, event)` for every event.
pub fn replay<C: MatchingCore>(
    cores: &mut [C],
    map: &PartitionMap,
    cmds: impl IntoIterator<Item = (Symbol, Command)>,
    mut emit: impl FnMut(u32, Symbol, u64, &Event),
) {
    for (sym, cmd) in cmds {
        let p = map.partition(sym);
        cores[p as usize].apply(sym, cmd, &mut |s, seq, ev| emit(p, s, seq, ev));
    }
}

/// The outcome of a full recovery.
pub struct Recovery<C> {
    pub book: BookConfig,
    pub cores: Vec<C>,
    /// Snapshot cut (`0` without a snapshot).
    pub snapshot_iseq: u64,
    /// Highest `iseq` recovered (snapshot cut or last replayed record).
    pub last_iseq: u64,
    pub replayed: u64,
}

impl<C> Recovery<C> {
    /// Starting state for `PipelineBuilder::initial`.
    pub fn into_initial(self) -> Initial<C> {
        Initial {
            cores: self.cores,
            next_iseq: self.last_iseq + 1,
        }
    }
}

/// spec/JOURNAL.md §5: snapshot (optional) + every command journal in
/// `journal_dir`, merged by `iseq`, records after the cut replayed.
pub fn recover<C: MatchingCore>(
    book: BookConfig,
    map: &PartitionMap,
    snapshot: Option<&Snapshot>,
    journal: Option<(&Path, JournalFormat)>,
    emit: impl FnMut(u32, Symbol, u64, &Event),
) -> Result<Recovery<C>, RecoverError> {
    let (book, mut cores) = restore::<C>(book, map, snapshot)?;
    let cut = snapshot.map_or(0, |s| s.iseq);
    let records = match journal {
        Some((dir, format)) => merge_journals(read_cmd_dir(dir, format)?.1, cut)?,
        None => Vec::new(),
    };
    let last_iseq = records.last().map_or(cut, |r| r.0.max(cut));
    let replayed = records.len() as u64;
    replay(
        &mut cores,
        map,
        records.into_iter().map(|(_, s, c)| (s, c)),
        emit,
    );
    Ok(Recovery {
        book,
        cores,
        snapshot_iseq: cut,
        last_iseq,
        replayed,
    })
}
