//! Asynchronous journal writer: keeps every syscall off the pipeline's
//! threads.
//!
//! The owning stage (a journal or egress thread) encodes records into a
//! chunk — a plain `memcpy`, no syscall. Full chunks (or partial ones on
//! idle/barrier/shutdown) are handed to a dedicated I/O thread that blocks
//! in `write`, group-commits `fsync`s, then advances the `flushed` and
//! `durable` watermarks and recycles the chunk. A slow disk only costs
//! chunks: the stage blocks only when every chunk is in flight
//! (backpressure), never on an individual `write` or `fsync` stall — which
//! on macOS (`F_FULLFSYNC`, page-cache pressure) reach tens of ms.
//!
//! Chunks are allocated once at start: no allocation in steady state.

use std::fs::File;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::journal::FsyncPolicy;

pub(crate) const CHUNK: usize = 1 << 18;
const CHUNKS: usize = 64;

enum Msg {
    /// Bytes, the last record id they contain, and how many records.
    Data(Vec<u8>, u64, u64),
    /// Finish the current file (written, and synced per policy) and continue
    /// in this one: a new segment (spec/JOURNAL.md §6 step 2).
    Rotate(File),
    Stop,
}

/// Watermarks an I/O thread advances (record ids are `iseq` for command
/// journals; event journals don't track them).
pub(crate) struct Marks {
    pub flushed: Arc<AtomicU64>,
    pub durable: Arc<AtomicU64>,
    pub io: Arc<crate::stats::IoStats>,
}

/// Call once on any thread that will own a [`ChunkWriter`]: std allocates
/// a per-thread channel context on the thread's first blocking receive,
/// which must happen at startup rather than when the disk first lags.
pub(crate) fn prewarm_thread() {
    let (_tx, rx) = sync_channel::<()>(1);
    let _ = rx.recv_timeout(Duration::from_millis(1));
}

pub(crate) struct ChunkWriter {
    cur: Vec<u8>,
    last: u64,
    records: u64,
    tx: SyncSender<Msg>,
    pool: Receiver<Vec<u8>>,
    io: Option<JoinHandle<io::Result<()>>>,
}

impl ChunkWriter {
    /// Take ownership of `file` (positioned at its end) and start its I/O
    /// thread. `fsync: None` never syncs; `durable` then follows `flushed`.
    pub(crate) fn start(
        file: File,
        name: String,
        fsync: Option<FsyncPolicy>,
        marks: Marks,
    ) -> io::Result<ChunkWriter> {
        let (tx, rx) = sync_channel::<Msg>(CHUNKS);
        let (pool_tx, pool) = sync_channel::<Vec<u8>>(CHUNKS);
        // Block once on the empty pool now: std lazily allocates a channel's
        // waiter state the first time a receive blocks, and that must not
        // happen in steady state (when the disk first falls behind). The
        // timeout must outlast std's spin phase so the waiter registers.
        let _ = pool.recv_timeout(Duration::from_millis(1));
        for _ in 0..CHUNKS - 1 {
            pool_tx.send(Vec::with_capacity(CHUNK)).unwrap();
        }
        let io = std::thread::Builder::new().name(name).spawn(move || {
            crate::affinity::set_current(crate::affinity::Role::Background);
            io_thread(file, rx, pool_tx, fsync, marks)
        })?;
        Ok(ChunkWriter {
            cur: Vec::with_capacity(CHUNK),
            last: 0,
            records: 0,
            tx,
            pool,
            io: Some(io),
        })
    }

    /// Room for one more record of `len` bytes, handing off the chunk if not.
    #[inline]
    pub(crate) fn reserve(&mut self, len: usize) {
        if self.cur.len() + len > CHUNK {
            self.hand_off();
        }
    }

    /// The current chunk, to encode one record into (after `reserve`).
    #[inline]
    pub(crate) fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.cur
    }

    /// Note that a complete record with id `id` was appended.
    #[inline]
    pub(crate) fn record(&mut self, id: u64) {
        self.last = id;
        self.records += 1;
    }

    /// Bytes encoded but not yet handed to the I/O thread.
    #[inline]
    pub(crate) fn pending(&self) -> usize {
        self.cur.len()
    }

    /// Hand the current chunk to the I/O thread. Blocks only if every chunk
    /// is in flight.
    pub(crate) fn hand_off(&mut self) {
        if self.cur.is_empty() {
            return;
        }
        let next = self
            .pool
            .recv()
            .unwrap_or_else(|_| Vec::with_capacity(CHUNK));
        let full = std::mem::replace(&mut self.cur, next);
        let records = std::mem::take(&mut self.records);
        // a send error means the I/O thread died; `finish` reports why
        let _ = self.tx.send(Msg::Data(full, self.last, records));
    }

    /// Continue in `next` (a new segment, header written): everything
    /// encoded so far goes to the current file, which is synced per policy
    /// and closed by the I/O thread.
    pub(crate) fn rotate(&mut self, next: File) {
        self.hand_off();
        let _ = self.tx.send(Msg::Rotate(next));
    }

    /// Hand off what's left, wait for the I/O thread to write (and, per its
    /// policy, sync) everything, and report any I/O error.
    pub(crate) fn finish(&mut self) -> io::Result<()> {
        self.hand_off();
        let _ = self.tx.send(Msg::Stop);
        match self.io.take() {
            Some(h) => h
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("journal I/O thread panicked"))),
            None => Ok(()),
        }
    }
}

impl Drop for ChunkWriter {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn io_thread(
    mut file: File,
    rx: Receiver<Msg>,
    pool: SyncSender<Vec<u8>>,
    fsync: Option<FsyncPolicy>,
    marks: Marks,
) -> io::Result<()> {
    let mut unsynced = 0u64;
    let mut written = marks.flushed.load(Ordering::Acquire);
    let mut last_sync = Instant::now();
    let idle = match fsync {
        Some(FsyncPolicy::EveryN { idle, .. }) => idle,
        Some(FsyncPolicy::Every(iv)) => iv,
        _ => Duration::from_secs(3600),
    };
    let sync = |file: &File, written: u64| -> io::Result<()> {
        let t = Instant::now();
        file.sync_data()?;
        marks.io.record(t.elapsed().as_nanos() as u64);
        marks.durable.store(written, Ordering::Release);
        Ok(())
    };
    // Close the current segment: everything in it is written already; sync
    // it if anything is unsynced (and the policy syncs at all).
    let rotate =
        |file: &mut File, next: File, unsynced: &mut u64, written: u64| -> io::Result<()> {
            if *unsynced > 0 && !matches!(fsync, None | Some(FsyncPolicy::Never)) {
                sync(file, written)?;
            }
            *unsynced = 0;
            file.flush()?;
            *file = next;
            Ok(())
        };
    loop {
        let msg = if unsynced > 0 {
            match rx.recv_timeout(idle) {
                Ok(m) => Some(m),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => Some(Msg::Stop),
            }
        } else {
            Some(rx.recv().unwrap_or(Msg::Stop))
        };
        match msg {
            Some(Msg::Data(buf, last, records)) => {
                // Group commit: write everything already queued, then decide
                // on one fsync covering all of it.
                let mut stop = false;
                let mut next = Some((buf, last, records));
                while let Some((mut buf, last, records)) = next.take() {
                    file.write_all(&buf)?;
                    written = last;
                    marks.flushed.store(written, Ordering::Release);
                    unsynced += records.max(1);
                    buf.clear();
                    let _ = pool.send(buf);
                    match rx.try_recv() {
                        Ok(Msg::Data(b, l, r)) => next = Some((b, l, r)),
                        Ok(Msg::Rotate(f)) => {
                            rotate(&mut file, f, &mut unsynced, written)?;
                            last_sync = Instant::now();
                        }
                        Ok(Msg::Stop) => stop = true,
                        Err(_) => {}
                    }
                }
                let due = match fsync {
                    None | Some(FsyncPolicy::Never) => {
                        marks.durable.store(written, Ordering::Release);
                        unsynced = 0;
                        false
                    }
                    Some(FsyncPolicy::EveryN { n, .. }) => unsynced >= n,
                    Some(FsyncPolicy::Every(iv)) => last_sync.elapsed() >= iv,
                };
                if due || (stop && unsynced > 0) {
                    sync(&file, written)?;
                    unsynced = 0;
                    last_sync = Instant::now();
                }
                if stop {
                    file.flush()?;
                    return Ok(());
                }
            }
            None => {
                // idle with unsynced records: group commit now
                sync(&file, written)?;
                unsynced = 0;
                last_sync = Instant::now();
            }
            Some(Msg::Rotate(f)) => {
                rotate(&mut file, f, &mut unsynced, written)?;
                last_sync = Instant::now();
            }
            Some(Msg::Stop) => {
                if unsynced > 0 {
                    sync(&file, written)?;
                }
                file.flush()?;
                return Ok(());
            }
        }
    }
}
