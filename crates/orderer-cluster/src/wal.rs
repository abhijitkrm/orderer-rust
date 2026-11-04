//! The replicated log's write-ahead file (`spec/REPLICATION.md` §6).
//!
//! One consensus write ([`Persist`]) is one frame:
//! `len u32 | crc32c u32 | body`, where the checksum covers the body. A frame
//! is the unit of atomicity: replaying frames in order with `Disk::apply`
//! reproduces exactly the state the protocol core handed to the disk, so the
//! core's crash assumptions (a crash leaves a prefix of the writes) hold.
//!
//! Recovery distinguishes the two ways a tail can be bad:
//! * **torn**: the last frame is incomplete or fails its checksum and nothing
//!   valid follows it. It was never acknowledged (acks wait for fsync), so
//!   it is cut and the node starts normally.
//! * **corrupt**: a bad frame is followed by a valid one. That is real disk
//!   damage to data that may have been acknowledged; the node refuses to
//!   start rather than risk forgetting a vote or an acknowledged entry.

use crate::codec::{decode_persist, encode_persist};
use crate::snapstore::sync_dir;
use orderer::journal::crc32c;
use orderer_consensus::{Disk, Persist};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"ORDWAL\0\x01";
const FRAME_HEAD: usize = 8;
/// A frame body larger than this is treated as garbage, not allocated.
const MAX_FRAME: usize = 256 << 20;

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    /// Valid frames exist after a bad one: not a torn tail.
    Corrupt {
        offset: u64,
    },
    BadHeader,
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "wal i/o: {e}"),
            WalError::Corrupt { offset } => {
                write!(
                    f,
                    "wal corrupt at byte {offset}: valid data follows a bad frame"
                )
            }
            WalError::BadHeader => write!(f, "wal header missing or unrecognised"),
        }
    }
}

impl std::error::Error for WalError {}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Repair {
    pub frames: u64,
    /// Bytes cut from a torn tail.
    pub torn_bytes: u64,
}

pub struct Wal {
    file: File,
    path: PathBuf,
    len: u64,
    buf: Vec<u8>,
}

fn frame(p: &Persist, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&[0; FRAME_HEAD]);
    encode_persist(p, out);
    let body = &out[start + FRAME_HEAD..];
    let (len, crc) = (body.len() as u32, crc32c(body));
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

/// Parse a frame at `off`: its body and total size, if intact.
fn frame_at(data: &[u8], off: usize) -> Option<(&[u8], usize)> {
    let head = data.get(off..off + FRAME_HEAD)?;
    let len = u32::from_le_bytes(head[..4].try_into().expect("4")) as usize;
    let crc = u32::from_le_bytes(head[4..8].try_into().expect("4"));
    if len > MAX_FRAME {
        return None;
    }
    let body = data.get(off + FRAME_HEAD..off + FRAME_HEAD + len)?;
    (crc32c(body) == crc).then_some((body, FRAME_HEAD + len))
}

/// Replay `data` (a whole WAL file). Returns the state, the length of the
/// valid prefix and the number of frames in it.
fn replay(data: &[u8]) -> Result<(Disk, usize, u64), WalError> {
    if data.len() < MAGIC.len() || &data[..MAGIC.len()] != MAGIC {
        return Err(WalError::BadHeader);
    }
    let mut disk = Disk::default();
    let (mut off, mut frames) = (MAGIC.len(), 0u64);
    while off < data.len() {
        match frame_at(data, off) {
            Some((body, size)) => {
                // A frame that checksums but does not decode, or breaks the
                // log's shape, is damage, not a tear.
                let p =
                    decode_persist(body).map_err(|_| WalError::Corrupt { offset: off as u64 })?;
                if !fits(&disk, &p) {
                    return Err(WalError::Corrupt { offset: off as u64 });
                }
                disk.apply(&p);
                off += size;
                frames += 1;
            }
            None => {
                // Torn only if nothing valid follows. Probe where the next
                // frame would start if this one's length is plausible, then
                // fall back to scanning a bounded window.
                if valid_frame_follows(data, off) {
                    return Err(WalError::Corrupt { offset: off as u64 });
                }
                break;
            }
        }
    }
    Ok((disk, off, frames))
}

/// Whether `p` can be applied to `disk` without leaving a hole in the log.
fn fits(disk: &Disk, p: &Persist) -> bool {
    if p.from == 0 {
        return false;
    }
    let (base, len) = match p.compact {
        Some((i, _)) if i > disk.base => {
            (i, disk.log.len().saturating_sub((i - disk.base) as usize))
        }
        _ => (disk.base, disk.log.len()),
    };
    // After any compaction, `from - 1` must land inside (or just after) what remains.
    p.from - 1 >= base && p.from - 1 - base <= len as u64
}

fn valid_frame_follows(data: &[u8], bad: usize) -> bool {
    if let Some(head) = data.get(bad..bad + FRAME_HEAD) {
        let len = u32::from_le_bytes(head[..4].try_into().expect("4")) as usize;
        if len <= MAX_FRAME {
            let next = bad + FRAME_HEAD + len;
            if next < data.len() && frame_at(data, next).is_some() {
                return true;
            }
        }
    }
    // Bounded scan for a frame starting anywhere in the next 1 MiB.
    let end = (bad + 1 + (1 << 20)).min(data.len());
    (bad + 1..end).any(|o| {
        // Cheap reject before checksumming.
        data.get(o..o + 4)
            .map(|h| (u32::from_le_bytes(h.try_into().expect("4")) as usize) <= data.len() - o)
            .unwrap_or(false)
            && frame_at(data, o).is_some_and(|(b, _)| decode_persist(b).is_ok())
    })
}

impl Wal {
    /// Open (creating if absent) the WAL in `dir`, repairing a torn tail.
    pub fn open(dir: impl AsRef<Path>) -> Result<(Wal, Disk, Repair), WalError> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let path = dir.join("raft.wal");
        let tmp = dir.join("raft.wal.tmp");
        let _ = fs::remove_file(&tmp);
        if !path.exists() {
            let mut f = File::create(&tmp)?;
            f.write_all(MAGIC)?;
            f.sync_all()?;
            fs::rename(&tmp, &path)?;
            sync_dir(dir)?;
        }
        let data = fs::read(&path)?;
        let (disk, valid, frames) = replay(&data)?;
        let torn = (data.len() - valid) as u64;
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        if torn > 0 {
            file.set_len(valid as u64)?;
            file.sync_all()?;
        }
        let mut wal = Wal {
            file,
            path,
            len: valid as u64,
            buf: Vec::new(),
        };
        wal.seek_end()?;
        Ok((
            wal,
            disk,
            Repair {
                frames,
                torn_bytes: torn,
            },
        ))
    }

    fn seek_end(&mut self) -> io::Result<()> {
        use std::io::{Seek, SeekFrom};
        self.file.seek(SeekFrom::Start(self.len)).map(|_| ())
    }

    /// Buffer one write. Nothing is durable until [`Wal::sync`].
    pub fn append(&mut self, p: &Persist) {
        frame(p, &mut self.buf);
    }

    /// Write buffered frames and fsync. After this returns, every appended
    /// write is durable.
    pub fn sync(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.file.write_all(&self.buf)?;
            self.len += self.buf.len() as u64;
            self.buf.clear();
        }
        self.file.sync_data()
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len <= MAGIC.len() as u64
    }

    /// Replace the file with a single frame holding the current state, so a
    /// compacted log's prefix no longer occupies disk. Crash-safe: the new
    /// file is complete and fsynced before it replaces the old one.
    pub fn rewrite(&mut self) -> Result<(), WalError> {
        self.sync()?;
        let data = fs::read(&self.path)?;
        let (disk, _, _) = replay(&data)?;
        let p = Persist {
            token: 0,
            term: disk.term,
            voted_for: disk.voted_for,
            from: disk.base + 1,
            entries: disk.log.clone(),
            compact: (disk.base > 0).then_some((disk.base, disk.base_term)),
        };
        let dir = self
            .path
            .parent()
            .expect("wal has a parent dir")
            .to_path_buf();
        let tmp = dir.join("raft.wal.tmp");
        let mut out = MAGIC.to_vec();
        frame(&p, &mut out);
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&out)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        sync_dir(&dir)?;
        self.file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.len = out.len() as u64;
        self.seek_end()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orderer_consensus::Entry;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("orderer-wal-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    fn e(term: u32, b: u8) -> Entry {
        Entry {
            term,
            data: vec![b; 5],
        }
    }

    fn w(from: u64, entries: Vec<Entry>, term: u32) -> Persist {
        Persist {
            token: 0,
            term,
            voted_for: Some(1),
            from,
            entries,
            compact: None,
        }
    }

    #[test]
    fn replays_what_was_written() {
        let d = tmp("replay");
        let (mut wal, disk, rep) = Wal::open(&d).unwrap();
        assert_eq!((disk, rep.frames), (Disk::default(), 0));
        wal.append(&w(1, vec![e(1, 1), e(1, 2), e(1, 3)], 1));
        wal.append(&w(3, vec![e(2, 9)], 2)); // truncates index 3 onwards
        wal.sync().unwrap();
        drop(wal);
        let (_, disk, rep) = Wal::open(&d).unwrap();
        assert_eq!(
            rep,
            Repair {
                frames: 2,
                torn_bytes: 0
            }
        );
        assert_eq!(disk.term, 2);
        assert_eq!(disk.log, vec![e(1, 1), e(1, 2), e(2, 9)]);
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn unsynced_appends_are_not_durable() {
        let d = tmp("unsynced");
        let (mut wal, _, _) = Wal::open(&d).unwrap();
        wal.append(&w(1, vec![e(1, 1)], 1));
        wal.sync().unwrap();
        wal.append(&w(2, vec![e(1, 2)], 1)); // buffered only
        drop(wal);
        let (_, disk, _) = Wal::open(&d).unwrap();
        assert_eq!(disk.log.len(), 1);
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn every_torn_prefix_recovers_a_valid_prefix_of_the_writes() {
        let d = tmp("torn");
        let (mut wal, _, _) = Wal::open(&d).unwrap();
        for i in 0..6u64 {
            wal.append(&w(i + 1, vec![e(1, i as u8)], 1));
        }
        wal.sync().unwrap();
        drop(wal);
        let full = fs::read(d.join("raft.wal")).unwrap();
        for cut in MAGIC.len()..=full.len() {
            fs::write(d.join("raft.wal"), &full[..cut]).unwrap();
            let (_, disk, rep) = Wal::open(&d).unwrap_or_else(|e| panic!("cut {cut}: {e}"));
            assert_eq!(disk.log.len() as u64, rep.frames, "cut {cut}");
            for (i, en) in disk.log.iter().enumerate() {
                assert_eq!(*en, e(1, i as u8), "cut {cut} entry {i}");
            }
            // The torn tail was physically removed.
            assert_eq!(
                fs::metadata(d.join("raft.wal")).unwrap().len() + rep.torn_bytes,
                cut as u64
            );
        }
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn damage_before_valid_data_is_refused() {
        let d = tmp("corrupt");
        let (mut wal, _, _) = Wal::open(&d).unwrap();
        for i in 0..4u64 {
            wal.append(&w(i + 1, vec![e(1, i as u8)], 1));
        }
        wal.sync().unwrap();
        drop(wal);
        let mut data = fs::read(d.join("raft.wal")).unwrap();
        data[MAGIC.len() + FRAME_HEAD + 2] ^= 0xff; // inside frame 1's body
        fs::write(d.join("raft.wal"), &data).unwrap();
        assert!(matches!(Wal::open(&d), Err(WalError::Corrupt { .. })));
        // A flipped bit in the very last frame is a tear, not corruption.
        let (mut wal2, _, _) = {
            fs::remove_dir_all(&d).unwrap();
            Wal::open(&d).unwrap()
        };
        for i in 0..4u64 {
            wal2.append(&w(i + 1, vec![e(1, i as u8)], 1));
        }
        wal2.sync().unwrap();
        drop(wal2);
        let mut data = fs::read(d.join("raft.wal")).unwrap();
        let n = data.len();
        data[n - 1] ^= 1;
        fs::write(d.join("raft.wal"), &data).unwrap();
        let (_, disk, rep) = Wal::open(&d).unwrap();
        assert_eq!(
            (disk.log.len(), rep.frames > 0, rep.torn_bytes > 0),
            (3, true, true)
        );
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn bad_header_and_garbage_are_refused() {
        let d = tmp("hdr");
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join("raft.wal"), b"not a wal at all").unwrap();
        assert!(matches!(Wal::open(&d), Err(WalError::BadHeader)));
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn rewrite_keeps_state_and_shrinks_the_file() {
        let d = tmp("rewrite");
        let (mut wal, _, _) = Wal::open(&d).unwrap();
        for i in 0..100u64 {
            wal.append(&w(i + 1, vec![e(1, i as u8)], 1));
        }
        wal.append(&Persist {
            token: 0,
            term: 1,
            voted_for: Some(1),
            from: 101,
            entries: vec![],
            compact: Some((90, 1)),
        });
        wal.sync().unwrap();
        let before = wal.len();
        wal.rewrite().unwrap();
        assert!(wal.len() < before);
        wal.append(&w(101, vec![e(1, 200)], 1));
        wal.sync().unwrap();
        drop(wal);
        let (_, disk, _) = Wal::open(&d).unwrap();
        assert_eq!((disk.base, disk.base_term, disk.log.len()), (90, 1, 11));
        assert_eq!(disk.log.last(), Some(&e(1, 200)));
        fs::remove_dir_all(d).unwrap();
    }
}
