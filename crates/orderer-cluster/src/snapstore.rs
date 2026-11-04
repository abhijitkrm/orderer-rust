//! Durable snapshots: `snap-<index>.snap` files, written atomically.
//!
//! The state-machine snapshot is opaque bytes. A file is
//! `ORDS | index u64 | term u32 | len u64 | crc u32 | data`, with the CRC-32C
//! over the data. Files are written to a temp name, fsynced, renamed, and the
//! directory is fsynced, so a crash leaves either the whole file or none.

use orderer::journal::crc32c;
use orderer_consensus::{Index, Term};
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"ORDS";
const HEADER: usize = 4 + 8 + 4 + 8 + 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapMeta {
    pub index: Index,
    pub term: Term,
    pub len: u64,
    pub crc: u32,
}

pub struct SnapStore {
    dir: PathBuf,
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

impl SnapStore {
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        // Leftover temp files are incomplete writes.
        for e in fs::read_dir(&dir)? {
            let p = e?.path();
            if p.extension().is_some_and(|x| x == "tmp") {
                let _ = fs::remove_file(p);
            }
        }
        Ok(SnapStore { dir })
    }

    fn path(&self, index: Index) -> PathBuf {
        self.dir.join(format!("snap-{index:020}.snap"))
    }

    fn header(index: Index, term: Term, len: u64, crc: u32) -> Vec<u8> {
        let mut h = Vec::with_capacity(HEADER);
        h.extend_from_slice(MAGIC);
        h.extend_from_slice(&index.to_le_bytes());
        h.extend_from_slice(&term.to_le_bytes());
        h.extend_from_slice(&len.to_le_bytes());
        h.extend_from_slice(&crc.to_le_bytes());
        h
    }

    fn parse_header(h: &[u8]) -> io::Result<SnapMeta> {
        if h.len() < HEADER || &h[..4] != MAGIC {
            return Err(invalid("not a snapshot file"));
        }
        Ok(SnapMeta {
            index: u64::from_le_bytes(h[4..12].try_into().expect("8")),
            term: u32::from_le_bytes(h[12..16].try_into().expect("4")),
            len: u64::from_le_bytes(h[16..24].try_into().expect("8")),
            crc: u32::from_le_bytes(h[24..28].try_into().expect("4")),
        })
    }

    pub fn put(&self, index: Index, term: Term, data: &[u8]) -> io::Result<()> {
        let tmp = self.dir.join(format!("snap-{index:020}.tmp"));
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&Self::header(index, term, data.len() as u64, crc32c(data)))?;
            f.write_all(data)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, self.path(index))?;
        sync_dir(&self.dir)
    }

    /// Indexes of intact snapshot files, ascending. A corrupt file is skipped.
    pub fn list(&self) -> io::Result<Vec<Index>> {
        let mut v = Vec::new();
        for e in fs::read_dir(&self.dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            if let Some(n) = name
                .strip_prefix("snap-")
                .and_then(|n| n.strip_suffix(".snap"))
            {
                if let Ok(i) = n.parse::<u64>() {
                    if self.meta(i).is_ok() {
                        v.push(i);
                    }
                }
            }
        }
        v.sort_unstable();
        Ok(v)
    }

    pub fn meta(&self, index: Index) -> io::Result<SnapMeta> {
        let mut h = [0u8; HEADER];
        File::open(self.path(index))?.read_exact(&mut h)?;
        let m = Self::parse_header(&h)?;
        if m.index != index {
            return Err(invalid("snapshot file index mismatch"));
        }
        let size = fs::metadata(self.path(index))?.len();
        if size != HEADER as u64 + m.len {
            return Err(invalid("snapshot file truncated"));
        }
        Ok(m)
    }

    /// Load and verify a snapshot.
    pub fn get(&self, index: Index) -> io::Result<(SnapMeta, Vec<u8>)> {
        let mut all = Vec::new();
        File::open(self.path(index))?.read_to_end(&mut all)?;
        let m = Self::parse_header(&all)?;
        let data = all.split_off(HEADER);
        if m.index != index || data.len() as u64 != m.len || crc32c(&data) != m.crc {
            return Err(invalid(format!("snapshot {index} is corrupt")));
        }
        Ok((m, data))
    }

    /// Open for streaming to a peer: returns the meta and the data file
    /// positioned after the header.
    pub fn open_stream(&self, index: Index) -> io::Result<(SnapMeta, File)> {
        let m = self.meta(index)?;
        let mut f = File::open(self.path(index))?;
        let mut h = [0u8; HEADER];
        f.read_exact(&mut h)?;
        Ok((m, f))
    }

    /// Begin receiving a snapshot streamed from a peer.
    pub fn receive(&self, index: Index, term: Term, len: u64, crc: u32) -> io::Result<Incoming> {
        let tmp = self.dir.join(format!("snap-{index:020}.tmp"));
        let mut file = File::create(&tmp)?;
        file.write_all(&Self::header(index, term, len, crc))?;
        Ok(Incoming {
            file,
            tmp,
            dst: self.path(index),
            dir: self.dir.clone(),
            len,
            crc,
            got: 0,
            running: Crc::new(),
        })
    }

    /// Delete snapshots older than `keep_from`.
    pub fn gc(&self, keep_from: Index) -> io::Result<()> {
        for i in self.list()? {
            if i < keep_from {
                let _ = fs::remove_file(self.path(i));
            }
        }
        Ok(())
    }
}

/// Incremental CRC-32C (same polynomial and conventions as
/// `orderer::journal::crc32c`) so a streamed snapshot is never held in memory.
struct Crc(u32);

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

impl Crc {
    fn new() -> Self {
        Crc(!0)
    }
    fn update(&mut self, data: &[u8]) {
        for &b in data {
            self.0 = TABLE[((self.0 ^ u32::from(b)) & 0xff) as usize] ^ (self.0 >> 8);
        }
    }
    fn finish(&self) -> u32 {
        !self.0
    }
}

pub struct Incoming {
    file: File,
    tmp: PathBuf,
    dst: PathBuf,
    dir: PathBuf,
    len: u64,
    crc: u32,
    got: u64,
    running: Crc,
}

impl Incoming {
    pub fn write(&mut self, chunk: &[u8]) -> io::Result<()> {
        if self.got + chunk.len() as u64 > self.len {
            return Err(invalid("snapshot longer than announced"));
        }
        self.got += chunk.len() as u64;
        self.running.update(chunk);
        self.file.write_all(chunk)
    }

    /// Verify the length and checksum and publish the file.
    pub fn finish(self) -> io::Result<()> {
        if self.got != self.len {
            let _ = fs::remove_file(&self.tmp);
            return Err(invalid("snapshot shorter than announced"));
        }
        if self.running.finish() != self.crc {
            let _ = fs::remove_file(&self.tmp);
            return Err(invalid("snapshot checksum mismatch"));
        }
        self.file.sync_all()?;
        fs::rename(&self.tmp, &self.dst)?;
        sync_dir(&self.dir)
    }

    pub fn abort(self) {
        let _ = fs::remove_file(&self.tmp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("orderer-snap-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn incremental_crc_matches_journal_crc() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i * 7) as u8).collect();
        let mut c = Crc::new();
        for part in data.chunks(13) {
            c.update(part);
        }
        assert_eq!(c.finish(), crc32c(&data));
    }

    #[test]
    fn put_get_list_gc() {
        let d = tmp("pgl");
        let s = SnapStore::open(&d).unwrap();
        s.put(10, 2, b"ten").unwrap();
        s.put(20, 3, b"twenty").unwrap();
        assert_eq!(s.list().unwrap(), vec![10, 20]);
        let (m, data) = s.get(20).unwrap();
        assert_eq!((m.index, m.term, data.as_slice()), (20, 3, &b"twenty"[..]));
        s.gc(20).unwrap();
        assert_eq!(s.list().unwrap(), vec![20]);
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn corruption_and_truncation_are_detected() {
        let d = tmp("corrupt");
        let s = SnapStore::open(&d).unwrap();
        s.put(5, 1, b"hello world").unwrap();
        let p = s.path(5);
        let mut bytes = fs::read(&p).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&p, &bytes).unwrap();
        assert!(s.get(5).is_err());
        fs::write(&p, &bytes[..bytes.len() - 3]).unwrap();
        assert!(s.get(5).is_err());
        assert!(s.list().unwrap().is_empty());
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn streaming_receive_verifies() {
        let d = tmp("recv");
        let s = SnapStore::open(&d).unwrap();
        let data: Vec<u8> = (0..100_000u32).map(|i| i as u8).collect();
        let crc = crc32c(&data);
        let mut inc = s.receive(7, 1, data.len() as u64, crc).unwrap();
        for c in data.chunks(4096) {
            inc.write(c).unwrap();
        }
        inc.finish().unwrap();
        assert_eq!(s.get(7).unwrap().1, data);
        // Wrong checksum is refused and leaves nothing behind.
        let mut bad = s.receive(8, 1, 4, crc).unwrap();
        bad.write(b"abcd").unwrap();
        assert!(bad.finish().is_err());
        assert_eq!(s.list().unwrap(), vec![7]);
        // Overlong stream is refused.
        let mut long = s.receive(9, 1, 2, 0).unwrap();
        assert!(long.write(b"abc").is_err());
        fs::remove_dir_all(d).unwrap();
    }
}
