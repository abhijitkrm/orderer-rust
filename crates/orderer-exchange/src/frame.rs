//! The framed envelope shared by the gateway and the tier client: `len u32 |
//! crc32c u32 | kind u8 | body` (`len` counts kind and body). No authentication.

use orderer::journal::crc32c;
use std::io::{self, Read, Write};

pub(crate) const MAX_FRAME: usize = 64 << 20;

pub(crate) fn write_frame(w: &mut impl Write, kind: u8, body: &[u8]) -> io::Result<()> {
    let mut b = Vec::with_capacity(1 + body.len());
    b.push(kind);
    b.extend_from_slice(body);
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(&crc32c(&b).to_le_bytes())?;
    w.write_all(&b)?;
    w.flush()
}

pub(crate) fn read_frame(r: &mut impl Read) -> io::Result<(u8, Vec<u8>)> {
    let mut h = [0u8; 8];
    r.read_exact(&mut h)?;
    let len = u32::from_le_bytes(h[..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(h[4..].try_into().unwrap());
    if len == 0 || len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame length"));
    }
    let mut b = vec![0u8; len];
    r.read_exact(&mut b)?;
    if crc32c(&b) != crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame checksum"));
    }
    let kind = b.remove(0);
    Ok((kind, b))
}
