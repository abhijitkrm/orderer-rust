//! The framed envelope shared by the gateway and the exchange port: `len u32 |
//! crc32c u32 | kind u8 | body [| mac 16]` (`len` counts kind, body and mac). With a
//! session (see `secure.rs`) every frame carries a MAC over a per-direction
//! sequence number, so a frame cannot be altered, replayed, reordered, dropped
//! mid-stream or reflected.

use orderer::journal::crc32c;
use orderer_cluster::auth::{Session, MAC_LEN};
use std::io::{self, Read, Write};

pub(crate) const MAX_FRAME: usize = 64 << 20;

pub(crate) fn write_frame(
    w: &mut impl Write,
    kind: u8,
    body: &[u8],
    sess: Option<&mut Session>,
) -> io::Result<()> {
    let mac = sess.map(|s| s.seal(kind, body));
    let mut b = Vec::with_capacity(1 + body.len() + MAC_LEN);
    b.push(kind);
    b.extend_from_slice(body);
    if let Some(m) = mac {
        b.extend_from_slice(&m);
    }
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(&crc32c(&b).to_le_bytes())?;
    w.write_all(&b)?;
    w.flush()
}

pub(crate) fn read_frame(
    r: &mut impl Read,
    sess: Option<&mut Session>,
) -> io::Result<(u8, Vec<u8>)> {
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
    let Some(sess) = sess else {
        return Ok((kind, b));
    };
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "frame authentication");
    if b.len() < MAC_LEN {
        return Err(bad());
    }
    let mac = b.split_off(b.len() - MAC_LEN);
    if sess.open(kind, &b, &mac) {
        Ok((kind, b))
    } else {
        Err(bad())
    }
}
