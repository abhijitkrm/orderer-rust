//! Pre-shared-key authentication for the replica transport and the client
//! protocol.
//!
//! This gives **authentication and integrity** (a peer must know the cluster
//! key; frames cannot be altered, replayed, reordered, dropped mid-stream or
//! reflected back) but **not confidentiality**: payloads still cross the wire
//! in the clear. For secrecy, run the ports over a tunnel (WireGuard, mTLS
//! proxy). A real TLS stack means a third-party dependency, which this
//! workspace does not take.
//!
//! Handshake (initiator I, responder R, shared key K):
//! `I -> R: nonce_i`; `R -> I: nonce_r, MAC(K, "chal" | nonce_i | nonce_r | ids)`;
//! I checks that MAC (R knows K), derives `SK = HMAC(K, "sess" | nonce_i |
//! nonce_r | ids)`, and sends its first frame sealed under SK (I knows K).
//! Every later frame carries `MAC(SK, direction | sequence | kind | body)`,
//! truncated to 128 bits, with a strictly increasing per-direction sequence.

use crate::sha256::sha256;
use std::hash::{BuildHasher, Hasher};

pub const MAC_LEN: usize = 16;
pub const NONCE_LEN: usize = 16;

pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&sha256(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(64 + msg.len());
    inner.extend(k.iter().map(|b| b ^ 0x36));
    inner.extend_from_slice(msg);
    let ih = sha256(&inner);
    let mut outer = Vec::with_capacity(96);
    outer.extend(k.iter().map(|b| b ^ 0x5c));
    outer.extend_from_slice(&ih);
    sha256(&outer)
}

/// Constant-time comparison.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// A fresh nonce. Uses the OS-seeded randomness behind `RandomState` plus
/// the clock, with no external crate.
pub fn nonce() -> [u8; NONCE_LEN] {
    let mut out = [0u8; NONCE_LEN];
    let rs = std::collections::hash_map::RandomState::new();
    for chunk in out.chunks_mut(8) {
        let mut h = rs.build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        chunk.copy_from_slice(&h.finish().to_le_bytes());
    }
    out
}

/// MAC for the handshake's challenge/session derivations.
pub fn handshake(key: &[u8], label: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut m = label.to_vec();
    for p in parts {
        m.extend_from_slice(p);
    }
    hmac_sha256(key, &m)
}

/// Per-connection frame sealing and checking.
pub struct Session {
    key: [u8; 32],
    send_dir: u8,
    send_seq: u64,
    recv_seq: u64,
}

impl Session {
    /// `send_dir` is 0 for the initiator and 1 for the responder; each side
    /// checks frames with the other's direction, so a frame cannot be
    /// reflected back at its sender.
    pub fn new(key: [u8; 32], send_dir: u8) -> Session {
        Session {
            key,
            send_dir,
            send_seq: 0,
            recv_seq: 0,
        }
    }

    fn mac(&self, dir: u8, seq: u64, kind: u8, body: &[u8]) -> [u8; MAC_LEN] {
        let mut m = Vec::with_capacity(10 + body.len());
        m.push(dir);
        m.extend_from_slice(&seq.to_le_bytes());
        m.push(kind);
        m.extend_from_slice(body);
        hmac_sha256(&self.key, &m)[..MAC_LEN]
            .try_into()
            .expect("16 bytes")
    }

    pub fn seal(&mut self, kind: u8, body: &[u8]) -> [u8; MAC_LEN] {
        let mac = self.mac(self.send_dir, self.send_seq, kind, body);
        self.send_seq += 1;
        mac
    }

    /// Check the next incoming frame. Advances the sequence only on success.
    pub fn open(&mut self, kind: u8, body: &[u8], mac: &[u8]) -> bool {
        let want = self.mac(1 - self.send_dir, self.recv_seq, kind, body);
        let ok = ct_eq(&want, mac);
        if ok {
            self.recv_seq += 1;
        }
        ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sha256::hex;

    #[test]
    fn hmac_matches_rfc_4231() {
        // Test case 1
        assert_eq!(
            hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // Test case 2
        assert_eq!(
            hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Test case 6: key longer than the block size
        assert_eq!(
            hex(&hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn nonces_differ() {
        let a: std::collections::HashSet<_> = (0..200).map(|_| nonce()).collect();
        assert_eq!(a.len(), 200);
    }

    #[test]
    fn sessions_detect_tampering_replay_reordering_and_reflection() {
        let mut a = Session::new([7; 32], 0);
        let mut b = Session::new([7; 32], 1);
        let m1 = a.seal(1, b"one");
        let m2 = a.seal(1, b"two");
        assert!(b.open(1, b"one", &m1));
        // Replay of an accepted frame, an altered body or kind, an old MAC
        // on a new frame: all rejected, and rejection does not advance state.
        assert!(!b.open(1, b"one", &m1));
        assert!(!b.open(1, b"twp", &m2));
        assert!(!b.open(2, b"two", &m2));
        assert!(b.open(1, b"two", &m2));
        // Skipping ahead (a dropped frame) is rejected.
        let _lost = a.seal(1, b"three");
        let m4 = a.seal(1, b"four");
        assert!(!b.open(1, b"four", &m4));
        // A frame cannot be reflected to its own sender.
        let mut a2 = Session::new([7; 32], 0);
        let m = a2.seal(1, b"x");
        assert!(!a2.open(1, b"x", &m));
        // A different key never verifies.
        let mut c = Session::new([8; 32], 1);
        let mut a3 = Session::new([7; 32], 0);
        let m = a3.seal(1, b"x");
        assert!(!c.open(1, b"x", &m));
    }
}
