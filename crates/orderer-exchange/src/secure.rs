//! Connection setup shared by the gateway port and the exchange port:
//! pre-shared-key authentication and a configuration check, the scheme of the
//! cluster client port (`orderer_cluster::auth`) plus the config checksum.
//!
//! Handshake (initiator I, responder R, key K or none, config checksum C):
//! `I -> R: HELLO (0xF0) nonce_i | C_i`. R refuses (`ERR`, 0xF2) unless `C_i` equals
//! its own `C`: a process built from a different configuration never talks to
//! this one. `R -> I: CHALLENGE (0xF1) nonce_r | MAC(K, label "chal" | nonce_i |
//! nonce_r | C)` (zeros when there is no key). I checks that MAC (R knows K) and
//! both derive `SK = HMAC(K, label "sess" | nonce_i | nonce_r | C)`; every later
//! frame in both directions carries a MAC under SK with a strictly increasing
//! per-direction sequence, and I's first request is the proof that I knows K.
//! Authentication and integrity only, not secrecy (as in the cluster port).
//!
//! `label` separates the two ports, so a session of one cannot be used on the
//! other.

use crate::frame::{read_frame, write_frame};
use orderer_cluster::auth::{ct_eq, handshake, hmac_sha256, nonce, Session, MAC_LEN, NONCE_LEN};
use orderer_cluster::sha256::hex;
use std::io;
use std::net::TcpStream;
use std::time::Duration;

const HELLO: u8 = 0xF0;
const CHALLENGE: u8 = 0xF1;
const ERR: u8 = 0xF2;

/// What a port or a client holds: the pre-shared key (if any) and the checksum of
/// the configuration it was built from (zeros if it does not check).
#[derive(Clone, Default)]
pub struct Auth {
    pub key: Option<Vec<u8>>,
    pub config: [u8; 32],
}

impl Auth {
    /// No key, no config check: for tests and tools on a private network.
    pub fn none() -> Auth {
        Auth::default()
    }
    pub fn new(key: Option<Vec<u8>>, config: [u8; 32]) -> Auth {
        Auth { key, config }
    }
    /// The key from the environment variable `ORDERER_KEY`, if set and not empty.
    pub fn from_env(config: [u8; 32]) -> Auth {
        Auth {
            key: std::env::var("ORDERER_KEY")
                .ok()
                .filter(|k| !k.is_empty())
                .map(String::into_bytes),
            config,
        }
    }
    /// The key for the replica transport of a node: bound to the configuration, so
    /// replicas built from different configurations cannot form a group.
    pub fn replica_key(&self) -> Option<Vec<u8>> {
        let key = self.key.clone().unwrap_or_default();
        if self.config == [0u8; 32] {
            return self.key.clone();
        }
        let mut m = b"orderer-exchange/replica-key".to_vec();
        m.extend_from_slice(&self.config);
        Some(hmac_sha256(&key, &m).to_vec())
    }
}

fn bad(m: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, m.into())
}

fn chal_label(label: &[u8]) -> Vec<u8> {
    [label, b"chal"].concat()
}
fn sess_label(label: &[u8]) -> Vec<u8> {
    [label, b"sess"].concat()
}

/// The responder's side. Leaves the stream's read timeout as it found it.
pub(crate) fn accept(
    conn: &mut TcpStream,
    auth: &Auth,
    label: &[u8],
) -> io::Result<Option<Session>> {
    let prev = conn.read_timeout()?;
    conn.set_read_timeout(Some(Duration::from_secs(5)))?;
    let (kind, hello) = read_frame(conn, None)?;
    if kind != HELLO || hello.len() != NONCE_LEN + 32 {
        return Err(bad("expected a hello"));
    }
    let (nc, cfg) = hello.split_at(NONCE_LEN);
    if cfg != auth.config {
        let msg = format!(
            "config mismatch: peer {} here {}",
            &hex(cfg)[..16],
            &hex(&auth.config)[..16]
        );
        let _ = write_frame(conn, ERR, msg.as_bytes(), None);
        return Err(bad(msg));
    }
    let ns = nonce();
    let mut reply = ns.to_vec();
    let sess = match &auth.key {
        Some(k) => {
            let mac = handshake(k, &chal_label(label), &[nc, &ns, cfg]);
            reply.extend_from_slice(&mac[..MAC_LEN]);
            Some(Session::new(
                handshake(k, &sess_label(label), &[nc, &ns, cfg]),
                1,
            ))
        }
        None => {
            reply.extend_from_slice(&[0u8; MAC_LEN]);
            None
        }
    };
    write_frame(conn, CHALLENGE, &reply, None)?;
    conn.set_read_timeout(prev)?;
    Ok(sess)
}

/// The initiator's side.
pub(crate) fn connect(
    conn: &mut TcpStream,
    auth: &Auth,
    label: &[u8],
) -> io::Result<Option<Session>> {
    let nc = nonce();
    let mut hello = nc.to_vec();
    hello.extend_from_slice(&auth.config);
    write_frame(conn, HELLO, &hello, None)?;
    let (kind, body) = read_frame(conn, None)?;
    if kind == ERR {
        return Err(bad(String::from_utf8_lossy(&body).into_owned()));
    }
    if kind != CHALLENGE || body.len() != NONCE_LEN + MAC_LEN {
        return Err(bad("expected a challenge"));
    }
    let (ns, mac) = body.split_at(NONCE_LEN);
    match &auth.key {
        Some(k) => {
            let want = handshake(k, &chal_label(label), &[&nc, ns, &auth.config]);
            if !ct_eq(&want[..MAC_LEN], mac) {
                return Err(bad("the server does not know the key"));
            }
            Ok(Some(Session::new(
                handshake(k, &sess_label(label), &[&nc, ns, &auth.config]),
                0,
            )))
        }
        None => {
            // A server that holds a key sends a real MAC; an unkeyed client cannot
            // check it, and its first request will be refused.
            Ok(None)
        }
    }
}

/// A connected, set-up stream: calls seal and check frames.
pub(crate) struct Link {
    pub(crate) s: TcpStream,
    sess: Option<Session>,
}

impl Link {
    pub(crate) fn open(
        mut s: TcpStream,
        auth: &Auth,
        label: &[u8],
        read_timeout: Duration,
    ) -> io::Result<Link> {
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(read_timeout))?;
        let sess = connect(&mut s, auth, label)?;
        Ok(Link { s, sess })
    }
    pub(crate) fn call(&mut self, kind: u8, body: &[u8]) -> io::Result<(u8, Vec<u8>)> {
        write_frame(&mut self.s, kind, body, self.sess.as_mut())?;
        read_frame(&mut self.s, self.sess.as_mut())
    }
}

/// A server-side connection after the handshake.
pub(crate) struct Peer {
    pub(crate) s: TcpStream,
    pub(crate) sess: Option<Session>,
}

impl Peer {
    pub(crate) fn accept(mut s: TcpStream, auth: &Auth, label: &[u8]) -> io::Result<Peer> {
        let sess = accept(&mut s, auth, label)?;
        Ok(Peer { s, sess })
    }
    pub(crate) fn recv(&mut self) -> io::Result<(u8, Vec<u8>)> {
        read_frame(&mut self.s, self.sess.as_mut())
    }
    pub(crate) fn send(&mut self, kind: u8, body: &[u8]) -> io::Result<()> {
        write_frame(&mut self.s, kind, body, self.sess.as_mut())
    }
}
