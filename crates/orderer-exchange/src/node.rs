//! [`LedgerNodeSm`]: a [`Ledger`] as a replicated state machine that can also
//! tell the proposer what happened.
//!
//! Log entries:
//! - **session command** `0xA1 | client u64 | seq u64 | ledger command`. The
//!   machine keeps, per client, the last sequence number and its reply. A repeat
//!   of `(client, seq)` is not applied again and gets the stored reply, so a
//!   gateway client that retries after an unknown outcome gets the *same* order id
//!   and cannot double-reserve. The rule is a pure function of the log; the table
//!   is part of the snapshot and digest. Use one client id per sequential stream.
//!   The table holds at most [`MAX_SESSIONS`] clients; the oldest is evicted
//!   first (deterministically), so a client idle across that many newer ones must
//!   take a fresh id.
//! - **raw command**: a bare ledger command (first byte 1..), for the relays,
//!   whose commands are idempotent by construction: `Settle` by its chained mark,
//!   `OutboxAck` and `Clock` by monotonicity, `Sweep` by the `cancelling` flag.
//!   Session protection is for `Reserve`, `Cancel`, `Deposit`, `Withdraw`,
//!   `Finalize`, which are not.
//! - **batch** `0xA2 | n u32 | n x (len u32 | command)`: n raw commands applied in
//!   order in one entry (one log round trip instead of n), for the settler. The
//!   reply is `n u32 | n x (len u32 | reply)`. Chained settles in a batch depend on
//!   each other in order, which the order of application preserves.
//! - empty: a read barrier, ignored.
//!
//! The reply of every applied entry is kept in a bounded book keyed by log
//! index ([`BOOK`] entries, volatile: it is not in the snapshot) so the node
//! that proposed an entry can read its outcome after it is applied.

use orderer_cluster::sha256::sha256;
use orderer_cluster::StateMachine;
use orderer_consensus::{Entry, Index};
use orderer_ledger::{decode_cmd, Config, Ledger, Outcome, Symbol};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

pub const TAG_SESSION: u8 = 0xA1;
/// A batch of bare commands in one entry: `0xA2 | n u32 | n x (len u32 | command)`.
pub const TAG_BATCH: u8 = 0xA2;
/// At most this many commands in a batch entry.
pub const MAX_BATCH: usize = 4096;
pub const MAX_SESSIONS: usize = 1 << 16;
pub const BOOK: usize = 1 << 16;

const SNAP_MAGIC: &[u8; 4] = b"LNS1";

/// Reply kinds in [`reply_of`].
pub const RK_RESERVED: u8 = 1;
pub const RK_CANCEL_QUEUED: u8 = 2;
pub const RK_OTHER: u8 = 3;
pub const RK_REJECTED: u8 = 4;
pub const RK_ANOMALY: u8 = 5;
pub const RK_STALE: u8 = 6;
pub const RK_UNDECODABLE: u8 = 7;

/// The gateway reply for an outcome: `kind u8 | a u64 | b u64 | text`. Outcomes
/// that carry data have a kind of their own; the rest travel as their `Debug`
/// name, which keeps this stable while the ledger grows variants.
pub fn reply_of(o: &Outcome) -> Vec<u8> {
    let (kind, a, b, text) = match o {
        Outcome::Reserved { order, outbox } => (RK_RESERVED, *order, *outbox, String::new()),
        Outcome::CancelQueued { outbox } => (RK_CANCEL_QUEUED, 0, *outbox, String::new()),
        Outcome::Rejected(r) => (RK_REJECTED, 0, 0, format!("{r:?}")),
        Outcome::Anomaly => (RK_ANOMALY, 0, 0, String::new()),
        other => (RK_OTHER, 0, 0, format!("{other:?}")),
    };
    let mut v = vec![kind];
    v.extend_from_slice(&a.to_le_bytes());
    v.extend_from_slice(&b.to_le_bytes());
    v.extend_from_slice(text.as_bytes());
    v
}

fn plain_reply(kind: u8) -> Vec<u8> {
    let mut v = vec![kind];
    v.extend_from_slice(&[0u8; 16]);
    v
}

/// A decoded gateway reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Reserved {
        order: u64,
        outbox: u64,
    },
    CancelQueued {
        outbox: u64,
    },
    /// Any other accepted outcome, by name (`Deposited`, `Settled`, ...).
    Other(String),
    Rejected(String),
    Anomaly,
    /// A retry older than the client's latest sequence number.
    Stale,
    Undecodable,
}

impl Reply {
    /// The replies of a batch entry (`n u32 | n x (len u32 | reply)`).
    pub fn decode_batch(b: &[u8]) -> Option<Vec<Reply>> {
        let n = u32::from_le_bytes(b.get(..4)?.try_into().ok()?) as usize;
        let mut at = 4;
        let mut out = Vec::with_capacity(n.min(MAX_BATCH));
        for _ in 0..n {
            let len = u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?) as usize;
            out.push(Reply::decode(b.get(at + 4..at + 4 + len)?)?);
            at += 4 + len;
        }
        (at == b.len()).then_some(out)
    }

    pub fn decode(b: &[u8]) -> Option<Reply> {
        if b.len() < 17 {
            return None;
        }
        let a = u64::from_le_bytes(b[1..9].try_into().ok()?);
        let c = u64::from_le_bytes(b[9..17].try_into().ok()?);
        let text = String::from_utf8_lossy(&b[17..]).into_owned();
        Some(match b[0] {
            RK_RESERVED => Reply::Reserved {
                order: a,
                outbox: c,
            },
            RK_CANCEL_QUEUED => Reply::CancelQueued { outbox: c },
            RK_OTHER => Reply::Other(text),
            RK_REJECTED => Reply::Rejected(text),
            RK_ANOMALY => Reply::Anomaly,
            RK_STALE => Reply::Stale,
            RK_UNDECODABLE => Reply::Undecodable,
            _ => return None,
        })
    }
}

/// Reference data every ledger node of a deployment shares (a configuration file
/// is a follow-up): two spot symbols over three assets, small fees.
pub fn demo_config(shard: u16) -> Config {
    let mut symbols = BTreeMap::new();
    symbols.insert(
        10,
        Symbol {
            base: 1,
            quote: 2,
            lot: 1_000,
        },
    );
    symbols.insert(
        11,
        Symbol {
            base: 3,
            quote: 2,
            lot: 10,
        },
    );
    Config {
        shard,
        fee_ppm: 500,
        symbols,
        fees: Default::default(),
    }
}

struct Inner {
    ledger: Ledger,
    sessions: BTreeMap<u64, (u64, Vec<u8>)>,
    order: VecDeque<u64>,
    book: VecDeque<(Index, Vec<u8>)>,
}

/// Read access to a node's applied state, for the gateway.
#[derive(Clone)]
pub struct LedgerView {
    inner: Arc<Mutex<Inner>>,
}

impl LedgerView {
    pub fn with<R>(&self, f: impl FnOnce(&Ledger) -> R) -> R {
        f(&self.inner.lock().expect("ledger lock").ledger)
    }

    /// The reply recorded for log index `index`, if still in the book.
    pub fn reply(&self, index: Index) -> Option<Vec<u8>> {
        let g = self.inner.lock().expect("ledger lock");
        let pos = g.book.binary_search_by_key(&index, |(i, _)| *i).ok()?;
        Some(g.book[pos].1.clone())
    }
}

pub struct LedgerNodeSm {
    inner: Arc<Mutex<Inner>>,
}

impl LedgerNodeSm {
    pub fn new(cfg: Config) -> (LedgerNodeSm, LedgerView) {
        let inner = Arc::new(Mutex::new(Inner {
            ledger: Ledger::new(cfg),
            sessions: BTreeMap::new(),
            order: VecDeque::new(),
            book: VecDeque::new(),
        }));
        (
            LedgerNodeSm {
                inner: inner.clone(),
            },
            LedgerView { inner },
        )
    }
}

impl Inner {
    fn record(&mut self, index: Index, reply: Vec<u8>) {
        self.book.push_back((index, reply));
        while self.book.len() > BOOK {
            self.book.pop_front();
        }
    }

    /// Apply a batch body; the reply is the batch reply, or a single undecodable
    /// reply if the framing is broken (nothing is applied then).
    fn apply_batch(&mut self, body: &[u8]) -> Vec<u8> {
        let mut cmds = Vec::new();
        let framed = (|| {
            let n = u32::from_le_bytes(body.get(..4)?.try_into().ok()?) as usize;
            if n > MAX_BATCH {
                return None;
            }
            let mut at = 4;
            for _ in 0..n {
                let len = u32::from_le_bytes(body.get(at..at + 4)?.try_into().ok()?) as usize;
                cmds.push(body.get(at + 4..at + 4 + len)?);
                at += 4 + len;
            }
            (at == body.len()).then_some(())
        })();
        if framed.is_none() {
            self.ledger.note_undecodable();
            return plain_reply(RK_UNDECODABLE);
        }
        let mut out = (cmds.len() as u32).to_le_bytes().to_vec();
        for c in cmds {
            let r = match decode_cmd(c) {
                Ok(c) => reply_of(&self.ledger.apply(&c)),
                Err(_) => {
                    self.ledger.note_undecodable();
                    plain_reply(RK_UNDECODABLE)
                }
            };
            out.extend_from_slice(&(r.len() as u32).to_le_bytes());
            out.extend_from_slice(&r);
        }
        out
    }

    fn apply_entry(&mut self, index: Index, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        if data[0] == TAG_BATCH {
            let reply = self.apply_batch(&data[1..]);
            self.record(index, reply);
            return;
        }
        if data[0] != TAG_SESSION {
            let reply = match decode_cmd(data) {
                Ok(c) => reply_of(&self.ledger.apply(&c)),
                Err(_) => {
                    self.ledger.note_undecodable();
                    plain_reply(RK_UNDECODABLE)
                }
            };
            self.record(index, reply);
            return;
        }
        if data.len() < 17 {
            self.ledger.note_undecodable();
            self.record(index, plain_reply(RK_UNDECODABLE));
            return;
        }
        let client = u64::from_le_bytes(data[1..9].try_into().unwrap());
        let seq = u64::from_le_bytes(data[9..17].try_into().unwrap());
        if let Some((last, reply)) = self.sessions.get(&client) {
            if seq <= *last {
                let r = if seq == *last {
                    reply.clone()
                } else {
                    plain_reply(RK_STALE)
                };
                self.record(index, r);
                return;
            }
        }
        let reply = match decode_cmd(&data[17..]) {
            Ok(c) => reply_of(&self.ledger.apply(&c)),
            Err(_) => {
                self.ledger.note_undecodable();
                plain_reply(RK_UNDECODABLE)
            }
        };
        if self.sessions.insert(client, (seq, reply.clone())).is_none() {
            self.order.push_back(client);
            while self.order.len() > MAX_SESSIONS {
                if let Some(old) = self.order.pop_front() {
                    self.sessions.remove(&old);
                }
            }
        }
        self.record(index, reply);
    }

    fn snapshot(&self) -> Vec<u8> {
        let mut out = SNAP_MAGIC.to_vec();
        out.extend_from_slice(&(self.order.len() as u32).to_le_bytes());
        for client in &self.order {
            let (seq, reply) = &self.sessions[client];
            out.extend_from_slice(&client.to_le_bytes());
            out.extend_from_slice(&seq.to_le_bytes());
            out.extend_from_slice(&(reply.len() as u32).to_le_bytes());
            out.extend_from_slice(reply);
        }
        out.extend_from_slice(&self.ledger.snapshot());
        out
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), String> {
        let bad = || "short ledger-node snapshot".to_string();
        if data.get(..4) != Some(SNAP_MAGIC) {
            return Err("bad ledger-node snapshot magic".into());
        }
        let n = u32::from_le_bytes(data.get(4..8).ok_or_else(bad)?.try_into().unwrap()) as usize;
        let mut at = 8;
        let (mut sessions, mut order) = (BTreeMap::new(), VecDeque::new());
        for _ in 0..n {
            let h = data.get(at..at + 20).ok_or_else(bad)?;
            let client = u64::from_le_bytes(h[..8].try_into().unwrap());
            let seq = u64::from_le_bytes(h[8..16].try_into().unwrap());
            let len = u32::from_le_bytes(h[16..20].try_into().unwrap()) as usize;
            let reply = data.get(at + 20..at + 20 + len).ok_or_else(bad)?.to_vec();
            at += 20 + len;
            if sessions.insert(client, (seq, reply)).is_some() {
                return Err("duplicate session".into());
            }
            order.push_back(client);
        }
        self.ledger.restore(&data[at..])?;
        self.sessions = sessions;
        self.order = order;
        self.book.clear();
        Ok(())
    }
}

impl StateMachine for LedgerNodeSm {
    fn apply(&mut self, index: Index, entry: &Entry) {
        self.inner
            .lock()
            .expect("ledger lock")
            .apply_entry(index, &entry.data);
    }
    fn snapshot(&mut self, _index: Index) -> Vec<u8> {
        self.inner.lock().expect("ledger lock").snapshot()
    }
    fn restore(&mut self, _index: Index, data: &[u8]) -> Result<(), String> {
        self.inner.lock().expect("ledger lock").restore(data)
    }
    /// Cheap: the ledger's incremental digest (cost: the entries changed since the
    /// last one) and the session table (bounded by [`MAX_SESSIONS`]).
    fn digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        let g = self.inner.lock().expect("ledger lock");
        let mut b = b"orderer-ledger-node-digest/1".to_vec();
        b.extend_from_slice(&(g.order.len() as u32).to_le_bytes());
        for client in &g.order {
            let (seq, reply) = &g.sessions[client];
            b.extend_from_slice(&client.to_le_bytes());
            b.extend_from_slice(&seq.to_le_bytes());
            b.extend_from_slice(&(reply.len() as u32).to_le_bytes());
            b.extend_from_slice(reply);
        }
        b.extend_from_slice(&g.ledger.digest());
        Some(sha256(&b))
    }
    /// O(state): the SHA-256 of the whole snapshot, for on-demand checks. The
    /// audit wrapper in `ledger-node` must forward this as well as `digest`.
    fn deep_digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        Some(sha256(&self.inner.lock().expect("ledger lock").snapshot()))
    }
}
