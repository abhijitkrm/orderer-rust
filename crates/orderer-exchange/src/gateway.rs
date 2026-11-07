//! The ledger node's gateway: a small framed TCP service next to each ledger
//! replica. Commands go through the replicated log and are answered with the
//! outcome once applied; reads come from the applied state.
//!
//! Frame: see `frame.rs`. No authentication: run it on a private network.
//!
//! Requests (integers little-endian):
//! - `CMD (1)`: `client u64 | seq u64 | ledger command` -> reply `OUTCOME`. Leader
//!   only. Exactly once by session: retrying the same `(client, seq)` is always
//!   safe and returns the original reply (see `node.rs`).
//! - `RAW (2)`: a bare ledger command, for the relays' idempotent commands
//!   (`Settle`, `OutboxAck`, `Clock`, `Sweep`). Leader only.
//! - `STATE (3)`: `lin u8 | n u32 | n x source u32` -> `STATE`:
//!   `applied | clock | outbox_base | outbox_next | open_orders | anomalies` (u64
//!   each) then `n x mark u64` (0 = none).
//! - `OUTBOX (4)`: `lin u8 | from u64 | max u32` -> `OUTBOX`: `base u64 | next u64 |`
//!   the messages `max(from, base)..` as a forward entry (`wire.rs`).
//! - `SNAPSHOT (5)`: `` -> `BYTES`, the ledger's snapshot (linearizable, leader only).
//! - `BALANCE (6)`: `lin u8 | account u64 | asset u32` -> `BALANCE`: `available u128
//!   | held u128`.
//! - `ORDER (7)`: `lin u8 | order u64` -> `ORDER`: `found u8 | account u64 | symbol u32
//!   | side u8 | price u64 | remaining u64 | held u128 | cancelling u8`.
//!
//! `lin = 1` makes a read linearizable (a barrier through the log, leader only);
//! `lin = 0` reads the applied state of whichever replica answers. A stale read is
//! safe for the relays (a stale outbox base or mark only makes them resend what is
//! already accepted) and wrong only for the audit, which asks for `lin = 1`.
//!
//! Replies: `OUTCOME (1)`: see `node::reply_of`; `STATE (2)`; `OUTBOX (3)`; `BYTES (4)`;
//! `BALANCE (5)`; `ORDER (6)`; `NOT_LEADER (7)`: `hint i64`; `BUSY (8)`;
//! `UNKNOWN (9)` (the proposal may have committed: retry); `ERR (10)`: text.

use crate::frame::{read_frame, write_frame};
use crate::node::{LedgerView, Reply, TAG_SESSION};
use crate::wire::{decode_forward, encode_forward, Forward, Rd};
use orderer_cluster::{ProposeError, ReplicaHandle};
use orderer_ledger::{decode_cmd, encode_cmd, Cmd, OutMsg, Side};
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const Q_CMD: u8 = 1;
const Q_RAW: u8 = 2;
const Q_STATE: u8 = 3;
const Q_OUTBOX: u8 = 4;
const Q_SNAPSHOT: u8 = 5;
const Q_BALANCE: u8 = 6;
const Q_ORDER: u8 = 7;
const R_OUTCOME: u8 = 1;
const R_STATE: u8 = 2;
const R_OUTBOX: u8 = 3;
const R_BYTES: u8 = 4;
const R_BALANCE: u8 = 5;
const R_ORDER: u8 = 6;
const R_NOT_LEADER: u8 = 7;
const R_BUSY: u8 = 8;
const R_UNKNOWN: u8 = 9;
const R_ERR: u8 = 10;

pub struct GatewayServer {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    pub addr: SocketAddr,
}

impl GatewayServer {
    pub fn start(
        addr: SocketAddr,
        replica: ReplicaHandle,
        view: LedgerView,
    ) -> io::Result<GatewayServer> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let s2 = stop.clone();
        let thread = thread::spawn(move || {
            while !s2.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((conn, _)) => {
                        let (h, v, stop) = (replica.clone(), view.clone(), s2.clone());
                        thread::spawn(move || {
                            let _ = serve(conn, h, v, stop);
                        });
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(_) => thread::sleep(Duration::from_millis(20)),
                }
            }
        });
        Ok(GatewayServer {
            stop,
            thread: Some(thread),
            addr,
        })
    }

    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn serve(
    mut conn: TcpStream,
    h: ReplicaHandle,
    v: LedgerView,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    conn.set_nonblocking(false)?;
    conn.set_nodelay(true)?;
    conn.set_read_timeout(Some(Duration::from_millis(200)))?;
    while !stop.load(Ordering::Relaxed) {
        let (kind, body) = match read_frame(&mut conn) {
            Ok(x) => x,
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue
            }
            Err(e) => return Err(e),
        };
        let (rk, rb) = handle(kind, &body, &h, &v);
        write_frame(&mut conn, rk, &rb)?;
    }
    Ok(())
}

fn refusal(e: ProposeError) -> (u8, Vec<u8>) {
    match e {
        ProposeError::NotLeader(hint) => (
            R_NOT_LEADER,
            hint.map(|n| n as i64).unwrap_or(-1).to_le_bytes().to_vec(),
        ),
        ProposeError::Busy | ProposeError::Stopped => (R_BUSY, vec![]),
        ProposeError::Timeout | ProposeError::Unknown | ProposeError::Superseded => {
            (R_UNKNOWN, vec![])
        }
        #[allow(unreachable_patterns)]
        _ => (R_ERR, b"propose failed".to_vec()),
    }
}

fn wait_applied(h: &ReplicaHandle, index: u64) -> bool {
    let end = Instant::now() + Duration::from_secs(30);
    while h.status().applied < index {
        if Instant::now() > end {
            return false;
        }
        thread::sleep(Duration::from_micros(200));
    }
    true
}

fn propose_for_reply(h: &ReplicaHandle, v: &LedgerView, payload: Vec<u8>) -> (u8, Vec<u8>) {
    match h.propose(payload) {
        Ok(index) => {
            if !wait_applied(h, index) {
                return (R_UNKNOWN, vec![]);
            }
            match v.reply(index) {
                Some(r) => (R_OUTCOME, r),
                None => (R_UNKNOWN, vec![]),
            }
        }
        Err(e) => refusal(e),
    }
}

/// `Ok(())` once the read may be served: linearizable reads commit a barrier first.
fn gate(lin: u8, h: &ReplicaHandle) -> Result<(), (u8, Vec<u8>)> {
    if lin == 1 {
        h.barrier().map(|_| ()).map_err(refusal)
    } else {
        Ok(())
    }
}

fn handle(kind: u8, body: &[u8], h: &ReplicaHandle, v: &LedgerView) -> (u8, Vec<u8>) {
    let err = |m: &str| (R_ERR, m.as_bytes().to_vec());
    let mut r = Rd::new(body);
    match kind {
        Q_CMD => {
            let (Ok(client), Ok(seq)) = (r.u64(), r.u64()) else {
                return err("short command");
            };
            let cmd = &body[16..];
            if decode_cmd(cmd).is_err() {
                return err("bad ledger command");
            }
            let mut p = Vec::with_capacity(17 + cmd.len());
            p.push(TAG_SESSION);
            p.extend_from_slice(&client.to_le_bytes());
            p.extend_from_slice(&seq.to_le_bytes());
            p.extend_from_slice(cmd);
            propose_for_reply(h, v, p)
        }
        Q_RAW => {
            if decode_cmd(body).is_err() {
                return err("bad ledger command");
            }
            propose_for_reply(h, v, body.to_vec())
        }
        Q_STATE => {
            let (Ok(lin), Ok(n)) = (r.u8(), r.u32()) else {
                return err("short state request");
            };
            if n as usize * 4 != r.left() {
                return err("state request length");
            }
            let sources: Vec<u32> = (0..n).map(|_| r.u32().unwrap_or(0)).collect();
            if let Err(e) = gate(lin, h) {
                return e;
            }
            let applied = h.status().applied;
            let mut b = Vec::new();
            v.with(|l| {
                let (base, _) = l.outbox();
                for x in [
                    applied,
                    l.clock(),
                    base,
                    l.outbox_next(),
                    l.open_orders() as u64,
                    l.anomalies(),
                ] {
                    b.extend_from_slice(&x.to_le_bytes());
                }
                for s in &sources {
                    b.extend_from_slice(&l.high_water(*s).unwrap_or(0).to_le_bytes());
                }
            });
            (R_STATE, b)
        }
        Q_OUTBOX => {
            let (Ok(lin), Ok(from), Ok(max)) = (r.u8(), r.u64(), r.u32()) else {
                return err("short outbox request");
            };
            if let Err(e) = gate(lin, h) {
                return e;
            }
            let (base, next, msgs) = v.with(|l| {
                let (base, it) = l.outbox();
                let start = from.max(base);
                let msgs: Vec<OutMsg> = it
                    .skip((start - base) as usize)
                    .take((max as usize).min(4096))
                    .copied()
                    .collect();
                (base, l.outbox_next(), msgs)
            });
            let first = from.max(base);
            let mut b = Vec::new();
            b.extend_from_slice(&base.to_le_bytes());
            b.extend_from_slice(&next.to_le_bytes());
            b.extend_from_slice(&encode_forward(&Forward {
                shard: 0,
                first_idx: first,
                msgs,
            }));
            (R_OUTBOX, b)
        }
        Q_SNAPSHOT => match h.barrier() {
            Ok(_) => (R_BYTES, v.with(|l| l.snapshot())),
            Err(e) => refusal(e),
        },
        Q_BALANCE => {
            let (Ok(lin), Ok(account)) = (r.u8(), r.u64()) else {
                return err("short balance request");
            };
            let Ok(asset) = r.u32() else {
                return err("short balance request");
            };
            if let Err(e) = gate(lin, h) {
                return e;
            }
            let bal = v.with(|l| l.balance(account, asset));
            let mut b = bal.available.to_le_bytes().to_vec();
            b.extend_from_slice(&bal.held.to_le_bytes());
            (R_BALANCE, b)
        }
        Q_ORDER => {
            let (Ok(lin), Ok(id)) = (r.u8(), r.u64()) else {
                return err("short order request");
            };
            if let Err(e) = gate(lin, h) {
                return e;
            }
            let mut b = Vec::new();
            v.with(|l| match l.order(id) {
                None => b.push(0),
                Some(o) => {
                    b.push(1);
                    b.extend_from_slice(&o.account.to_le_bytes());
                    b.extend_from_slice(&o.symbol.to_le_bytes());
                    b.push(if o.side == Side::Buy { 0 } else { 1 });
                    b.extend_from_slice(&o.price.to_le_bytes());
                    b.extend_from_slice(&o.remaining.to_le_bytes());
                    b.extend_from_slice(&o.held.to_le_bytes());
                    b.push(o.cancelling as u8);
                }
            });
            (R_ORDER, b)
        }
        _ => err("unknown request"),
    }
}

/// Decoded `STATE` reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LedgerState {
    pub applied: u64,
    pub clock: u64,
    pub outbox_base: u64,
    pub outbox_next: u64,
    pub open_orders: u64,
    pub anomalies: u64,
    /// The marks asked for, in order (0 = none yet).
    pub marks: Vec<u64>,
}

/// An open order as the ledger reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderInfo {
    pub account: u64,
    pub symbol: u32,
    pub side: Side,
    pub price: u64,
    pub remaining: u64,
    pub held: u128,
    pub cancelling: bool,
}

/// A client of one ledger shard's replicas. `addrs[i]` is node id `i`'s gateway
/// (a leader hint names a node id). It follows the leader, survives restarts and
/// retries until it has a definite answer or `budget` is spent.
pub struct GatewayClient {
    addrs: Vec<SocketAddr>,
    conns: Vec<Option<TcpStream>>,
    current: usize,
    pub budget: Duration,
}

fn bad(m: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, m)
}

impl GatewayClient {
    pub fn new(addrs: Vec<SocketAddr>) -> GatewayClient {
        let n = addrs.len();
        GatewayClient {
            addrs,
            conns: (0..n).map(|_| None).collect(),
            current: 0,
            budget: Duration::from_secs(60),
        }
    }

    fn call(
        &mut self,
        kind: u8,
        body: &[u8],
        mut want: impl FnMut(u8) -> bool,
    ) -> io::Result<(u8, Vec<u8>)> {
        let end = Instant::now() + self.budget;
        loop {
            if Instant::now() > end {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "no definite answer in budget",
                ));
            }
            let i = self.current % self.addrs.len();
            if self.conns[i].is_none() {
                self.conns[i] = TcpStream::connect_timeout(&self.addrs[i], Duration::from_secs(2))
                    .and_then(|c| {
                        c.set_nodelay(true)?;
                        c.set_read_timeout(Some(Duration::from_secs(40)))?;
                        Ok(c)
                    })
                    .ok();
            }
            let Some(conn) = self.conns[i].as_mut() else {
                self.current += 1;
                thread::sleep(Duration::from_millis(20));
                continue;
            };
            let res = write_frame(conn, kind, body).and_then(|_| read_frame(conn));
            match res {
                Ok((k, b)) if want(k) => return Ok((k, b)),
                Ok((R_NOT_LEADER, b)) => {
                    let hint = i64::from_le_bytes(b.try_into().unwrap_or([0xff; 8]));
                    if hint >= 0 && (hint as usize) < self.addrs.len() {
                        self.current = hint as usize;
                    } else {
                        self.current += 1;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Ok((R_BUSY, _)) => thread::sleep(Duration::from_millis(3)),
                // Unknown outcome: every caller retries something safe to repeat.
                Ok((R_UNKNOWN, _)) => {
                    self.conns[i] = None;
                    thread::sleep(Duration::from_millis(20));
                }
                Ok((R_ERR, b)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        String::from_utf8_lossy(&b).into_owned(),
                    ))
                }
                Ok(_) => return Err(bad("unexpected reply")),
                Err(_) => {
                    self.conns[i] = None;
                    self.current += 1;
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }

    fn outcome(&mut self, kind: u8, body: &[u8]) -> io::Result<Reply> {
        let (_, b) = self.call(kind, body, |k| k == R_OUTCOME)?;
        Reply::decode(&b).ok_or_else(|| bad("outcome reply"))
    }

    /// A session command with exactly-once effect. Use one `client` id per
    /// sequential stream and a `seq` that only grows; a repeat of a `(client, seq)`
    /// returns the original reply.
    pub fn cmd(&mut self, client: u64, seq: u64, cmd: &Cmd) -> io::Result<Reply> {
        let mut body = client.to_le_bytes().to_vec();
        body.extend_from_slice(&seq.to_le_bytes());
        body.extend_from_slice(&encode_cmd(cmd));
        self.outcome(Q_CMD, &body)
    }

    /// An idempotent relay command (`Settle`, `OutboxAck`, `Clock`, `Sweep`).
    pub fn raw(&mut self, cmd: &Cmd) -> io::Result<Reply> {
        self.outcome(Q_RAW, &encode_cmd(cmd))
    }

    pub fn state(&mut self, lin: bool, sources: &[u32]) -> io::Result<LedgerState> {
        let mut body = vec![lin as u8];
        body.extend_from_slice(&(sources.len() as u32).to_le_bytes());
        for s in sources {
            body.extend_from_slice(&s.to_le_bytes());
        }
        let (_, b) = self.call(Q_STATE, &body, |k| k == R_STATE)?;
        let mut r = Rd::new(&b);
        let mut f = || r.u64().map_err(|_| bad("state reply"));
        let st = LedgerState {
            applied: f()?,
            clock: f()?,
            outbox_base: f()?,
            outbox_next: f()?,
            open_orders: f()?,
            anomalies: f()?,
            marks: sources.iter().map(|_| f()).collect::<io::Result<_>>()?,
        };
        Ok(st)
    }

    /// `(base, next, first, messages)`: the outbox holds indices `base..next`;
    /// `messages` start at index `first = max(from, base)`.
    pub fn outbox(
        &mut self,
        lin: bool,
        from: u64,
        max: u32,
    ) -> io::Result<(u64, u64, u64, Vec<OutMsg>)> {
        let mut body = vec![lin as u8];
        body.extend_from_slice(&from.to_le_bytes());
        body.extend_from_slice(&max.to_le_bytes());
        let (_, b) = self.call(Q_OUTBOX, &body, |k| k == R_OUTBOX)?;
        if b.len() < 16 {
            return Err(bad("outbox reply"));
        }
        let base = u64::from_le_bytes(b[..8].try_into().unwrap());
        let next = u64::from_le_bytes(b[8..16].try_into().unwrap());
        let f = decode_forward(&b[16..]).map_err(|_| bad("outbox messages"))?;
        Ok((base, next, f.first_idx, f.msgs))
    }

    /// The ledger snapshot, linearizable.
    pub fn snapshot(&mut self) -> io::Result<Vec<u8>> {
        Ok(self.call(Q_SNAPSHOT, &[], |k| k == R_BYTES)?.1)
    }

    /// `(available, held)`.
    pub fn balance(&mut self, lin: bool, account: u64, asset: u32) -> io::Result<(u128, u128)> {
        let mut body = vec![lin as u8];
        body.extend_from_slice(&account.to_le_bytes());
        body.extend_from_slice(&asset.to_le_bytes());
        let (_, b) = self.call(Q_BALANCE, &body, |k| k == R_BALANCE)?;
        if b.len() != 32 {
            return Err(bad("balance reply"));
        }
        Ok((
            u128::from_le_bytes(b[..16].try_into().unwrap()),
            u128::from_le_bytes(b[16..].try_into().unwrap()),
        ))
    }

    pub fn order(&mut self, lin: bool, id: u64) -> io::Result<Option<OrderInfo>> {
        let mut body = vec![lin as u8];
        body.extend_from_slice(&id.to_le_bytes());
        let (_, b) = self.call(Q_ORDER, &body, |k| k == R_ORDER)?;
        let mut r = Rd::new(&b);
        let e = |_| bad("order reply");
        if r.u8().map_err(e)? == 0 {
            return Ok(None);
        }
        let account = r.u64().map_err(e)?;
        let symbol = r.u32().map_err(e)?;
        let side = if r.u8().map_err(e)? == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let (price, remaining) = (r.u64().map_err(e)?, r.u64().map_err(e)?);
        let held = u128::from_le_bytes(r.take(16).map_err(e)?.try_into().unwrap());
        let cancelling = r.u8().map_err(e)? == 1;
        Ok(Some(OrderInfo {
            account,
            symbol,
            side,
            price,
            remaining,
            held,
            cancelling,
        }))
    }
}
