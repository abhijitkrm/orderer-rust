//! A client of a matching group's replicas for the relays: `forward` and
//! `cursor` go to the leader (found by trying each address and following
//! refusals), `read` to any replica.

use crate::secure::Auth;
use crate::server::{ExchangeClient, ForwardAck, Refusal, SliceAnswer, SliceState};
use crate::wire::{SliceBody, SliceReq};
use orderer_ledger::{Event, OutMsg, Reject, Side};
use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

pub struct TierClient {
    addrs: Vec<SocketAddr>,
    auth: Auth,
    conns: Vec<Option<ExchangeClient>>,
    current: usize,
    pub budget: Duration,
}

impl TierClient {
    /// `addrs[i]` is the exchange address of matching node `i`.
    pub fn new(addrs: Vec<SocketAddr>) -> TierClient {
        Self::with_auth(addrs, Auth::none())
    }

    pub fn with_auth(addrs: Vec<SocketAddr>, auth: Auth) -> TierClient {
        let n = addrs.len();
        TierClient {
            addrs,
            auth,
            conns: (0..n).map(|_| None).collect(),
            current: 0,
            budget: Duration::from_secs(60),
        }
    }

    fn conn(&mut self, i: usize) -> Option<&mut ExchangeClient> {
        if self.conns[i].is_none() {
            self.conns[i] = ExchangeClient::connect_with(self.addrs[i], &self.auth).ok();
        }
        self.conns[i].as_mut()
    }

    fn with<T>(
        &mut self,
        mut f: impl FnMut(&mut ExchangeClient) -> io::Result<Result<T, Refusal>>,
    ) -> io::Result<Result<T, Refusal>> {
        let end = Instant::now() + self.budget;
        loop {
            if Instant::now() > end {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "no definite answer in budget",
                ));
            }
            let i = self.current % self.addrs.len();
            let Some(c) = self.conn(i) else {
                self.current += 1;
                thread::sleep(Duration::from_millis(20));
                continue;
            };
            match f(c) {
                Ok(Ok(v)) => return Ok(Ok(v)),
                Ok(Err(Refusal::NotLeader(hint))) => {
                    match hint {
                        Some(h) if (h as usize) < self.addrs.len() => self.current = h as usize,
                        _ => self.current += 1,
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(Err(Refusal::Busy)) => thread::sleep(Duration::from_millis(3)),
                // Forwarding twice is safe (the intake dedups), so unknown is retried.
                Ok(Err(Refusal::Unknown)) => {
                    self.conns[i] = None;
                    thread::sleep(Duration::from_millis(20));
                }
                Ok(Err(other)) => return Ok(Err(other)),
                Err(_) => {
                    self.conns[i] = None;
                    self.current += 1;
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }

    pub fn forward(
        &mut self,
        shard: u16,
        first: u64,
        msgs: &[OutMsg],
    ) -> io::Result<Result<ForwardAck, Refusal>> {
        self.with(|c| c.forward(shard, first, msgs))
    }

    /// The thinned form of [`forward`](Self::forward): `msgs` are the messages for
    /// this group among outbox indices `first..end`.
    pub fn forward_span(
        &mut self,
        shard: u16,
        first: u64,
        end: u64,
        msgs: &[(u64, OutMsg)],
    ) -> io::Result<Result<ForwardAck, Refusal>> {
        self.with(|c| c.forward_span(shard, first, end, msgs))
    }

    /// One slice request, repeated (same `client`, `seq`) through leader changes
    /// until the group gives a definite answer.
    pub fn slice_request(&mut self, q: &SliceReq) -> io::Result<Result<SliceAnswer, Refusal>> {
        self.with(|c| c.slice_request(q))
    }

    /// The credit slice from the leader (linearizable).
    pub fn slice_state(&mut self) -> io::Result<Result<SliceState, Refusal>> {
        self.with(|c| c.slice_state())
    }

    /// Ask the leader to close the credit epoch.
    pub fn tick(&mut self) -> io::Result<Result<(), Refusal>> {
        self.with(|c| c.tick())
    }

    pub fn cursor(&mut self, shard: u16) -> io::Result<Result<u64, Refusal>> {
        self.with(|c| c.cursor(shard))
    }

    /// The cursor from the applied state of the replica the client is talking to,
    /// with no round trip through the log: for observers that poll fast.
    pub fn cursor_local(&mut self, shard: u16) -> io::Result<Result<u64, Refusal>> {
        self.with(|c| c.cursor_local(shard))
    }

    /// Events of stream `source` from `from`, from whichever replica answers:
    /// `(events, end)`. A replica behind the caller's position just returns
    /// fewer events; the next call moves on.
    #[allow(clippy::type_complexity)]
    pub fn read(
        &mut self,
        source: u32,
        from: u64,
        max: u32,
    ) -> io::Result<Result<(Vec<(u64, Event)>, u64), Refusal>> {
        let end = Instant::now() + self.budget;
        loop {
            if Instant::now() > end {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "no replica answered",
                ));
            }
            let i = self.current % self.addrs.len();
            match self.conn(i).map(|c| c.read(source, from, max)) {
                Some(Ok(r)) => return Ok(r),
                _ => {
                    self.conns[i] = None;
                    self.current += 1;
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }
}

/// A gateway's session with a credit-mode group (phase 2): orders entered one at a
/// time, each with a sequence number the group deduplicates.
///
/// **Exactly once.** The retry key is `(client id, seq)`. The group applies a request
/// only if its `seq` is above the last it has seen for that client id and keeps the
/// answer to that last request, so a request repeated through a leader change, a
/// dropped connection or a timeout takes effect once and gets the same answer. The
/// session is stop-and-wait: a new request goes out only after the previous one has a
/// definite answer. If the client cannot get one within its budget the request stays
/// pending ([`SliceClient::resolve_pending`] repeats it) and no new request is sent.
/// A restarted gateway must use a **new client id** (its sequence counter is gone;
/// reusing the id would have its early requests ignored as already seen); each id
/// costs the group one table entry, so ids belong to long-lived gateway processes.
pub struct SliceClient {
    tier: TierClient,
    client: u64,
    seq: u64,
    pending: Option<SliceReq>,
    /// Fault injection (0 = off): every `chaos`-th request is sent a second time after
    /// its answer, as a network that duplicates would deliver it; the group must answer
    /// identically and enter nothing twice, else the call fails.
    pub chaos: u32,
}

impl SliceClient {
    pub fn new(tier: TierClient, client: u64) -> SliceClient {
        SliceClient {
            tier,
            client,
            seq: 0,
            pending: None,
            chaos: 0,
        }
    }

    /// A client id unlikely to repeat: time, process id and a counter.
    pub fn fresh_id() -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        (t ^ ((std::process::id() as u64) << 40)).wrapping_add(N.fetch_add(1, Ordering::Relaxed))
            | 1 << 63
    }

    pub fn client_id(&self) -> u64 {
        self.client
    }

    pub fn tier(&mut self) -> &mut TierClient {
        &mut self.tier
    }

    fn run(&mut self, q: SliceReq) -> io::Result<Result<u64, Reject>> {
        self.pending = Some(q);
        let first = self.tier.slice_request(&q)?;
        if let (Ok(a), true) = (&first, self.chaos > 0 && q.seq % self.chaos as u64 == 0) {
            let again = self.tier.slice_request(&q)?;
            if again.as_ref().ok() != Some(a) {
                return Err(io::Error::other(format!(
                    "exactly-once violated: request {} of client {:#x} answered {a:?} then {again:?}",
                    q.seq, q.client
                )));
            }
        }
        match first {
            Ok(SliceAnswer::Entered(o)) => {
                self.pending = None;
                Ok(Ok(o))
            }
            Ok(SliceAnswer::Refused(r)) => {
                self.pending = None;
                Ok(Err(r))
            }
            Ok(SliceAnswer::Stale(last)) => Err(io::Error::other(format!(
                "slice request {} is stale: the group has seen {last} from client {:#x}",
                q.seq, q.client
            ))),
            Err(r) => Err(io::Error::other(format!("slice request refused: {r:?}"))),
        }
    }

    fn start(&mut self, body: SliceBody) -> io::Result<Result<u64, Reject>> {
        if self.pending.is_some() {
            return Err(io::Error::other(
                "the previous slice request has no answer yet: call resolve_pending",
            ));
        }
        self.seq += 1;
        self.run(SliceReq {
            client: self.client,
            seq: self.seq,
            body,
        })
    }

    /// Repeat the request that has no answer yet. `Ok(None)` when nothing is pending.
    pub fn resolve_pending(&mut self) -> io::Result<Option<(SliceBody, Result<u64, Reject>)>> {
        let Some(q) = self.pending else {
            return Ok(None);
        };
        Ok(Some((q.body, self.run(q)?)))
    }

    /// Enter a limit order (`ioc`: immediate-or-cancel). `Ok(Ok(order id))`, or the
    /// reason the group refused it (`Reject::Insufficient`: no credit).
    pub fn enter(
        &mut self,
        account: u64,
        symbol: u32,
        side: Side,
        price: u64,
        qty: u64,
        ioc: bool,
    ) -> io::Result<Result<u64, Reject>> {
        let (account, symbol, side, price, qty) = (account, symbol, side, price, qty);
        self.start(if ioc {
            SliceBody::EnterIoc {
                account,
                symbol,
                side,
                price,
                qty,
            }
        } else {
            SliceBody::Enter {
                account,
                symbol,
                side,
                price,
                qty,
            }
        })
    }

    pub fn cancel(&mut self, account: u64, order: u64) -> io::Result<Result<u64, Reject>> {
        self.start(SliceBody::Cancel { account, order })
    }
}
