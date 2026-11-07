//! A client of a matching group's replicas for the relays: `forward` and
//! `cursor` go to the leader (found by trying each address and following
//! refusals), `read` to any replica.

use crate::server::{ExchangeClient, ForwardAck, Refusal};
use orderer_ledger::{Event, OutMsg};
use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

pub struct TierClient {
    addrs: Vec<SocketAddr>,
    conns: Vec<Option<ExchangeClient>>,
    current: usize,
    pub budget: Duration,
}

impl TierClient {
    /// `addrs[i]` is the exchange address of matching node `i`.
    pub fn new(addrs: Vec<SocketAddr>) -> TierClient {
        let n = addrs.len();
        TierClient {
            addrs,
            conns: (0..n).map(|_| None).collect(),
            current: 0,
            budget: Duration::from_secs(60),
        }
    }

    fn conn(&mut self, i: usize) -> Option<&mut ExchangeClient> {
        if self.conns[i].is_none() {
            self.conns[i] = ExchangeClient::connect(self.addrs[i]).ok();
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

    pub fn cursor(&mut self, shard: u16) -> io::Result<Result<u64, Refusal>> {
        self.with(|c| c.cursor(shard))
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
