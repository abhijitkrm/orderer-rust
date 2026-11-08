//! The two relays of `docs/design/LEDGER-RISK.md` as pure logic, so a
//! simulator can drive them through lost, duplicated, reordered and zombie
//! messages and crashes. Neither relay's memory is trusted: both rebuild from
//! the replicated state on either side.
//!
//! - **Outbox, ledger -> matching.** Outbox indices are contiguous, so the
//!   matching tier's intake accepts a message only if its index is exactly the
//!   next one for that ledger shard ([`Intake`]). That makes delivery in order and
//!   exactly once whatever the network does; the reply carries the cursor, from
//!   which a [`Forwarder`] with no memory recovers (go-back-N).
//! - **Settlement, matching -> ledger.** Event indices are contiguous per
//!   matching group but a shard sees only its own events, so each settle carries
//!   `prev` and the ledger applies it only on a match (see `Cmd::Settle`).
//!   [`settle_cmds`] derives the chain from the event stream and the shard's mark.

use crate::ledger::{order_shard, AccountId, Cmd, Event, Ledger, OutMsg};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Accept {
    /// The next message for this shard: apply it.
    Applied,
    /// Already accepted: ignore.
    Duplicate,
    /// A message was skipped: refuse; the sender resumes from the cursor.
    Gap,
}

/// Replicated state of a matching group: how far each ledger shard's outbox has
/// been accepted. Part of the group's snapshot and digest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Intake {
    cursor: BTreeMap<u16, u64>,
}

impl Intake {
    pub fn new() -> Intake {
        Intake::default()
    }
    /// The next outbox index expected from `shard`; everything below is accepted.
    pub fn cursor(&self, shard: u16) -> u64 {
        self.cursor.get(&shard).copied().unwrap_or(0)
    }
    pub fn accept(&mut self, shard: u16, idx: u64) -> Accept {
        let c = self.cursor.entry(shard).or_insert(0);
        if idx == *c {
            *c += 1;
            Accept::Applied
        } else if idx < *c {
            Accept::Duplicate
        } else {
            Accept::Gap
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Send {
    pub shard: u16,
    pub idx: u64,
    pub msg: OutMsg,
}

/// Delivers one ledger shard's outbox. Its fields are only an optimisation:
/// a fresh forwarder is always correct.
#[derive(Clone, Debug)]
pub struct Forwarder {
    shard: u16,
    sent: u64,
    acked: u64,
}

impl Forwarder {
    pub fn new(shard: u16) -> Forwarder {
        Forwarder {
            shard,
            sent: 0,
            acked: 0,
        }
    }

    /// Up to `max` outbox messages not yet sent, from the ledger's own outbox.
    pub fn poll(&mut self, ledger: &Ledger, max: usize) -> Vec<Send> {
        let (base, msgs) = ledger.outbox();
        let start = self.sent.max(base);
        let out: Vec<Send> = msgs
            .enumerate()
            .map(|(i, m)| Send {
                shard: self.shard,
                idx: base + i as u64,
                msg: *m,
            })
            .filter(|s| s.idx >= start)
            .take(max)
            .collect();
        if let Some(last) = out.last() {
            self.sent = last.idx + 1;
        }
        out
    }

    /// The matching tier's reply to a send: its cursor, and whether it refused
    /// for a gap. Returns the ledger command that trims what is now accepted.
    pub fn on_reply(&mut self, cursor: u64, gap: bool) -> Option<Cmd> {
        if gap || cursor > self.sent {
            self.sent = cursor;
        }
        if cursor > self.acked {
            self.acked = cursor;
            return Some(Cmd::OutboxAck { upto: cursor });
        }
        None
    }

    /// A timeout: resend everything not yet known to be accepted.
    pub fn rewind(&mut self) {
        self.sent = self.acked;
    }
}

fn involves(shard: u16, e: &Event) -> bool {
    match *e {
        Event::Trade { buy, sell, .. } => order_shard(buy) == shard || order_shard(sell) == shard,
        Event::Closed { order } => order_shard(order) == shard,
        // Phase 2 events are routed by account, not by order id: see `settle_cmds_routed`.
        _ => false,
    }
}

fn involves_routed(
    shard: u16,
    e: &Event,
    shard_of: &impl Fn(AccountId) -> u16,
    fee_shard: u16,
) -> bool {
    match *e {
        Event::SliceIn { account, .. }
        | Event::SliceOut { account, .. }
        | Event::Returned { account, .. } => shard_of(account) == shard,
        Event::SliceFee { .. } => shard == fee_shard,
        _ => involves(shard, e),
    }
}

/// The next settle commands for `shard` from the stream of `source`, given the
/// shard's mark `hw`: up to `max` events involving the shard after `hw`, chained
/// by `prev`. `events` is the stream (index, event) in order; entries at or below
/// `hw` are skipped, so passing the whole stream is correct, only slower.
pub fn settle_cmds(
    source: u32,
    shard: u16,
    hw: u64,
    events: impl IntoIterator<Item = (u64, Event)>,
    max: usize,
) -> Vec<Cmd> {
    let mut prev = hw;
    let mut out = Vec::new();
    for (idx, event) in events {
        if out.len() >= max {
            break;
        }
        if idx > hw && involves(shard, &event) {
            out.push(Cmd::Settle {
                source,
                prev,
                idx,
                event,
            });
            prev = idx;
        }
    }
    out
}

/// Like [`settle_cmds`], for a stream that also carries phase 2 events. Those
/// name an account, not an order, so the caller says which shard owns an account
/// (`shard_of`, the same function the gateway uses) and which shard books slice
/// fees (`fee_shard`).
pub fn settle_cmds_routed(
    source: u32,
    shard: u16,
    hw: u64,
    events: impl IntoIterator<Item = (u64, Event)>,
    max: usize,
    shard_of: impl Fn(AccountId) -> u16,
    fee_shard: u16,
) -> Vec<Cmd> {
    let mut prev = hw;
    let mut out = Vec::new();
    for (idx, event) in events {
        if out.len() >= max {
            break;
        }
        if idx > hw && involves_routed(shard, &event, &shard_of, fee_shard) {
            out.push(Cmd::Settle {
                source,
                prev,
                idx,
                event,
            });
            prev = idx;
        }
    }
    out
}

/// One pass over `events` that builds, for every ledger shard, the chain of settle
/// commands it is owed after its mark: the same commands that
/// [`settle_cmds_routed`] gives each shard separately, in O(events + commands)
/// instead of O(events x shards). `marks[s]` is shard `s`'s mark for `source`; a
/// shard's chain stops after `max` commands. Events that name an order of a shard
/// outside `marks` (a slice order, or a shard this caller does not serve) are not
/// routed to it.
pub fn route_settles(
    source: u32,
    marks: &[u64],
    events: impl IntoIterator<Item = (u64, Event)>,
    max: usize,
    shard_of: impl Fn(AccountId) -> u16,
    fee_shard: u16,
) -> Vec<Vec<Cmd>> {
    let n = marks.len();
    let mut prev: Vec<u64> = marks.to_vec();
    let mut out: Vec<Vec<Cmd>> = vec![Vec::new(); n];
    for (idx, event) in events {
        let (a, b): (Option<u16>, Option<u16>) = match event {
            Event::Trade { buy, sell, .. } => (Some(order_shard(buy)), Some(order_shard(sell))),
            Event::Closed { order } => (Some(order_shard(order)), None),
            Event::SliceIn { account, .. }
            | Event::SliceOut { account, .. }
            | Event::Returned { account, .. } => (Some(shard_of(account)), None),
            Event::SliceFee { .. } => (Some(fee_shard), None),
        };
        for (k, sh) in [a, b].into_iter().enumerate() {
            let Some(sh) = sh else { continue };
            let s = sh as usize;
            // a trade between two accounts of one shard is one command, not two
            if s >= n || (k == 1 && a == b) {
                continue;
            }
            if idx > marks[s] && out[s].len() < max {
                out[s].push(Cmd::Settle {
                    source,
                    prev: prev[s],
                    idx,
                    event,
                });
                prev[s] = idx;
            }
        }
    }
    out
}
