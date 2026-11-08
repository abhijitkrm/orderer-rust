//! Credit mode of the matching tier (phase 2, `CREDIT-SLICES.md`): the group holds
//! a [`Slice`] of account balances the ledger lent it, checks orders against it
//! locally, and reports what happened on a stream of its own.
//!
//! **Determinism.** The pre-trade check runs in `apply`, in log order, so it is
//! deterministic by construction. Fills arrive asynchronously (the matcher's
//! partitions emit them on their own threads), so they are applied to the slice
//! only at *credit epochs*: every `epoch` applied entries the state machine drains
//! the pipeline, then reads each partition's new archive events in partition order
//! and settles them into the slice. Everything the slice does is thus a function of
//! the log; the cost is that proceeds and released holds become usable credit up to
//! one epoch after the fill.
//!
//! **Group stream.** The slice's reports (`SliceIn`, `SliceOut`, `SliceFee`,
//! `Returned`) go to one extra stream, `source_base + partitions`, with contiguous
//! indices from 1, read through the same [`TierView::read`] as the partition
//! streams and settled by the ledgers with the same chained marks.

use crate::wire::{get_event, put_event, Rd, WireError};
use orderer_cluster::sha256::sha256;
use orderer_ledger::{Config, Event, OrderId, Reject, Slice};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct CreditConfig {
    /// Reference data, identical to the ledgers' (symbols and fees).
    pub ledger: Config,
    /// This matching group's number (below `SLICE_SHARD_BIT`).
    pub group: u16,
    /// Settle fills into the slice every this many applied entries (>= 1).
    pub epoch: u64,
    /// Events of the group stream kept in memory and in snapshots.
    pub retain: usize,
}

/// The result of a gateway request, kept for its last sequence number so a retry
/// of the same request gets the same answer.
pub type SliceResult = Result<OrderId, Reject>;

pub struct CreditState {
    pub slice: Slice,
    pub(crate) next: u64,
    pub(crate) base: u64,
    pub(crate) events: VecDeque<Event>,
    pub(crate) retain: usize,
    /// Per partition: index of the next archive event to settle into the slice.
    pub(crate) cursors: Vec<u64>,
    pub(crate) entries: u64,
    /// Archive events that had left the retained window before the slice read
    /// them (should stay zero; raise `TierConfig::retain`).
    pub lost: u64,
    pub(crate) clients: BTreeMap<u64, (u64, SliceResult)>,
    /// `(account, asset)` pairs an order was refused for lack of credit, until the
    /// ledger's next grant for the pair arrives: what a slice driver should lend to
    /// next. A function of the log, so every replica has the same set.
    pub(crate) demand: BTreeSet<(u64, u32)>,
    /// Rolling hash over the group stream: the cheap divergence digest of it.
    pub(crate) chain: [u8; 32],
    pub(crate) epoch: u64,
}

pub type SharedCredit = Arc<Mutex<Option<CreditState>>>;

const REJECTS: [Reject; 13] = [
    Reject::UnknownSymbol,
    Reject::BadAmount,
    Reject::BadPrice,
    Reject::BadQuantity,
    Reject::Insufficient,
    Reject::Overflow,
    Reject::UnknownOrder,
    Reject::NotOwner,
    Reject::AlreadyCancelling,
    Reject::DuplicateId,
    Reject::UnknownId,
    Reject::Gap,
    Reject::ReservedAccount,
];

pub fn reject_code(r: Reject) -> u8 {
    REJECTS.iter().position(|x| *x == r).unwrap() as u8
}

pub fn reject_from_code(c: u8) -> Option<Reject> {
    REJECTS.get(c as usize).copied()
}

impl CreditState {
    pub fn new(cfg: &CreditConfig, partitions: u32) -> CreditState {
        CreditState {
            slice: Slice::new(cfg.ledger.clone(), cfg.group),
            next: 1,
            base: 1,
            events: VecDeque::new(),
            retain: cfg.retain.max(1),
            cursors: vec![1; partitions as usize],
            entries: 0,
            lost: 0,
            clients: BTreeMap::new(),
            demand: BTreeSet::new(),
            chain: [0; 32],
            epoch: cfg.epoch.max(1),
        }
    }

    /// Append reports to the group stream.
    pub fn push(&mut self, evs: impl IntoIterator<Item = Event>) {
        for e in evs {
            let mut b = self.chain.to_vec();
            put_event(&mut b, &e);
            self.chain = sha256(&b);
            self.events.push_back(e);
            self.next += 1;
            while self.events.len() > self.retain {
                self.events.pop_front();
                self.base += 1;
            }
        }
    }

    /// Index of the last event of the group stream (0 when empty).
    pub fn end(&self) -> u64 {
        self.next - 1
    }

    pub fn read(&self, from: u64, max: usize) -> Result<Vec<(u64, Event)>, u64> {
        let from = from.max(1);
        if from < self.base {
            return Err(self.base);
        }
        Ok(self
            .events
            .iter()
            .skip((from - self.base) as usize)
            .take(max)
            .enumerate()
            .map(|(i, e)| (from + i as u64, *e))
            .collect())
    }

    /// The cheap part of the state's digest: stream length and rolling hash,
    /// epoch position, and the slice's open-order count.
    pub fn cheap_digest(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.next.to_le_bytes());
        out.extend_from_slice(&self.chain);
        out.extend_from_slice(&self.entries.to_le_bytes());
        for c in &self.cursors {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out.extend_from_slice(&(self.slice.open_orders() as u64).to_le_bytes());
        out.extend_from_slice(&(self.demand.len() as u64).to_le_bytes());
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        let s = self.slice.snapshot();
        out.extend_from_slice(&(s.len() as u64).to_le_bytes());
        out.extend_from_slice(&s);
        out.extend_from_slice(&self.next.to_le_bytes());
        out.extend_from_slice(&self.base.to_le_bytes());
        out.extend_from_slice(&(self.events.len() as u64).to_le_bytes());
        for e in &self.events {
            put_event(out, e);
        }
        out.extend_from_slice(&self.chain);
        out.extend_from_slice(&(self.cursors.len() as u32).to_le_bytes());
        for c in &self.cursors {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out.extend_from_slice(&self.entries.to_le_bytes());
        out.extend_from_slice(&self.lost.to_le_bytes());
        out.extend_from_slice(&(self.clients.len() as u32).to_le_bytes());
        for (c, (seq, res)) in &self.clients {
            out.extend_from_slice(&c.to_le_bytes());
            out.extend_from_slice(&seq.to_le_bytes());
            match res {
                Ok(o) => {
                    out.push(0);
                    out.extend_from_slice(&o.to_le_bytes());
                }
                Err(r) => {
                    out.push(1);
                    out.extend_from_slice(&(reject_code(*r) as u64).to_le_bytes());
                }
            }
        }
        out.extend_from_slice(&(self.demand.len() as u32).to_le_bytes());
        for (a, asset) in &self.demand {
            out.extend_from_slice(&a.to_le_bytes());
            out.extend_from_slice(&asset.to_le_bytes());
        }
    }

    /// Replace this state from a snapshot section (configuration is kept).
    pub fn decode_into(&mut self, r: &mut Rd) -> Result<(), String> {
        let bad = |e: WireError| format!("credit snapshot: {}", e.0);
        let n = r.u64().map_err(bad)? as usize;
        let slice = r.take(n).map_err(bad)?.to_vec();
        let next = r.u64().map_err(bad)?;
        let base = r.u64().map_err(bad)?;
        let k = r.u64().map_err(bad)? as usize;
        if k > r.left() / 9 || base + k as u64 != next {
            return Err("credit snapshot: stream window".into());
        }
        let mut events = VecDeque::with_capacity(k);
        for _ in 0..k {
            events.push_back(get_event(r).map_err(bad)?);
        }
        let chain: [u8; 32] = r.take(32).map_err(bad)?.try_into().unwrap();
        let nc = r.u32().map_err(bad)? as usize;
        if nc != self.cursors.len() {
            return Err("credit snapshot: partition count".into());
        }
        let mut cursors = Vec::with_capacity(nc);
        for _ in 0..nc {
            cursors.push(r.u64().map_err(bad)?);
        }
        let entries = r.u64().map_err(bad)?;
        let lost = r.u64().map_err(bad)?;
        let nclients = r.u32().map_err(bad)? as usize;
        if nclients > r.left() / 25 {
            return Err("credit snapshot: client count".into());
        }
        let mut clients = BTreeMap::new();
        for _ in 0..nclients {
            let (c, seq) = (r.u64().map_err(bad)?, r.u64().map_err(bad)?);
            let kind = r.u8().map_err(bad)?;
            let v = r.u64().map_err(bad)?;
            let res = match kind {
                0 => Ok(v),
                1 => Err(*REJECTS
                    .get(v as usize)
                    .ok_or("credit snapshot: reject code")?),
                _ => return Err("credit snapshot: result kind".into()),
            };
            if clients.insert(c, (seq, res)).is_some() {
                return Err("credit snapshot: duplicate client".into());
            }
        }
        let nd = r.u32().map_err(bad)? as usize;
        if nd > r.left() / 12 {
            return Err("credit snapshot: demand count".into());
        }
        let mut demand = BTreeSet::new();
        for _ in 0..nd {
            let (a, asset) = (r.u64().map_err(bad)?, r.u32().map_err(bad)?);
            demand.insert((a, asset));
        }
        self.slice.restore(&slice)?;
        self.demand = demand;
        self.next = next;
        self.base = base;
        self.events = events;
        self.chain = chain;
        self.cursors = cursors;
        self.entries = entries;
        self.lost = lost;
        self.clients = clients;
        Ok(())
    }
}
