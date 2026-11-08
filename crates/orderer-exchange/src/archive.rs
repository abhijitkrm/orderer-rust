//! The event archive: the matcher's emitted events mapped to what the ledger
//! settles, in a stream whose indices every replica derives identically.
//!
//! **Streams.** One stream per pipeline partition, `source = source_base +
//! partition`, indices contiguous from 1. A partition's events are totally
//! ordered and deterministic (spec PIPELINE.md 7), so the index of an archived
//! event is a function of the log alone. A single global stream would depend on
//! how the partitions' egress threads interleave, which the pipeline leaves
//! unspecified, and replicas would disagree.
//!
//! **Mapping** (pure, see [`map_event`]): a trade becomes `Trade { buy, sell, taker_is_buy }`
//! using the sides recorded when each order was forwarded (the taker is the
//! matcher's aggressor); a cancel or expiry
//! becomes `Closed`; a rejection of a *new order* becomes `Closed` (the ledger
//! releases its hold); acks, `Filled` closes (the ledger removes a filled order
//! itself), rejections of cancels and duplicates, and replaces produce nothing.
//!
//! **Retention.** Each stream keeps the last `retain` events in memory and
//! persists that window in snapshots. A settler that falls further behind than
//! the window gets [`Truncated`]; a durable archive (the event journal) behind
//! this is a deployment concern and is not built here.

use crate::wire::{get_event, get_side, put_event, put_side, Rd, WireError};
use orderer::{Egress, EgressCtx, EgressFactory, EvtBody, EvtMsg};
use orderer_core::{CloseReason, Event as MEvent, RejectReason};
use orderer_ledger::{Event, Side};
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    /// Index the next archived event will get.
    pub next: u64,
    /// Index of the first retained event (`next` when none).
    pub base: u64,
    pub events: VecDeque<Event>,
}

impl Part {
    fn new() -> Part {
        Part {
            next: 1,
            base: 1,
            events: VecDeque::new(),
        }
    }
}

#[derive(Debug)]
pub struct ArchiveState {
    pub parts: Vec<Part>,
    /// Side of every order forwarded and not yet finished, by order id.
    pub sides: BTreeMap<u64, Side>,
    /// Trades whose orders were unknown: should stay zero.
    pub unmapped: u64,
    pub retain: usize,
    /// The durable store behind the window (`store.rs`), if one is attached. Not
    /// part of the replicated state, the snapshot or the digest.
    pub store: Option<crate::store::EventStore>,
    /// Why the store was dropped, if it failed; the window keeps serving.
    pub store_error: Option<String>,
}

pub enum Mapped {
    Emit(Event),
    Skip,
    Unmapped,
}

/// The archive's rule for one matcher event; updates `sides` as orders finish.
pub fn map_event(sides: &mut BTreeMap<u64, Side>, ev: &MEvent) -> Mapped {
    match *ev {
        MEvent::Accepted { .. } | MEvent::Replaced { .. } => Mapped::Skip,
        MEvent::Trade {
            maker,
            taker,
            price,
            qty,
        } => {
            let (Some(&m), Some(&t)) = (sides.get(&maker), sides.get(&taker)) else {
                return Mapped::Unmapped;
            };
            if price < 0 {
                return Mapped::Unmapped;
            }
            let (buy, sell) = match (m, t) {
                (Side::Buy, Side::Sell) => (maker, taker),
                (Side::Sell, Side::Buy) => (taker, maker),
                _ => return Mapped::Unmapped,
            };
            Mapped::Emit(Event::Trade {
                buy,
                sell,
                price: price as u64,
                qty,
                taker_is_buy: t == Side::Buy,
            })
        }
        MEvent::Closed { order_id, reason } => {
            let known = sides.remove(&order_id).is_some();
            match reason {
                CloseReason::Filled => Mapped::Skip,
                _ if known => Mapped::Emit(Event::Closed { order: order_id }),
                _ => Mapped::Skip,
            }
        }
        MEvent::Rejected { order_id, reason } => match reason {
            RejectReason::DuplicateOrderId | RejectReason::UnknownOrderId => Mapped::Skip,
            _ => {
                if sides.remove(&order_id).is_some() {
                    Mapped::Emit(Event::Closed { order: order_id })
                } else {
                    Mapped::Skip
                }
            }
        },
    }
}

impl ArchiveState {
    pub fn new(partitions: u32, retain: usize) -> ArchiveState {
        ArchiveState {
            parts: (0..partitions).map(|_| Part::new()).collect(),
            sides: BTreeMap::new(),
            unmapped: 0,
            retain: retain.max(1),
            store: None,
            store_error: None,
        }
    }

    /// Attach a recovered store, bringing it into line with every window.
    pub fn adopt_store(&mut self, store: Option<crate::store::EventStore>) {
        self.store = store;
        let Some(st) = self.store.as_mut() else {
            return;
        };
        for (i, p) in self.parts.iter().enumerate() {
            if let Err(e) = st.attach(i, p.base, &p.events) {
                self.store_error = Some(e.to_string());
                self.store = None;
                return;
            }
        }
    }

    /// Flush and fsync the store (called before a snapshot returns).
    pub fn sync_store(&mut self) {
        if let Some(st) = self.store.as_mut() {
            if let Err(e) = st.sync() {
                self.store_error = Some(e.to_string());
                self.store = None;
            }
        }
    }

    pub fn record(&mut self, partition: usize, ev: &MEvent) {
        match map_event(&mut self.sides, ev) {
            Mapped::Emit(e) => {
                let retain = self.retain;
                let p = &mut self.parts[partition];
                let idx = p.next;
                p.events.push_back(e);
                p.next += 1;
                while p.events.len() > retain {
                    p.events.pop_front();
                    p.base += 1;
                }
                if let Some(st) = self.store.as_mut() {
                    if let Err(err) = st.append(partition, idx, &e) {
                        self.store_error = Some(err.to_string());
                        self.store = None;
                    }
                }
            }
            Mapped::Skip => {}
            Mapped::Unmapped => self.unmapped += 1,
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.parts.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.retain as u64).to_le_bytes());
        out.extend_from_slice(&self.unmapped.to_le_bytes());
        out.extend_from_slice(&(self.sides.len() as u64).to_le_bytes());
        for (id, s) in &self.sides {
            out.extend_from_slice(&id.to_le_bytes());
            put_side(out, *s);
        }
        for p in &self.parts {
            out.extend_from_slice(&p.next.to_le_bytes());
            out.extend_from_slice(&p.base.to_le_bytes());
            out.extend_from_slice(&(p.events.len() as u64).to_le_bytes());
            for e in &p.events {
                put_event(out, e);
            }
        }
    }

    pub fn decode(r: &mut Rd, partitions: u32) -> Result<ArchiveState, WireError> {
        if r.u32()? != partitions {
            return Err(WireError("archive partition count"));
        }
        let retain = r.u64()? as usize;
        let unmapped = r.u64()?;
        let mut sides = BTreeMap::new();
        let n = r.u64()? as usize;
        if n > r.left() / 9 {
            return Err(WireError("sides count"));
        }
        for _ in 0..n {
            let id = r.u64()?;
            if sides.insert(id, get_side(r)?).is_some() {
                return Err(WireError("duplicate side"));
            }
        }
        let mut parts = Vec::new();
        for _ in 0..partitions {
            let (next, base) = (r.u64()?, r.u64()?);
            let n = r.u64()? as usize;
            if n > r.left() / 9 || base + n as u64 != next {
                return Err(WireError("archive window"));
            }
            let mut events = VecDeque::with_capacity(n);
            for _ in 0..n {
                events.push_back(get_event(r)?);
            }
            parts.push(Part { next, base, events });
        }
        Ok(ArchiveState {
            parts,
            sides,
            unmapped,
            retain: retain.max(1),
            store: None,
            store_error: None,
        })
    }
}

/// A request for events older than the retained window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Truncated {
    pub base: u64,
}

/// Shared by the state machine (writer of sides), the partitions' egress
/// threads (writers of events) and readers.
pub(crate) type Shared = Mutex<ArchiveState>;

/// Registers with the pipeline as an egress, one instance per partition.
#[derive(Clone)]
pub struct ArchiveEgress {
    pub(crate) shared: Arc<Shared>,
}

struct PartEgress {
    partition: usize,
    shared: Arc<Shared>,
}

impl EgressFactory for ArchiveEgress {
    fn create(&self, ctx: &EgressCtx) -> Box<dyn Egress> {
        Box::new(PartEgress {
            partition: ctx.partition as usize,
            shared: self.shared.clone(),
        })
    }
}

impl Egress for PartEgress {
    fn on_event(&mut self, m: &EvtMsg) {
        if let EvtBody::Event(ev) = &m.body {
            self.shared
                .lock()
                .expect("archive lock")
                .record(self.partition, ev);
        }
    }
}

/// Read access to the archive and to the intake cursors. Cheap to clone.
#[derive(Clone)]
pub struct TierView {
    pub(crate) archive: Arc<Shared>,
    pub(crate) intake: Arc<Mutex<crate::tier::TierIntake>>,
    pub(crate) source_base: u32,
    pub(crate) credit: crate::tier::credit::SharedCredit,
}

impl TierView {
    pub fn partitions(&self) -> u32 {
        self.archive.lock().expect("archive lock").parts.len() as u32
    }

    /// The ledger `source` id of partition `p`'s stream.
    pub fn source_of(&self, partition: u32) -> u32 {
        self.source_base + partition
    }

    /// Counters of the durable store behind the archive, if one is attached.
    pub fn store_stats(&self) -> Option<crate::store::StoreStats> {
        self.archive
            .lock()
            .expect("archive lock")
            .store
            .as_ref()
            .map(|s| s.stats)
    }

    /// Why the durable store was dropped, if it failed (the window keeps serving).
    pub fn store_error(&self) -> Option<String> {
        self.archive
            .lock()
            .expect("archive lock")
            .store_error
            .clone()
    }

    /// The next outbox index expected from ledger `shard`; everything below it
    /// has been accepted by the matching group.
    pub fn cursor(&self, shard: u16) -> u64 {
        self.intake.lock().expect("intake lock").cursor(shard)
    }

    /// The source id of the group stream (slice reports), in credit mode.
    pub fn group_source(&self) -> Option<u32> {
        let parts = self.partitions();
        self.credit
            .lock()
            .expect("credit lock")
            .as_ref()
            .map(|_| self.source_base + parts)
    }

    /// The answer to a gateway's last credit-slice request (`client`): its
    /// sequence number and the order id or the reason it was refused.
    pub fn slice_result(&self, client: u64) -> Option<(u64, Result<u64, orderer_ledger::Reject>)> {
        let c = self.credit.lock().expect("credit lock");
        c.as_ref()?.clients.get(&client).copied()
    }

    /// `(account, asset)` pairs whose orders were refused for lack of credit and that
    /// no grant has answered yet (credit mode).
    pub fn demand(&self) -> Vec<(u64, u32)> {
        let c = self.credit.lock().expect("credit lock");
        c.as_ref()
            .map(|c| c.demand.iter().copied().collect())
            .unwrap_or_default()
    }

    /// The credit slice's snapshot, the group stream's last index and the demand
    /// list, read together under one lock.
    #[allow(clippy::type_complexity)]
    pub fn slice_state(&self) -> Option<(Vec<u8>, u64, Vec<(u64, u32)>, bool)> {
        // same lock order as the credit epoch: archive, then credit
        let a = self.archive.lock().expect("archive lock");
        let c = self.credit.lock().expect("credit lock");
        let c = c.as_ref()?;
        // behind: the matcher has emitted events the slice has not settled yet
        let behind = a
            .parts
            .iter()
            .enumerate()
            .any(|(p, part)| c.cursors[p] < part.next);
        Some((
            c.slice.snapshot(),
            c.end(),
            c.demand.iter().copied().collect(),
            behind,
        ))
    }

    /// Run `f` on the credit slice (inspection in tests and audits).
    pub fn with_slice<R>(&self, f: impl FnOnce(&orderer_ledger::Slice) -> R) -> Option<R> {
        let c = self.credit.lock().expect("credit lock");
        c.as_ref().map(|c| f(&c.slice))
    }

    /// Index of the last event of stream `source` (0 when empty).
    pub fn end(&self, source: u32) -> Option<u64> {
        if let Some(g) = self.group_source() {
            if source == g {
                return self
                    .credit
                    .lock()
                    .expect("credit lock")
                    .as_ref()
                    .map(|c| c.end());
            }
        }
        let a = self.archive.lock().expect("archive lock");
        let p = a
            .parts
            .get(source.checked_sub(self.source_base)? as usize)?;
        Some(p.next - 1)
    }

    /// Up to `max` events of stream `source` from index `from` (>= 1). An empty
    /// answer means "caught up". `None` for an unknown source.
    pub fn read(
        &self,
        source: u32,
        from: u64,
        max: usize,
    ) -> Option<Result<Vec<(u64, Event)>, Truncated>> {
        if let Some(g) = self.group_source() {
            if source == g {
                let c = self.credit.lock().expect("credit lock");
                return c
                    .as_ref()
                    .map(|c| c.read(from, max).map_err(|base| Truncated { base }));
            }
        }
        let mut a = self.archive.lock().expect("archive lock");
        let stream = source.checked_sub(self.source_base)? as usize;
        let (pbase, pnext) = {
            let p = a.parts.get(stream)?;
            (p.base, p.next)
        };
        let from = from.max(1);
        if from < pbase {
            // older than the window: the durable store, if it has it
            if let Some(st) = a.store.as_mut() {
                return Some(match st.read(stream, from, max) {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(base)) => Err(Truncated { base }),
                    Err(_) => Err(Truncated { base: pbase }),
                });
            }
            return Some(Err(Truncated { base: pbase }));
        }
        let _ = pnext;
        let p = &a.parts[stream];
        let skip = (from - p.base) as usize;
        Some(Ok(p
            .events
            .iter()
            .skip(skip)
            .take(max)
            .enumerate()
            .map(|(i, e)| (from + i as u64, *e))
            .collect()))
    }

    /// Trades with an unknown order (should be zero) and live tracked orders.
    pub fn stats(&self) -> (u64, usize) {
        let a = self.archive.lock().expect("archive lock");
        (a.unmapped, a.sides.len())
    }
}
