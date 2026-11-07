//! The audit: reads every ledger shard and the matching streams from outside
//! and checks what must always hold.
//!
//! Always (safety):
//! - each shard's snapshot restores and passes `Ledger::check()` (holds equal
//!   open orders, balances add up against deposits, withdrawals and settled
//!   flows, no anomalies);
//! - a shard's outbox base never passes the lowest matching cursor over all groups,
//!   and no group's cursor passes the outbox end (nothing is trimmed that some
//!   group has not accepted, nothing is accepted that was never queued);
//! - a shard's settle mark never passes the end of its stream;
//! - replicas of a group that report the same log index report the same digest.
//!
//! Only when every shard has settled every event it is owed (`caught_up`): across
//! all shards, trades moved exactly what they took and money is conserved
//! (deposits and credits = holdings + withdrawals + debits).

use crate::gateway::GatewayClient;
use crate::tierclient::TierClient;
use orderer_cluster::client::Conn;
use orderer_ledger::{order_shard, Config, Event, Ledger};
use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Default)]
pub struct Report {
    pub violations: Vec<String>,
    /// Every shard has settled every event it is owed, so the equalities were checked.
    pub caught_up: bool,
    pub events: u64,
    pub open_orders: usize,
    pub summary: String,
}

pub struct Audit {
    ledgers: Vec<GatewayClient>,
    /// One client per matching group.
    tiers: Vec<TierClient>,
    /// `(stream id, group)`.
    sources: Vec<(u32, usize)>,
    cfg: Box<dyn Fn(u16) -> Config>,
    /// Assets the conservation check looks at (the deployment's reference data).
    assets: Vec<u32>,
    /// Next stream index to scan, per source.
    scan: BTreeMap<u32, u64>,
    /// The last event index involving a shard, per `(shard, source)`.
    last_for: BTreeMap<(usize, u32), u64>,
    events: u64,
}

fn involves(shard: usize, e: &Event) -> bool {
    match *e {
        Event::Trade { buy, sell, .. } => {
            order_shard(buy) as usize == shard || order_shard(sell) as usize == shard
        }
        Event::Closed { order } => order_shard(order) as usize == shard,
        // phase 2 account events are not about orders
        _ => false,
    }
}

impl Audit {
    pub fn new(
        ledgers: Vec<GatewayClient>,
        tiers: Vec<TierClient>,
        sources: Vec<(u32, usize)>,
        cfg: impl Fn(u16) -> Config + 'static,
    ) -> Audit {
        let mut assets: Vec<u32> = cfg(0)
            .symbols
            .values()
            .flat_map(|s| [s.base, s.quote])
            .collect();
        assets.sort_unstable();
        assets.dedup();
        Audit {
            ledgers,
            tiers,
            sources,
            cfg: Box::new(cfg),
            assets,
            scan: BTreeMap::new(),
            last_for: BTreeMap::new(),
            events: 0,
        }
    }

    /// Every group's cursor for ledger shard `s`.
    fn cursors(&mut self, s: usize) -> io::Result<Vec<u64>> {
        let mut v = Vec::new();
        for t in self.tiers.iter_mut() {
            v.push(
                t.cursor(s as u16)?
                    .map_err(|r| io::Error::other(format!("cursor: {r:?}")))?,
            );
        }
        Ok(v)
    }

    /// Read every stream to its current end, remembering per shard the last event it is owed.
    fn scan_streams(&mut self) -> io::Result<BTreeMap<u32, u64>> {
        let mut ends = BTreeMap::new();
        for (source, group) in self.sources.clone() {
            loop {
                let from = self.scan.get(&source).copied().unwrap_or(0) + 1;
                let (evs, end) = self.tiers[group]
                    .read(source, from, 4096)?
                    .map_err(|r| io::Error::other(format!("stream {source}: {r:?}")))?;
                for (idx, ev) in &evs {
                    for s in 0..self.ledgers.len() {
                        if involves(s, ev) {
                            self.last_for.insert((s, source), *idx);
                        }
                    }
                    self.events += 1;
                }
                if let Some(last) = evs.last() {
                    self.scan.insert(source, last.0);
                }
                if evs.is_empty() || self.scan.get(&source).copied().unwrap_or(0) >= end {
                    ends.insert(source, end);
                    break;
                }
            }
        }
        Ok(ends)
    }

    /// One audit round.
    pub fn round(&mut self) -> io::Result<Report> {
        let mut rep = Report::default();
        let n = self.ledgers.len();
        // Cursor before the ledger snapshot bounds the outbox end from below.
        // cursor_*[s][g]: matching group g's cursor for ledger shard s
        let mut cursor_before = Vec::new();
        for s in 0..n {
            cursor_before.push(self.cursors(s)?);
        }
        let mut ledgers: Vec<Ledger> = Vec::new();
        for s in 0..n {
            let snap = self.ledgers[s].snapshot()?;
            let mut l = Ledger::new((self.cfg)(s as u16));
            if let Err(e) = l.restore(&snap) {
                rep.violations
                    .push(format!("shard {s}: snapshot is inconsistent: {e}"));
            }
            if let Err(e) = l.check() {
                rep.violations.push(format!("shard {s}: {e}"));
            }
            ledgers.push(l);
        }
        let ends = self.scan_streams()?;
        let mut cursor_after = Vec::new();
        for s in 0..n {
            cursor_after.push(self.cursors(s)?);
        }
        let mut caught_up = true;
        for (s, l) in ledgers.iter().enumerate() {
            let (base, _) = l.outbox();
            let low = cursor_after[s].iter().copied().min().unwrap_or(0);
            if base > low {
                rep.violations.push(format!(
                    "shard {s}: outbox base {base} is past a matching cursor {low} ({:?}): orders were trimmed that a group never accepted",
                    cursor_after[s]
                ));
            }
            for (g, c) in cursor_before[s].iter().enumerate() {
                if *c > l.outbox_next() {
                    rep.violations.push(format!(
                        "shard {s}: group {g} accepted outbox index {c} but the ledger only queued {}",
                        l.outbox_next()
                    ));
                }
            }
            for &(source, _) in &self.sources {
                let mark = l.high_water(source).unwrap_or(0);
                let end = ends.get(&source).copied().unwrap_or(0);
                if mark > end {
                    rep.violations.push(format!(
                        "shard {s}: mark {mark} is past the end {end} of stream {source}"
                    ));
                }
                if mark != self.last_for.get(&(s, source)).copied().unwrap_or(0) {
                    caught_up = false;
                }
            }
        }
        rep.caught_up = caught_up;
        rep.events = self.events;
        rep.open_orders = ledgers.iter().map(|l| l.open_orders()).sum();
        if caught_up {
            for &asset in &self.assets {
                let (mut have, mut wd, mut dep, mut inn, mut out, mut ain, mut aout) =
                    (0u128, 0u128, 0u128, 0u128, 0u128, 0u128, 0u128);
                for l in &ledgers {
                    have += l.holdings(asset);
                    let (d, w) = l.external_net(asset);
                    dep += d;
                    wd += w;
                    let (i, o) = l.settled_flow(asset);
                    inn += i;
                    out += o;
                    let (ai, ao) = l.adjusted_flow(asset);
                    ain += ai;
                    aout += ao;
                }
                if inn != out {
                    rep.violations.push(format!(
                        "asset {asset}: trades took {out} but delivered {inn}"
                    ));
                }
                if have + wd + aout != dep + ain {
                    rep.violations.push(format!(
                        "asset {asset}: not conserved: holdings {have} + withdrawn {wd} + debits {aout} != deposits {dep} + credits {ain}"
                    ));
                }
            }
        }
        rep.summary = format!(
            "{} shards, {} events, {} open orders, caught_up={}, marks/outbox: {}",
            n,
            self.events,
            rep.open_orders,
            caught_up,
            ledgers
                .iter()
                .enumerate()
                .map(|(s, l)| format!(
                    "s{s} out={}..{} cur={:?}",
                    l.outbox().0,
                    l.outbox_next(),
                    cursor_after[s]
                ))
                .collect::<Vec<_>>()
                .join(" ")
        );
        Ok(rep)
    }

    /// Every acknowledged order is open, filled or closed, and nothing the
    /// archive mentions was never acknowledged. For a quiet system (after a
    /// drain); `acked` is `(order id, quantity)` from the load generator.
    pub fn verify_orders(&mut self, acked: &[(u64, u64)]) -> io::Result<Vec<String>> {
        let mut bad = Vec::new();
        self.scan_streams()?;
        // Read the archive afresh: per order, filled quantity and whether it closed.
        let mut filled: BTreeMap<u64, u64> = BTreeMap::new();
        let mut closed: BTreeMap<u64, u32> = BTreeMap::new();
        for (source, group) in self.sources.clone() {
            let mut from = 1;
            loop {
                let (evs, end) = self.tiers[group]
                    .read(source, from, 4096)?
                    .map_err(|r| io::Error::other(format!("stream {source}: {r:?}")))?;
                for (_, ev) in &evs {
                    match *ev {
                        Event::Trade { buy, sell, qty, .. } => {
                            *filled.entry(buy).or_default() += qty;
                            *filled.entry(sell).or_default() += qty;
                        }
                        Event::Closed { order } => *closed.entry(order).or_default() += 1,
                        _ => {}
                    }
                }
                match evs.last() {
                    Some(l) if l.0 < end => from = l.0 + 1,
                    _ => break,
                }
            }
        }
        let mut open = std::collections::BTreeSet::new();
        for (s, gw) in self.ledgers.iter_mut().enumerate() {
            let snap = gw.snapshot()?;
            let mut l = Ledger::new((self.cfg)(s as u16));
            l.restore(&snap).map_err(io::Error::other)?;
            for &(id, _) in acked {
                if order_shard(id) as usize == s && l.order(id).is_some() {
                    open.insert(id);
                }
            }
        }
        let known: std::collections::BTreeSet<u64> = acked.iter().map(|a| a.0).collect();
        for &(id, qty) in acked {
            let f = filled.get(&id).copied().unwrap_or(0);
            let c = closed.get(&id).copied().unwrap_or(0);
            if open.contains(&id) {
                bad.push(format!("order {id:#x} is still open in its ledger"));
            } else if f > qty {
                bad.push(format!("order {id:#x}: filled {f} of {qty}"));
            } else if f < qty && c == 0 {
                bad.push(format!("order {id:#x}: released by the ledger but neither filled ({f}/{qty}) nor closed in the archive"));
            } else if c > 1 {
                bad.push(format!("order {id:#x}: closed {c} times"));
            }
        }
        for id in filled.keys().chain(closed.keys()) {
            if !known.contains(id) {
                bad.push(format!(
                    "archive mentions order {id:#x} that was never acknowledged"
                ));
            }
        }
        Ok(bad)
    }

    /// The ledgers' deposit and paid-withdrawal totals equal what the load
    /// generator saw acknowledged, per asset (an independent count).
    pub fn verify_money(
        &mut self,
        deposits: &BTreeMap<u32, u128>,
        paid: &BTreeMap<u32, u128>,
    ) -> io::Result<Vec<String>> {
        let mut bad = Vec::new();
        let (mut dep, mut wd): (BTreeMap<u32, u128>, BTreeMap<u32, u128>) = Default::default();
        for (s, gw) in self.ledgers.iter_mut().enumerate() {
            let snap = gw.snapshot()?;
            let mut l = Ledger::new((self.cfg)(s as u16));
            l.restore(&snap).map_err(io::Error::other)?;
            for &asset in &self.assets {
                let (d, w) = l.external_net(asset);
                *dep.entry(asset).or_default() += d;
                *wd.entry(asset).or_default() += w;
            }
        }
        for &asset in &self.assets {
            let (d, w) = (dep[&asset], wd[&asset]);
            let (ed, ew) = (
                deposits.get(&asset).copied().unwrap_or(0),
                paid.get(&asset).copied().unwrap_or(0),
            );
            if d != ed {
                bad.push(format!(
                    "asset {asset}: ledgers deposited {d} but {ed} was acknowledged"
                ));
            }
            if w != ew {
                bad.push(format!(
                    "asset {asset}: ledgers paid out {w} but {ew} was acknowledged"
                ));
            }
        }
        Ok(bad)
    }
}

/// Replicas of a group that report the same log index must report the same
/// state digest. `clients` are the replicas' client-protocol addresses. Returns
/// a violation text, or `None` (also when the replicas were never at the same
/// index within `tries`, which is reported by the second element).
pub fn compare_digests(
    clients: &[SocketAddr],
    key: Option<&[u8]>,
    tries: usize,
) -> (Option<String>, bool) {
    for _ in 0..tries {
        let mut seen: BTreeMap<u64, Vec<(SocketAddr, [u8; 32])>> = BTreeMap::new();
        let mut answered = 0;
        for a in clients {
            if let Ok(Some((idx, d))) = Conn::connect_with_key(*a, key).and_then(|mut c| c.hash()) {
                seen.entry(idx).or_default().push((*a, d));
                answered += 1;
            }
        }
        for (idx, v) in &seen {
            if v.iter().any(|x| x.1 != v[0].1) {
                return (
                    Some(format!("replicas disagree at log index {idx}: {v:?}")),
                    true,
                );
            }
        }
        if answered > 1 && seen.len() == 1 {
            return (None, true);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    (None, false)
}
