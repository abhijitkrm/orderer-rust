//! The relay loops as library functions, so the binaries stay thin and the tests
//! can drive one step at a time. Every relay is memory-less: whatever it
//! remembers is an optimisation that is rebuilt from the ledger and the matching
//! group on any error, so killing and restarting one at any moment is safe.

use crate::gateway::GatewayClient;
use crate::node::Reply;
use crate::tierclient::TierClient;
use orderer_cluster::shard::ShardMap;
use orderer_ledger::policy::{plan, PolicyConfig, PolicyState};
use orderer_ledger::{route_settles, Cmd, Config, Ledger, OutMsg, Slice};
use std::io;

/// Outcome of one relay step, for logging and for the caller's idle sleep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    /// Messages forwarded or settle commands applied.
    pub moved: usize,
    /// Settles the ledger refused as a gap (the settler resynchronised).
    pub gaps: usize,
}

/// Delivers one ledger shard's outbox to every matching group.
///
/// A ledger shard has one outbox, in order. With several matching groups every
/// group's intake sees every message, in order, and applies only the symbols it
/// owns (`MatchingTierSm::with_group`), so a group's cursor counts the whole outbox.
/// The ledger may trim only what **all** groups have accepted: the acknowledged
/// index is the minimum of the groups' cursors, which is why one relay serves all
/// groups of a shard. With one group this is the plain relay.
pub struct ForwarderRelay {
    pub shard: u16,
    /// Each group's cursor for this shard as of its last reply; `None` after a
    /// start or an error, which makes the next step ask for it.
    known: Vec<Option<u64>>,
    pub batch: u32,
    /// Fault injection (0 = off): every `chaos`-th successful forward is followed by
    /// a duplicate of the same batch and an overlapping one, as a network that
    /// duplicates would deliver. The intake must make both no-ops.
    pub chaos: u32,
    sends: u32,
    /// Send each group only the messages of its own symbols (spans, `wire::Span`).
    thin: Option<Thinning>,
}

/// Which group a forwarded message is for.
#[derive(Clone, Debug)]
pub struct Thinning {
    pub map: ShardMap,
    /// The groups in credit mode, sorted: only those take grants and recalls, and
    /// each takes only the ones addressed to it.
    pub credit_groups: Vec<u32>,
}

impl Thinning {
    pub fn owns(&self, group: usize, m: &OutMsg) -> bool {
        match *m {
            OutMsg::Submit { symbol, .. }
            | OutMsg::SubmitIoc { symbol, .. }
            | OutMsg::Cancel { symbol, .. } => self.map.group_of(symbol) as usize == group,
            OutMsg::Grant { group: g, .. } | OutMsg::Recall { group: g, .. } => {
                g as usize == group && self.credit_groups.binary_search(&u32::from(g)).is_ok()
            }
        }
    }
}

/// `(first index, next index, (index, message) pairs)` of a thinned window.
pub type ThinSpan = (u64, u64, Vec<(u64, OutMsg)>);

/// The span to send group `group` whose intake is at `have`, from an outbox window:
/// `msgs[0]` has outbox index `first` (`have >= first`). `None` when the group is
/// already past the window. The span covers `have..first + msgs.len()`; its messages
/// are the window's messages for this group at or after `have`.
pub fn thin_span(
    t: &Thinning,
    group: usize,
    have: u64,
    first: u64,
    msgs: &[OutMsg],
) -> Option<ThinSpan> {
    let skip = have.checked_sub(first)? as usize;
    if skip >= msgs.len() {
        return None;
    }
    let own = msgs[skip..]
        .iter()
        .enumerate()
        .filter(|(_, m)| t.owns(group, m))
        .map(|(i, m)| (have + i as u64, *m))
        .collect();
    Some((have, first + msgs.len() as u64, own))
}

impl ForwarderRelay {
    pub fn new(shard: u16, groups: usize) -> ForwarderRelay {
        ForwarderRelay {
            shard,
            known: vec![None; groups],
            batch: 256,
            chaos: 0,
            sends: 0,
            thin: None,
        }
    }

    /// Thin the outbox per group: a group receives only its own messages, yet its
    /// intake cursor still advances over every index (a span). Cuts the apply work
    /// and bandwidth of every group to its own share of the outbox.
    pub fn thinned(mut self, t: Thinning) -> ForwarderRelay {
        self.thin = Some(t);
        self
    }

    /// `tiers[g]` is the client of matching group `g`.
    pub fn step(
        &mut self,
        ledger: &mut GatewayClient,
        tiers: &mut [TierClient],
    ) -> io::Result<Step> {
        assert_eq!(tiers.len(), self.known.len(), "one client per group");
        for (g, t) in tiers.iter_mut().enumerate() {
            if self.known[g].is_none() {
                match t.cursor(self.shard)? {
                    Ok(c) => self.known[g] = Some(c),
                    Err(r) => {
                        return Err(io::Error::other(format!(
                            "group {g}: cursor refused: {r:?}"
                        )))
                    }
                }
            }
        }
        let low = self
            .known
            .iter()
            .map(|k| k.expect("known"))
            .min()
            .unwrap_or(0);
        let (base, _next, first, msgs) = ledger.outbox(false, low, self.batch)?;
        if base > low {
            // The ledger trimmed past what a group has accepted: it must never happen.
            self.known.iter_mut().for_each(|k| *k = None);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "shard {}: ledger outbox base {base} is ahead of a matching cursor {low}",
                    self.shard
                ),
            ));
        }
        let mut moved = 0;
        let mut gaps = 0;
        for (g, tier) in tiers.iter_mut().enumerate() {
            let have = self.known[g].expect("known");
            let skip = have.saturating_sub(first) as usize;
            if skip >= msgs.len() {
                continue; // this group is already past the window
            }
            let (idx, part) = (first + skip as u64, &msgs[skip..]);
            if let Some(t) = &self.thin {
                let (_, end, own) =
                    thin_span(t, g, idx, first, &msgs).expect("the group is inside the window");
                let chaotic = self.chaos > 0 && (self.sends + 1) % self.chaos == 0;
                if chaotic && end > idx + 1 {
                    // a span that skips ahead of the group's cursor: must be refused
                    let ahead: Vec<(u64, OutMsg)> =
                        own.iter().filter(|(i, _)| *i > idx).copied().collect();
                    let _ = tier.forward_span(self.shard, idx + 1, end, &ahead);
                }
                let ack = match tier.forward_span(self.shard, idx, end, &own)? {
                    Ok(a) => a,
                    Err(r) => {
                        self.known[g] = None;
                        return Err(io::Error::other(format!(
                            "group {g}: forward refused: {r:?}"
                        )));
                    }
                };
                self.sends += 1;
                if chaotic {
                    // the same span again, and one that starts below the group's cursor
                    // (overlapping what it has): both must change nothing
                    let _ = tier.forward_span(self.shard, idx, end, &own);
                    if let Some((a, b, o)) = thin_span(t, g, first, first, &msgs) {
                        if a < idx {
                            let _ = tier.forward_span(self.shard, a, b, &o);
                        }
                    }
                }
                self.known[g] = if ack.gap { None } else { Some(ack.cursor) };
                moved += own.len();
                gaps += ack.gap as usize;
                continue;
            }
            let ack = match tier.forward(self.shard, idx, part)? {
                Ok(a) => a,
                Err(r) => {
                    self.known[g] = None;
                    return Err(io::Error::other(format!(
                        "group {g}: forward refused: {r:?}"
                    )));
                }
            };
            self.sends += 1;
            if self.chaos > 0 && self.sends % self.chaos == 0 {
                let _ = tier.forward(self.shard, idx, part);
                let k = part.len() / 2;
                let _ = tier.forward(self.shard, idx + k as u64, &part[k..]);
            }
            self.known[g] = if ack.gap { None } else { Some(ack.cursor) };
            moved += part.len();
            gaps += ack.gap as usize;
        }
        // Trim what every group has accepted, and only that.
        if self.known.iter().all(|k| k.is_some()) {
            let upto = self
                .known
                .iter()
                .map(|k| k.expect("known"))
                .min()
                .unwrap_or(0);
            if upto > base {
                ledger.raw(&Cmd::OutboxAck { upto })?;
            }
        }
        Ok(Step { moved, gaps })
    }
}

/// Settles every ledger shard from every matching stream, reading each stream
/// once. Memory-less: its read positions are an optimisation, rebuilt from the
/// ledgers' marks whenever anything goes wrong.
pub struct SettlerRelay {
    /// Ledger shard of `ledgers[k]` in `step`.
    pub shards: Vec<u16>,
    /// `(stream id, matching group)`.
    pub sources: Vec<(u32, usize)>,
    /// Per stream: everything up to here has been sent to every shard and accepted.
    scan: Vec<u64>,
    pub batch: u32,
    /// Fault injection (0 = off): every `chaos`-th batch of two or more settles is
    /// preceded by its last settle, out of order, as a zombie or a reordering
    /// network would deliver it. The ledger must refuse it as a gap.
    pub chaos: u32,
    batches: u32,
}

impl SettlerRelay {
    pub fn new(shards: Vec<u16>, sources: Vec<(u32, usize)>) -> SettlerRelay {
        let n = sources.len();
        SettlerRelay {
            shards,
            sources,
            scan: vec![0; n],
            batch: 256,
            chaos: 0,
            batches: 0,
        }
    }

    pub fn step(
        &mut self,
        ledgers: &mut [GatewayClient],
        tiers: &mut [TierClient],
    ) -> io::Result<Step> {
        assert_eq!(ledgers.len(), self.shards.len(), "one client per shard");
        let ids: Vec<u32> = self.sources.iter().map(|s| s.0).collect();
        let mut step = Step::default();
        let mut first_err: Option<io::Error> = None;
        // marks[k][j]: shard k's mark for stream j
        let mut marks = Vec::with_capacity(ledgers.len());
        for l in ledgers.iter_mut() {
            marks.push(l.state(false, &ids)?.marks);
        }
        for (j, &(source, group)) in self.sources.iter().enumerate() {
            let low = marks.iter().map(|m| m[j]).min().unwrap_or(0);
            let from = self.scan[j].max(low) + 1;
            let (evs, _end) = match tiers[group].read(source, from, self.batch)? {
                Ok(x) => x,
                Err(r) => {
                    return Err(io::Error::other(format!(
                        "read of stream {source} refused: {r:?}"
                    )))
                }
            };
            let Some(last) = evs.last().map(|e| e.0) else {
                continue;
            };
            let mut all_ok = true;
            let nshards = ledgers.len() as u64;
            let marks_j: Vec<u64> = marks.iter().map(|m| m[j]).collect();
            // one pass over the events for every shard (accounts live on shard
            // `account % shards`; slice fees are booked on shard 0)
            let chains = route_settles(
                source,
                &marks_j,
                evs,
                self.batch as usize,
                |a| (a % nshards) as u16,
                0,
            );
            for (k, ledger) in ledgers.iter_mut().enumerate() {
                let shard = self.shards[k];
                let cmds = &chains[k];
                if cmds.is_empty() {
                    continue;
                }
                self.batches += 1;
                if self.chaos > 0 && self.batches % self.chaos == 0 && cmds.len() >= 2 {
                    let _ = ledger.raw(cmds.last().expect("two or more"));
                }
                // One log entry for the whole chain, applied in order: a settle that
                // is refused as a gap makes the rest of the chain gaps too.
                match ledger.raw_batch(cmds) {
                    Ok(replies) => {
                        for (cmd, reply) in cmds.iter().zip(replies) {
                            match reply {
                                Reply::Other(_) => step.moved += 1, // Settled or a Duplicate
                                Reply::Rejected(r) if r == "Gap" => {
                                    step.gaps += 1;
                                    all_ok = false;
                                    break;
                                }
                                other => {
                                    eprintln!(
                                        "settler shard {shard} source {source}: {other:?} for {cmd:?}"
                                    );
                                    all_ok = false;
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        all_ok = false;
                        first_err.get_or_insert(e);
                    }
                }
            }
            // Past the batch only if every shard accepted everything; otherwise start
            // over from the ledgers' marks.
            self.scan[j] = if all_ok { last } else { 0 };
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(step),
        }
    }
}

/// Feeds a shard's clock and runs the reconciler.
pub struct TickRelay {
    pub max_age_ns: u64,
    pub retry_ns: u64,
    pub max: u32,
}

impl TickRelay {
    pub fn clock(&self, ledger: &mut GatewayClient, now_ns: u64) -> io::Result<()> {
        ledger.raw(&Cmd::Clock { now_ns })?;
        Ok(())
    }
    pub fn sweep(&self, ledger: &mut GatewayClient) -> io::Result<u32> {
        match ledger.raw(&Cmd::Sweep {
            max_age_ns: self.max_age_ns,
            retry_ns: self.retry_ns,
            max: self.max,
        })? {
            Reply::Other(s) => Ok(s
                .split("queued: ")
                .nth(1)
                .and_then(|x| x.trim_end_matches(" }").parse().ok())
                .unwrap_or(0)),
            _ => Ok(0),
        }
    }
}

/// What one [`SliceDriver`] step did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DriverStep {
    pub lends: usize,
    pub recalls: usize,
    /// Commands the ledger refused (a lend with too little `available`, say).
    pub refused: usize,
    pub ticked: bool,
}

/// Drives credit slices (phase 2) from outside: reads the credit group's slice and
/// every ledger shard, runs [`policy::plan`](orderer_ledger::policy::plan) and
/// issues the `Lend` and `Recall` commands it proposes, then asks the group to close
/// its credit epoch (a quiet group has no entries to count).
///
/// Memory-less in the sense that matters: its [`PolicyState`] is only the idle
/// clocks, and the ledger enforces everything that protects money (`available` is
/// checked on a lend; a recall returns only uncommitted credit), so a crashed,
/// restarted or duplicated driver costs at most an extra chunk lent or an early
/// recall, never a wrong balance. The *demand* it reacts to is the group's own list of
/// accounts refused for lack of credit (replicated state, cleared by the grant that
/// answers it). Credit already granted by the ledger but not yet accepted by the
/// group counts as credit, so a slow forwarder does not make the driver lend twice.
pub struct SliceDriver {
    pub cfg: PolicyConfig,
    states: Vec<PolicyState>,
    /// The credit group this driver serves (see `CreditConfig::group`); one driver
    /// per credit group, each lending and recalling for its own group only.
    pub group: u16,
    /// Close the credit epoch each step.
    pub tick: bool,
}

impl SliceDriver {
    pub fn new(mut cfg: PolicyConfig, shards: usize, group: u16) -> SliceDriver {
        cfg.group = group;
        SliceDriver {
            cfg,
            states: vec![PolicyState::new(); shards],
            group,
            tick: true,
        }
    }

    /// One look and its commands. `now_ns` is any monotonic clock (the policy's
    /// `idle_ns` is in the same unit).
    pub fn step(
        &mut self,
        ledgers: &mut [GatewayClient],
        tier: &mut TierClient,
        ledger_cfg: &dyn Fn(u16) -> Config,
        now_ns: u64,
    ) -> io::Result<DriverStep> {
        let refused = |what: &str, r| io::Error::other(format!("{what}: refused: {r:?}"));
        let st = tier.slice_state()?.map_err(|r| refused("slice state", r))?;
        let mut slice = Slice::new(ledger_cfg(0), self.group);
        slice.restore(&st.snapshot).map_err(io::Error::other)?;
        let n = ledgers.len() as u64;
        let mut step = DriverStep::default();
        for (s, gw) in ledgers.iter_mut().enumerate() {
            let snap = gw.snapshot()?;
            let mut l = Ledger::new(ledger_cfg(s as u16));
            l.restore(&snap).map_err(io::Error::other)?;
            let cursor = tier.cursor(s as u16)?.map_err(|r| refused("cursor", r))?;
            let mut pending: std::collections::BTreeMap<(u64, u32), u128> = Default::default();
            let (base, msgs) = l.outbox();
            for (i, m) in msgs.enumerate() {
                if let OutMsg::Grant {
                    group,
                    account,
                    asset,
                    amount,
                } = *m
                {
                    if group == self.group && base + i as u64 >= cursor {
                        *pending.entry((account, asset)).or_default() += amount;
                    }
                }
            }
            let credit = |a: u64, asset: u32| -> Option<u128> {
                Some(slice.available(a, asset) + pending.get(&(a, asset)).copied().unwrap_or(0))
            };
            let demand: Vec<(u64, u32)> = st
                .demand
                .iter()
                .copied()
                .filter(|(a, _)| a % n == s as u64)
                .collect();
            for c in plan(&self.cfg, &mut self.states[s], &l, &credit, &demand, now_ns) {
                let is_lend = matches!(c, Cmd::Lend { .. });
                match gw.raw(&c)? {
                    Reply::Rejected(_) => step.refused += 1,
                    _ if is_lend => step.lends += 1,
                    _ => step.recalls += 1,
                }
            }
        }
        if self.tick {
            tier.tick()?.map_err(|r| refused("tick", r))?;
            step.ticked = true;
        }
        Ok(step)
    }
}
