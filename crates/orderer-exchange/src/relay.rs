//! The relay loops as library functions, so the binaries stay thin and the tests
//! can drive one step at a time. Every relay is memory-less: whatever it
//! remembers is an optimisation that is rebuilt from the ledger and the matching
//! group on any error, so killing and restarting one at any moment is safe.

use crate::gateway::GatewayClient;
use crate::node::Reply;
use crate::tierclient::TierClient;
use orderer_ledger::{settle_cmds, Cmd};
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
}

impl ForwarderRelay {
    pub fn new(shard: u16, groups: usize) -> ForwarderRelay {
        ForwarderRelay {
            shard,
            known: vec![None; groups],
            batch: 256,
            chaos: 0,
            sends: 0,
        }
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
            for (k, ledger) in ledgers.iter_mut().enumerate() {
                let shard = self.shards[k];
                let cmds =
                    settle_cmds(source, shard, marks[k][j], evs.clone(), self.batch as usize);
                if cmds.is_empty() {
                    continue;
                }
                self.batches += 1;
                if self.chaos > 0 && self.batches % self.chaos == 0 && cmds.len() >= 2 {
                    let _ = ledger.raw(cmds.last().expect("two or more"));
                }
                // One log entry for the whole chain, applied in order: a settle that
                // is refused as a gap makes the rest of the chain gaps too.
                match ledger.raw_batch(&cmds) {
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
