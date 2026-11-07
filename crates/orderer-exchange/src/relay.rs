//! The relay loops as library functions, so the binaries stay thin and the tests
//! can drive one step at a time. Every relay is memory-less: whatever it
//! remembers is an optimisation that is rebuilt from the ledger and the matching
//! group on any error, so killing and restarting one at any moment is safe.

use crate::gateway::GatewayClient;
use crate::node::Reply;
use crate::tierclient::TierClient;
use orderer_ledger::{settle_cmds, Cmd, Forwarder};
use std::io;

/// Outcome of one relay step, for logging and for the caller's idle sleep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Step {
    /// Messages forwarded or settle commands applied.
    pub moved: usize,
    /// Settles the ledger refused as a gap (the settler resynchronised).
    pub gaps: usize,
}

/// Delivers one ledger shard's outbox to the matching group.
pub struct ForwarderRelay {
    pub shard: u16,
    fwd: Forwarder,
    /// The matching group's cursor for this shard as of the last reply; `None`
    /// after a start or an error, which makes the next step ask for it.
    known: Option<u64>,
    pub batch: u32,
    /// Fault injection (0 = off): every `chaos`-th successful forward is followed by
    /// a duplicate of the same batch and an overlapping one, as a network that
    /// duplicates would deliver. The intake must make both no-ops.
    pub chaos: u32,
    sends: u32,
}

impl ForwarderRelay {
    pub fn new(shard: u16) -> ForwarderRelay {
        ForwarderRelay {
            shard,
            fwd: Forwarder::new(shard),
            known: None,
            batch: 256,
            chaos: 0,
            sends: 0,
        }
    }

    pub fn step(&mut self, ledger: &mut GatewayClient, tier: &mut TierClient) -> io::Result<Step> {
        let cursor = match self.known {
            Some(c) => c,
            None => match tier.cursor(self.shard)? {
                Ok(c) => {
                    self.known = Some(c);
                    c
                }
                Err(r) => return Err(io::Error::other(format!("cursor refused: {r:?}"))),
            },
        };
        let (base, _next, first, msgs) = ledger.outbox(false, cursor, self.batch)?;
        if base > cursor {
            // The ledger trimmed past what matching has accepted: it must never happen.
            self.known = None;
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "shard {}: ledger outbox base {base} is ahead of the matching cursor {cursor}",
                    self.shard
                ),
            ));
        }
        if msgs.is_empty() {
            // Nothing to send. Re-issue the trim if the ledger has not seen it.
            if base < cursor {
                ledger.raw(&Cmd::OutboxAck { upto: cursor })?;
            }
            return Ok(Step::default());
        }
        let ack = match tier.forward(self.shard, first, &msgs)? {
            Ok(a) => a,
            Err(r) => {
                self.known = None;
                return Err(io::Error::other(format!("forward refused: {r:?}")));
            }
        };
        self.sends += 1;
        if self.chaos > 0 && self.sends % self.chaos == 0 {
            let _ = tier.forward(self.shard, first, &msgs);
            let k = msgs.len() / 2;
            let _ = tier.forward(self.shard, first + k as u64, &msgs[k..]);
        }
        self.known = if ack.gap { None } else { Some(ack.cursor) };
        if let Some(cmd) = self.fwd.on_reply(ack.cursor, ack.gap) {
            ledger.raw(&cmd)?;
        } else if base < ack.cursor {
            ledger.raw(&Cmd::OutboxAck { upto: ack.cursor })?;
        }
        Ok(Step {
            moved: msgs.len(),
            gaps: ack.gap as usize,
        })
    }
}

/// Settles one ledger shard from every matching stream.
pub struct SettlerRelay {
    pub shard: u16,
    pub sources: Vec<u32>,
    /// Last stream index read, per source: where to continue; rebuilt from the
    /// ledger's marks when zero.
    scan: Vec<u64>,
    pub batch: u32,
    /// Fault injection (0 = off): every `chaos`-th batch of two or more settles is
    /// preceded by its last settle, out of order, as a zombie or a reordering
    /// network would deliver it. The ledger must refuse it as a gap.
    pub chaos: u32,
    batches: u32,
}

impl SettlerRelay {
    pub fn new(shard: u16, sources: Vec<u32>) -> SettlerRelay {
        let n = sources.len();
        SettlerRelay {
            shard,
            sources,
            scan: vec![0; n],
            batch: 256,
            chaos: 0,
            batches: 0,
        }
    }

    pub fn step(&mut self, ledger: &mut GatewayClient, tier: &mut TierClient) -> io::Result<Step> {
        let st = ledger.state(false, &self.sources)?;
        let mut step = Step::default();
        for k in 0..self.sources.len() {
            let source = self.sources[k];
            let hw = st.marks[k];
            let from = self.scan[k].max(hw) + 1;
            let (evs, _end) = match tier.read(source, from, self.batch)? {
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
            let mut ok = true;
            let cmds = settle_cmds(source, self.shard, hw, evs, self.batch as usize);
            self.batches += 1;
            if self.chaos > 0 && self.batches % self.chaos == 0 && cmds.len() >= 2 {
                let _ = ledger.raw(cmds.last().expect("two or more"));
            }
            for cmd in cmds {
                match ledger.raw(&cmd)? {
                    Reply::Other(_) => step.moved += 1, // Settled or a Duplicate
                    Reply::Rejected(r) if r == "Gap" => {
                        step.gaps += 1;
                        ok = false;
                        break;
                    }
                    other => {
                        eprintln!(
                            "settler shard {} source {source}: {other:?} for {cmd:?}",
                            self.shard
                        );
                        ok = false;
                        break;
                    }
                }
            }
            // Past the batch only if every settle was accepted; otherwise start over
            // from the ledger's mark.
            self.scan[k] = if ok { last } else { 0 };
        }
        Ok(step)
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
