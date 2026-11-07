//! [`MatchingTierSm`]: the replicated state machine of a matching group.
//!
//! It wraps [`PipelineSm`] and understands one more log entry, the forwarded
//! batch (`wire.rs`, tag 5). A forwarded message is applied only if the
//! [`TierIntake`] accepts its outbox index (exactly the next one for that ledger
//! shard), so a ledger's orders enter the matcher exactly once and in order
//! however often and in whatever order they were proposed. The intake, the
//! archive window and the live-order sides are part of the snapshot.
//!
//! Translation: `Submit` becomes a GTC limit order with the ledger's order id
//! (a market buy reaches the ledger as a limit order at its protection price, so
//! the outbox has no market or IOC orders); a price above `i64::MAX` becomes -1,
//! which the matcher rejects, and the rejection closes the order in the ledger.
//! `Cancel` becomes a cancel.
//!
//! Digest: SHA-256 over the pipeline's event-chain digest, the intake cursors,
//! every stream's next index and the number of live orders. O(shards +
//! partitions) after one pipeline barrier. `deep_digest` also covers the
//! pipeline's full state, the live-order sides and the retained archive windows.

use crate::archive::{ArchiveEgress, ArchiveState, Shared, TierView};
use crate::wire::{decode_forward, Rd, TAG_FORWARD};
use orderer::{EventDigest, Initial, PartitionMap, Pipeline};
use orderer_cluster::codec::encode_batch;
use orderer_cluster::pipeline_sm::{PipelineSm, Tap};
use orderer_cluster::sha256::sha256;
use orderer_cluster::StateMachine;
use orderer_consensus::{Entry, Index};
use orderer_core::{BookConfig, Command, MatchingCore, Side as MSide, Tif};
use orderer_ledger::{Accept, OutMsg, Side};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const SNAP_MAGIC: &[u8; 4] = b"XTS1";
const DIGEST_TAG: &[u8] = b"orderer-tier/1";

/// How far each ledger shard's outbox has been accepted. Same rule as
/// `orderer_ledger::Intake` (which has no serialisation, so this crate cannot
/// snapshot it); a test pins the two to each other.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TierIntake {
    cursor: BTreeMap<u16, u64>,
}

impl TierIntake {
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
    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.cursor.len() as u32).to_le_bytes());
        for (s, c) in &self.cursor {
            out.extend_from_slice(&s.to_le_bytes());
            out.extend_from_slice(&c.to_le_bytes());
        }
    }
    fn decode(r: &mut Rd) -> Result<TierIntake, crate::wire::WireError> {
        let n = r.u32()? as usize;
        if n > r.left() / 10 {
            return Err(crate::wire::WireError("intake count"));
        }
        let mut cursor = BTreeMap::new();
        for _ in 0..n {
            let (s, c) = (r.u16()?, r.u64()?);
            if cursor.insert(s, c).is_some() {
                return Err(crate::wire::WireError("duplicate shard"));
            }
        }
        Ok(TierIntake { cursor })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TierConfig {
    /// Stream ids are `source_base + partition`; give each matching group its own range.
    pub source_base: u32,
    /// Events kept per stream (and persisted in snapshots).
    pub retain: usize,
}

impl Default for TierConfig {
    fn default() -> Self {
        TierConfig {
            source_base: 0,
            retain: 1 << 20,
        }
    }
}

/// What the pipeline builder closure receives besides the restored state: the
/// divergence digest and the archive, both to be registered with `.egress(..)`.
pub struct Plugs {
    pub digest: EventDigest,
    pub archive: ArchiveEgress,
}

pub struct MatchingTierSm<C: MatchingCore> {
    inner: PipelineSm<C>,
    archive: Arc<Shared>,
    intake: Arc<Mutex<TierIntake>>,
    partitions: u32,
    /// Forwarded entries that failed to decode, skipped identically everywhere.
    pub bad_forwards: u64,
}

impl<C: MatchingCore> MatchingTierSm<C> {
    /// `build` constructs the pipeline (see [`PipelineSm::new`]); it must register
    /// both plugs, as `.egress(plugs.digest).egress(plugs.archive)`.
    pub fn new(
        book: BookConfig,
        map: PartitionMap,
        cfg: TierConfig,
        mut build: impl FnMut(Option<Initial<C>>, Plugs) -> Pipeline<C> + Send + 'static,
    ) -> (Self, TierView) {
        let partitions = map.partitions();
        let archive: Arc<Shared> = Arc::new(Mutex::new(ArchiveState::new(partitions, cfg.retain)));
        let intake = Arc::new(Mutex::new(TierIntake::default()));
        let a2 = archive.clone();
        let inner = PipelineSm::new(book, map, move |init, digest| {
            build(
                init,
                Plugs {
                    digest,
                    archive: ArchiveEgress { shared: a2.clone() },
                },
            )
        });
        let view = TierView {
            archive: archive.clone(),
            intake: intake.clone(),
            source_base: cfg.source_base,
        };
        (
            MatchingTierSm {
                inner,
                archive,
                intake,
                partitions,
                bad_forwards: 0,
            },
            view,
        )
    }

    /// Record every applied command batch, in order (tests and audit trails).
    pub fn with_tap(mut self, tap: Tap) -> Self {
        self.inner = self.inner.with_tap(tap);
        self
    }

    fn apply_forward(&mut self, index: Index, data: &[u8]) {
        let Ok(f) = decode_forward(data) else {
            self.bad_forwards += 1;
            return;
        };
        let mut cmds = Vec::new();
        {
            let mut intake = self.intake.lock().expect("intake lock");
            let mut archive = self.archive.lock().expect("archive lock");
            for (i, m) in f.msgs.iter().enumerate() {
                if intake.accept(f.shard, f.first_idx.wrapping_add(i as u64)) != Accept::Applied {
                    continue;
                }
                match *m {
                    OutMsg::Submit {
                        order,
                        symbol,
                        side,
                        price,
                        qty,
                    } => {
                        let price = i64::try_from(price).unwrap_or(-1);
                        let (mside, lside) = match side {
                            Side::Buy => (MSide::Bid, Side::Buy),
                            Side::Sell => (MSide::Ask, Side::Sell),
                        };
                        archive.sides.insert(order, lside);
                        cmds.push((symbol, Command::new(order, mside, price, qty, Tif::Gtc)));
                    }
                    OutMsg::Cancel { order, symbol } => {
                        cmds.push((symbol, Command::Cancel { order_id: order }));
                    }
                }
            }
        }
        if cmds.is_empty() {
            return;
        }
        let mut payload = Vec::new();
        encode_batch(&cmds, &mut payload);
        self.inner.apply(
            index,
            &Entry {
                term: 0,
                data: payload,
            },
        );
    }

    fn summary(&self, inner: [u8; 32], deep: bool) -> [u8; 32] {
        let intake = self.intake.lock().expect("intake lock");
        let a = self.archive.lock().expect("archive lock");
        let mut b = Vec::new();
        b.extend_from_slice(DIGEST_TAG);
        b.extend_from_slice(&inner);
        b.extend_from_slice(&self.partitions.to_le_bytes());
        intake.encode(&mut b);
        for p in &a.parts {
            b.extend_from_slice(&p.next.to_le_bytes());
        }
        b.extend_from_slice(&(a.sides.len() as u64).to_le_bytes());
        if deep {
            a.encode(&mut b);
        }
        sha256(&b)
    }
}

impl<C: MatchingCore> StateMachine for MatchingTierSm<C> {
    fn apply(&mut self, index: Index, entry: &Entry) {
        if entry.data.first() == Some(&TAG_FORWARD) {
            self.apply_forward(index, &entry.data);
        } else {
            self.inner.apply(index, entry);
        }
    }

    fn snapshot(&mut self, index: Index) -> Vec<u8> {
        // The inner snapshot drains the pipeline, so the archive is complete up
        // to this point when it is read below.
        let inner = self.inner.snapshot(index);
        let mut out = Vec::with_capacity(64 + inner.len());
        out.extend_from_slice(SNAP_MAGIC);
        out.extend_from_slice(&self.partitions.to_le_bytes());
        self.intake.lock().expect("intake lock").encode(&mut out);
        self.archive.lock().expect("archive lock").encode(&mut out);
        out.extend_from_slice(&(inner.len() as u64).to_le_bytes());
        out.extend_from_slice(&inner);
        out
    }

    fn restore(&mut self, index: Index, data: &[u8]) -> Result<(), String> {
        let bad = |e: crate::wire::WireError| format!("tier snapshot: {}", e.0);
        let mut r = Rd::new(data);
        if r.take(4).map_err(bad)? != SNAP_MAGIC {
            return Err("not a tier snapshot (XTS1)".into());
        }
        if r.u32().map_err(bad)? != self.partitions {
            return Err("tier snapshot: partition count".into());
        }
        let intake = TierIntake::decode(&mut r).map_err(bad)?;
        let archive = ArchiveState::decode(&mut r, self.partitions).map_err(bad)?;
        let n = r.u64().map_err(bad)? as usize;
        let inner = r.take(n).map_err(bad)?;
        r.done().map_err(bad)?;
        // The inner restore shuts the old pipeline down (flushing its last
        // events) before the new one exists; only then is the archive replaced.
        self.inner.restore(index, inner)?;
        *self.intake.lock().expect("intake lock") = intake;
        *self.archive.lock().expect("archive lock") = archive;
        Ok(())
    }

    fn digest(&mut self, index: Index) -> Option<[u8; 32]> {
        let inner = self.inner.digest(index)?;
        Some(self.summary(inner, false))
    }

    fn deep_digest(&mut self, index: Index) -> Option<[u8; 32]> {
        let inner = self.inner.deep_digest(index)?;
        Some(self.summary(inner, true))
    }
}
