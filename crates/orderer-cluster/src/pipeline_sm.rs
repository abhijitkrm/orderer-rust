//! The orderer pipeline as the replicated state machine.
//!
//! A committed log entry carries a batch of commands (`codec::encode_batch`).
//! Applying it publishes the batch into the local [`Pipeline`] in order, so
//! every replica's partitions see the same ingress order and, by the
//! pipeline's contract (`spec/PIPELINE.md` §3), emit byte-identical events.
//! The pipeline runs with journaling off here: the replicated log is the
//! journal (`spec/REPLICATION.md` §2).
//!
//! Divergence detection uses an [`EventDigest`]: a rolling hash over the events
//! each partition emits, so a digest costs one pipeline barrier, not a snapshot
//! of every book (`spec/REPLICATION.md` 13.16). [`StateMachine::deep_digest`] is
//! the old full-state hash, computed on demand.

use crate::codec::decode_batch;
use crate::replica::StateMachine;
use crate::sha256::sha256;
use orderer::recover::restore;
use orderer::{EventDigest, EventDigestHandle, Initial, PartitionMap, Pipeline, Snapshot};
use orderer_consensus::{Entry, Index};
use orderer_core::{BookConfig, MatchingCore, Symbol};
use std::sync::{Arc, Mutex};

type Build<C> = Box<dyn FnMut(Option<Initial<C>>, EventDigest) -> Pipeline<C> + Send>;

/// Marks the snapshot layout: `PSM2 | iseq u64 | partitions u32 | one chain u64
/// per partition | matcher-snap/1 body`.
const SNAP_MAGIC: &[u8; 4] = b"PSM2";

/// A replica's digest: SHA-256 over this tag, the number of commands applied,
/// the partition count and every partition's event chain.
const DIGEST_TAG: &[u8] = b"orderer-evdigest/1";

/// Called with every applied batch, in order (tests and audit trails).
pub type Tap = Arc<Mutex<Vec<(Index, Vec<(Symbol, orderer_core::Command)>)>>>;

pub struct PipelineSm<C: MatchingCore> {
    book: BookConfig,
    map: PartitionMap,
    build: Build<C>,
    pipeline: Option<Pipeline<C>>,
    chains: EventDigestHandle,
    partitions: u32,
    /// Commands applied so far (the pipeline's `iseq`).
    iseq: u64,
    tap: Option<Tap>,
    /// Entries that failed to decode. Every replica skips them identically,
    /// so determinism holds; a non-zero count means a buggy proposer.
    pub bad_entries: u64,
}

impl<C: MatchingCore> PipelineSm<C> {
    /// `build` constructs a pipeline, optionally from restored state. It must
    /// configure `book` and `map`'s partition count, no journaling, and
    /// register the [`EventDigest`] it is given as an egress
    /// (`.egress(digest)`).
    pub fn new(
        book: BookConfig,
        map: PartitionMap,
        mut build: impl FnMut(Option<Initial<C>>, EventDigest) -> Pipeline<C> + Send + 'static,
    ) -> Self {
        let partitions = map.partitions();
        let (digest, chains) = EventDigest::new(partitions, &[]);
        let pipeline = build(None, digest);
        PipelineSm {
            book,
            map,
            build: Box::new(build),
            pipeline: Some(pipeline),
            chains,
            partitions,
            iseq: 0,
            tap: None,
            bad_entries: 0,
        }
    }

    pub fn with_tap(mut self, tap: Tap) -> Self {
        self.tap = Some(tap);
        self
    }

    fn pipeline(&self) -> &Pipeline<C> {
        self.pipeline.as_ref().expect("pipeline present")
    }

    /// The state serialised for a snapshot: the old full-state encoding plus
    /// the event chains, so a replica that restores continues the same chains.
    fn encode_snapshot(&mut self) -> Vec<u8> {
        // Egress has seen everything before the snapshot is taken.
        self.pipeline().drain().expect("pipeline drain");
        let snap = self.pipeline().snapshot().expect("pipeline snapshot");
        let chains = self.chains.values();
        let mut out = Vec::with_capacity(16 + 8 * chains.len() + snap.body.len());
        out.extend_from_slice(SNAP_MAGIC);
        out.extend_from_slice(&snap.iseq.to_le_bytes());
        out.extend_from_slice(&snap.partitions.to_le_bytes());
        for c in &chains {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out.extend_from_slice(snap.body.as_bytes());
        out
    }
}

impl<C: MatchingCore> StateMachine for PipelineSm<C> {
    fn apply(&mut self, index: Index, entry: &Entry) {
        match decode_batch(&entry.data) {
            Ok(cmds) => {
                if !cmds.is_empty() {
                    self.pipeline()
                        .publish_batch(&cmds)
                        .expect("pipeline accepts committed commands");
                    self.iseq += cmds.len() as u64;
                    if let Some(t) = &self.tap {
                        t.lock().expect("tap lock").push((index, cmds));
                    }
                }
            }
            Err(_) => self.bad_entries += 1,
        }
    }

    fn snapshot(&mut self, _index: Index) -> Vec<u8> {
        self.encode_snapshot()
    }

    fn restore(&mut self, _index: Index, data: &[u8]) -> Result<(), String> {
        let head = 16 + 8 * self.partitions as usize;
        if data.len() < head || &data[..4] != SNAP_MAGIC {
            return Err("not a pipeline snapshot (PSM2)".into());
        }
        let iseq = u64::from_le_bytes(data[4..12].try_into().expect("8"));
        let partitions = u32::from_le_bytes(data[12..16].try_into().expect("4"));
        if partitions != self.partitions {
            return Err(format!(
                "snapshot has {partitions} partitions, this replica runs {}",
                self.partitions
            ));
        }
        let chains: Vec<u64> = (0..partitions as usize)
            .map(|i| u64::from_le_bytes(data[16 + 8 * i..24 + 8 * i].try_into().expect("8")))
            .collect();
        let body = String::from_utf8(data[head..].to_vec()).map_err(|e| e.to_string())?;
        let snap = Snapshot {
            body,
            iseq,
            partitions,
        };
        let (_, cores) =
            restore::<C>(self.book, &self.map, Some(&snap)).map_err(|e| e.to_string())?;
        if let Some(mut old) = self.pipeline.take() {
            old.shutdown().map_err(|e| e.to_string())?;
        }
        let (digest, handle) = EventDigest::new(partitions, &chains);
        self.chains = handle;
        self.iseq = iseq;
        self.pipeline = Some((self.build)(
            Some(Initial {
                cores,
                next_iseq: iseq + 1,
            }),
            digest,
        ));
        Ok(())
    }

    /// The event-chain digest: O(partitions) after one pipeline barrier.
    fn digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        self.pipeline().drain().expect("pipeline drain");
        let mut b = Vec::with_capacity(DIGEST_TAG.len() + 12 + 8 * self.partitions as usize);
        b.extend_from_slice(DIGEST_TAG);
        b.extend_from_slice(&self.iseq.to_le_bytes());
        b.extend_from_slice(&self.partitions.to_le_bytes());
        for c in self.chains.values() {
            b.extend_from_slice(&c.to_le_bytes());
        }
        Some(sha256(&b))
    }

    /// The full-state hash: SHA-256 of `iseq | partitions | matcher-snap/1 body`.
    /// Costs a snapshot of every book.
    fn deep_digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        self.pipeline().drain().expect("pipeline drain");
        let snap = self.pipeline().snapshot().expect("pipeline snapshot");
        let mut b = Vec::with_capacity(12 + snap.body.len());
        b.extend_from_slice(&snap.iseq.to_le_bytes());
        b.extend_from_slice(&snap.partitions.to_le_bytes());
        b.extend_from_slice(snap.body.as_bytes());
        Some(sha256(&b))
    }
}

impl<C: MatchingCore> Drop for PipelineSm<C> {
    fn drop(&mut self) {
        if let Some(mut p) = self.pipeline.take() {
            let _ = p.shutdown();
        }
    }
}
