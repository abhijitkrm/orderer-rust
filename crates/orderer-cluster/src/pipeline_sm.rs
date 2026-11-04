//! The orderer pipeline as the replicated state machine.
//!
//! A committed log entry carries a batch of commands (`codec::encode_batch`).
//! Applying it publishes the batch into the local [`Pipeline`] in order, so
//! every replica's partitions see the same ingress order and, by the
//! pipeline's contract (`spec/PIPELINE.md` §3), emit byte-identical events.
//! The pipeline runs with journaling off here: the replicated log is the
//! journal (`spec/REPLICATION.md` §2).

use crate::codec::decode_batch;
use crate::replica::StateMachine;
use crate::sha256::sha256;
use orderer::recover::restore;
use orderer::{Initial, PartitionMap, Pipeline, Snapshot};
use orderer_consensus::{Entry, Index};
use orderer_core::{BookConfig, MatchingCore, Symbol};
use std::sync::{Arc, Mutex};

type Build<C> = Box<dyn FnMut(Option<Initial<C>>) -> Pipeline<C> + Send>;

/// Called with every applied batch, in order (tests and audit trails).
pub type Tap = Arc<Mutex<Vec<(Index, Vec<(Symbol, orderer_core::Command)>)>>>;

pub struct PipelineSm<C: MatchingCore> {
    book: BookConfig,
    map: PartitionMap,
    build: Build<C>,
    pipeline: Option<Pipeline<C>>,
    tap: Option<Tap>,
    /// Entries that failed to decode. Every replica skips them identically,
    /// so determinism holds; a non-zero count means a buggy proposer.
    pub bad_entries: u64,
}

impl<C: MatchingCore> PipelineSm<C> {
    /// `build` constructs a pipeline, optionally from restored state. It must
    /// configure `book` and `map`'s partition count, and no journaling.
    pub fn new(
        book: BookConfig,
        map: PartitionMap,
        mut build: impl FnMut(Option<Initial<C>>) -> Pipeline<C> + Send + 'static,
    ) -> Self {
        let pipeline = build(None);
        PipelineSm {
            book,
            map,
            build: Box::new(build),
            pipeline: Some(pipeline),
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

    fn encode_state(&mut self) -> Vec<u8> {
        // Egress has seen everything before the snapshot is taken.
        self.pipeline().drain().expect("pipeline drain");
        let snap = self.pipeline().snapshot().expect("pipeline snapshot");
        let mut out = Vec::with_capacity(12 + snap.body.len());
        out.extend_from_slice(&snap.iseq.to_le_bytes());
        out.extend_from_slice(&snap.partitions.to_le_bytes());
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
                    if let Some(t) = &self.tap {
                        t.lock().expect("tap lock").push((index, cmds));
                    }
                }
            }
            Err(_) => self.bad_entries += 1,
        }
    }

    fn snapshot(&mut self, _index: Index) -> Vec<u8> {
        self.encode_state()
    }

    fn restore(&mut self, _index: Index, data: &[u8]) -> Result<(), String> {
        if data.len() < 12 {
            return Err("short pipeline snapshot".into());
        }
        let iseq = u64::from_le_bytes(data[..8].try_into().expect("8"));
        let partitions = u32::from_le_bytes(data[8..12].try_into().expect("4"));
        let body = String::from_utf8(data[12..].to_vec()).map_err(|e| e.to_string())?;
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
        self.pipeline = Some((self.build)(Some(Initial {
            cores,
            next_iseq: iseq + 1,
        })));
        Ok(())
    }

    fn digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        Some(sha256(&self.encode_state()))
    }
}

impl<C: MatchingCore> Drop for PipelineSm<C> {
    fn drop(&mut self) {
        if let Some(mut p) = self.pipeline.take() {
            let _ = p.shutdown();
        }
    }
}
