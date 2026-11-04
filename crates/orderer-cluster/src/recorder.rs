//! A minimal, language-neutral state machine: it records every non-empty log
//! entry it applies. Its snapshot is the list `index u64 | len u32 | data`,
//! and its digest is the SHA-256 of that list. Ports of the replication
//! runtime implement the same machine, so replicas written in different
//! languages can share a cluster and still agree on state digests.

use crate::replica::StateMachine;
use crate::sha256::sha256;
use orderer_consensus::{Entry, Index};

#[derive(Default)]
pub struct RecorderSm {
    applied: Vec<(Index, Vec<u8>)>,
}

impl RecorderSm {
    pub fn new() -> Self {
        Self::default()
    }

    fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (i, d) in &self.applied {
            out.extend_from_slice(&i.to_le_bytes());
            out.extend_from_slice(&(d.len() as u32).to_le_bytes());
            out.extend_from_slice(d);
        }
        out
    }
}

impl StateMachine for RecorderSm {
    fn apply(&mut self, index: Index, entry: &Entry) {
        if !entry.data.is_empty() {
            self.applied.push((index, entry.data.clone()));
        }
    }

    fn snapshot(&mut self, _index: Index) -> Vec<u8> {
        self.serialize()
    }

    fn restore(&mut self, _index: Index, data: &[u8]) -> Result<(), String> {
        let mut v = Vec::new();
        let mut at = 0;
        while at < data.len() {
            if data.len() - at < 12 {
                return Err("short recorder snapshot".into());
            }
            let i = u64::from_le_bytes(data[at..at + 8].try_into().expect("8"));
            let n = u32::from_le_bytes(data[at + 8..at + 12].try_into().expect("4")) as usize;
            let end = at + 12 + n;
            if end > data.len() {
                return Err("short recorder snapshot".into());
            }
            v.push((i, data[at + 12..end].to_vec()));
            at = end;
        }
        self.applied = v;
        Ok(())
    }

    fn digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        Some(sha256(&self.serialize()))
    }
}
