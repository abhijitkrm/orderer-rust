//! Exactly-once effect for client retries.
//!
//! A client that loses a connection mid-request does not know whether its
//! batch committed (`ProposeError::Unknown`). With sessions it retries the
//! **same** `(client, seq)` until it gets a definite answer. Every replica
//! wraps its state machine in [`DedupSm`], which applies a session's batch
//! only when `seq` is newer than the last it applied for that client, so a
//! retry that lands in the log twice takes effect once. The rule is a pure
//! function of the log, so all replicas skip the same entries, and the table
//! is part of every snapshot and state digest.
//!
//! Contract for clients: one session id is one sequential stream. Do not
//! send `seq + 1` before `seq` has a definite answer; the client library
//! enforces that.

use crate::codec::session_of;
use crate::replica::StateMachine;
use crate::sha256::sha256;
use orderer_consensus::{Entry, Index};
use std::collections::BTreeMap;

pub struct DedupSm<S: StateMachine> {
    inner: S,
    last: BTreeMap<u64, u64>,
    /// Entries skipped as duplicates (observability and tests).
    pub duplicates: u64,
}

impl<S: StateMachine> DedupSm<S> {
    pub fn new(inner: S) -> Self {
        DedupSm {
            inner,
            last: BTreeMap::new(),
            duplicates: 0,
        }
    }

    fn table_bytes(&self) -> Vec<u8> {
        let mut out = (self.last.len() as u32).to_le_bytes().to_vec();
        for (c, s) in &self.last {
            out.extend_from_slice(&c.to_le_bytes());
            out.extend_from_slice(&s.to_le_bytes());
        }
        out
    }
}

impl<S: StateMachine> StateMachine for DedupSm<S> {
    fn apply(&mut self, index: Index, entry: &Entry) {
        if let Some((client, seq)) = session_of(&entry.data) {
            let last = self.last.entry(client).or_insert(0);
            if seq <= *last {
                self.duplicates += 1;
                return;
            }
            *last = seq;
        }
        self.inner.apply(index, entry);
    }

    fn snapshot(&mut self, index: Index) -> Vec<u8> {
        let table = self.table_bytes();
        let inner = self.inner.snapshot(index);
        let mut out = Vec::with_capacity(table.len() + 8 + inner.len());
        out.extend_from_slice(&table);
        out.extend_from_slice(&(inner.len() as u64).to_le_bytes());
        out.extend_from_slice(&inner);
        out
    }

    fn restore(&mut self, index: Index, data: &[u8]) -> Result<(), String> {
        let bad = || "short dedup snapshot".to_string();
        let n = u32::from_le_bytes(data.get(..4).ok_or_else(bad)?.try_into().expect("4")) as usize;
        let table_end = 4 + n * 16;
        if data.len() < table_end + 8 {
            return Err(bad());
        }
        let mut last = BTreeMap::new();
        for i in 0..n {
            let at = 4 + i * 16;
            last.insert(
                u64::from_le_bytes(data[at..at + 8].try_into().expect("8")),
                u64::from_le_bytes(data[at + 8..at + 16].try_into().expect("8")),
            );
        }
        let len =
            u64::from_le_bytes(data[table_end..table_end + 8].try_into().expect("8")) as usize;
        let inner = data
            .get(table_end + 8..table_end + 8 + len)
            .ok_or_else(bad)?;
        self.inner.restore(index, inner)?;
        self.last = last;
        Ok(())
    }

    fn digest(&mut self, index: Index) -> Option<[u8; 32]> {
        let inner = self.inner.digest(index)?;
        let mut b = self.table_bytes();
        b.extend_from_slice(&inner);
        Some(sha256(&b))
    }

    fn deep_digest(&mut self, index: Index) -> Option<[u8; 32]> {
        let inner = self.inner.deep_digest(index)?;
        let mut b = self.table_bytes();
        b.extend_from_slice(&inner);
        Some(sha256(&b))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{encode_batch, encode_session_batch};
    use orderer_core::{Command, Side, Tif};

    #[derive(Default)]
    struct Count(Vec<Index>);
    impl StateMachine for Count {
        fn apply(&mut self, i: Index, _: &Entry) {
            self.0.push(i);
        }
        fn snapshot(&mut self, _: Index) -> Vec<u8> {
            self.0.iter().flat_map(|i| i.to_le_bytes()).collect()
        }
        fn restore(&mut self, _: Index, d: &[u8]) -> Result<(), String> {
            self.0 = d
                .chunks(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
                .collect();
            Ok(())
        }
        fn digest(&mut self, i: Index) -> Option<[u8; 32]> {
            Some(sha256(&self.snapshot(i)))
        }
    }

    fn entry(client: u64, seq: u64) -> Entry {
        let cmds = vec![(1, Command::new(seq, Side::Bid, 10, 1, Tif::Gtc))];
        let mut d = Vec::new();
        encode_session_batch(client, seq, &cmds, &mut d);
        Entry { term: 1, data: d }
    }

    /// A state machine whose cheap and deep digests differ, to prove a wrapper
    /// forwards each to the matching one (a wrapper that forwards only `digest`
    /// silently answers the on-demand state hash with the cheap one).
    struct TwoDigests;
    impl StateMachine for TwoDigests {
        fn apply(&mut self, _: Index, _: &Entry) {}
        fn snapshot(&mut self, _: Index) -> Vec<u8> {
            Vec::new()
        }
        fn restore(&mut self, _: Index, _: &[u8]) -> Result<(), String> {
            Ok(())
        }
        fn digest(&mut self, _: Index) -> Option<[u8; 32]> {
            Some([1; 32])
        }
        fn deep_digest(&mut self, _: Index) -> Option<[u8; 32]> {
            Some([2; 32])
        }
    }

    #[test]
    fn the_deep_digest_is_forwarded_not_replaced_by_the_cheap_one() {
        let mut sm = DedupSm::new(TwoDigests);
        let (cheap, deep) = (sm.digest(1).unwrap(), sm.deep_digest(1).unwrap());
        assert_ne!(cheap, deep);
        let mut b = sm.table_bytes();
        b.extend_from_slice(&[2; 32]);
        assert_eq!(
            deep,
            sha256(&b),
            "deep digest does not come from the inner deep digest"
        );
    }

    #[test]
    fn a_retried_batch_takes_effect_once() {
        let mut sm = DedupSm::new(Count::default());
        sm.apply(1, &entry(7, 1));
        sm.apply(2, &entry(7, 1)); // the retry, committed again
        sm.apply(3, &entry(7, 2));
        sm.apply(4, &entry(8, 1)); // another client's seq 1 is its own
        sm.apply(5, &entry(7, 2));
        assert_eq!(sm.inner.0, vec![1, 3, 4]);
        assert_eq!(sm.duplicates, 2);
    }

    #[test]
    fn plain_batches_and_noops_are_never_deduplicated() {
        let mut sm = DedupSm::new(Count::default());
        let mut plain = Vec::new();
        encode_batch(
            &[(1, Command::new(1, Side::Bid, 10, 1, Tif::Gtc))],
            &mut plain,
        );
        let e = Entry {
            term: 1,
            data: plain,
        };
        sm.apply(1, &e);
        sm.apply(2, &e);
        sm.apply(
            3,
            &Entry {
                term: 1,
                data: vec![],
            },
        );
        assert_eq!(sm.inner.0, vec![1, 2, 3]);
    }

    #[test]
    fn the_table_survives_snapshot_and_restore_and_feeds_the_digest() {
        let mut a = DedupSm::new(Count::default());
        a.apply(1, &entry(7, 1));
        a.apply(2, &entry(7, 2));
        let snap = a.snapshot(2);
        let mut b = DedupSm::new(Count::default());
        b.restore(2, &snap).unwrap();
        // A retry of an already-applied seq is still skipped after restore.
        b.apply(3, &entry(7, 2));
        assert_eq!(b.duplicates, 1);
        assert_eq!(a.digest(2), b.digest(2));
        // Same inner state but a different table must differ.
        let mut c = DedupSm::new(Count::default());
        c.restore(2, &snap).unwrap();
        c.last.insert(99, 1);
        assert_ne!(a.digest(2), c.digest(2));
        assert!(b.restore(2, &snap[..3]).is_err());
    }
}
