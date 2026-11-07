//! Adapter that runs a [`Ledger`] as a replicated state machine.

use crate::codec::decode_cmd;
use crate::ledger::Ledger;
use orderer_cluster::StateMachine;
use orderer_consensus::{Entry, Index};

/// Applies each non-empty log entry as one encoded command. An entry that does
/// not decode is counted as an anomaly and otherwise ignored, identically on
/// every replica.
pub struct LedgerSm {
    pub ledger: Ledger,
}

impl LedgerSm {
    pub fn new(ledger: Ledger) -> Self {
        LedgerSm { ledger }
    }
}

impl StateMachine for LedgerSm {
    fn apply(&mut self, _index: Index, entry: &Entry) {
        if entry.data.is_empty() {
            return;
        }
        match decode_cmd(&entry.data) {
            Ok(c) => {
                self.ledger.apply(&c);
            }
            Err(_) => {
                self.ledger.note_undecodable();
            }
        }
    }
    fn snapshot(&mut self, _index: Index) -> Vec<u8> {
        self.ledger.snapshot()
    }
    fn restore(&mut self, _index: Index, data: &[u8]) -> Result<(), String> {
        self.ledger.restore(data)
    }
    fn digest(&mut self, _index: Index) -> Option<[u8; 32]> {
        Some(self.ledger.digest())
    }
}
