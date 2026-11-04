//! orderer-consensus — the replicated-log protocol of `spec/REPLICATION.md`,
//! as a pure state machine ([`core`]) plus a deterministic cluster
//! simulator ([`sim`]) that checks its safety properties under injected
//! faults.
//!
//! Nothing here touches the network, the disk or a clock. Integration with
//! the pipeline (ingress log, commit-gated apply, transport) is a later
//! phase; this crate is what that integration will trust.

#![forbid(unsafe_code)]

pub mod core;
pub mod rng;
pub mod sim;
pub mod trace;

pub use crate::core::{
    Config, Disk, Effect, Entry, Index, Msg, Mutation, Node, NodeId, Persist, Role, Term,
};
