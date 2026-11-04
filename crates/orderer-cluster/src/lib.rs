//! orderer-cluster — the replicated orderer: durable WAL and snapshot
//! storage, TCP transport and the node runtime around the
//! [`orderer_consensus`] protocol core (`spec/REPLICATION.md`).

#![forbid(unsafe_code)]

pub mod codec;
pub mod replica;
pub mod sha256;
pub mod snapstore;
pub mod transport;
pub mod wal;

pub use replica::{
    ProposeError, Replica, ReplicaConfig, ReplicaHandle, StartError, StateMachine, Status,
};
