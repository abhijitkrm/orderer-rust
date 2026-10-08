//! orderer — an LMAX-style, multi-core order-matching engine.
//!
//! Commands from any number of producer threads are sequenced on one
//! ingress ring, routed by symbol to `P` partitions, journaled before they
//! are applied, matched by each partition's single-writer
//! [`MatchingCore`](orderer_core::MatchingCore), and fanned out to pluggable
//! egress consumers. The contract is the orderer spec (`spec/PIPELINE.md`):
//! per symbol, the event stream is byte-identical to matcher's.
//!
//! ```
//! use orderer::*;
//! use orderer_core::*;
//!
//! let (collect, events) = Collect::new(true);
//! let mut p = Pipeline::<FifoCore>::builder()
//!     .partitions(2)
//!     .egress(collect)
//!     .build()
//!     .unwrap();
//! p.publish(7, Command::new(1, Side::Ask, 100, 10, Tif::Gtc)).unwrap();
//! p.publish(7, Command::new(2, Side::Bid, 100, 4, Tif::Gtc)).unwrap();
//! p.drain().unwrap();
//! let listing = String::from_utf8(events.listing()).unwrap();
//! assert!(listing.contains(r#"{"seq":2,"ev":"trade","symbol":7,"maker":1,"taker":2,"price":100,"qty":4}"#));
//! p.shutdown().unwrap();
//! ```

#![forbid(unsafe_code)]

pub mod egress;
#[doc(hidden)]
pub mod harness;
pub mod journal;
pub mod msg;
mod pipeline;
pub mod recover;
pub mod routing;
mod writer;

pub use egress::{
    Acks, Callback, Collect, CollectHandle, Egress, EgressCtx, EgressFactory, Metrics,
    MetricsHandle, PartitionMetrics,
};
pub use journal::{FsyncPolicy, JournalConfig, JournalFormat};
pub use msg::{Body, CmdMsg, Control, EvtBody, EvtMsg};
pub use orderer_disruptor::WaitStrategy;
pub use pipeline::{meta_path, Error, Handle, Initial, Pipeline, PipelineBuilder, Snapshot, Waits};
pub use routing::{hash_partition, PartitionMap, RoutingError};
