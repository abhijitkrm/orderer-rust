//! orderer-core — the matching semantics of the orderer engine.
//!
//! A vendored port of [matcher-rust](https://github.com/abhijitkrm/matcher-rust)
//! (`docs/VENDORED.md`): a deterministic, zero-allocation FIFO limit order
//! book and multi-symbol `Engine`, unchanged, plus the [`MatchingCore`] seam
//! the orderer pipeline drives each partition through.
//!
//! Semantics are defined by `spec/matcher/SPEC.md`; the golden corpus in
//! `vectors/matcher/` proves byte-identical event streams.
//!
//! ```
//! use orderer_core::*;
//!
//! let mut book = OrderBook::new(BookConfig::default());
//! let mut sink = VecSink::new();
//!
//! book.apply(Command::new(1, Side::Ask, 100, 10, Tif::Gtc), &mut sink);
//! book.apply(Command::new(2, Side::Bid, 100, 4, Tif::Gtc), &mut sink);
//!
//! // order 2 filled 4 @100 against order 1 and closed; order 1 keeps 6 resting.
//! assert_eq!(book.order(1).unwrap().qty, 6);
//! assert!(book.order(2).is_none());
//! ```

#![forbid(unsafe_code)]

mod book;
mod core;
mod engine;
mod index;
mod level;
mod ordermap;
mod pool;
mod sink;
mod types;

pub mod journal;
#[doc(hidden)]
pub mod jsonflat;
pub mod snapshot;

pub use crate::core::{FifoCore, MatchingCore, NoopCore, RestoreError};
pub use book::{OrderBook, OrderInfo};
pub use engine::Engine;
pub use sink::{LinesSink, NullSink, Sink, VecSink};
pub use types::*;
