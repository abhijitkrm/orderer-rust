//! orderer-exchange: the matching tier of the exchange.
//!
//! - [`MatchingTierSm`]: a replicated matching group that applies ledger-forwarded
//!   orders exactly once (an intake cursor per ledger shard) on top of the
//!   existing [`orderer_cluster::pipeline_sm::PipelineSm`];
//! - the event archive: per-partition settlement streams with deterministic
//!   indices, [`TierView::read`];
//! - [`ExchangeServer`] / [`ExchangeClient`]: a small framed TCP service to forward
//!   a ledger's outbox, read the intake cursor and read the archive.
//!
//! Design: `docs/design/LEDGER-RISK.md` (orderer repo), sections 4, 5 and 13.

#![forbid(unsafe_code)]

pub mod archive;
pub mod audit;
pub mod cli;
pub mod config;
mod frame;
pub mod gateway;
pub mod node;
pub mod relay;
pub mod secure;
pub mod server;
pub mod store;
pub mod tier;
pub mod tierclient;
pub mod wire;

pub use config::{ConfigError, ExchangeConfig, SymbolCfg};
pub use gateway::{GatewayClient, GatewayServer, LedgerState, OrderInfo};
pub use node::{demo_config, LedgerNodeSm, LedgerView, Reply};
pub use secure::Auth;
pub use tierclient::{SliceClient, TierClient};

pub use archive::TierView;
pub use archive::{map_event, ArchiveEgress, ArchiveState, Mapped, Truncated};
pub use server::{ExchangeClient, ExchangeServer, ForwardAck, Refusal, SliceAnswer, SliceState};
pub use store::{EventStore, StoreConfig, StoreStats, SyncPolicy};
pub use tier::{MatchingTierSm, Plugs, TierConfig, TierIntake};
pub use wire::{decode_forward, decode_span, encode_forward, encode_span, Forward, Span};
