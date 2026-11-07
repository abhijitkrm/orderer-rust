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
pub mod server;
pub mod tier;
pub mod wire;

pub use archive::TierView;
pub use archive::{map_event, ArchiveEgress, ArchiveState, Mapped, Truncated};
pub use server::{ExchangeClient, ExchangeServer, ForwardAck, Refusal};
pub use tier::{MatchingTierSm, Plugs, TierConfig, TierIntake};
pub use wire::{decode_forward, encode_forward, Forward};
