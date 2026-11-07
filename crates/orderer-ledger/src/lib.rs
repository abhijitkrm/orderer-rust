//! orderer-ledger — balances and pre-trade risk for the orderer exchange
//! (`docs/design/LEDGER-RISK.md`, phase 1: reserve, then match).
//!
//! [`Ledger`] is a deterministic state machine with no I/O: the same command
//! stream always produces the same state, outcomes and digest. [`LedgerSm`]
//! plugs it into the replication runtime.
//!
//! Money is `u128` in an asset's smallest unit; there is no floating point.
//! A symbol trades `qty` lots of `lot` base atoms at `price` quote atoms per
//! lot, so every amount is an exact product of integers.

#![forbid(unsafe_code)]

mod codec;
mod ledger;
mod sm;

pub use codec::{decode_cmd, encode_cmd, DecodeError};
pub use ledger::{
    order_shard, Amount, AssetId, Balance, Cmd, Config, Event, Ledger, Order, OrderId, OutMsg,
    Outcome, Reject, Side, Symbol, SymbolId, SHARD_SHIFT,
};
pub use sm::LedgerSm;
