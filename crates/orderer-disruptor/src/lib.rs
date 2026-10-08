//! orderer-disruptor — the LMAX Disruptor in Rust.
//!
//! A preallocated ring of slots, one or many producers claiming sequence
//! numbers, and consumers that each track how far they have read. Consumers
//! can depend on each other (a journal stage before an engine stage), and
//! the producer never laps the slowest terminal consumer. Batching falls out
//! naturally: a consumer handles everything available in one pass and
//! publishes its watermark once.
//!
//! Names follow LMAX so ports map one-to-one: [`Sequence`], [`RingBuffer`],
//! sequencers ([`ProducerKind`]), [`Consumer`] (a `SequenceBarrier` plus its
//! own sequence), [`WaitStrategy`], [`EventProcessor`].
//!
//! ```
//! use orderer_disruptor::*;
//!
//! let mut b = RingBuilder::<u64>::new(64);
//! let journal = b.consumer(&[]);
//! let engine = b.consumer(&[journal]); // sees an event only after `journal` did
//! let (mut producer, mut consumers) = b.build_single();
//! producer.publish(|slot| *slot = 7).unwrap();
//!
//! let mut seen = 0;
//! assert_eq!(consumers[engine.index()].poll(|v, _, _| seen = *v), 0); // gated
//! consumers[journal.index()].poll(|_, _, _| {});
//! consumers[engine.index()].poll(|v, _, _| seen = *v);
//! assert_eq!(seen, 7);
//! ```
//!
//! `unsafe` lives only in [`ring`] — see its protocol notes.

#![deny(unsafe_code)]

mod barrier;
mod builder;
#[cfg(not(loom))]
mod dsl;
mod processor;
mod producer;
pub mod ring;
mod sequence;
mod sequencer;
mod sync;
mod wait;

pub use barrier::{Alerted, Consumer, DEFAULT_MAX_BATCH};
pub use builder::{ConsumerId, RingBuilder};
#[cfg(not(loom))]
pub use dsl::{Disruptor, DisruptorHandle};
pub use processor::{EventHandler, EventProcessor, MultiRingProcessor};
pub use producer::{MultiProducer, PublishError, RingControl, SingleProducer};
pub use ring::RingBuffer;
pub use sequence::{Sequence, INITIAL};
pub use sequencer::ProducerKind;
pub use wait::WaitStrategy;
