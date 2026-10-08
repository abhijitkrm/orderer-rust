//! Ring topology is fixed up front: declare consumers and their upstream
//! dependencies, then build the producer and all consumers at once. The
//! producer gates on the **terminal** consumers (those nothing depends
//! on) — upstream ones are always ahead of them.

use crate::barrier::Consumer;
use crate::producer::{MultiProducer, SingleProducer};
use crate::sequence::{Sequence, INITIAL};
use crate::sequencer::{ProducerKind, Shared};
use crate::sync::Arc;
use crate::wait::WaitStrategy;

/// Handle to a consumer declared on a [`RingBuilder`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerId(usize);

impl ConsumerId {
    /// Index of this consumer in the `Vec` returned by `build_*`.
    pub fn index(self) -> usize {
        self.0
    }
}

pub struct RingBuilder<T> {
    size: usize,
    consumers: Vec<(Vec<usize>, WaitStrategy)>,
    _t: std::marker::PhantomData<fn() -> T>,
}

impl<T: Default + Send> RingBuilder<T> {
    /// `size` must be a power of two.
    pub fn new(size: usize) -> RingBuilder<T> {
        assert!(size.is_power_of_two(), "ring size must be a power of two");
        RingBuilder {
            size,
            consumers: Vec::new(),
            _t: std::marker::PhantomData,
        }
    }

    /// Declare a consumer that may only read what every consumer in `deps`
    /// has already processed (and, with no deps, what is published).
    pub fn consumer(&mut self, deps: &[ConsumerId]) -> ConsumerId {
        self.consumer_with(deps, WaitStrategy::default())
    }

    pub fn consumer_with(&mut self, deps: &[ConsumerId], wait: WaitStrategy) -> ConsumerId {
        let id = self.consumers.len();
        for d in deps {
            assert!(d.0 < id, "dependencies must be declared first");
        }
        self.consumers
            .push((deps.iter().map(|d| d.0).collect(), wait));
        ConsumerId(id)
    }

    fn build(self, kind: ProducerKind) -> (Arc<Shared<T>>, Vec<Consumer<T>>) {
        let seqs: Vec<Arc<Sequence>> = (0..self.consumers.len())
            .map(|_| Arc::new(Sequence::new(INITIAL)))
            .collect();
        let mut depended = vec![false; self.consumers.len()];
        for (deps, _) in &self.consumers {
            for &d in deps {
                depended[d] = true;
            }
        }
        let gating: Box<[Arc<Sequence>]> = seqs
            .iter()
            .zip(&depended)
            .filter(|(_, &d)| !d)
            .map(|(s, _)| s.clone())
            .collect();
        let shared = Arc::new(Shared::new(self.size, kind, gating));
        let consumers = self
            .consumers
            .into_iter()
            .enumerate()
            .map(|(i, (deps, wait))| {
                let deps = deps.iter().map(|&d| seqs[d].clone()).collect();
                Consumer::new(shared.clone(), deps, seqs[i].clone(), wait)
            })
            .collect();
        (shared, consumers)
    }

    /// Build with one producer thread.
    pub fn build_single(self) -> (SingleProducer<T>, Vec<Consumer<T>>) {
        let (shared, consumers) = self.build(ProducerKind::Single);
        (SingleProducer::new(shared), consumers)
    }

    /// Build for any number of producer threads (clone the producer).
    pub fn build_multi(self) -> (MultiProducer<T>, Vec<Consumer<T>>) {
        let (shared, consumers) = self.build(ProducerKind::Multi);
        (MultiProducer::new(shared), consumers)
    }
}
