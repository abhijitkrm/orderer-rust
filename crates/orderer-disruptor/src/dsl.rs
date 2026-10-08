//! A small LMAX-style DSL: declare handler stages, get a producer and a
//! handle that owns one thread per handler.
//!
//! ```
//! use orderer_disruptor::*;
//! use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
//!
//! let total = Arc::new(AtomicI64::new(0));
//! let t = total.clone();
//! let (mut producer, handle) = Disruptor::<i64>::new(1024)
//!     .handle(vec![Box::new(|_: &i64, _seq: i64, _eob: bool| {})])    // stage 1 (e.g. journal)
//!     .then(vec![Box::new(move |v: &i64, _seq: i64, _eob: bool| {     // stage 2 sees only
//!         t.fetch_add(*v, Ordering::Relaxed);                          // what stage 1 passed
//!     })])
//!     .spawn_single();
//! for i in 1..=100 {
//!     producer.publish(|slot| *slot = i).unwrap();
//! }
//! handle.shutdown(); // drains, then stops and joins
//! assert_eq!(total.load(Ordering::Relaxed), 5050);
//! ```

use std::thread::JoinHandle;

use crate::barrier::Consumer;
use crate::builder::{ConsumerId, RingBuilder};
use crate::processor::{EventHandler, EventProcessor};
use crate::producer::{MultiProducer, RingControl, SingleProducer};
use crate::sequence::Sequence;
use crate::sync::Arc;
use crate::wait::WaitStrategy;

type BoxedHandler<T> = Box<dyn EventHandler<T>>;

pub struct Disruptor<T> {
    builder: RingBuilder<T>,
    handlers: Vec<(ConsumerId, BoxedHandler<T>)>,
    last_stage: Vec<ConsumerId>,
    wait: WaitStrategy,
}

impl<T: Default + Send + 'static> Disruptor<T> {
    pub fn new(size: usize) -> Disruptor<T> {
        Disruptor {
            builder: RingBuilder::new(size),
            handlers: Vec::new(),
            last_stage: Vec::new(),
            wait: WaitStrategy::default(),
        }
    }

    /// Wait strategy for handlers declared after this call.
    pub fn wait_strategy(mut self, wait: WaitStrategy) -> Self {
        self.wait = wait;
        self
    }

    /// A first stage: these handlers read published events in parallel.
    pub fn handle(mut self, handlers: Vec<BoxedHandler<T>>) -> Self {
        self.stage(&[], handlers);
        self
    }

    /// A stage gated on the previous one: each handler sees an event only
    /// after every handler of the previous stage has processed it.
    pub fn then(mut self, handlers: Vec<BoxedHandler<T>>) -> Self {
        let deps = std::mem::take(&mut self.last_stage);
        self.stage(&deps, handlers);
        self
    }

    fn stage(&mut self, deps: &[ConsumerId], handlers: Vec<BoxedHandler<T>>) {
        self.last_stage.clear();
        for h in handlers {
            let id = self.builder.consumer_with(deps, self.wait);
            self.last_stage.push(id);
            self.handlers.push((id, h));
        }
    }

    fn spawn(handlers: Vec<(ConsumerId, BoxedHandler<T>)>, consumers: Vec<Consumer<T>>) -> Workers {
        let seqs = consumers.iter().map(|c| c.sequence()).collect();
        let mut by_id: Vec<Option<Consumer<T>>> = consumers.into_iter().map(Some).collect();
        let threads = handlers
            .into_iter()
            .map(|(id, mut h)| {
                let mut c = by_id[id.index()].take().expect("one handler per consumer");
                std::thread::Builder::new()
                    .name(format!("disruptor-{}", id.index()))
                    .spawn(move || EventProcessor::run(&mut c, &mut *h))
                    .expect("spawn handler thread")
            })
            .collect();
        Workers { threads, seqs }
    }

    pub fn spawn_single(self) -> (SingleProducer<T>, DisruptorHandle<T>) {
        let (producer, consumers) = self.builder.build_single();
        let control = producer.control();
        let workers = Self::spawn(self.handlers, consumers);
        (producer, DisruptorHandle { control, workers })
    }

    pub fn spawn_multi(self) -> (MultiProducer<T>, DisruptorHandle<T>) {
        let (producer, consumers) = self.builder.build_multi();
        let control = producer.control();
        let workers = Self::spawn(self.handlers, consumers);
        (producer, DisruptorHandle { control, workers })
    }
}

struct Workers {
    threads: Vec<JoinHandle<()>>,
    seqs: Vec<Arc<Sequence>>,
}

/// Owns the handler threads.
pub struct DisruptorHandle<T> {
    control: RingControl<T>,
    workers: Workers,
}

impl<T> DisruptorHandle<T> {
    /// Wait until every handler has processed everything published so far.
    /// Producers must have stopped publishing (and committed staged work).
    pub fn drain(&self) {
        let target = self.control.published();
        let mut step = 0u32;
        while self.workers.seqs.iter().any(|s| s.get() < target) {
            step = step.saturating_add(1);
            if step < 128 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }

    /// Drain, then stop and join every handler thread.
    pub fn shutdown(self) {
        self.drain();
        self.control.alert();
        for t in self.workers.threads {
            t.join().expect("handler thread panicked");
        }
    }

    pub fn control(&self) -> &RingControl<T> {
        &self.control
    }
}
