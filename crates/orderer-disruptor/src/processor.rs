//! Event processors: the consumer loop, packaged.
//!
//! `EventProcessor::run` is LMAX's `BatchEventProcessor`: wait for the
//! barrier, hand the handler every available event (`end_of_batch` marks
//! the last), publish the watermark once per batch.

use crate::barrier::Consumer;

/// Receives every event of a ring, in sequence order.
pub trait EventHandler<T>: Send {
    fn on_event(&mut self, event: &T, seq: i64, end_of_batch: bool);

    /// Called once when the processor stops (ring alerted).
    fn on_shutdown(&mut self) {}
}

impl<T, F: FnMut(&T, i64, bool) + Send> EventHandler<T> for F {
    #[inline]
    fn on_event(&mut self, event: &T, seq: i64, end_of_batch: bool) {
        self(event, seq, end_of_batch)
    }
}

pub struct EventProcessor;

impl EventProcessor {
    /// Run `handler` over `consumer` until the ring is alerted and drained
    /// of everything already available.
    pub fn run<T, H: EventHandler<T> + ?Sized>(consumer: &mut Consumer<T>, handler: &mut H) {
        while consumer
            .wait_poll(|ev, seq, eob| handler.on_event(ev, seq, eob))
            .is_ok()
        {}
        handler.on_shutdown();
    }
}

/// Drains several rings round-robin on one thread — e.g. one egress
/// thread serving every partition's outbox. Each ring's events stay in
/// order; rings interleave by batch.
pub struct MultiRingProcessor<T> {
    consumers: Vec<Consumer<T>>,
}

impl<T> MultiRingProcessor<T> {
    pub fn new(consumers: Vec<Consumer<T>>) -> MultiRingProcessor<T> {
        assert!(!consumers.is_empty());
        MultiRingProcessor { consumers }
    }

    /// One pass over every ring: `f(ring_index, event, seq, end_of_batch)`.
    /// Returns events processed.
    pub fn poll(&mut self, mut f: impl FnMut(usize, &T, i64, bool)) -> usize {
        let mut n = 0;
        for (i, c) in self.consumers.iter_mut().enumerate() {
            n += c.poll(|ev, seq, eob| f(i, ev, seq, eob));
        }
        n
    }

    /// Poll until alerted, idling (first ring's wait strategy) when every
    /// ring is empty.
    pub fn run(&mut self, mut f: impl FnMut(usize, &T, i64, bool)) {
        loop {
            if self.poll(&mut f) > 0 {
                self.consumers[0].reset_idle();
                continue;
            }
            if self.consumers.iter().all(|c| c.is_alerted()) {
                // final sweep: anything published before the alert
                while self.poll(&mut f) > 0 {}
                return;
            }
            self.consumers[0].idle();
        }
    }

    pub fn into_consumers(self) -> Vec<Consumer<T>> {
        self.consumers
    }
}
