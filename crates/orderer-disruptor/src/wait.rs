//! Wait strategies — what a consumer does while nothing is available.
//!
//! The trade is latency vs. CPU: `BusySpin` reacts in nanoseconds and burns
//! a core; `Blocking` sleeps on a condvar and costs a syscall to wake.
//! Strategies are per consumer, so a pipeline can spin on its hot path
//! (engine threads) and park everywhere else.

use std::time::Duration;

use crate::sync::{spin_hint, yield_now, AtomicUsize, Condvar, Mutex, Ordering};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitStrategy {
    /// Spin with a CPU pause hint. Lowest latency; one core per waiter.
    BusySpin,
    /// Spin briefly, then `yield` to the OS scheduler each step.
    Yield,
    /// Spin `spin` times, yield `yields` times, then sleep with exponential
    /// backoff from `park_min` to `park_max`. Good default for threads off
    /// the hot path (journal, egress).
    Backoff {
        spin: u32,
        yields: u32,
        park_min: Duration,
        park_max: Duration,
    },
    /// Sleep on a condvar until a producer (or an upstream consumer)
    /// signals. Lowest CPU; wakeups cost a syscall. The wait is bounded
    /// (1 ms) so a missed signal only delays, never hangs.
    Blocking,
}

impl WaitStrategy {
    /// `Backoff` with defaults tuned for idle-mostly threads.
    pub const fn backoff() -> WaitStrategy {
        WaitStrategy::Backoff {
            spin: 256,
            yields: 64,
            park_min: Duration::from_micros(20),
            park_max: Duration::from_millis(1),
        }
    }
}

impl Default for WaitStrategy {
    fn default() -> Self {
        WaitStrategy::backoff()
    }
}

const BLOCKING_TIMEOUT: Duration = Duration::from_millis(1);
const YIELD_SPINS: u32 = 100;

/// Wakes `Blocking` waiters. Signalling is one relaxed load when nobody
/// is blocked, so producers can call it on every publish.
#[derive(Default)]
pub(crate) struct Notifier {
    waiters: AtomicUsize,
    lock: Mutex<()>,
    cv: Condvar,
}

impl Notifier {
    #[inline(always)]
    pub(crate) fn signal(&self) {
        if self.waiters.load(Ordering::Relaxed) != 0 {
            self.wake_all();
        }
    }

    #[cold]
    pub(crate) fn wake_all(&self) {
        let _g = self.lock.lock().unwrap();
        self.cv.notify_all();
    }

    fn block(&self) {
        self.waiters.fetch_add(1, Ordering::SeqCst);
        {
            let g = self.lock.lock().unwrap();
            #[cfg(not(loom))]
            drop(self.cv.wait_timeout(g, BLOCKING_TIMEOUT).unwrap());
            #[cfg(loom)]
            {
                drop(g);
                yield_now();
            }
        }
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Per-waiter strategy state (backoff progress).
pub(crate) struct Waiter {
    strategy: WaitStrategy,
    step: u32,
    park: Duration,
}

impl Waiter {
    pub(crate) fn new(strategy: WaitStrategy) -> Waiter {
        Waiter {
            strategy,
            step: 0,
            park: Duration::ZERO,
        }
    }

    pub(crate) fn set(&mut self, strategy: WaitStrategy) {
        *self = Waiter::new(strategy);
    }

    pub(crate) fn strategy(&self) -> WaitStrategy {
        self.strategy
    }

    /// Call after making progress: the next idle restarts from spinning.
    #[inline(always)]
    pub(crate) fn reset(&mut self) {
        self.step = 0;
    }

    /// One idle step — call between availability checks.
    #[inline]
    pub(crate) fn idle(&mut self, notifier: &Notifier) {
        match self.strategy {
            WaitStrategy::BusySpin => spin_hint(),
            WaitStrategy::Yield => {
                if self.step < YIELD_SPINS {
                    self.step += 1;
                    spin_hint();
                } else {
                    yield_now();
                }
            }
            WaitStrategy::Backoff {
                spin,
                yields,
                park_min,
                park_max,
            } => {
                if self.step < spin {
                    self.step += 1;
                    spin_hint();
                } else if self.step < spin + yields {
                    self.step += 1;
                    yield_now();
                } else {
                    if self.step == spin + yields {
                        self.step += 1;
                        self.park = park_min;
                    }
                    crate::sync::sleep(self.park);
                    self.park = (self.park * 2).min(park_max);
                }
            }
            WaitStrategy::Blocking => {
                if self.step < YIELD_SPINS {
                    self.step += 1;
                    spin_hint();
                } else {
                    notifier.block();
                }
            }
        }
    }
}
