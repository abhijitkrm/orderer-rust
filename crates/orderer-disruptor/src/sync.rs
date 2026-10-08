//! Synchronization shim: std primitives normally, `loom`'s under
//! `--cfg loom` so the ring protocol can be model-checked
//! (`tests/loom.rs`). Every atomic, cell and wait in this crate goes
//! through here.

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::{Arc, Condvar, Mutex};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicUsize, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::{Arc, Condvar, Mutex};

/// One step of a busy wait.
#[inline(always)]
pub(crate) fn spin_hint() {
    #[cfg(loom)]
    loom::thread::yield_now();
    #[cfg(not(loom))]
    std::hint::spin_loop();
}

#[inline]
pub(crate) fn yield_now() {
    #[cfg(loom)]
    loom::thread::yield_now();
    #[cfg(not(loom))]
    std::thread::yield_now();
}

#[inline]
pub(crate) fn sleep(d: std::time::Duration) {
    #[cfg(loom)]
    {
        let _ = d;
        loom::thread::yield_now();
    }
    #[cfg(not(loom))]
    std::thread::sleep(d);
}

/// `UnsafeCell` with loom's closure API, so the same ring code runs under
/// both.
#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;

#[cfg(not(loom))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    pub(crate) fn new(v: T) -> Self {
        UnsafeCell(std::cell::UnsafeCell::new(v))
    }
    #[inline(always)]
    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }
    #[inline(always)]
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
