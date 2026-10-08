//! Thread placement hints (feature `affinity`): the second, and last,
//! sanctioned `unsafe` site in the workspace (plan A5).
//!
//! macOS has no hard core pinning. Its scheduler does honour QoS classes:
//! `USER_INTERACTIVE` threads prefer performance cores. Hot-path threads
//! (router, engines, bench producers) ask for it. Best-effort hint only; a
//! no-op elsewhere.
//!
//! Measured on an M1 (4P+4E), docs RESULTS phase 6: hot-only hints are
//! within noise of no hints (the scheduler already favours busy threads),
//! and *demoting* journal/egress/I-O threads to `UTILITY` cost 20–30% — they
//! move ~1 GB/s of journal bytes. So [`Role::Background`] deliberately
//! leaves the default class.

/// What a thread does, for placement purposes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Busy-spinning hot path: router, engines, benchmark producers.
    Hot,
    /// Batching, parking work: journal, egress, I/O threads. Left at the
    /// default class (see module docs).
    Background,
}

/// Apply `role`'s placement hint to the calling thread.
#[cfg(all(feature = "affinity", target_vendor = "apple"))]
#[allow(unsafe_code)]
pub fn set_current(role: Role) {
    // <pthread/qos.h>: qos_class_t value.
    const QOS_CLASS_USER_INTERACTIVE: u32 = 0x21;
    extern "C" {
        fn pthread_set_qos_class_self_np(qos_class: u32, relative_priority: i32) -> i32;
    }
    let class = match role {
        Role::Hot => QOS_CLASS_USER_INTERACTIVE,
        Role::Background => return, // measured: demotion hurts (module docs)
    };
    // SAFETY: a libSystem call with plain integer arguments that only
    // changes the calling thread's scheduling class; it touches no memory we
    // own. A nonzero return (unsupported class) is ignored: placement is a
    // hint.
    let _ = unsafe { pthread_set_qos_class_self_np(class, 0) };
}

/// Apply `role`'s placement hint to the calling thread (no-op build).
#[cfg(not(all(feature = "affinity", target_vendor = "apple")))]
pub fn set_current(role: Role) {
    let _ = role;
}
