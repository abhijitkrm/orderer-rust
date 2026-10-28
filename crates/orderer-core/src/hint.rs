//! Cache hints for the `apply_batch` look-ahead pipeline (docs/LAYOUT.md §4).
//! Hints read possibly-stale state through shared references and never
//! write, so they cannot change behaviour, only timing.
//!
//! * feature `prefetch`: a real software prefetch (`prfm` on aarch64,
//!   `prefetcht0` on x86_64). This is the crate's only `unsafe`: one audited
//!   helper below; the crate attribute relaxes to `deny(unsafe_code)`.
//! * default (`forbid(unsafe_code)`): a plain load of the referenced value
//!   fed to `black_box`. The out-of-order core overlaps the miss with the
//!   commands ahead of it, which is most of what a prefetch buys.

/// Hint that `*r` is about to be read.
#[inline(always)]
pub fn prefetch_read<T: Copy>(r: &T) {
    imp::hint::<false, T>(r)
}

/// Hint that `*r` (its cache line) is about to be written.
#[inline(always)]
pub fn prefetch_write<T: Copy>(r: &T) {
    imp::hint::<true, T>(r)
}

#[cfg(not(feature = "prefetch"))]
mod imp {
    #[inline(always)]
    pub fn hint<const W: bool, T: Copy>(r: &T) {
        core::hint::black_box(*r);
    }
}

#[cfg(feature = "prefetch")]
mod imp {
    /// Software prefetch of the line holding `*r`.
    ///
    /// SAFETY (audit): a prefetch is an architectural no-op hint — it never
    /// faults (even on an invalid address), never writes, and has no effect
    /// on program state. `r` is a live shared reference regardless.
    #[allow(unsafe_code)]
    #[inline(always)]
    pub fn hint<const W: bool, T: Copy>(r: &T) {
        let p = r as *const T;
        #[cfg(target_arch = "aarch64")]
        unsafe {
            if W {
                core::arch::asm!("prfm pstl1keep, [{0}]", in(reg) p,
                    options(nostack, preserves_flags, readonly));
            } else {
                core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p,
                    options(nostack, preserves_flags, readonly));
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch::<_MM_HINT_T0>(p as *const i8);
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
        {
            let _ = p;
        }
    }
}
