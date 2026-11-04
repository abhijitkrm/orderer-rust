//! Hardware CRC-32C (feature `hwcrc`): the third sanctioned `unsafe` site in
//! the workspace, after `affinity` and the optional `prefetch` hint.
//!
//! The replicated log checksums every byte several times on its way through
//! WAL, sender and reader; the portable slicing-by-8 code tops out near
//! 1.7 GB/s on an M1, while the CPU's CRC32C instruction runs several times
//! faster. Each call site is one intrinsic loop behind a runtime feature
//! check. Anything unsupported falls back to the portable code, and tests
//! prove the two agree.
//!
//! Needs Rust 1.80+ on aarch64 (the `crc` intrinsics were stabilised then);
//! the workspace MSRV applies to builds without this feature.
#![allow(unsafe_code, clippy::incompatible_msrv)]

/// Fold `data` into the running CRC state `c`, or `None` if this CPU lacks
/// the instruction.
#[inline]
pub fn update(c: u32, data: &[u8]) -> Option<u32> {
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            // SAFETY: the `crc` target feature was just detected at runtime.
            return Some(unsafe { aarch64(c, data) });
        }
    }
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: the `sse4.2` target feature was just detected at runtime.
            return Some(unsafe { x86(c, data) });
        }
    }
    let _ = (c, data);
    None
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn aarch64(mut c: u32, data: &[u8]) -> u32 {
    use core::arch::aarch64::{__crc32cb, __crc32cd};
    let mut chunks = data.chunks_exact(8);
    for ch in &mut chunks {
        c = __crc32cd(c, u64::from_le_bytes(ch.try_into().expect("8 bytes")));
    }
    for &b in chunks.remainder() {
        c = __crc32cb(c, b);
    }
    c
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn x86(c: u32, data: &[u8]) -> u32 {
    use core::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};
    let mut c = u64::from(c);
    let mut chunks = data.chunks_exact(8);
    for ch in &mut chunks {
        c = _mm_crc32_u64(c, u64::from_le_bytes(ch.try_into().expect("8 bytes")));
    }
    let mut c = c as u32;
    for &b in chunks.remainder() {
        c = _mm_crc32_u8(c, b);
    }
    c
}
