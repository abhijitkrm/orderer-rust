use std::time::Instant;
fn main() {
    let data = vec![0x5au8; 32 * 1024];
    let n = 20_000;
    let t = Instant::now();
    let mut acc = 0u32;
    for _ in 0..n {
        acc ^= orderer::journal::crc32c(std::hint::black_box(&data));
    }
    let s = t.elapsed().as_secs_f64();
    println!(
        "crc32c: {:.0} MB/s (acc {acc})",
        (n * data.len()) as f64 / s / 1e6
    );
    let t = Instant::now();
    let mut v = Vec::new();
    for _ in 0..n {
        v.clear();
        v.extend_from_slice(std::hint::black_box(&data));
    }
    let s = t.elapsed().as_secs_f64();
    println!(
        "memcpy: {:.0} MB/s ({})",
        (n * data.len()) as f64 / s / 1e6,
        v.len()
    );
}
