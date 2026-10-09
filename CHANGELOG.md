# Changelog

## Unreleased: orderer-spec/1.2

- Binary journals are version 2: every record is sealed with CRC-32C.
  Version-1 journals still read.
- Crash repair: `journal::repair_dir` / `orderrecover --repair` truncate a
  torn final record; anything else stays corruption.
- Checkpoints: `Pipeline::checkpoint` rotates every journal onto a new
  segment at a clean cut, writes the snapshot durably, and removes covered
  segments and older checkpoints. Recovery reads all segments and, by
  default, the newest checkpoint.
- Harness: `orderrun --checkpoint-every K` and `--durable`; `scripts/test.sh`
  runs the vendored `spec/conformance.sh`.
- Workspace excludes `upstream/`, so CI's sibling checkouts build.
- Vendors matcher-rust 8c4d05e: `Engine` hashes symbols with one multiply
  instead of SipHash, doubling multi-symbol core throughput (W6: ~15M to
  ~30M ops/s untimed on an M1).

## Unreleased: v0.1.0 candidate (implements orderer-spec/1.1)

- **orderer-core**: matcher-rust 71a35f4, vendored, plus the `MatchingCore`
  seam (`FifoCore`, `NoopCore`) and non-panicking snapshot parsing.
  Building orderer exposed an `OrderMap` backward-shift deletion bug in
  matcher-rust and matcher-cpp that silently dropped keys in dense maps.
  It is now fixed upstream (matcher-rust 71a35f4, matcher-cpp 1285f6c);
  see `vectors/regress/001_dense_map_churn`.
- **orderer-disruptor**: LMAX ring, single and multi producers (per-slot
  availability flags, CAS `try_publish`), dependency barriers, four wait
  strategies, `EventProcessor`, `MultiRingProcessor`, a DSL. `unsafe`
  confined to `ring.rs`; loom-checked.
- **orderer**: the pipeline:
  - router with `iseq` stamping and symbol routing
  - per-partition engines with inline (or staged) journal-before-apply
  - JSONL and binary journals written by I/O threads with group-commit fsync
  - durability-gated acks
  - drain, snapshot and shutdown controls with clean cuts
  - recovery from a snapshot and/or journals at any P
  - egress plugs; zero steady-state allocation
- Harnesses: `orderrun`, `ordererfuzz`, `orderrecover`, `ordersnap`,
  `orderbench` (spec/HARNESS.md), with the `scripts/build-harness.sh` and
  `scripts/test.sh` discovery contract.
- Feature `affinity`: macOS QoS hints for hot threads.
- Benchmarked on an Apple M1: the scaling gate is not met on that machine
  (durable-flush bandwidth, 4 performance cores). The spec repo's
  docs/RESULTS.md has the numbers.
