# Changelog

## Unreleased

- Workspace scaffold (`orderer-core`, `orderer-disruptor`, `orderer`).
  Vendored orderer spec + vectors (`docs/VENDORED.md`).
- `orderer-core`: matcher-rust port at `459a22a` (golden + snapshot parity),
  plus the `MatchingCore` seam with `FifoCore` and `NoopCore`.
