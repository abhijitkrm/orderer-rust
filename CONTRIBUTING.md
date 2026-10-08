# Contributing

orderer-rust implements the contract in the
[orderer spec repo](https://github.com/abhijitkrm/orderer). Read its
CONTRIBUTING first.

- `spec/` and `vectors/` are vendored. Never edit them here
  (`scripts/vendored.sh` fails if you do). Spec changes land in the spec
  repo, then get re-vendored (`docs/VENDORED.md`).
- `crates/orderer-core` ports matcher-rust. Matching semantics change only
  through matcher.
- Anything observable (stdout, journals, snapshots, routing) must match the
  spec byte-for-byte. Internals (rings, threads, wait strategies) are free.
- Before sending a change: `cargo fmt --check`,
  `cargo clippy --workspace --all-targets -- -D warnings`,
  `cargo test --workspace`.
