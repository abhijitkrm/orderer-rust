# VENDORED — upstream sources

orderer-rust vendors two things.

## 1. The orderer spec (verified)

`spec/` and `vectors/` are copied **verbatim** from the
[orderer](https://github.com/abhijitkrm/orderer) spec repo. They include
matcher's spec and corpus under `spec/matcher/` and `vectors/matcher/`.
Never edit them here: changes go to the spec repo first, then get
re-vendored.

- **upstream**: `orderer`
- **repo**: `https://github.com/abhijitkrm/orderer`
- **commit**: `c541cd0f265df0e2d939febb2c0b7694d4172230`
- **paths**: `spec=spec vectors=vectors`
- **tag**: `orderer-spec/1` (the frozen v1 contract)

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out, against
the pinned commit.

## 2. The matching core (ported)

`crates/orderer-core/src/` is a port of
[matcher-rust](https://github.com/abhijitkrm/matcher-rust) at
`459a22a6730b740eb464854482d3819a5e6e4020`:

| File | Status |
|---|---|
| `book.rs engine.rs index.rs level.rs pool.rs sink.rs types.rs journal.rs jsonflat.rs` | verbatim |
| `ordermap.rs` | **bug fix**: backward-shift deletion moved the hole onto an entry that could not move, so a later shift could overwrite a live key. The key was silently dropped, the map drifted from the pool, and level totals underflowed. Found by orderer's dense-map allocation test. Also present in matcher-cpp's `internals.hpp`; matcher-java and matcher-ts are correct, and matcher-go uses a built-in map. Fixed here with a model-based regression test; to be fixed upstream |
| `snapshot.rs` | verbatim, plus `write_header`, `try_parse` (errors instead of panics) and `validate_book` |
| `core.rs` | new: the `MatchingCore` seam, `FifoCore`, `NoopCore` |
| `lib.rs` | crate docs and exports adapted |
| `tests/golden.rs tests/snapshot.rs` | verbatim, except the crate name and vector paths |

The golden corpus is the parity proof: semantics may only change through
matcher and a re-vendor. When matcher-rust changes, diff its `src/` against
the files marked verbatim and port the change.

## Re-vendoring the spec

```bash
rm -rf spec vectors && cp -R ../orderer/spec ../orderer/vectors .
# bump the commit above, then:
scripts/vendored.sh --update && scripts/vendored.sh && cargo test --workspace
```
