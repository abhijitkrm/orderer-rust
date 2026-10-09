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
- **commit**: `ead0d43f8fd9e6ca5bdcfcf807049d4f2415e4a2`
- **paths**: `spec=spec vectors=vectors`
- **tag**: `orderer-spec/1.2` (draft: checksums, crash repair, checkpoints)

`docs/VENDORED.sha256` holds every file's checksum. `scripts/vendored.sh`
verifies the copy against it and, when `../orderer` is checked out, against
the pinned commit.

## 2. The matching core (ported)

`crates/orderer-core/src/` is a port of
[matcher-rust](https://github.com/abhijitkrm/matcher-rust) at
`71a35f46ea4658dd4da89b270cd48d0af01571b5` (the commit that fixed the `OrderMap` deletion bug orderer found):

| File | Status |
|---|---|
| `book.rs engine.rs index.rs level.rs ordermap.rs pool.rs sink.rs types.rs journal.rs jsonflat.rs` | verbatim (`ordermap.rs` carries the upstream fix for the deletion bug orderer reported) |
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
