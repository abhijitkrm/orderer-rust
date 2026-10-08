# orderer-rust

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

The reference implementation of [orderer](https://github.com/abhijitkrm/orderer):
an LMAX-Disruptor-style, multi-core order-matching engine built around the
[matcher](https://github.com/abhijitkrm/matcher) order book.

> Status: in progress. See the spec repo for the contract (`spec/`) and
> the plan.

## Crates

| Crate | What it is |
|---|---|
| `orderer-core` | the matching semantics: a vendored port of matcher-rust plus the `MatchingCore` seam. `#![forbid(unsafe_code)]` |
| `orderer-disruptor` | generic LMAX Disruptor machinery: ring buffer, sequences, barriers, wait strategies, batch processors |
| `orderer` | the assembled pipeline (ingress → router → per-partition journal/engine/egress) and the harness binaries |

Each crate is usable alone: just the book, just the ring, or the whole
engine.

## Verify

```bash
cargo test --workspace     # golden vectors, snapshot parity, ring + pipeline suites
scripts/vendored.sh        # vendored spec/ + vectors/ untouched
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
