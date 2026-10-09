#!/usr/bin/env bash
# test.sh — the full suite, as the spec repo's verify.sh runs it.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test --quiet --workspace --release
CHECKED=1 scripts/build-harness.sh
spec/conformance.sh harness/bin vectors
