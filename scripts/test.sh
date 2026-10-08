#!/usr/bin/env bash
# test.sh — the full suite, as the spec repo's verify.sh runs it.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test --quiet --workspace --release
