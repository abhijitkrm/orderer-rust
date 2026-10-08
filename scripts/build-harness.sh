#!/usr/bin/env bash
# build-harness.sh — the spec repo's harness discovery contract
# (spec/HARNESS.md §6): build the five tools and expose them as
# harness/bin/{orderrun,ordererfuzz,orderrecover,ordersnap,orderbench}.
#
#   scripts/build-harness.sh          # optimized (release profile)
#   CHECKED=1 scripts/build-harness.sh   # optimized + debug assertions:
#                                        # book invariants after every command
set -euo pipefail
cd "$(dirname "$0")/.."
PROFILE=release
[ "${CHECKED:-0}" = 1 ] && PROFILE=fuzz
cargo build --quiet --profile "$PROFILE" -p orderer --bins
mkdir -p harness/bin
for t in orderrun ordererfuzz orderrecover ordersnap orderbench; do
  ln -sf "../../target/$PROFILE/$t" "harness/bin/$t"
done
