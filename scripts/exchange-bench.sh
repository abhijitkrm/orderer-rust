#!/usr/bin/env bash
# Open-loop latency through the gateway: starts the whole exchange (see
# exchange-lib.sh), funds the accounts, then offers Reserve requests at each rate in
# turn on a fixed schedule and prints order-to-ack and ack-to-resting percentiles
# (exchange-bench; the definitions are at the top of its source). No faults, no audit
# while measuring; one audit at the end to check the system it measured was healthy.
#
#   scripts/exchange-bench.sh                          # relaxed fsync, 4 rates
#   FSYNC=on scripts/exchange-bench.sh                 # fsync before every commit
#   RATES="500 1500" SECS=20 WARM=3 scripts/exchange-bench.sh
#   SHARDS=2 MGROUPS=1 scripts/exchange-bench.sh       # topology
#   THREADS=64 ACCOUNTS=100 POLL_US=500 ...            # producers, accounts, observer pause
#
# Rates run in ascending order on one cluster, so the books and ledgers grow a little
# from one rate to the next (orders are not cancelled).
set -euo pipefail
export CHAOS=${CHAOS:-0} # no injected duplicates while measuring latency
source "$(dirname "$0")/exchange-lib.sh"
RATES=${RATES:-"200 500 1000 2000"}
SECS=${SECS:-12}
WARM=${WARM:-2}
THREADS=${THREADS:-32}
ACCOUNTS=${ACCOUNTS:-40}
POLL_US=${POLL_US:-200}

start_cluster
"$BIN/exchange-load" deposit --config "$CONF" --ledgers "$ALL_GATEWAYS" --accounts "$ACCOUNTS" --acked "$TMP/acked"
MODE=phase1
if [[ "$SLICES" == 1 ]]; then
  MODE=phase2 # orders go to the credit group directly, against slices lent below
  "$BIN/exchange-load" lend --config "$CONF" --ledgers "$ALL_GATEWAYS" --tier "$ALL_TIER_EX" --accounts "$ACCOUNTS" --acked "$TMP/acked-lend" --lend "${LEND:-8000000}"
fi
echo "topology: $NS ledger shards x3, $MG matching groups x3, fsync=$FSYNC, auth=$([[ -n "${ORDERER_KEY:-}" ]] && echo on || echo off), chaos=$CHAOS"
for r in $RATES; do
  sleep 2
  "$BIN/exchange-bench" --config "$CONF" --ledgers "$ALL_GATEWAYS" --tier "$ALL_TIER_EX" --rate "$r" \
    --secs "$SECS" --warmup "$WARM" --threads "$THREADS" --accounts "$ACCOUNTS" --poll-us "$POLL_US" \
    --mode "$MODE" --label "$MODE fsync=$FSYNC rate=$r"
done
echo "settling and auditing what was measured"
"$BIN/exchange-audit" --config "$CONF" --ledgers "$ALL_GATEWAYS" --tier "$ALL_TIER_EX" \
  --ledger-clients "$ALL_CLIENTS" --tier-clients "$ALL_TIER_CL" --once --wait 180
echo "audit clean"
