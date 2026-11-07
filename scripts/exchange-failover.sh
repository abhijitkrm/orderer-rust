#!/usr/bin/env bash
# Process-level failover test of the whole exchange: MG matching groups of 3
# replicas each, NS ledger shards of 3 replicas each, a forwarder per ledger shard,
# one settler for all shards, a tick driver (clock and reconciler), a continuous audit
# and a load generator, all real processes, all authenticated with a pre-shared key
# and started from one configuration file. While load runs it `kill -9`s, in turn:
# each matching group's leader, the tick driver, a forwarder, the settler and each
# ledger shard's leader, restarting each. Passes only if, after the load stops and
# everything is drained:
#   - the audit never saw a violation (continuous) and ends clean and caught up
#     (per-shard invariants, cross-shard conservation, outbox and mark bounds, every
#     group's cursor);
#   - every replica of every group is at the same log index with the same digest;
#   - every acknowledged order is filled or cancelled, none is open, no hold is left;
#   - the ledgers' deposit and payout totals equal what the load generator saw
#     acknowledged (an independent count).
#
#   scripts/exchange-failover.sh                 # 40 s of load, 7 kills, 2 shards, 2 groups
#   SECS=60 KILLS=14 SHARDS=3 MGROUPS=2 scripts/exchange-failover.sh
#   PROFILE=debug scripts/exchange-failover.sh   # skip the release build (slower nodes)
#   FSYNC=on scripts/exchange-failover.sh        # default is relaxed:10 (a killed process loses nothing)
#   CHAOS=0 scripts/exchange-failover.sh         # no injected duplicate/out-of-order relay deliveries
#   AUTH=0 scripts/exchange-failover.sh          # no pre-shared key (the config check stays on)
#   KEEP_ALIVE=1 scripts/exchange-failover.sh    # on failure, leave everything running
#   KEEP=1 ...                                   # keep the temp dir (logs, data)
set -euo pipefail
source "$(dirname "$0")/exchange-lib.sh"
SECS=${SECS:-40}
KILLS=${KILLS:-7}
ACCOUNTS=${ACCOUNTS:-40}
THREADS=${THREADS:-4}

check_audit_alive() {
  if ! kill -0 "$AUDIT_PID" 2>/dev/null; then
    wait "$AUDIT_PID" 2>/dev/null || true
    echo "FAIL: the audit process exited; its log:"
    tail -20 "$TMP/audit.log"
    exit 1
  fi
}

start_cluster
start_audit

ACKED="$TMP/acked"
"$BIN/exchange-load" deposit --config "$CONF" --ledgers "$ALL_GATEWAYS" --accounts "$ACCOUNTS" --acked "$ACKED"
"$BIN/exchange-load" run --config "$CONF" --ledgers "$ALL_GATEWAYS" --accounts "$ACCOUNTS" --secs "$SECS" --threads "$THREADS" \
  --acked "$ACKED" >"$TMP/load.out" 2>&1 &
LOAD_PID=$!

# --- kills -----------------------------------------------------------------
kill_pid() { # name pid
  echo "kill -9 $1 (pid $2)"
  kill -9 "$2" 2>/dev/null || true
  wait "$2" 2>/dev/null || true
}
# Each kill costs about 2 s besides the pause (finding the leader, the 1 s before the restart).
PAUSE=$(((SECS - 2 * KILLS) / (KILLS + 1)))
((PAUSE < 1)) && PAUSE=1
for k in $(seq 1 "$KILLS"); do
  sleep "$PAUSE"
  if ! kill -0 "$LOAD_PID" 2>/dev/null; then
    echo "FAIL: the load generator ended before kill $k of $KILLS (raise SECS or lower KILLS); its output:"
    cat "$TMP/load.out"
    exit 1
  fi
  check_audit_alive
  m=$(((k - 1) % (MG + NS + 3)))
  if ((m < MG)); then # a matching group's leader
    g=$m
    L=$(leader_of "$(tier_clients "$g")" || true)
    [[ -z "$L" ]] && { sleep 1; L=$(leader_of "$(tier_clients "$g")" || true); }
    [[ -z "$L" ]] && { echo "FAIL: no leader for matching group $g before kill $k"; exit 1; }
    kill_pid "matching group $g leader node $L" "${TIER_PID[$((g * 3 + L))]}"
    sleep 1
    start_tier "$g" "$L"
  elif ((m == MG)); then
    kill_pid "tick driver" "$TICK_PID"; sleep 1; start_tick
  elif ((m == MG + 1)); then
    s=$((k % NS)); kill_pid "forwarder $s" "${FWD_PID[$s]}"; sleep 1; start_fwd "$s"
  elif ((m == MG + 2)); then
    kill_pid "settler" "$SET_PID"; sleep 1; start_set
  else # a ledger shard's leader
    s=$((m - MG - 3))
    L=$(leader_of "$(led_clients "$s")" || true)
    [[ -z "$L" ]] && { sleep 1; L=$(leader_of "$(led_clients "$s")" || true); }
    [[ -z "$L" ]] && { echo "FAIL: no leader for ledger shard $s before kill $k"; exit 1; }
    kill_pid "ledger shard $s leader node $L" "${LED_PID[$((s * 3 + L))]}"
    sleep 1
    start_ledger "$s" "$L"
  fi
  echo "  kill $k/$KILLS done"
done

wait "$LOAD_PID" || { echo "FAIL: load generator failed:"; cat "$TMP/load.out"; exit 1; }
LOAD_PID=
cat "$TMP/load.out"
check_audit_alive

# --- drain and verify ------------------------------------------------------
echo "draining: cancelling every acknowledged order that is still open"
"$BIN/exchange-load" drain --config "$CONF" --ledgers "$ALL_GATEWAYS" --acked "$ACKED" --timeout 180
# the continuous audit has to have survived all of that
check_audit_alive
kill "$AUDIT_PID" 2>/dev/null || true
wait "$AUDIT_PID" 2>/dev/null || true
AUDIT_PID=
echo "continuous audit rounds that passed: $(grep -c "ok;" "$TMP/audit.log" || true)"
if grep -q "VIOLATION" "$TMP/audit.log"; then echo "FAIL: violation in the continuous audit"; grep VIOLATION "$TMP/audit.log" | head; exit 1; fi

"$BIN/exchange-audit" --config "$CONF" --ledgers "$ALL_GATEWAYS" --tier "$ALL_TIER_EX" \
  --ledger-clients "$ALL_CLIENTS" --tier-clients "$ALL_TIER_CL" --once --wait 180 --expect-idle --acked "$ACKED"

echo "replica digests, every group:"
for ((g = 0; g < MG; g++)); do "$BIN/orderer-load" settle --clients "$(tier_clients "$g")" --timeout 90; done
for ((s = 0; s < NS; s++)); do "$BIN/orderer-load" settle --clients "$(led_clients "$s")" --timeout 90; done

if grep -l "panicked" "$TMP"/*.log >/dev/null 2>&1; then echo "FAIL: a process panicked:"; grep -h "panicked" "$TMP"/*.log | head; exit 1; fi
echo "PASS: $KILLS kills under load, audit clean, every acknowledged order accounted for, replicas agree"
