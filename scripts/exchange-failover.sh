#!/usr/bin/env bash
# Process-level failover test of the whole exchange: a 3-replica matching group,
# N ledger shards of 3 replicas each, a forwarder and a settler per shard, a tick
# driver (clock and reconciler), a continuous audit and a load generator, all real
# processes. While load runs it `kill -9`s, in turn: the matching leader, each
# ledger shard's leader, a forwarder, a settler and the tick driver, restarting
# each. Passes only if, after the load stops and everything is drained:
#   - the audit never saw a violation (continuous) and ends clean and caught up
#     (per-shard invariants, cross-shard conservation, outbox and mark bounds);
#   - every replica of every group is at the same log index with the same digest;
#   - every acknowledged order is filled or cancelled, none is open, no hold is left;
#   - the ledgers' deposit and payout totals equal what the load generator saw
#     acknowledged (an independent count).
#
#   scripts/exchange-failover.sh                 # 30 s of load, 6 kills, 2 shards
#   SECS=60 KILLS=12 SHARDS=3 scripts/exchange-failover.sh
#   PROFILE=debug scripts/exchange-failover.sh   # skip the release build (slower nodes)
#   FSYNC=on scripts/exchange-failover.sh        # default is relaxed:10 (a killed process loses nothing)
#   CHAOS=0 scripts/exchange-failover.sh         # no injected duplicate/out-of-order relay deliveries
#   KEEP_ALIVE=1 scripts/exchange-failover.sh    # on failure, leave everything running
#   KEEP=1 ...                                   # keep the temp dir (logs, data)
set -euo pipefail

cd "$(dirname "$0")/.."
SECS=${SECS:-30}
KILLS=${KILLS:-6}
NS=${SHARDS:-2}
ACCOUNTS=${ACCOUNTS:-40}
THREADS=${THREADS:-4}
PARTS=4
SB=100 # matching stream ids are SB .. SB+PARTS-1
BASE=${BASE_PORT:-47500}
PROFILE=${PROFILE:-release}
FSYNC=${FSYNC:-relaxed:10}
CHAOS=${CHAOS:-5} # every 5th relay delivery is also duplicated, overlapped or sent out of order (0 = off)
BIN=target/$PROFILE

if [[ "$PROFILE" == release ]]; then
  cargo build --release -q -p orderer-cluster --bin orderer-load -p orderer-exchange --bins
else
  cargo build -q -p orderer-cluster --bin orderer-load -p orderer-exchange --bins
fi

TMP=$(mktemp -d)
LOAD_PID=
AUDIT_PID=
declare -a TIER_PID=(0 0 0)
declare -a LED_PID=()
declare -a FWD_PID=()
declare -a SET_PID=()
TICK_PID=0
for ((i = 0; i < NS * 3; i++)); do LED_PID[$i]=0; done
for ((s = 0; s < NS; s++)); do FWD_PID[$s]=0; SET_PID[$s]=0; done

# --- addresses -------------------------------------------------------------
tier_members() { echo "0=127.0.0.1:$BASE,1=127.0.0.1:$((BASE + 1)),2=127.0.0.1:$((BASE + 2))"; }
tier_clients() { echo "127.0.0.1:$((BASE + 100)),127.0.0.1:$((BASE + 101)),127.0.0.1:$((BASE + 102))"; }
tier_exchange() { echo "127.0.0.1:$((BASE + 200)),127.0.0.1:$((BASE + 201)),127.0.0.1:$((BASE + 202))"; }
led_port() { echo $((BASE + $1 + 10 + $2 * 10 + $3)); } # kind offset, shard, node
led_members() { local s=$1; echo "0=127.0.0.1:$(led_port 0 "$s" 0),1=127.0.0.1:$(led_port 0 "$s" 1),2=127.0.0.1:$(led_port 0 "$s" 2)"; }
led_clients() { local s=$1; echo "127.0.0.1:$(led_port 100 "$s" 0),127.0.0.1:$(led_port 100 "$s" 1),127.0.0.1:$(led_port 100 "$s" 2)"; }
led_gateways() { local s=$1; echo "127.0.0.1:$(led_port 200 "$s" 0),127.0.0.1:$(led_port 200 "$s" 1),127.0.0.1:$(led_port 200 "$s" 2)"; }
join_shards() { local sep=";" out="" s; for ((s = 0; s < NS; s++)); do out+="${out:+$sep}$("$1" "$s")"; done; echo "$out"; }
ALL_GATEWAYS=$(join_shards led_gateways)
ALL_CLIENTS=$(join_shards led_clients)

cleanup() {
  local rc=$?
  if [[ -n "${KEEP_ALIVE:-}" && $rc -ne 0 ]]; then
    echo "KEEP_ALIVE: failed (status $rc); processes still running, logs in $TMP"
    return
  fi
  local p
  for p in "${TIER_PID[@]}" "${LED_PID[@]}" "${FWD_PID[@]}" "${SET_PID[@]}" "$TICK_PID" "${LOAD_PID:-0}" "${AUDIT_PID:-0}"; do
    [[ "$p" != 0 && -n "$p" ]] && kill -9 "$p" 2>/dev/null || true
  done
  if [[ $rc -ne 0 ]]; then echo "FAILED (status $rc); logs kept in $TMP"; return; fi
  [[ -z "${KEEP:-}" ]] && rm -rf "$TMP"
  return 0
}
trap cleanup EXIT

# --- process control -------------------------------------------------------
start_tier() {
  local i=$1
  "$BIN/tier-node" --id "$i" --members "$(tier_members)" --client "127.0.0.1:$((BASE + 100 + i))" \
    --exchange "127.0.0.1:$((BASE + 200 + i))" --dir "$TMP/tier$i" --partitions $PARTS --source-base $SB \
    --compact-every 4000 --digest-every 2000 --fsync "$FSYNC" >>"$TMP/tier$i.log" 2>&1 &
  TIER_PID[$i]=$!
}
start_ledger() {
  local s=$1 i=$2
  "$BIN/ledger-node" --shard "$s" --id "$i" --members "$(led_members "$s")" \
    --client "127.0.0.1:$(led_port 100 "$s" "$i")" --gateway "127.0.0.1:$(led_port 200 "$s" "$i")" \
    --dir "$TMP/ledger$s-$i" --compact-every 4000 --digest-every 2000 --fsync "$FSYNC" >>"$TMP/ledger$s-$i.log" 2>&1 &
  LED_PID[$((s * 3 + i))]=$!
}
start_fwd() {
  local s=$1
  "$BIN/exchange-relay" forwarder --shard "$s" --ledger "$(led_gateways "$s")" --tier "$(tier_exchange)" --chaos "$CHAOS" >>"$TMP/fwd$s.log" 2>&1 &
  FWD_PID[$s]=$!
}
start_set() {
  local s=$1
  "$BIN/exchange-relay" settler --shard "$s" --ledger "$(led_gateways "$s")" --tier "$(tier_exchange)" \
    --source-base $SB --partitions $PARTS --chaos "$CHAOS" >>"$TMP/set$s.log" 2>&1 &
  SET_PID[$s]=$!
}
start_tick() {
  "$BIN/exchange-relay" tick --ledgers "$ALL_GATEWAYS" --interval-ms 200 --sweep-every-ms 2000 \
    --max-age-ms 6000 --retry-ms 6000 >>"$TMP/tick.log" 2>&1 &
  TICK_PID=$!
}
start_audit() {
  "$BIN/exchange-audit" --ledgers "$ALL_GATEWAYS" --tier "$(tier_exchange)" --partitions $PARTS --source-base $SB \
    --ledger-clients "$ALL_CLIENTS" --tier-clients "$(tier_clients)" --interval-ms 1000 >>"$TMP/audit.log" 2>&1 &
  AUDIT_PID=$!
}

# the node id of the current leader of a group, given its client addresses
leader_of() {
  "$BIN/orderer-load" status --clients "$1" 2>/dev/null | grep "role=Leader" | head -1 | sed -E 's/.*id=([0-9]+).*/\1/'
}
wait_leader() {
  local what=$1 clients=$2 _
  for _ in $(seq 1 150); do [[ -n "$(leader_of "$clients")" ]] && return 0; sleep 0.1; done
  echo "FAIL: no leader elected for $what"
  exit 1
}
check_audit_alive() {
  if ! kill -0 "$AUDIT_PID" 2>/dev/null; then
    wait "$AUDIT_PID" 2>/dev/null || true
    echo "FAIL: the audit process exited; its log:"
    tail -20 "$TMP/audit.log"
    exit 1
  fi
}

# --- start everything ------------------------------------------------------
for i in 0 1 2; do start_tier "$i"; done
for ((s = 0; s < NS; s++)); do for i in 0 1 2; do start_ledger "$s" "$i"; done; done
echo "started 1 matching group and $NS ledger shards, 3 replicas each (logs in $TMP)"
wait_leader "matching group" "$(tier_clients)"
for ((s = 0; s < NS; s++)); do wait_leader "ledger shard $s" "$(led_clients "$s")"; done
for ((s = 0; s < NS; s++)); do start_fwd "$s"; start_set "$s"; done
start_tick
sleep 1
start_audit

ACKED="$TMP/acked"
"$BIN/exchange-load" deposit --ledgers "$ALL_GATEWAYS" --accounts "$ACCOUNTS" --acked "$ACKED"
"$BIN/exchange-load" run --ledgers "$ALL_GATEWAYS" --accounts "$ACCOUNTS" --secs "$SECS" --threads "$THREADS" \
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
  case $((k % (NS + 4))) in
    0) # the matching leader
      L=$(leader_of "$(tier_clients)" || true)
      [[ -z "$L" ]] && { sleep 1; L=$(leader_of "$(tier_clients)" || true); }
      [[ -z "$L" ]] && { echo "FAIL: no matching leader before kill $k"; exit 1; }
      kill_pid "matching leader node $L" "${TIER_PID[$L]}"
      sleep 1
      start_tier "$L"
      ;;
    1) kill_pid "tick driver" "$TICK_PID"; sleep 1; start_tick ;;
    2) s=$((k % NS)); kill_pid "forwarder $s" "${FWD_PID[$s]}"; sleep 1; start_fwd "$s" ;;
    3) s=$((k % NS)); kill_pid "settler $s" "${SET_PID[$s]}"; sleep 1; start_set "$s" ;;
    *) # a ledger shard's leader
      s=$(((k % (NS + 4)) - 4))
      L=$(leader_of "$(led_clients "$s")" || true)
      [[ -z "$L" ]] && { sleep 1; L=$(leader_of "$(led_clients "$s")" || true); }
      [[ -z "$L" ]] && { echo "FAIL: no leader for ledger shard $s before kill $k"; exit 1; }
      kill_pid "ledger shard $s leader node $L" "${LED_PID[$((s * 3 + L))]}"
      sleep 1
      start_ledger "$s" "$L"
      ;;
  esac
  echo "  kill $k/$KILLS done"
done

wait "$LOAD_PID" || { echo "FAIL: load generator failed:"; cat "$TMP/load.out"; exit 1; }
LOAD_PID=
cat "$TMP/load.out"
check_audit_alive

# --- drain and verify ------------------------------------------------------
echo "draining: cancelling every acknowledged order that is still open"
"$BIN/exchange-load" drain --ledgers "$ALL_GATEWAYS" --acked "$ACKED" --timeout 180
# the continuous audit has to have survived all of that
check_audit_alive
kill "$AUDIT_PID" 2>/dev/null || true
wait "$AUDIT_PID" 2>/dev/null || true
AUDIT_PID=
echo "continuous audit rounds that passed: $(grep -c "ok;" "$TMP/audit.log" || true)"
if grep -q "VIOLATION" "$TMP/audit.log"; then echo "FAIL: violation in the continuous audit"; grep VIOLATION "$TMP/audit.log" | head; exit 1; fi

"$BIN/exchange-audit" --ledgers "$ALL_GATEWAYS" --tier "$(tier_exchange)" --partitions $PARTS --source-base $SB \
  --ledger-clients "$ALL_CLIENTS" --tier-clients "$(tier_clients)" --once --wait 180 --expect-idle --acked "$ACKED"

echo "replica digests, every group:"
"$BIN/orderer-load" settle --clients "$(tier_clients)" --timeout 90
for ((s = 0; s < NS; s++)); do "$BIN/orderer-load" settle --clients "$(led_clients "$s")" --timeout 90; done

if grep -l "panicked" "$TMP"/*.log >/dev/null 2>&1; then echo "FAIL: a process panicked:"; grep -h "panicked" "$TMP"/*.log | head; exit 1; fi
echo "PASS: $KILLS kills under load, audit clean, every acknowledged order accounted for, replicas agree"
