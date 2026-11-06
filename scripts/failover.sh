#!/usr/bin/env bash
# Process-level failover test: three real orderer-node processes, a load
# generator, and `kill -9` of the current leader under load, repeatedly.
# Passes only if every acknowledged batch survives on every replica, in the
# same order and exactly once, and all replicas end in the same state.
#
#   scripts/failover.sh            # defaults: 30 s of load, 5 leader kills
#   SECS=60 KILLS=10 scripts/failover.sh
#   DIGEST_EVERY=1024 scripts/failover.sh  # a digest costs a full state snapshot; see REPLICATION.md
#   KEEP_ALIVE=1 scripts/failover.sh       # on failure, leave the nodes running to inspect
#   FSYNC=relaxed:20 scripts/failover.sh   # commit waits for the write, not the flush
set -euo pipefail

cd "$(dirname "$0")/.."
SECS=${SECS:-30}
KILLS=${KILLS:-5}
BASE=${BASE_PORT:-47100}
BIN=target/release

cargo build --release -q -p orderer-cluster --bin orderer-node --bin orderer-load

TMP=$(mktemp -d)
MEMBERS="0=127.0.0.1:$BASE,1=127.0.0.1:$((BASE + 1)),2=127.0.0.1:$((BASE + 2))"
CLIENTS="127.0.0.1:$((BASE + 100)),127.0.0.1:$((BASE + 101)),127.0.0.1:$((BASE + 102))"
declare -a PIDS=(0 0 0)

cleanup() {
  local rc=$?
  if [[ -n "${KEEP_ALIVE:-}" && $rc -ne 0 ]]; then
    echo "KEEP_ALIVE: failed (status $rc); nodes still running, pids ${PIDS[*]}, logs in $TMP"
    return
  fi
  for i in 0 1 2; do kill -9 "${PIDS[$i]}" 2>/dev/null || true; done
  [[ -n "${LOAD_PID:-}" ]] && kill "$LOAD_PID" 2>/dev/null || true
  [[ -z "${KEEP:-}" ]] && rm -rf "$TMP"
}
trap cleanup EXIT

start_node() {
  local i=$1
  "$BIN/orderer-node" --id "$i" --members "$MEMBERS" --client "127.0.0.1:$((BASE + 100 + i))" \
    --dir "$TMP/n$i" --audit --digest-every "${DIGEST_EVERY:-64}" --fsync "${FSYNC:-on}" >>"$TMP/n$i.log" 2>&1 &
  PIDS[$i]=$!
}

leader() {
  "$BIN/orderer-load" status --clients "$CLIENTS" 2>/dev/null | grep "role=Leader" | head -1 \
    | sed -E 's/.*id=([0-9]+).*/\1/'
}

for i in 0 1 2; do start_node "$i"; done
echo "started 3 nodes (logs in $TMP)"
for _ in $(seq 1 100); do [[ -n "$(leader)" ]] && break; sleep 0.1; done
[[ -n "$(leader)" ]] || { echo "FAIL: no leader elected"; exit 1; }

"$BIN/orderer-load" run --clients "$CLIENTS" --secs "$SECS" --acked "$TMP/acked" --maybe "$TMP/maybe" \
  --batch 50 --threads 3 >"$TMP/load.out" 2>&1 &
LOAD_PID=$!

for k in $(seq 1 "$KILLS"); do
  sleep $(( (SECS - 4) / (KILLS + 1) + 1 ))
  L=$(leader || true)
  [[ -z "$L" ]] && { sleep 1; L=$(leader || true); }
  [[ -z "$L" ]] && { echo "FAIL: no leader before kill $k"; exit 1; }
  echo "kill $k/$KILLS: kill -9 leader node $L"
  kill -9 "${PIDS[$L]}"
  wait "${PIDS[$L]}" 2>/dev/null || true
  sleep 1
  start_node "$L"
done

wait "$LOAD_PID"
LOAD_PID=
cat "$TMP/load.out"

"$BIN/orderer-load" settle --clients "$CLIENTS" --timeout 60
"$BIN/orderer-load" verify --acked "$TMP/acked" --maybe "$TMP/maybe" \
  --applied "$TMP/n0/applied.log,$TMP/n1/applied.log,$TMP/n2/applied.log"
