#!/usr/bin/env bash
# conformance.sh — the executable part of HARNESS.md: every orderer vector
# through an implementation's harness tools (§4), byte-exact.
#
#   spec/conformance.sh <bin-dir> <vectors-dir>
#
# <bin-dir> holds orderrun, ordererfuzz, orderrecover, ordersnap (§6). Each
# implementation repo runs this from its vendored spec/ in scripts/test.sh,
# so all ports run the same checks and a spec change needs no per-port edits.
#
#   pipeline/    listings + per-partition journals, both encodings, P=1,2,4;
#                journal-form recovery of each
#   checkpoint/  --checkpoint-every K: listing, journal dirs, recovery (1.2)
#   repair/      torn tails: strict exits 2, --repair fixes them (1.2)
#   compat/      version-1 binary journals still recover (1.2)
#   recovery/    snapshot + tail, across partition counts
#   regress/     matcher-format golden vectors
#   exit codes   §5
set -uo pipefail
BIN=$1; V=$2
W=$(mktemp -d "${TMPDIR:-/tmp}/orderer-conformance.XXXXXX")
trap 'rm -rf "$W"' EXIT
fail=0
bad() { echo "FAIL $*"; fail=1; }
bysym() { awk 'match($0, /"symbol":[0-9]+/) { print substr($0, RSTART+9, RLENGTH-9) "\t" $0 }' "$1" | sort -s -n -k1,1 | cut -f2-; }
enc_flag() { [ "$1" = binary ] && echo --binary; }

for input in multisymbol fuzz_s11; do
  for P in 1 2 4; do
    want=$V/pipeline/$input/P$P
    for enc in jsonl binary; do
      "$BIN/orderrun" "$V/pipeline/$input.cmd.jsonl" --partitions $P --journal-dir "$W/$enc" $(enc_flag $enc) > "$W/listing" \
        || bad "$input P=$P $enc: orderrun failed"
      cmp -s "$W/listing" "$want/listing.evt" || bad "$input P=$P $enc: listing"
      diff -r -q "$W/$enc" "$want/$enc" > /dev/null || bad "$input P=$P $enc: journals differ byte-for-byte"
      "$BIN/orderrecover" --journal-dir "$W/$enc" $(enc_flag $enc) --partitions $P > "$W/rec" || bad "$input P=$P $enc: recover failed"
      cmp -s <(bysym "$W/rec") <(bysym "$W/listing") || bad "$input P=$P $enc: journal-form recovery"
      rm -rf "${W:?}/$enc"
    done
  done
  echo "ok   pipeline/$input"
done

for input in multisymbol fuzz_s11; do
  d=$V/checkpoint/$input
  [ -d "$d" ] || continue
  K=$(cat "$d/K")
  for enc in jsonl binary; do
    "$BIN/orderrun" "$V/pipeline/$input.cmd.jsonl" --partitions 2 --journal-dir "$W/$enc" $(enc_flag $enc) --checkpoint-every "$K" > "$W/listing" \
      || bad "checkpoint $input $enc: orderrun failed"
    cmp -s "$W/listing" "$V/pipeline/$input/P2/listing.evt" || bad "checkpoint $input $enc: listing"
    diff -r -q "$W/$enc" "$d/$enc" > /dev/null || bad "checkpoint $input $enc: directory differs byte-for-byte"
    "$BIN/orderrecover" --journal-dir "$W/$enc" $(enc_flag $enc) --partitions 2 > "$W/rec" || bad "checkpoint $input $enc: recover failed"
    cmp -s "$W/rec" "$d/recov.evt" || bad "checkpoint $input $enc: recovery from the checkpoint"
    rm -rf "${W:?}/$enc"
  done
  echo "ok   checkpoint/$input"
done

d=$V/repair/fuzz_s11
if [ -d "$d" ]; then
  for enc in jsonl binary; do
    cp -R "$d/$enc.torn" "$W/$enc"
    "$BIN/orderrecover" --journal-dir "$W/$enc" $(enc_flag $enc) --partitions 2 > /dev/null 2>&1
    [ $? = 2 ] || bad "repair $enc: strict recovery of a torn journal must exit 2"
    "$BIN/orderrecover" --journal-dir "$W/$enc" $(enc_flag $enc) --repair --partitions 2 > "$W/rec" 2> /dev/null \
      || bad "repair $enc: --repair failed"
    cmp -s "$W/rec" "$d/recov.$enc.evt" || bad "repair $enc: recovered events"
    diff -r -q "$W/$enc" "$d/$enc.repaired" > /dev/null || bad "repair $enc: repaired files differ"
    rm -rf "${W:?}/$enc"
  done
  echo "ok   repair/fuzz_s11"
fi

d=$V/compat/v1/fuzz_s11_P2
if [ -d "$d" ]; then
  "$BIN/orderrecover" --journal-dir "$d/binary" --binary --partitions 2 > "$W/rec" || bad "compat v1: recover failed"
  cmp -s <(bysym "$W/rec") <(bysym "$d/listing.evt") || bad "compat v1: recovery"
  echo "ok   compat/v1"
fi

d=$V/recovery/fuzz_s11
"$BIN/orderrun" "$d/prefix.cmd.jsonl" --partitions 3 --snap "$W/prefix.snap" > /dev/null
cmp -s "$W/prefix.snap" "$d/prefix.snap" || bad "recovery: snapshot body"
cmp -s "$W/prefix.snap.meta" "$d/prefix.snap.meta" || bad "recovery: snapshot sidecar"
for P in 1 3; do
  "$BIN/orderrecover" "$d/prefix.snap" "$d/tail.cmd.jsonl" --partitions $P | cmp -s - "$d/recov.P$P.evt" \
    || bad "recovery at P=$P"
done
echo "ok   recovery/fuzz_s11"

for f in "$V"/regress/*.cmd.jsonl; do
  "$BIN/ordererfuzz" "$f" | cmp -s - <(tail -n +2 "${f%.cmd.jsonl}.evt.jsonl") || bad "regress $(basename "$f")"
done
echo "ok   regress"

expect2() { "$@" > /dev/null 2>&1; [ $? = 2 ] || bad "exit code != 2: $*"; }
in=$V/pipeline/multisymbol.cmd.jsonl
expect2 "$BIN/orderrun"
expect2 "$BIN/orderrun" "$in" --bogus
expect2 "$BIN/orderrun" "$in" --partitions 0
expect2 "$BIN/orderrun" "$in" --partitions 3 --partition-map "$V/routing/table.map.jsonl"
expect2 "$BIN/ordererfuzz" "$in" --binary
expect2 "$BIN/orderrun" /nonexistent/file
expect2 "$BIN/orderrun" "$in" --checkpoint-every 5
printf '{"cmd":"cancel","symbol":1,"order_id":5}\n{"cmd":"new","sym' > "$W/trunc"
expect2 "$BIN/orderrecover" "$d/prefix.snap" "$W/trunc"
expect2 "$BIN/orderrecover" --journal-dir "$W/empty"
echo "ok   exit codes"
[ $fail = 0 ] && echo "conformance: all passed" || { echo "conformance: FAILURES"; exit 1; }
