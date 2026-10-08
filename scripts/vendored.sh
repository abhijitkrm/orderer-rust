#!/usr/bin/env bash
# vendored.sh — prove the vendored copy of an upstream spec is untouched.
#
# Works in two places (copy this script verbatim into impl repos):
#   spec repo  (orderer):      checks spec/matcher/ + vectors/matcher/ against
#                              the matcher commit pinned in docs/VENDORED.md
#   impl repo  (orderer-<x>):  checks spec/ + vectors/ against the orderer
#                              spec commit pinned in docs/VENDORED.md
#
# Two checks:
#   1. local:    every vendored file matches docs/VENDORED.sha256 (no local edits,
#                no missing/extra files).
#   2. upstream: if the upstream repo is checked out (UPSTREAM_DIR, default the
#                sibling named in VENDORED.md), every file equals the blob at the
#                pinned commit, and upstream HEAD drift is reported (not fatal).
#
#   scripts/vendored.sh            # verify
#   scripts/vendored.sh --update   # rewrite docs/VENDORED.sha256 from disk
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)

field() { sed -n "s/^- \*\*$1\*\*: \`\(.*\)\`$/\1/p" docs/VENDORED.md | head -1; }
UPSTREAM=$(field upstream)        # e.g. matcher
COMMIT=$(field commit)            # full sha
read -r -a PATHS <<< "$(field paths)"   # "local=upstream local=upstream"

[ -n "$UPSTREAM" ] && [ -n "$COMMIT" ] && [ ${#PATHS[@]} -gt 0 ] \
  || { echo "vendored: docs/VENDORED.md missing upstream/commit/paths"; exit 1; }

list_files() {
  for m in "${PATHS[@]}"; do
    find "${m%%=*}" -type f ! -name .DS_Store
  done | LC_ALL=C sort
}

if [ "${1:-}" = "--update" ]; then
  list_files | xargs shasum -a 256 > docs/VENDORED.sha256
  echo "vendored: wrote docs/VENDORED.sha256 ($(wc -l < docs/VENDORED.sha256 | tr -d ' ') files)"
  exit 0
fi

# ---- 1. local integrity ------------------------------------------------------
shasum -a 256 -c --quiet docs/VENDORED.sha256 \
  || { echo "vendored: local copy modified (see above)"; exit 1; }
diff <(list_files) <(awk '{print $2}' docs/VENDORED.sha256) >/dev/null \
  || { echo "vendored: file set differs from docs/VENDORED.sha256:";
       diff <(list_files) <(awk '{print $2}' docs/VENDORED.sha256) | head; exit 1; }
echo "vendored: $(wc -l < docs/VENDORED.sha256 | tr -d ' ') files match docs/VENDORED.sha256"

# ---- 2. upstream equality at the pinned commit -------------------------------
UP_DIR=${UPSTREAM_DIR:-$ROOT/../$UPSTREAM}
if ! git -C "$UP_DIR" cat-file -e "$COMMIT^{commit}" 2>/dev/null; then
  echo "vendored: upstream $UPSTREAM@${COMMIT:0:7} not available at $UP_DIR — skipped upstream check"
  exit 0
fi
bad=0
for m in "${PATHS[@]}"; do
  local_dir=${m%%=*}; up_dir=${m#*=}
  # every upstream file at the pin must exist locally with identical bytes
  while IFS= read -r up_path; do
    rel=${up_path#"$up_dir"/}
    if ! cmp -s <(git -C "$UP_DIR" show "$COMMIT:$up_path") "$local_dir/$rel"; then
      echo "vendored: $local_dir/$rel differs from $UPSTREAM@${COMMIT:0:7}:$up_path"; bad=1
    fi
  done < <(git -C "$UP_DIR" ls-tree -r --name-only "$COMMIT" -- "$up_dir")
  # and nothing extra locally
  n_up=$(git -C "$UP_DIR" ls-tree -r --name-only "$COMMIT" -- "$up_dir" | wc -l | tr -d ' ')
  n_local=$(find "$local_dir" -type f ! -name .DS_Store | wc -l | tr -d ' ')
  [ "$n_up" = "$n_local" ] || { echo "vendored: $local_dir has $n_local files, upstream $up_dir has $n_up"; bad=1; }
done
[ "$bad" = 0 ] || exit 1
echo "vendored: identical to $UPSTREAM@${COMMIT:0:7}"

head=$(git -C "$UP_DIR" rev-parse HEAD)
if [ "$head" != "$COMMIT" ]; then
  changed=$(for m in "${PATHS[@]}"; do git -C "$UP_DIR" diff --name-only "$COMMIT" HEAD -- "${m#*=}"; done | wc -l | tr -d ' ')
  echo "vendored: note — upstream HEAD is ${head:0:7}; $changed vendored file(s) changed since pin"
fi
