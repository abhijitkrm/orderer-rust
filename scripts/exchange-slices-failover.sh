#!/usr/bin/env bash
# Phase 2 (credit slices) as real processes under `kill -9`: the same test as
# exchange-failover.sh with SLICES=1 (see its header). Every setting of that script
# applies, e.g. SECS=60 KILLS=14 SHARDS=2 MGROUPS=2 scripts/exchange-slices-failover.sh;
# CREDIT_GROUPS=2 (with MGROUPS=2) puts both matching groups in credit mode.
export SLICES=1
exec "$(dirname "$0")/exchange-failover.sh" "$@"
