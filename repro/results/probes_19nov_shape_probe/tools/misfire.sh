#!/usr/bin/env bash
# Does the graph shape probe misfire on the graphs of Section 5? Random pairs on the index route, the default
# (baseline: probe on) against no_shape_probe, per graph. Usage, from the worktree root:
#   tools/misfire.sh prepare|time <graph>...     graphs: sf10 sf100 sf300 cit-Patents datagen-8_4-fb graph500-24
# MISFIRE_THREADS overrides the thread count (all cores by default).
# prepare: one repeat with 64 pairs, so that the pair tables exist and the CSR index is rebuilt in this build's
# format (not timed). time: three repeats; the harness CSV is copied here as misfire_<graph>.csv.
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "${HERE}/../../.." && pwd)"
LOG="${HERE}/run.log"
ACTION="$1"; shift
cd "${ROOT}"
for graph in "$@"; do
    case "${graph}" in
        sf*) target=(--scale-factor "${graph#sf}"); results="data/ldbc-pathfinding/results/${graph}"; counts=(64 1024 65536) ;;
        *)   target=(--dataset "${graph}"); results="data/ldbc-pathfinding/results/graphalytics_${graph//-/_}"; counts=(64 1024) ;;
    esac
    if [ "${ACTION}" = prepare ]; then
        repeats=1; counts=(64)
    else
        repeats=3
    fi
    echo "$(date -u +%FT%TZ) misfire ${ACTION} graph=${graph} commit=$(git rev-parse --short HEAD)" \
         "extension_sha256=$(sha256sum build/release/extension/duckpgq/duckpgq.duckdb_extension | cut -c1-16)" \
         "load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "${LOG}"
    timeout 1500 python3 scripts/pathfinding_benchmark.py microbench "${target[@]}" --threads "${MISFIRE_THREADS:-$(nproc)}" \
        --repeats "${repeats}" --timeout-s 600 --configs baseline no_shape_probe --shapes random \
        --pair-counts "${counts[@]}" --routes index --notes "shape probe misfire check (my-beans-83ld)" \
        > "${HERE}/misfire_${ACTION}_${graph}.out" 2>&1
    status=$?
    csv="$(grep -o 'Wrote .*\.csv' "${HERE}/misfire_${ACTION}_${graph}.out" | tail -1 | cut -d' ' -f2)"
    if [ "${ACTION}" = time ] && [ -n "${csv}" ]; then
        cp "${csv}" "${HERE}/misfire_${graph}.csv"
    fi
    echo "$(date -u +%FT%TZ) end misfire ${ACTION} graph=${graph} exit=${status} load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "${LOG}"
done
