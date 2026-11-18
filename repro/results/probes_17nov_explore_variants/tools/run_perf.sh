#!/usr/bin/env bash
# perf counters of one warm query per explore variant (my-beans-rvnn), 32 threads, index route.
set -uo pipefail
WT=/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/explore-variants
OUT=/home/dwolde/.claude/jobs/e5491b9b/tmp
DEST="${WT}/repro/results/probes_17nov_explore_variants"
CONFIGS="baseline explore_flat_serial explore_flat_ranges explore_flat_atomic explore_flat_locked"
cd "$WT"
{
  python3 "${OUT}/perf_variants.py" sf 300 random:2048 "${DEST}/perf_sf300_random_2048.csv" $CONFIGS
  echo "exit=$? sf300 random:2048"
  python3 "${OUT}/perf_variants.py" sf 300 multi:1024 "${DEST}/perf_sf300_multi_1024.csv" $CONFIGS
  echo "exit=$? sf300 multi:1024"
  python3 "${OUT}/perf_variants.py" dataset graph500-24 multi:256 "${DEST}/perf_graph500_24_multi_256.csv" $CONFIGS
  echo "exit=$? graph500-24 multi:256"
} > "${OUT}/perf.out" 2>&1
tail -4 "${OUT}/perf.out" | cut -c1-200
