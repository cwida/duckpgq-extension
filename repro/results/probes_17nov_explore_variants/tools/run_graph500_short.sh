#!/usr/bin/env bash
# Explore variants on graph500-24 without the 65,536-pair point (152 s per baseline query; measured for the baseline
# and flat_serial in the first, stopped run). Index route, 3 repeats.
set -uo pipefail
WT=/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/explore-variants
OUT=/home/dwolde/.claude/jobs/e5491b9b/tmp
cd "$WT"
python3 scripts/pathfinding_benchmark.py microbench --dataset graph500-24 --threads 32 --repeats 3 --timeout-s 600 \
  --configs baseline explore_flat_serial explore_flat_ranges explore_flat_atomic explore_flat_locked no_sub_range_tasks \
  --routes index --shapes random multi_source --pair-counts 2048 --source-counts 256 1024 \
  --notes "explore variants graph500-24 short (my-beans-rvnn)" > "${OUT}/variants_graph500_short.out" 2>&1
echo "exit=$? graph500-24 short" >> "${OUT}/variants_graph500_short.out"
tail -3 "${OUT}/variants_graph500_short.out"
