#!/usr/bin/env bash
# Thread sweep of the explore variants (my-beans-rvnn): SF300, index route, 3 repeats.
set -uo pipefail
WT=/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/explore-variants
OUT=/home/dwolde/.claude/jobs/e5491b9b/tmp
cd "$WT"
python3 scripts/pathfinding_benchmark.py microbench --scale-factor 300 --threads 32 --repeats 3 --timeout-s 600 \
  --configs explore_threads --routes index --shapes random multi_source --pair-counts 2048 --source-counts 256 1024 \
  --notes "explore variants thread sweep sf300 (my-beans-rvnn)" > "${OUT}/threads_sf300.out" 2>&1
echo "exit=$? thread sweep" >> "${OUT}/threads_sf300.out"
tail -3 "${OUT}/threads_sf300.out"
