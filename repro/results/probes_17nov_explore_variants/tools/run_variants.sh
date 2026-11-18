#!/usr/bin/env bash
# Explore-variant ablation (my-beans-rvnn): one microbench step per target, index route, 3 repeats.
# usage: run_variants.sh <target> <configs...>   target = sf<N> or a Graphalytics dataset name
set -uo pipefail
WT=/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/explore-variants
OUT=/home/dwolde/.claude/jobs/e5491b9b/tmp
target="$1"; shift
case "$target" in
  sf*) where=(--scale-factor "${target#sf}") ;;
  *)   where=(--dataset "$target") ;;
esac
cd "$WT"
python3 scripts/pathfinding_benchmark.py microbench "${where[@]}" --threads 32 --repeats 3 --timeout-s 600 \
  --configs "$@" --routes index --shapes random multi_source --pair-counts 2048 65536 --source-counts 256 1024 \
  --notes "explore variants ${target} (my-beans-rvnn, 1bd6b8e2)" > "${OUT}/variants_${target}.out" 2>&1
echo "exit=$? target=${target}" >> "${OUT}/variants_${target}.out"
tail -3 "${OUT}/variants_${target}.out"
