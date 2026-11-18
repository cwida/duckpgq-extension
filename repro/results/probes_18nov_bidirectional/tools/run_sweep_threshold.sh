#!/usr/bin/env bash
# The sweep threshold of the search from both ends: a level is swept when its frontier holds at least
# 1 / sparse_frontier_divisor of the vertices (64 is the default, 1 never sweeps, 0 sweeps every level). 200 single
# pairs and the harness's 16 pairs with distinct sources, at SF300 and SF100. The setting also moves the threshold
# of the search from the source, so only the both-ends figures compare across divisors. Run from the root of the
# worktree, after run.sh.
set -u
ROOT="$(pwd)"
OUT="${ROOT}/repro/results/probes_18nov_bidirectional/sweep_threshold"
CLI="${ROOT}/build/release/duckdb"
EXT="${ROOT}/build/release/extension/duckpgq/duckpgq.duckdb_extension"
THREADS="$(nproc)"
mkdir -p "${OUT}"
for sf in 300 100; do
	db="${ROOT}/data/ldbc-pathfinding/db/ldbc_sf${sf}.duckdb"
	for divisor in 64 1 8 0; do
		PROBE_SETTINGS="sparse_frontier_divisor=${divisor}" python3 "${ROOT}/repro/results/probes_18nov_bidirectional/tools/probe_pairs.py" \
			"${CLI}" "${EXT}" "${db}" "${sf}" "${THREADS}" "${OUT}/pairs_lengths_sf${sf}_divisor${divisor}.csv" lengths \
			>"${OUT}/pairs_lengths_sf${sf}_divisor${divisor}.out" 2>&1
	done
	python3 "${ROOT}/scripts/pathfinding_benchmark.py" microbench --scale-factor "${sf}" --threads "${THREADS}" --repeats 5 \
		--timeout-s 600 --configs bidirectional_sweep --shapes random --pair-counts 1 16 --routes index \
		--notes "my-beans-zlnm sweep threshold SF${sf}" >"${OUT}/microbench_sf${sf}.out" 2>&1
done
