#!/usr/bin/env bash
# One pair searched from both ends (my-beans-zlnm): the whole measurement. Run from the root of the worktree whose
# build/release holds the branch's DuckDB CLI and extension and whose data/ldbc-pathfinding/db holds private copies
# of the benchmark databases (the harness creates the index WITH (reverse = true) and the pair tables in them).
#
#   repro/results/probes_18nov_bidirectional/tools/run.sh [scale factors, default "10 100 300"]
set -u
ROOT="$(pwd)"
OUT="${ROOT}/repro/results/probes_18nov_bidirectional"
HARNESS="python3 ${ROOT}/scripts/pathfinding_benchmark.py"
CLI="${ROOT}/build/release/duckdb"
EXT="${ROOT}/build/release/extension/duckpgq/duckpgq.duckdb_extension"
RC13="/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension"
THREADS="$(nproc)"
SCALES="${1:-10 100 300}"
LOG="${OUT}/run.log"

step() {
	local name="$1"
	shift
	echo "$(date -u +%FT%TZ) start ${name}" >>"${LOG}"
	"$@" >"${OUT}/${name}.out" 2>&1
	echo "$(date -u +%FT%TZ) end ${name} exit=$?" >>"${LOG}"
}

echo "$(date -u +%FT%TZ) commit $(git rev-parse HEAD) extension $(sha256sum "${EXT}" | cut -c1-16) threads ${THREADS}" >>"${LOG}"
for sf in ${SCALES}; do
	db="${ROOT}/data/ldbc-pathfinding/db/ldbc_sf${sf}.duckdb"
	# The harness's pair (pairs=1 of the sweep) and 16 pairs with distinct sources, index route: the directed index
	# (baseline, as in the frozen run), the index with a reverse side searched from both ends (reverse_index) and
	# from the source only (reverse_index_one_end)
	step "microbench_sf${sf}" ${HARNESS} microbench --scale-factor "${sf}" --threads "${THREADS}" --repeats 5 \
		--timeout-s 600 --configs bidirectional --shapes random --pair-counts 1 16 --routes index \
		--notes "my-beans-zlnm bidirectional pair SF${sf}"
	# The same pair on the rc13 extension (directed index, search from the source)
	DUCKPGQ_EXTENSION="${RC13}" step "microbench_rc13_sf${sf}" ${HARNESS} microbench --scale-factor "${sf}" \
		--threads "${THREADS}" --repeats 5 --timeout-s 600 --configs baseline --shapes random --pair-counts 1 16 \
		--routes index --notes "my-beans-zlnm rc13 extension SF${sf}"
	# 200 pairs, each as its own query, lengths and paths
	step "pairs_lengths_sf${sf}" python3 "${OUT}/tools/probe_pairs.py" "${CLI}" "${EXT}" "${db}" "${sf}" "${THREADS}" \
		"${OUT}/pairs_lengths_sf${sf}.csv" lengths
	step "pairs_paths_sf${sf}" python3 "${OUT}/tools/probe_pairs.py" "${CLI}" "${EXT}" "${db}" "${sf}" "${THREADS}" \
		"${OUT}/pairs_paths_sf${sf}.csv" paths
	cp "${ROOT}"/data/ldbc-pathfinding/results/sf"${sf}"/microbench_sf"${sf}"_*.csv "${OUT}/" 2>/dev/null
done
echo "$(date -u +%FT%TZ) done" >>"${LOG}"
