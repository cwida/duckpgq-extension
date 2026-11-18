#!/usr/bin/env bash
# my-beans-sqnu, 17 Nov 2026: the CSR index with its memory in DuckDB's buffer pool (branch csr-index-buffer-manager)
# against the rc13 extension, on one DuckDB CLI (this worktree's build, DuckDB 45f4fdf3) and the shared databases.
#   1. traversal: 2048 and 65 536 random pairs and 1024 sources x all at SF300, index route, 32 threads; two rounds
#      of three repeats per extension, alternating
#   2. CREATE INDEX and explicit fold time on a copy of knows at SF300 (probe_build_fold.sh), three runs each
#   3. the both-routes correctness check of the update workload at SF10 on the branch (mismatches must be 0)
# The harness writes its CSVs under data/ldbc-pathfinding/results of this worktree (a local directory, not the shared
# one, so that no table printer takes them for a newer microbenchmark); they are copied next to this script's output.
set -uo pipefail
TOOLS="$(cd "$(dirname "$0")" && pwd)"
OUT="$(cd "${TOOLS}/.." && pwd)"
W="$(cd "${OUT}/../../.." && pwd)"
RC13="${RC13_EXTENSION:-/local/home/dwolde/bootstrap/duckpgq-extension/build/release/extension/duckpgq/duckpgq.duckdb_extension}"
NEW="${W}/build/release/extension/duckpgq/duckpgq.duckdb_extension"
HARNESS="python3 ${W}/scripts/pathfinding_benchmark.py"
MICROBENCH=(microbench --scale-factor 300 --threads 32 --repeats 3 --timeout-s 600 --configs baseline
            --shapes random multi_source --pair-counts 2048 65536 --source-counts 1024 --routes index)
cd "${W}"
echo "start $(date -u +%FT%TZ) commit $(git rev-parse --short HEAD)" >"${OUT}/run.log"

for round in 1 2; do
  DUCKPGQ_EXTENSION="${RC13}" ${HARNESS} "${MICROBENCH[@]}" --notes "my-beans-sqnu rc13 extension round ${round}" \
    >>"${OUT}/traversal_rc13.out" 2>&1
  echo "traversal rc13 round ${round} exit $? $(date -u +%T)" >>"${OUT}/run.log"
  DUCKPGQ_EXTENSION="${NEW}" ${HARNESS} "${MICROBENCH[@]}" --notes "my-beans-sqnu branch round ${round}" \
    >>"${OUT}/traversal_branch.out" 2>&1
  echo "traversal branch round ${round} exit $? $(date -u +%T)" >>"${OUT}/run.log"
done

for run in 1 2 3; do
  "${TOOLS}/probe_build_fold.sh" rc13 "${RC13}" 300 "${OUT}/build_fold_rc13.out"
  "${TOOLS}/probe_build_fold.sh" branch "${NEW}" 300 "${OUT}/build_fold_branch.out"
  echo "build and fold run ${run} done $(date -u +%T)" >>"${OUT}/run.log"
done

${HARNESS} update-concurrent --scale-factor 10 --threads 32 --batch-size 2000 --batches 190 --readers 4 \
  --writer-pause-ms 200 --fold-ratio 0.01 --notes "my-beans-sqnu correctness check, both routes, branch" \
  >"${OUT}/oracle_sf10.out" 2>&1
echo "oracle exit $? $(date -u +%T)" >>"${OUT}/run.log"

cp "${W}"/data/ldbc-pathfinding/results/sf300/microbench_sf300_threads32_*.csv "${OUT}/" 2>/dev/null
cp "${W}"/data/ldbc-pathfinding/results/sf10/update_concurrent_*.csv "${OUT}/" 2>/dev/null
echo "end $(date -u +%FT%TZ)" >>"${OUT}/run.log"
