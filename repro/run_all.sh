#!/usr/bin/env bash
# The full measurement matrix of the paper, in the order it was run. Every step is resumable:
# a finished step leaves logs/<name>.done and is skipped on rerun. Total wall time on the paper
# machine was roughly 3.5 days, dominated by the baselines at SF300 (10 minute timeouts) and the
# FalkorDB/Neo4j update workloads (about 30 minutes each).
#
# Subsets:  ./run_all.sh sweep | ldbc | client | microbench | updates | all   (default all)
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "${REPO_ROOT}"
what="${1:-all}"
COMMON=(--threads "${THREADS}" --repeats "${REPEATS}" --timeout-s "${TIMEOUT_S}")
NOTE="repro $(git rev-parse --short HEAD) $(hostname)"

if [ "$what" = all ] || [ "$what" = sweep ]; then
  # W27: random pairs 1..65536 and sources x all persons (1, 16, 128, 1024) on every system.
  # Baselines run one client thread here; Kuzu/Memgraph/FalkorDB get the 32-client pass below.
  for sf in ${SCALE_FACTORS}; do
    step "sweep_pairs_sf${sf}" ${HARNESS} sweep-pairs --scale-factor "$sf" "${COMMON[@]}" --notes "W27 sweep SF${sf} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = ldbc ]; then
  # W17: LDBC IC13, BI19, BI20 (hop-count variants) on every system.
  for sf in ${SCALE_FACTORS}; do
    step "ldbc_queries_sf${sf}" ${HARNESS} ldbc-queries --scale-factor "$sf" "${COMMON[@]}" --notes "W17 SF${sf} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = client ]; then
  # Client-side concurrency for the per-query baselines (SF10 and SF100 only; SF300 times out).
  for sf in 10 100; do
    step "sweep_pairs_sf${sf}_client${CLIENT_THREADS}" ${HARNESS} sweep-pairs --scale-factor "$sf" "${COMMON[@]}" \
      --client-threads "${CLIENT_THREADS}" --systems memgraph falkordb kuzu --notes "W27 SF${sf} client threads ${CLIENT_THREADS} ${NOTE}"
    step "ldbc_queries_sf${sf}_client${CLIENT_THREADS}" ${HARNESS} ldbc-queries --scale-factor "$sf" "${COMMON[@]}" \
      --client-threads "${CLIENT_THREADS}" --systems memgraph falkordb kuzu --notes "W17 SF${sf} client threads ${CLIENT_THREADS} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = microbench ]; then
  # W19: one mechanism switched off at a time on the DuckPGQ routes (docs/MICROBENCHMARK_SWITCHES.md)
  for sf in 10 100; do
    step "microbench_sf${sf}" ${HARNESS} microbench --scale-factor "$sf" "${COMMON[@]}" --configs all --notes "W19 microbenchmark SF${sf} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = updates ]; then
  # W16: a stream of 190 commits of 2000 knows insert/delete operations, one query per commit.
  UPD=(--scale-factor 10 --threads "${THREADS}" --batch-size 2000 --batches 190)
  step "update_sf10_sssp"           ${HARNESS} update-run "${UPD[@]}" --query-pattern sssp           --notes "W16 ${NOTE}"
  step "update_sf10_p2p"            ${HARNESS} update-run "${UPD[@]}" --query-pattern point_to_point --notes "W16 ${NOTE}"
  step "update_sf10_sssp_t1"        ${HARNESS} update-run --scale-factor 10 --threads 1 --batch-size 2000 --batches 190 --query-pattern sssp --notes "W16 ${NOTE}"
  step "update_sf100_sssp"          ${HARNESS} update-run --scale-factor 100 --threads "${THREADS}" --batch-size 10000 --batches 150 --query-pattern sssp --notes "W16 ${NOTE}"
  for readers in 1 4 8 16; do
    step "concurrent_sf10_r${readers}" ${HARNESS} update-concurrent "${UPD[@]}" --readers "$readers" --writer-pause-ms 200 --notes "W16 concurrent ${NOTE}"
  done
  step "update_kuzu_sf10_sssp"      ${HARNESS} update-kuzu     "${UPD[@]}" --query-pattern sssp --notes "W16 ${NOTE}"
  step "update_memgraph_sf10_sssp"  ${HARNESS} update-memgraph "${UPD[@]}" --query-pattern sssp --notes "W16 ${NOTE}"
  step "update_falkordb_sf10_sssp"  ${HARNESS} update-falkordb "${UPD[@]}" --query-pattern sssp --notes "W16 ${NOTE}"
  step "update_neo4j_sf10_sssp"     ${HARNESS} update-neo4j    "${UPD[@]}" --query-pattern sssp --notes "W16 ${NOTE}"
  step "update_neo4j_sf10_p2p"      ${HARNESS} update-neo4j    "${UPD[@]}" --query-pattern point_to_point --notes "W16 ${NOTE}"
  for sys in kuzu memgraph falkordb; do
    step "update_${sys}_sf10_p2p"   ${HARNESS} "update-${sys}" --scale-factor 10 --threads "${THREADS}" --batch-size 2000 --batches 20 --query-pattern point_to_point --notes "W16 ${NOTE}"
  done
fi

echo "matrix finished; next: ./collect.sh"
