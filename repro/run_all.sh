#!/usr/bin/env bash
# The full measurement matrix of the paper, in the order it was run. Every step is resumable:
# a finished step leaves logs/<name>.done and is skipped on rerun. Total wall time on the paper
# machine was roughly 3.5 days, dominated by the baselines at SF300 (10 minute timeouts) and the
# FalkorDB/Neo4j update workloads (about 30 minutes each).
#
# Subsets:  ./run_all.sh fixed | sweep | graphalytics | ldbc | client | native | microbench | throughput | updates | all   (default all)
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "${REPO_ROOT}"
what="${1:-all}"
COMMON=(--threads "${THREADS}" --repeats "${REPEATS}" --timeout-s "${TIMEOUT_S}")
NOTE="repro $(git rev-parse --short HEAD) $(hostname)"
# Steps written for one scale factor run only when SCALE_FACTORS lists it (SCALE_FACTORS=10 is the dry run of the matrix)
has_sf() { [[ " ${SCALE_FACTORS} " == *" $1 "* ]]; }

if [ "$what" = all ] || [ "$what" = fixed ]; then
  # Fixed costs: database size, CSR index build time and bytes per edge, every baseline's fresh load and store size
  for sf in ${SCALE_FACTORS}; do
    step "fixed_costs_sf${sf}" ${HARNESS} fixed-costs --scale-factor "$sf" --threads "${THREADS}" --repeats "${REPEATS}" --notes "fixed costs SF${sf} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = sweep ]; then
  # W27: random pairs 1..65536 and sources x all persons (1, 16, 128, 1024) on every system.
  # Baselines run one client thread here; Kuzu/Memgraph/FalkorDB get the 32-client pass below.
  for sf in ${SCALE_FACTORS}; do
    step "sweep_pairs_sf${sf}" ${HARNESS} sweep-pairs --scale-factor "$sf" "${COMMON[@]}" --notes "W27 sweep SF${sf} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = graphalytics ]; then
  # W30: three Graphalytics graphs next to LDBC. cit-Patents (3.8M vertices, 16.5M edges, directed, long tail of BFS
  # levels) is small enough for every baseline; graph500-24 (8.9M, 260M, heavy hubs) and datagen-8_4-fb (3.8M, 269M,
  # dense LDBC family) run DuckPGQ and Kuzu only. Pairs and sources are sampled from vertices with edges; the Cypher
  # systems run without the ..30 hop bound and Kuzu with var_length_extend_max_depth raised.
  step "fixed_costs_cit_patents" ${HARNESS} fixed-costs --dataset cit-Patents --threads "${THREADS}" --repeats "${REPEATS}" --notes "fixed costs cit-Patents ${NOTE}"
  step "sweep_pairs_cit_patents" ${HARNESS} sweep-pairs --dataset cit-Patents "${COMMON[@]}" --notes "W30 sweep cit-Patents ${NOTE}"
  step "sweep_pairs_cit_patents_client${CLIENT_THREADS}" ${HARNESS} sweep-pairs --dataset cit-Patents "${COMMON[@]}" \
    --client-threads "${CLIENT_THREADS}" --systems memgraph falkordb kuzu --notes "W30 cit-Patents client threads ${CLIENT_THREADS} ${NOTE}"
  for ds in graph500-24 datagen-8_4-fb; do
    name="${ds//-/_}"
    step "fixed_costs_${name}" ${HARNESS} fixed-costs --dataset "$ds" --threads "${THREADS}" --repeats "${REPEATS}" --systems kuzu --notes "fixed costs ${ds} ${NOTE}"
    step "sweep_pairs_${name}" ${HARNESS} sweep-pairs --dataset "$ds" "${COMMON[@]}" --systems duckpgq_build duckpgq_index kuzu --notes "W30 sweep ${ds} ${NOTE}"
  done
  # The batch scheduler on a graph whose multi-source working set (192 B per vertex, 1.7 GB per wave) exceeds the
  # 512 MiB budget: the memory-bound regime of the adaptive scheduler, index route
  step "microbench_scheduling_graph500_24" ${HARNESS} microbench --dataset graph500-24 "${COMMON[@]}" --configs scheduling --shapes random --pair-counts 2048 16384 65536 --routes index --notes "W31 scheduling graph500-24 ${NOTE}"
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
    has_sf "$sf" || continue
    step "sweep_pairs_sf${sf}_client${CLIENT_THREADS}" ${HARNESS} sweep-pairs --scale-factor "$sf" "${COMMON[@]}" \
      --client-threads "${CLIENT_THREADS}" --systems memgraph falkordb kuzu --notes "W27 SF${sf} client threads ${CLIENT_THREADS} ${NOTE}"
    step "ldbc_queries_sf${sf}_client${CLIENT_THREADS}" ${HARNESS} ldbc-queries --scale-factor "$sf" "${COMMON[@]}" \
      --client-threads "${CLIENT_THREADS}" --systems memgraph falkordb kuzu --notes "W17 SF${sf} client threads ${CLIENT_THREADS} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = native ]; then
  # DuckDB-native baselines: WITH RECURSIVE ... USING KEY and the CIDR 2023 scalar UDF route, next to the index route
  # for the agreement check (SF10 and SF100; both time out early on the large points)
  for sf in 10 100; do
    has_sf "$sf" || continue
    step "native_baselines_sf${sf}" ${HARNESS} sweep-pairs --scale-factor "$sf" "${COMMON[@]}" --systems duckpgq_index duckpgq_recursive duckpgq_scalar --notes "native baselines SF${sf} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = microbench ]; then
  # W19: one mechanism switched off at a time on the DuckPGQ routes (docs/MICROBENCHMARK_SWITCHES.md)
  for sf in 10 100; do
    has_sf "$sf" || continue
    step "microbench_sf${sf}" ${HARNESS} microbench --scale-factor "$sf" "${COMMON[@]}" --configs all --notes "W19 microbenchmark SF${sf} ${NOTE}"
    step "microbench_memory_sf${sf}" ${HARNESS} microbench --scale-factor "$sf" "${COMMON[@]}" --configs memory --shapes random --pair-counts 2048 16384 65536 --notes "W31 explore memory traffic SF${sf} ${NOTE}"
    step "microbench_partitions_sf${sf}" ${HARNESS} microbench --scale-factor "$sf" "${COMMON[@]}" --configs partitions --shapes random multi_source --pair-counts 128 2048 16384 65536 --source-counts 1 1024 --notes "W32 partition geometry SF${sf} ${NOTE}"
    step "microbench_scheduling_sf${sf}" ${HARNESS} microbench --scale-factor "$sf" "${COMMON[@]}" --configs scheduling --shapes random --pair-counts 2048 16384 65536 --notes "W31 scheduling SF${sf} ${NOTE}"
  done
  # Section 3 of the paper: one pair on the lane kernel vs the per-source kernel (1 pair, no_source_grouping),
  # 20 sources x 200 destinations with and without source grouping, paths against lengths
  for sf in 10 100; do
    has_sf "$sf" || continue
    step "microbench_section3_sf${sf}" ${HARNESS} microbench --scale-factor "$sf" "${COMMON[@]}" --configs section3 --shapes random all_pairs --pair-counts 1 2048 65536 --source-count 20 --target-count 200 --notes "Section 3 switches SF${sf} ${NOTE}"
    # Section 3 crossover (2 Nov): where the single-source traversal stops winning, by k distinct sources, for k
    # random pairs and for k sources x 200 destinations, the three kernels forced (index route; median and the
    # kernel's working set per point from the strategy string, process peak RSS per point)
    step "microbench_crossover_sf${sf}" ${HARNESS} microbench --scale-factor "$sf" "${COMMON[@]}" --configs crossover --shapes random all_pairs --pair-counts 1 2 4 8 16 32 64 128 256 512 1024 4096 --source-count 1 2 4 8 16 32 64 128 256 512 1024 4096 --target-count 200 --routes index --notes "Section 3 crossover SF${sf} ${NOTE}"
  done
  # The scheduling crossover figure needs SF300 as well; index route only
  has_sf 300 && step "microbench_scheduling_sf300" ${HARNESS} microbench --scale-factor 300 "${COMMON[@]}" --configs scheduling --shapes random --pair-counts 2048 16384 65536 --routes index --notes "W31 scheduling SF300 ${NOTE}"
fi

if [ "$what" = all ] || [ "$what" = throughput ]; then
  # W35 (29 Oct): single-pair queries per second from 1 to 32 clients, index route against build-per-query route;
  # the index's natural workload (many short queries from many clients)
  for sf in ${SCALE_FACTORS}; do
    step "throughput_sf${sf}" ${HARNESS} throughput --scale-factor "$sf" --threads "${THREADS}" --clients 1 2 4 8 16 32 --duration-s 20 --notes "W35 throughput SF${sf} ${NOTE}"
  done
fi

if [ "$what" = all ] || [ "$what" = updates ]; then
  # W16: a stream of 190 commits of 2000 knows insert/delete operations, one query per commit.
  UPD=(--scale-factor 10 --threads "${THREADS}" --batch-size 2000 --batches 190)
  step "update_sf10_sssp"           ${HARNESS} update-run "${UPD[@]}" --query-pattern sssp           --notes "W16 ${NOTE}"
  step "update_sf10_p2p"            ${HARNESS} update-run "${UPD[@]}" --query-pattern point_to_point --notes "W16 ${NOTE}"
  step "update_sf10_sssp_t1"        ${HARNESS} update-run --scale-factor 10 --threads 1 --batch-size 2000 --batches 190 --query-pattern sssp --notes "W16 ${NOTE}"
  has_sf 100 && step "update_sf100_sssp"          ${HARNESS} update-run --scale-factor 100 --threads "${THREADS}" --batch-size 10000 --batches 150 --query-pattern sssp --notes "W16 ${NOTE}"
  # One route per run (2 Nov): with both routes in one driver run every reader alternates an index and a build
  # query, so the index latency is measured while the other readers saturate the cores with per-query builds and the
  # writer's commit latency mixes both routes (SF300, 16 readers: index 142 ms per query in a shared run against
  # 68 ms alone; commits 846 ms shared, 126 ms with index readers, 883 ms with build readers)
  for readers in 1 4 8 16; do
    for route in index build; do
      step "concurrent_sf10_r${readers}_${route}" ${HARNESS} update-concurrent "${UPD[@]}" --readers "$readers" --writer-pause-ms 200 --routes "$route" --notes "W16 concurrent ${route} ${NOTE}"
    done
  done
  # The unpaced writer: commits as fast as it can while 4 readers query
  for route in index build; do
    step "concurrent_sf10_r4_unpaced_${route}" ${HARNESS} update-concurrent "${UPD[@]}" --readers 4 --writer-pause-ms 0 --routes "$route" --notes "W16 concurrent unpaced ${route} ${NOTE}"
  done
  # Where the index matters (29 Oct): the same concurrent readers at SF100 and SF300, where building the graph inside
  # the query costs 128 ms and 264 ms instead of SF10's 22 ms (the SF300 log is staged from the generator's batches)
  for readers in 1 4 16; do
    for route in index build; do
      has_sf 100 && step "concurrent_sf100_r${readers}_${route}" ${HARNESS} update-concurrent --scale-factor 100 --threads "${THREADS}" --batch-size 2000 --batches 100 --readers "$readers" --writer-pause-ms 200 --routes "$route" --notes "W16 concurrent SF100 ${route} ${NOTE}"
      has_sf 300 && step "concurrent_sf300_r${readers}_${route}" ${HARNESS} update-concurrent --scale-factor 300 --threads "${THREADS}" --batch-size 2000 --batches 60 --readers "$readers" --writer-pause-ms 200 --routes "$route" --notes "W16 concurrent SF300 ${route} ${NOTE}"
    done
  done
  # Correctness oracle (2 Nov): one run with both routes in the same process, so the driver compares every index
  # answer with the build route's answer ("mismatches" must be 0; the harness fails the step otherwise). The low fold
  # ratio makes folds happen under the readers and the writer (SF10: a fold about every 9 commits). Not a timing step.
  step "concurrent_sf10_r4_check" ${HARNESS} update-concurrent "${UPD[@]}" --readers 4 --writer-pause-ms 200 --fold-ratio 0.01 --notes "W16 correctness check, both routes ${NOTE}"
  # Fold latency at SF300 (W36 item e, 2 Nov): at the default fold ratio 0.1 the 60 commits above add 120k log entries
  # against 55.6M edges, so no fold happens at SF100 or SF300 in the matrix. This step lowers the ratio so a fold runs
  # under 16 index readers (about every 14 commits); duckpgq_csr_index_fold_stats in the stats column gives its latency
  has_sf 300 && step "concurrent_sf300_r16_fold" ${HARNESS} update-concurrent --scale-factor 300 --threads "${THREADS}" --batch-size 2000 --batches 60 --readers 16 --writer-pause-ms 200 --routes index --fold-ratio 0.0005 --notes "W36 fold latency SF300 ${NOTE}"
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
