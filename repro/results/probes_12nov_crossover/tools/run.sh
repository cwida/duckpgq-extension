#!/bin/bash
# Crossover rerun of 12 Nov on the frozen build (rc13, 256 lanes): single-source kernel against lane waves of one lane
# per source, k = 2..256, k random pairs and k sources x 200 destinations, index route, three repeats.
cd /local/home/dwolde/bootstrap/duckpgq-extension
for ds in cit-Patents datagen-8_4-fb graph500-24; do
  python3 scripts/pathfinding_benchmark.py microbench --dataset "$ds" --threads 32 --repeats 3 --timeout-s 600 \
    --configs single_source_kernel multi_source_dedup --shapes random all_pairs \
    --pair-counts 2 4 8 16 32 64 128 256 --source-count 2 4 8 16 32 64 128 256 --target-count 200 --routes index \
    --notes "Section 3 crossover $ds rc13 12 Nov (256 lanes) repro $(git rev-parse --short HEAD) $(hostname)" \
    > /tmp/crossover_12nov/$ds.out 2>&1
  echo "$ds rc=$? $(date -u +%H:%M:%S)" >> /tmp/crossover_12nov/progress.txt
done
