#!/usr/bin/env bash
# Rerun of the rows the rc14 sweep lost: the four flat explore variants of the CSR ablation, which crashed at process
# exit on 3025ab1b. Binary: 3025ab1b + the teardown fix 39181a6d (branch flat-csr-cache-teardown). Same points, same
# notes as the steps of the matrix, so that the table printer puts them next to the partitioned rows of the sweep;
# the repo_commit column of the new files says 39181a6d.
set -u
source ~/sweep_env.sh
FIX=39181a6d
cd ~/duckpgq-extension || exit 1
LOG=~/sweep/flat_rerun.log
echo "$(date -u +%FT%TZ) start" >> $LOG
mkdir -p ~/sweep/builds/rc14
[ -f ~/sweep/builds/rc14/duckpgq.duckdb_extension ] || cp -p build/release/extension/duckpgq/duckpgq.duckdb_extension ~/sweep/builds/rc14/
git fetch -q origin flat-csr-cache-teardown || { echo "fetch failed" >> $LOG; exit 1; }
git checkout -q --detach FETCH_HEAD || exit 1
case "$(git rev-parse HEAD)" in ${FIX}*) ;; *) echo "unexpected commit $(git rev-parse HEAD)" >> $LOG; exit 1;; esac
[ "$(git rev-parse HEAD~1)" = 3025ab1b8e1d90989ac4f459d54692812980e41b ] || { echo "parent is not rc14" >> $LOG; exit 1; }
GEN=ninja EXT_FLAGS="-DDUCKPGQ_LANE_LIMIT=256" make release > ~/sweep/flat_rerun_build.out 2>&1 || { echo "build failed" >> $LOG; exit 1; }
./build/release/test/unittest "test/*" > ~/sweep/flat_rerun_tests.out 2>&1; echo "$(date -u +%FT%TZ) suite exit=$? $(tail -2 ~/sweep/flat_rerun_tests.out | head -1)" >> $LOG
grep -q "All tests passed" ~/sweep/flat_rerun_tests.out || exit 1
H="python3 scripts/pathfinding_benchmark.py microbench --threads 32 --repeats 3 --timeout-s 600 --routes index"
NOTE="repro 3025ab1b $(hostname)"
V="explore_flat_serial explore_flat_ranges explore_flat_atomic explore_flat_locked"
T=""; for v in serial ranges atomic locked; do for n in 1 2 4 8 16; do T="$T explore_flat_${v}_t${n}"; done; T="$T explore_flat_${v}"; done
run() { name="$1"; shift; s=$(date +%s); "$@" > repro/logs/flat_rerun_${name}.out 2>&1; echo "$(date -u +%FT%TZ) ${name} exit=$? seconds=$(( $(date +%s) - s ))" >> $LOG; }
for sf in 10 100 300; do
  run explore_scaling_sf${sf}  $H --scale-factor $sf --configs $T --shapes random multi_source --pair-counts 2048 --source-counts 256 1024 --notes "CSR ablation scaling SF${sf} ${NOTE}"
  run explore_variants_sf${sf} $H --scale-factor $sf --configs $V --shapes random --pair-counts 65536 --notes "CSR ablation variants SF${sf} ${NOTE}"
done
run explore_variants_graph500_24 $H --dataset graph500-24 --configs $V --shapes random multi_source --pair-counts 2048 --source-counts 256 1024 --notes "CSR ablation variants graph500-24 ${NOTE}"
run explore_scaling_graph500_24  $H --dataset graph500-24 --configs $T --shapes random multi_source --pair-counts 2048 --source-counts 256 --notes "CSR ablation scaling graph500-24 ${NOTE}"
echo "$(date -u +%FT%TZ) FLAT_RERUN_DONE" >> $LOG
