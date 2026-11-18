#!/bin/sh
# run_policies.sh <log file>: fold policies under the concurrent driver, index route, 16 readers, one writer with
# 100 ms between commits of 2000 changes, over a long stretch of the update stream:
#   second part: SF100 200 commits (2.1% of the edges), SF300 150 commits (0.54%); the policies that fold early
#   first, the two that fold at a tenth (and so never fold in this stretch) last
# full    the fold as it was: scan of the table at a tenth of the edges
# merge_* merge fold and snapshot fast path, fold at the given fraction of the edges
# cost    merge fold and fast path, fold when the readers' snapshot time equals a fold (a tenth as the cap)
# fullcost the scanning fold with the cost rule
LOG=$1
H="python3 scripts/pathfinding_benchmark.py update-concurrent --threads 32 --recreate-index --no-prepare --routes index --readers 16 --batch-size 2000 --writer-pause-ms 100"
NEW="--set duckpgq_csr_index_merge_fold=true --set duckpgq_csr_index_snapshot_fast_path=true"
step() {
	NAME=$1
	shift
	echo "=== $NAME $(date '+%H:%M:%S')" >> "$LOG"
	timeout 420 "$@" >> "$LOG" 2>&1
	echo "=== $NAME exit $? $(date '+%H:%M:%S')" >> "$LOG"
}
: > "$LOG"
for point in "10 190" "100 200" "300 150"; do
	set -- $point
	SF=$1
	B=$2
	step "sf${SF}_merge_0.01" $H --scale-factor $SF --batches $B --fold-ratio 0.01 --tag policy_merge $NEW
	step "sf${SF}_merge_0.001" $H --scale-factor $SF --batches $B --fold-ratio 0.001 --tag policy_merge $NEW
	step "sf${SF}_cost" $H --scale-factor $SF --batches $B --fold-ratio 0.1 --tag policy_cost $NEW --set duckpgq_csr_index_fold_by_cost=true
	step "sf${SF}_fullcost" $H --scale-factor $SF --batches $B --fold-ratio 0.1 --tag policy_fullcost --set duckpgq_csr_index_fold_by_cost=true
done
for point in "10 190" "100 200" "300 150"; do
	set -- $point
	SF=$1
	B=$2
	step "sf${SF}_merge_0.1" $H --scale-factor $SF --batches $B --fold-ratio 0.1 --tag policy_merge $NEW
	step "sf${SF}_full" $H --scale-factor $SF --batches $B --fold-ratio 0.1 --tag policy_full
done
echo "=== done $(date '+%H:%M:%S')" >> "$LOG"
