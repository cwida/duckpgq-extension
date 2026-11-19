#!/bin/sh
# run_concurrent.sh <log file>: the concurrent update driver with the merge fold and the snapshot fast path.
# Part 1, correctness: both routes in every read transaction, the driver compares the index answer with the answer
# over a CSR built from the transaction's view of the table (mismatches must be 0).
# Part 2, timing: index route only, the branch as it is against the merge fold and the fast path.
LOG=$1
H="python3 scripts/pathfinding_benchmark.py update-concurrent --threads 32 --recreate-index --no-prepare"
NEW="--set duckpgq_csr_index_merge_fold=true --set duckpgq_csr_index_snapshot_fast_path=true"
step() {
	NAME=$1
	shift
	echo "=== $NAME $(date '+%H:%M:%S')" >> "$LOG"
	timeout 600 "$@" >> "$LOG" 2>&1
	echo "=== $NAME exit $? $(date '+%H:%M:%S')" >> "$LOG"
}
: > "$LOG"
# Part 1
step check_sf10_r4_verify $H --scale-factor 10 --batch-size 2000 --batches 190 --readers 4 --writer-pause-ms 200 --fold-ratio 0.01 --tag check_verify $NEW --set duckpgq_csr_index_merge_fold_verify=true
step check_sf10_r16_unpaced $H --scale-factor 10 --batch-size 2000 --batches 190 --readers 16 --writer-pause-ms 0 --fold-ratio 0.01 --tag check_unpaced $NEW
step check_sf100_r16 $H --scale-factor 100 --batch-size 2000 --batches 100 --readers 16 --writer-pause-ms 200 --fold-ratio 0.001 --tag check $NEW
step check_sf300_r16 $H --scale-factor 300 --batch-size 2000 --batches 60 --readers 16 --writer-pause-ms 200 --fold-ratio 0.0005 --tag check $NEW
# Part 2
for repeat in 1 2 3; do
	step time_sf300_r16_old_$repeat $H --scale-factor 300 --batch-size 2000 --batches 60 --readers 16 --writer-pause-ms 200 --fold-ratio 0.0005 --routes index --tag time_old
	step time_sf300_r16_new_$repeat $H --scale-factor 300 --batch-size 2000 --batches 60 --readers 16 --writer-pause-ms 200 --fold-ratio 0.0005 --routes index --tag time_new $NEW
done
step time_sf100_r16_old $H --scale-factor 100 --batch-size 2000 --batches 100 --readers 16 --writer-pause-ms 200 --fold-ratio 0.001 --routes index --tag time_old
step time_sf100_r16_new $H --scale-factor 100 --batch-size 2000 --batches 100 --readers 16 --writer-pause-ms 200 --fold-ratio 0.001 --routes index --tag time_new $NEW
echo "=== done $(date '+%H:%M:%S')" >> "$LOG"
