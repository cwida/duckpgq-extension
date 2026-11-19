#!/bin/sh
# run_deadlock.sh <output directory> <tag> <repeats> [settings...]: the SF10 run with 16 readers and an unpaced
# writer, <repeats> times. A run that is still going after 100 seconds is taken for hung: its stacks and the owner
# of every contended mutex are written to <output directory>/<tag>_<n>_stacks.txt and the driver is killed.
D=$1
TAG=$2
REPEATS=$3
shift 3
mkdir -p "$D"
n=1
while [ $n -le $REPEATS ]; do
	LOG="$D/${TAG}_${n}.log"
	python3 scripts/pathfinding_benchmark.py update-concurrent --threads 32 --recreate-index --no-prepare --scale-factor 10 --batch-size 2000 --batches 190 --readers 16 --writer-pause-ms 0 --fold-ratio 0.01 --tag "deadlock_${TAG}" "$@" > "$LOG" 2>&1 &
	HARNESS=$!
	waited=0
	while kill -0 $HARNESS 2>/dev/null && [ $waited -lt 100 ]; do
		sleep 2
		waited=$((waited + 2))
	done
	if kill -0 $HARNESS 2>/dev/null; then
		PID=$(pgrep -f '[u]pdate_concurrent_driver' | head -1)
		echo "$TAG run $n: hung after ${waited}s, driver pid $PID"
		timeout 90 gdb -p "$PID" -batch -ex 'info threads' -ex 'thread apply all bt 30' -ex 'thread apply all p *(int*)($rdi+8)' > "$D/${TAG}_${n}_stacks.txt" 2>/dev/null
		kill "$PID"
		sleep 2
		kill $HARNESS 2>/dev/null
	else
		echo "$TAG run $n: $(grep -E '^events=' "$LOG" | cut -c1-80)"
	fi
	n=$((n + 1))
done
