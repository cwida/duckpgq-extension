#!/bin/bash
# First write transaction of the concurrent update driver (my-beans-kzlq, 18 Nov 2026).
# usage: run_ab.sh <sf>...   Three runs per scale factor, each on a fresh copy of the shared database:
#   rc13_asis     rc13 driver and extension, database as it is (six CSR indexes on person_knows_person)
#   rc13_dropped  the same binaries, the five other indexes dropped (and checkpointed) before the driver starts
#   fix           the patched driver (drops the other indexes itself) with the build of the bidirectional worktree
M=/local/home/dwolde/bootstrap/duckpgq-extension
B=$M/.claude/worktrees/bidirectional/build/release
W=$M/.claude/worktrees/first-txn-index-load
D=$(dirname $(readlink -f $0))
RC13=$M/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension
ARGS="--pair-table benchmark_pairs_same_src_1024 --index-name pathfinding_benchmark_csr --threads 32 --readers 16 --batches 60 --batch-size 2000 --writer-pause-ms 200 --fold-ratio 0.1 --no-build"
g++ -O2 -std=c++17 -I$M/duckdb/src/include $W/scripts/tools/update_concurrent_driver.cpp -L$B/src -lduckdb -Wl,-rpath,$B/src -o $D/driver_fix || exit 1
summary() {
  echo "$1: $(grep -h -E 'events=|dropped_other|rror' $D/$1.out | tr '\n' ' ') first commits: $(grep '^commit' $D/$1.csv | head -3 | cut -d, -f5 | tr '\n' ' ')"
}
for sf in "$@"; do
  db=$D/work_sf$sf.duckdb
  fresh() { rm -f $db $db.wal; cp $M/data/ldbc-pathfinding/db/ldbc_sf$sf.duckdb $db; }
  t=sf${sf}_rc13_asis; fresh
  timeout 300 $M/build/release/update_concurrent_driver --database $db --extension $RC13 --output $D/$t.csv $ARGS > $D/$t.out 2>&1
  summary $t
  t=sf${sf}_rc13_dropped; fresh
  { echo "LOAD '$RC13';"
    for i in _rb0 _rb2 _rb8 _rb64 _undirected; do echo "DROP INDEX IF EXISTS pathfinding_benchmark_csr$i;"; done
    echo "CHECKPOINT;"; } | timeout 120 $M/build/release/duckdb -unsigned -no-agent $db > $D/$t.prep 2>&1
  timeout 300 $M/build/release/update_concurrent_driver --database $db --extension $RC13 --output $D/$t.csv $ARGS > $D/$t.out 2>&1
  summary $t
  t=sf${sf}_fix; fresh
  timeout 300 $D/driver_fix --database $db --extension $B/extension/duckpgq/duckpgq.duckdb_extension --output $D/$t.csv $ARGS > $D/$t.out 2>&1
  summary $t
  rm -f $db $db.wal
done
