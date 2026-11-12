#!/bin/bash
# usage: run.sh <tag> <readers> <route-flag> [extra args]
R=/local/home/dwolde/bootstrap/duckpgq-extension
tag=$1; readers=$2; flag=$3; shift 3
bin=${BIN:-/tmp/wc_probe/probe}
db=/tmp/wc_probe/work_$tag.duckdb
rm -f $db $db.wal; cp $R/data/ldbc-pathfinding/db/ldbc_sf10.duckdb $db
LD_PRELOAD=$PRELOAD $bin --database $db --extension $R/build/release/extension/duckpgq/duckpgq.duckdb_extension --output /tmp/wc_probe/$tag.csv --pair-table benchmark_pairs_same_src_1024 --index-name pathfinding_benchmark_csr --threads 32 --readers $readers --batches ${BATCHES:-190} --batch-size 2000 --writer-pause-ms 200 --fold-ratio 0.1 $flag "$@" > /tmp/wc_probe/$tag.out 2>&1
rm -f $db $db.wal
