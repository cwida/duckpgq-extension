#!/usr/bin/env bash
# Pair grouping in the operator against DuckDB's own operators (my-beans-ij12). Read-only on the shared database.
#
#   run.sh <root of the checkout> <output directory> [scale factor, default 300] [repeats, default 5]
#
# Per pair table: the operator's query as the harness runs it over the CSR index (phase timings on), the same query
# with DuckDB's operators planned below the operator (DISTINCT pairs joined back; pairs sorted by source), and the
# grouping statements on their own (hash aggregate on (src, dst), on src with list(dst), sort on (src, dst)).
set -eu
ROOT="$1"
OUT="$2"
SF="${3:-300}"
REPEATS="${4:-5}"
CLI="${ROOT}/build/release/duckdb"
EXT="${DUCKPGQ_EXTENSION:-${ROOT}/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension}"
DB="${ROOT}/data/ldbc-pathfinding/db/ldbc_sf${SF}.duckdb"
THREADS="$(nproc)"
KEY="csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr"
mkdir -p "${OUT}"
SQL="${OUT}/probe_sf${SF}.sql"
RAW="${OUT}/probe_sf${SF}.out"
rm -rf "${OUT}/phases"
mkdir -p "${OUT}/phases"

distinct_below() { # $1 = pair table: the operator over the distinct pairs, joined back to every pair
	echo "SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM $1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, '${KEY}') AS len FROM (SELECT DISTINCT src, dst FROM $1)) r ON p.src = r.src AND p.dst = r.dst;"
}

op() { # $1 = pair input
	echo "SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, '${KEY}') AS len FROM $1);"
}

{
	echo "ATTACH '${DB}' AS ldbc (READ_ONLY);"
	echo "LOAD '${EXT}';"
	echo "SET threads=${THREADS};"
	echo "SET experimental_path_finding_operator=true;"
	echo "SET experimental_path_finding_operator_benchmark=false;"
	for table in benchmark_pairs_1 benchmark_pairs_1024 benchmark_pairs_65536 benchmark_pairs_all_pairs_s1024_t200; do
		echo "CREATE OR REPLACE TEMP TABLE ${table} AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.${table} p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;"
	done
	# Twice the 65,536 pairs: every pair has one duplicate
	echo "CREATE OR REPLACE TEMP TABLE benchmark_pairs_65536_twice AS SELECT * FROM benchmark_pairs_65536 UNION ALL SELECT * FROM benchmark_pairs_65536;"
	# The first query loads the index
	echo ".print @@ warmup"
	op benchmark_pairs_1024
	echo ".timer on"
	for table in benchmark_pairs_1 benchmark_pairs_1024 benchmark_pairs_65536 benchmark_pairs_65536_twice benchmark_pairs_all_pairs_s1024_t200; do
		for repeat in $(seq 1 "${REPEATS}"); do
			echo ".print @@ ${table} operator ${repeat}"
			op "${table}"
			echo ".print @@ ${table} distinct_below ${repeat}"
			distinct_below "${table}"
			echo ".print @@ ${table} sorted_below ${repeat}"
			op "(SELECT src, dst FROM ${table} ORDER BY src, dst)"
			echo ".print @@ ${table} scan_only ${repeat}"
			echo "SELECT count(*), sum(src), sum(dst) FROM ${table};"
			echo ".print @@ ${table} aggregate_src_dst ${repeat}"
			echo "SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM ${table} GROUP BY src, dst);"
			echo ".print @@ ${table} aggregate_src_list_dst ${repeat}"
			echo "SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM ${table} GROUP BY src);"
			echo ".print @@ ${table} sort_src_dst ${repeat}"
			echo "SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM ${table} ORDER BY src, dst);"
		done
	done
	# Phase timings (the switch writes a file per wave, so these runs are not the timed ones)
	echo ".timer off"
	echo "SET experimental_path_finding_operator_benchmark=true;"
	for table in benchmark_pairs_1 benchmark_pairs_1024 benchmark_pairs_65536 benchmark_pairs_65536_twice benchmark_pairs_all_pairs_s1024_t200; do
		for repeat in 1 2 3; do
			echo "SET experimental_path_finding_operator_benchmark_prefix='${OUT}/phases/sf${SF}_${table}_operator';"
			op "${table}"
			echo "SET experimental_path_finding_operator_benchmark_prefix='${OUT}/phases/sf${SF}_${table}_distinct_below';"
			distinct_below "${table}"
			echo "SET experimental_path_finding_operator_benchmark_prefix='${OUT}/phases/sf${SF}_${table}_sorted_below';"
			op "(SELECT src, dst FROM ${table} ORDER BY src, dst)"
		done
	done
} >"${SQL}"

echo "$(date -u +%FT%TZ) sf ${SF} threads ${THREADS} repeats ${REPEATS} cli $(sha256sum "${CLI}" | cut -c1-16) extension $(sha256sum "${EXT}" | cut -c1-16)" >"${OUT}/run_sf${SF}.log"
"${CLI}" -no-agent -unsigned <"${SQL}" >"${RAW}" 2>&1
echo "$(date -u +%FT%TZ) done exit=$?" >>"${OUT}/run_sf${SF}.log"
# Keep the operator's phase rows; the per-wave files of the kernel are not part of this probe
find "${OUT}/phases" -type f ! -name '*_phase_timing.csv' -delete
