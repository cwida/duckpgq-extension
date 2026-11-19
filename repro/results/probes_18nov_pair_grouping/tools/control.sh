#!/usr/bin/env bash
# Why are pairs sorted by source faster? Control runs for the order effect (my-beans-ij12). Read-only on the databases.
#
#   control.sh <checkout> <out> <label> <database> <index name> <pair table> <repeats> <variants> [diagnosed variants]
#
# The pairs are materialized once with rnk, the rank of the pair's source key among the sources (0-based). Every
# variant is the harness's query over the pairs in one order (a sort below the operator):
#   arrival     the table the harness builds, unsorted
#   src         by source key: a wave holds 256 consecutive sources
#   dst         by destination key
#   hash        by hash(source): random order through the same sort operator
#   win<N>      by rnk / N, then hash: a wave's sources are random within a window of N consecutive sources
#   waves_shuf  the waves of src (rnk / 256) in random order, sources sorted inside
# Diagnosed variants run once more, untimed, with the lane-activity switch: per wave and level, the vertices with any
# active lane and the frontier of every lane.
set -eu
ROOT="$1"
OUT="$2"
LABEL="$3"
DB="$4"
INDEX="$5"
TABLE="$6"
REPEATS="$7"
VARIANTS="$8"
DIAGNOSED="${9:-}"
CLI="${ROOT}/build/release/duckdb"
EXT="${DUCKPGQ_EXTENSION:-${ROOT}/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension}"
THREADS="$(nproc)"
KEY="csr-index|ldbc|main|person_knows_person|${INDEX}"
mkdir -p "${OUT}"
SQL="${OUT}/control_${LABEL}.sql"
RAW="${OUT}/control_${LABEL}.out"
ACTIVITY="${OUT}/activity_${LABEL}"
rm -rf "${ACTIVITY}"
mkdir -p "${ACTIVITY}"

order_of() {
	case "$1" in
	arrival) echo "" ;;
	src) echo "ORDER BY src" ;;
	dst) echo "ORDER BY dst" ;;
	hash) echo "ORDER BY hash(src)" ;;
	waves_shuf) echo "ORDER BY hash(rnk // 256), src" ;;
	win*) echo "ORDER BY rnk // ${1#win}, hash(src)" ;;
	*) echo "unknown variant $1" >&2; exit 1 ;;
	esac
}

op() { # $1 = variant
	# arrival reads the table the harness's statement builds; the other orderings sort the table with the ranks
	local input="(SELECT src, dst FROM pairs $(order_of "$1"))"
	if [ "$1" = arrival ]; then
		input="pairs_arrival"
	fi
	echo "SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, '${KEY}') AS len FROM ${input});"
}

{
	echo "ATTACH '${DB}' AS ldbc (READ_ONLY);"
	echo "LOAD '${EXT}';"
	echo "SET threads=${THREADS};"
	echo "SET experimental_path_finding_operator=true;"
	echo "SET experimental_path_finding_operator_benchmark=false;"
	# The harness's statement (index_query_sql): its row order is what the frozen runs measured
	echo "CREATE TEMP TABLE pairs_arrival AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.${TABLE} p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;"
	# The window leaves this table sorted by source; every ordering that reads it sorts again
	echo "CREATE TEMP TABLE pairs AS SELECT src, dst, dense_rank() OVER (ORDER BY src) - 1 AS rnk FROM pairs_arrival;"
	# How sorted the arrival order is: the share of adjacent rows whose sources ascend (0.5 for a random order)
	echo ".print @@ arrival_order"
	echo "SELECT round(avg((src > prev)::INT), 3) AS ascending_share FROM (SELECT src, lag(src) OVER (ORDER BY rowid) AS prev FROM pairs_arrival) WHERE prev IS NOT NULL;"
	echo ".print @@ shape"
	echo "SELECT count(*) AS pairs, count(DISTINCT src) AS sources, (SELECT count(*) FROM ldbc.person) AS vertices FROM pairs;"
	# The first query loads the index
	echo ".print @@ warmup"
	op arrival
	echo ".timer on"
	for repeat in $(seq 1 "${REPEATS}"); do
		for variant in ${VARIANTS}; do
			echo ".print @@ ${variant} ${repeat}"
			op "${variant}"
		done
	done
	echo ".timer off"
	if [ -n "${DIAGNOSED}" ]; then
		# Per pair: the length by decile of the source's rank (how depth and reach depend on the source key)
		echo ".print @@ deciles"
		echo "SELECT rnk * 10 // (SELECT max(rnk) + 1 FROM pairs) AS source_decile, count(*) AS pairs, count(len) AS reachable, round(avg(len), 2) AS mean_len, max(len) AS max_len FROM (SELECT rnk, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, '${KEY}') AS len FROM pairs) GROUP BY ALL ORDER BY 1;"
		echo "SET experimental_path_finding_operator_benchmark=true;"
		echo "SET experimental_path_finding_operator_benchmark_lane_activity=true;"
		for variant in ${DIAGNOSED}; do
			mkdir -p "${ACTIVITY}/${variant}"
			echo "SET experimental_path_finding_operator_benchmark_prefix='${ACTIVITY}/${variant}/w';"
			echo ".print @@ diagnosed ${variant}"
			op "${variant}"
		done
	fi
} >"${SQL}"

echo "$(date -u +%FT%TZ) ${LABEL} db ${DB} index ${INDEX} table ${TABLE} threads ${THREADS} repeats ${REPEATS} cli $(sha256sum "${CLI}" | cut -c1-16) extension $(sha256sum "${EXT}" | cut -c1-16)" >"${OUT}/control_${LABEL}.log"
"${CLI}" -no-agent -unsigned <"${SQL}" >"${RAW}" 2>&1
echo "$(date -u +%FT%TZ) done exit=$?" >>"${OUT}/control_${LABEL}.log"
# The lane-activity files are reduced to one row per (variant, wave, level); the raw files are 257 rows per level
if [ -n "${DIAGNOSED}" ]; then
	python3 "$(dirname "$0")/reduce_activity.py" "${ACTIVITY}" "${OUT}/activity_${LABEL}.csv"
	rm -rf "${ACTIVITY}"
fi
