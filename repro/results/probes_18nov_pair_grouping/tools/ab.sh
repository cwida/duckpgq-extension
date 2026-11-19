#!/usr/bin/env bash
# Source groups ordered by source id inside the operator (switch experimental_path_finding_operator_sort_source_groups)
# against the order of first appearance, on one build. Read-only on the databases.
#
#   ab.sh <worktree with build/release> <out> <label> <database> <index name> <repeats> <pair tables> [reach tables]
#
# Per pair table the harness's length query runs with the switch on (by_id) and off (arrival), interleaved; per
# reach table (one row per source) the reach query, sources x all vertices.
set -eu
ROOT="$1"
OUT="$2"
LABEL="$3"
DB="$4"
INDEX="$5"
REPEATS="$6"
TABLES="$7"
REACH_TABLES="${8:-}"
CLI="${ROOT}/build/release/duckdb"
EXT="${ROOT}/build/release/extension/duckpgq/duckpgq.duckdb_extension"
THREADS="$(nproc)"
KEY="csr-index|ldbc|main|person_knows_person|${INDEX}"
OPTION="experimental_path_finding_operator_sort_source_groups"
mkdir -p "${OUT}"
SQL="${OUT}/ab_${LABEL}.sql"
RAW="${OUT}/ab_${LABEL}.out"

{
	echo "ATTACH '${DB}' AS ldbc (READ_ONLY);"
	echo "LOAD '${EXT}';"
	echo "SET threads=${THREADS};"
	echo "SET experimental_path_finding_operator=true;"
	for table in ${TABLES} ${REACH_TABLES}; do
		echo "CREATE TEMP TABLE ${table} AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.${table} p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;"
	done
	first="${TABLES%% *}"
	echo ".print @@ warmup"
	echo "SELECT count(len) FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, '${KEY}') AS len FROM ${first});"
	echo ".timer on"
	for table in ${TABLES}; do
		for repeat in $(seq 1 "${REPEATS}"); do
			for variant in by_id arrival; do
				echo "SET ${OPTION}=$([ "${variant}" = by_id ] && echo true || echo false);"
				echo ".print @@ ${table} ${variant} ${repeat}"
				echo "SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, '${KEY}') AS len FROM ${table});"
			done
		done
	done
	for table in ${REACH_TABLES}; do
		for repeat in $(seq 1 "${REPEATS}"); do
			for variant in by_id arrival; do
				echo "SET ${OPTION}=$([ "${variant}" = by_id ] && echo true || echo false);"
				echo ".print @@ reach_${table} ${variant} ${repeat}"
				echo "SELECT count(DISTINCT src) AS pairs, count(*) AS reachable, sum(r[2]) AS total_len FROM (SELECT src, reachoperator(src, src, 0::BIGINT, 0::BIGINT, '${KEY}') AS r FROM ${table});"
			done
		done
	done
	echo ".timer off"
	echo ".print @@ strategy"
	echo "SET ${OPTION}=true;"
	echo "SELECT count(len) FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, '${KEY}') AS len FROM ${TABLES##* });"
	echo "SELECT duckpgq_path_finding_strategy();"
} >"${SQL}"

echo "$(date -u +%FT%TZ) ${LABEL} db ${DB} index ${INDEX} threads ${THREADS} repeats ${REPEATS} commit $(git -C "${ROOT}" rev-parse --short HEAD) cli $(sha256sum "${CLI}" | cut -c1-16) extension $(sha256sum "${EXT}" | cut -c1-16)" >"${OUT}/ab_${LABEL}.log"
"${CLI}" -no-agent -unsigned <"${SQL}" >"${RAW}" 2>&1
echo "$(date -u +%FT%TZ) done exit=$?" >>"${OUT}/ab_${LABEL}.log"
