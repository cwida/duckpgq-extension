#!/usr/bin/env bash
# Probe (c) at SF10: commit latency of 2000 edge changes with and without a FOREIGN KEY on the edge table.
# One writer, no readers: the concurrent driver (scripts/tools/update_concurrent_driver.cpp) with --readers 0 replays
# 190 commits of 2000 operations from the staged LDBC update log (the matrix's SF10 update workload).
#
# Two private copies of the SF10 database, built the same way so that only the constraint differs:
#   plain  person_knows_person(person1id BIGINT, person2id BIGINT), re-created and re-filled in the stored order
#   fk     person gets PRIMARY KEY (id); person_knows_person(person1id BIGINT REFERENCES person(id),
#          person2id BIGINT REFERENCES person(id)), filled with the same rows in the same order
# Each has one CSR index (reverse = true), created before the clock starts. Every run starts from a fresh copy.
#
#   fk_commit.sh <duckdb cli> <extension> <driver binary> <output dir> [repeats]
set -euo pipefail
CLI="$1"; EXT="$2"; DRIVER="$3"; OUT="$4"; REPEATS="${5:-3}"
SRC=/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/db/ldbc_sf10.duckdb
WORK="$(pwd)/data/ldbc-pathfinding/work"
mkdir -p "${OUT}" "${WORK}"

prepare() {
  local variant="$1" pk="" ref=""
  if [ "${variant}" = fk ]; then
    pk="ALTER TABLE person ADD PRIMARY KEY (id);"
    ref=" REFERENCES person(id)"
  fi
  local base="${WORK}/sf10_fkprobe_${variant}_base.duckdb"
  rm -f "${base}" "${base}.wal"
  cp "${SRC}" "${base}"
  "${CLI}" -unsigned "${base}" -c "
LOAD '${EXT}';
CREATE TABLE fkprobe_stage AS SELECT person1id, person2id FROM person_knows_person;
DROP TABLE person_knows_person;
${pk}
CREATE TABLE person_knows_person(person1id BIGINT${ref}, person2id BIGINT${ref});
INSERT INTO person_knows_person SELECT person1id, person2id FROM fkprobe_stage;
DROP TABLE fkprobe_stage;
CREATE INDEX pathfinding_benchmark_csr ON person_knows_person USING CSR (person1id, person2id) WITH (reverse = true);
CHECKPOINT;
SELECT '${variant}' AS variant, (SELECT count(*) FROM person_knows_person) AS edges,
       (SELECT count(*) FROM duckdb_constraints() WHERE table_name = 'person_knows_person' AND constraint_type = 'FOREIGN KEY') AS foreign_keys,
       (SELECT count(*) FROM duckdb_indexes() WHERE table_name = 'person_knows_person') AS indexes;
" > "${OUT}/prepare_${variant}.txt" 2>&1
  cat "${OUT}/prepare_${variant}.txt"
}

prepare plain
prepare fk

for rep in $(seq 1 "${REPEATS}"); do
  for variant in plain fk; do
    run="${WORK}/sf10_fkprobe_run.duckdb"
    rm -f "${run}" "${run}.wal"
    cp "${WORK}/sf10_fkprobe_${variant}_base.duckdb" "${run}"
    "${DRIVER}" --database "${run}" --extension "${EXT}" --output "${OUT}/commit_${variant}_${rep}.csv" \
      --threads 32 --readers 0 --batches 190 --batch-size 2000 --writer-pause-ms 0 --fold-ratio 0.001 --no-build \
      > "${OUT}/commit_${variant}_${rep}.log" 2>&1
    tail -n 2 "${OUT}/commit_${variant}_${rep}.log"
    rm -f "${run}" "${run}.wal"
  done
done
rm -f "${WORK}"/sf10_fkprobe_*_base.duckdb "${WORK}"/sf10_fkprobe_*_base.duckdb.wal
