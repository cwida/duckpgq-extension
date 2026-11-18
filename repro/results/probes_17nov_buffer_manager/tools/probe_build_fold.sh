#!/usr/bin/env bash
# CREATE INDEX and explicit fold time of the CSR index on a copy of knows, plus what the buffer pool reports under
# the EXTENSION tag. The shared database is attached read-only; the copy and the index live in an in-memory database.
# Usage: probe_build_fold.sh <label> <duckpgq extension> <scale factor> <output file>
set -euo pipefail
label="$1"
extension="$2"
sf="$3"
out="$4"
W="$(cd "$(dirname "$0")/../../../.." && pwd)"
cli="${W}/build/release/duckdb"
db="${W}/data/ldbc-pathfinding/db/ldbc_sf${sf}.duckdb"
{
  echo "== ${label} sf${sf} $(date -u +%FT%TZ) extension=${extension}"
  "$cli" -unsigned -csv <<SQL
LOAD '${extension}';
SET threads=32;
ATTACH '${db}' AS ldbc (READ_ONLY);
CREATE TABLE knows AS SELECT person1id, person2id FROM ldbc.person_knows_person;
SELECT 'edges', count(*) FROM knows;
SELECT 'extension_bytes_before', memory_usage_bytes FROM duckdb_memory() WHERE tag = 'EXTENSION';
.timer on
CREATE INDEX knows_csr ON knows USING CSR (person1id, person2id);
.timer off
SELECT 'index', duckpgq_csr_index_size('knows', 'knows_csr');
SELECT 'extension_bytes_one_epoch', memory_usage_bytes FROM duckdb_memory() WHERE tag = 'EXTENSION';
INSERT INTO knows SELECT person2id, person1id FROM knows LIMIT 1;
.timer on
SELECT duckpgq_csr_index_fold('knows', 'knows_csr');
.timer off
SELECT 'fold', duckpgq_csr_index_fold_stats('knows', 'knows_csr');
SELECT 'index', duckpgq_csr_index_size('knows', 'knows_csr');
SELECT 'extension_bytes_two_epochs', memory_usage_bytes FROM duckdb_memory() WHERE tag = 'EXTENSION';
INSERT INTO knows SELECT person2id, person1id FROM knows LIMIT 1;
.timer on
SELECT duckpgq_csr_index_fold('knows', 'knows_csr');
.timer off
SELECT 'fold', duckpgq_csr_index_fold_stats('knows', 'knows_csr');
SELECT 'extension_bytes_after_second_fold', memory_usage_bytes FROM duckdb_memory() WHERE tag = 'EXTENSION';
SQL
} >>"$out" 2>&1
