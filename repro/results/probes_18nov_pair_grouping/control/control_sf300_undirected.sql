ATTACH '/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/db/ldbc_sf300.duckdb' AS ldbc (READ_ONLY);
LOAD '/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension';
SET threads=32;
SET experimental_path_finding_operator=true;
SET experimental_path_finding_operator_benchmark=false;
CREATE TEMP TABLE pairs_arrival AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.benchmark_pairs_16384 p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;
CREATE TEMP TABLE pairs AS SELECT src, dst, dense_rank() OVER (ORDER BY src) - 1 AS rnk FROM pairs_arrival;
.print @@ arrival_order
SELECT round(avg((src > prev)::INT), 3) AS ascending_share FROM (SELECT src, lag(src) OVER (ORDER BY rowid) AS prev FROM pairs_arrival) WHERE prev IS NOT NULL;
.print @@ shape
SELECT count(*) AS pairs, count(DISTINCT src) AS sources, (SELECT count(*) FROM ldbc.person) AS vertices FROM pairs;
.print @@ warmup
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM pairs_arrival);
.timer on
.print @@ arrival 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM pairs_arrival);
.print @@ src 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM (SELECT src, dst FROM pairs ORDER BY src));
.print @@ hash 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(src)));
.print @@ arrival 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM pairs_arrival);
.print @@ src 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM (SELECT src, dst FROM pairs ORDER BY src));
.print @@ hash 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(src)));
.print @@ arrival 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM pairs_arrival);
.print @@ src 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM (SELECT src, dst FROM pairs ORDER BY src));
.print @@ hash 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(src)));
.timer off
