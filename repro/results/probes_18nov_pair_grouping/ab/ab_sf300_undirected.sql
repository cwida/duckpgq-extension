ATTACH '/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/db/ldbc_sf300.duckdb' AS ldbc (READ_ONLY);
LOAD '/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/pair-grouping-probe/build/release/extension/duckpgq/duckpgq.duckdb_extension';
SET threads=32;
SET experimental_path_finding_operator=true;
CREATE TEMP TABLE benchmark_pairs_1024 AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.benchmark_pairs_1024 p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;
CREATE TEMP TABLE benchmark_pairs_65536 AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.benchmark_pairs_65536 p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;
.print @@ warmup
SELECT count(len) FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_1024);
.timer on
SET experimental_path_finding_operator_sort_source_groups=true;
.print @@ benchmark_pairs_1024 by_id 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_sort_source_groups=false;
.print @@ benchmark_pairs_1024 arrival 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_sort_source_groups=true;
.print @@ benchmark_pairs_1024 by_id 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_sort_source_groups=false;
.print @@ benchmark_pairs_1024 arrival 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_sort_source_groups=true;
.print @@ benchmark_pairs_1024 by_id 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_sort_source_groups=false;
.print @@ benchmark_pairs_1024 arrival 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_sort_source_groups=true;
.print @@ benchmark_pairs_65536 by_id 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_65536);
SET experimental_path_finding_operator_sort_source_groups=false;
.print @@ benchmark_pairs_65536 arrival 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_65536);
SET experimental_path_finding_operator_sort_source_groups=true;
.print @@ benchmark_pairs_65536 by_id 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_65536);
SET experimental_path_finding_operator_sort_source_groups=false;
.print @@ benchmark_pairs_65536 arrival 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_65536);
SET experimental_path_finding_operator_sort_source_groups=true;
.print @@ benchmark_pairs_65536 by_id 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_65536);
SET experimental_path_finding_operator_sort_source_groups=false;
.print @@ benchmark_pairs_65536 arrival 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_65536);
.timer off
.print @@ strategy
SET experimental_path_finding_operator_sort_source_groups=true;
SELECT count(len) FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr_undirected') AS len FROM benchmark_pairs_65536);
SELECT duckpgq_path_finding_strategy();
