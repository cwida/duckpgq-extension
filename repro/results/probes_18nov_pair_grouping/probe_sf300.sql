ATTACH '/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/db/ldbc_sf300.duckdb' AS ldbc (READ_ONLY);
LOAD '/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension';
SET threads=32;
SET experimental_path_finding_operator=true;
SET experimental_path_finding_operator_benchmark=false;
CREATE OR REPLACE TEMP TABLE benchmark_pairs_1 AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.benchmark_pairs_1 p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;
CREATE OR REPLACE TEMP TABLE benchmark_pairs_1024 AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.benchmark_pairs_1024 p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;
CREATE OR REPLACE TEMP TABLE benchmark_pairs_65536 AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.benchmark_pairs_65536 p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;
CREATE OR REPLACE TEMP TABLE benchmark_pairs_all_pairs_s1024_t200 AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.benchmark_pairs_all_pairs_s1024_t200 p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;
CREATE OR REPLACE TEMP TABLE benchmark_pairs_65536_twice AS SELECT * FROM benchmark_pairs_65536 UNION ALL SELECT * FROM benchmark_pairs_65536;
.print @@ warmup
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
.timer on
.print @@ benchmark_pairs_1 operator 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1);
.print @@ benchmark_pairs_1 distinct_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1 sorted_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst));
.print @@ benchmark_pairs_1 scan_only 1
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1;
.print @@ benchmark_pairs_1 aggregate_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 GROUP BY src, dst);
.print @@ benchmark_pairs_1 aggregate_src_list_dst 1
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1 GROUP BY src);
.print @@ benchmark_pairs_1 sort_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst);
.print @@ benchmark_pairs_1 operator 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1);
.print @@ benchmark_pairs_1 distinct_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1 sorted_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst));
.print @@ benchmark_pairs_1 scan_only 2
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1;
.print @@ benchmark_pairs_1 aggregate_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 GROUP BY src, dst);
.print @@ benchmark_pairs_1 aggregate_src_list_dst 2
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1 GROUP BY src);
.print @@ benchmark_pairs_1 sort_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst);
.print @@ benchmark_pairs_1 operator 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1);
.print @@ benchmark_pairs_1 distinct_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1 sorted_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst));
.print @@ benchmark_pairs_1 scan_only 3
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1;
.print @@ benchmark_pairs_1 aggregate_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 GROUP BY src, dst);
.print @@ benchmark_pairs_1 aggregate_src_list_dst 3
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1 GROUP BY src);
.print @@ benchmark_pairs_1 sort_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst);
.print @@ benchmark_pairs_1 operator 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1);
.print @@ benchmark_pairs_1 distinct_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1 sorted_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst));
.print @@ benchmark_pairs_1 scan_only 4
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1;
.print @@ benchmark_pairs_1 aggregate_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 GROUP BY src, dst);
.print @@ benchmark_pairs_1 aggregate_src_list_dst 4
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1 GROUP BY src);
.print @@ benchmark_pairs_1 sort_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst);
.print @@ benchmark_pairs_1 operator 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1);
.print @@ benchmark_pairs_1 distinct_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1 sorted_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst));
.print @@ benchmark_pairs_1 scan_only 5
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1;
.print @@ benchmark_pairs_1 aggregate_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 GROUP BY src, dst);
.print @@ benchmark_pairs_1 aggregate_src_list_dst 5
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1 GROUP BY src);
.print @@ benchmark_pairs_1 sort_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst);
.print @@ benchmark_pairs_1024 operator 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
.print @@ benchmark_pairs_1024 distinct_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1024 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1024)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1024 sorted_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst));
.print @@ benchmark_pairs_1024 scan_only 1
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1024;
.print @@ benchmark_pairs_1024 aggregate_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 GROUP BY src, dst);
.print @@ benchmark_pairs_1024 aggregate_src_list_dst 1
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1024 GROUP BY src);
.print @@ benchmark_pairs_1024 sort_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst);
.print @@ benchmark_pairs_1024 operator 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
.print @@ benchmark_pairs_1024 distinct_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1024 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1024)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1024 sorted_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst));
.print @@ benchmark_pairs_1024 scan_only 2
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1024;
.print @@ benchmark_pairs_1024 aggregate_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 GROUP BY src, dst);
.print @@ benchmark_pairs_1024 aggregate_src_list_dst 2
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1024 GROUP BY src);
.print @@ benchmark_pairs_1024 sort_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst);
.print @@ benchmark_pairs_1024 operator 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
.print @@ benchmark_pairs_1024 distinct_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1024 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1024)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1024 sorted_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst));
.print @@ benchmark_pairs_1024 scan_only 3
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1024;
.print @@ benchmark_pairs_1024 aggregate_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 GROUP BY src, dst);
.print @@ benchmark_pairs_1024 aggregate_src_list_dst 3
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1024 GROUP BY src);
.print @@ benchmark_pairs_1024 sort_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst);
.print @@ benchmark_pairs_1024 operator 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
.print @@ benchmark_pairs_1024 distinct_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1024 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1024)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1024 sorted_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst));
.print @@ benchmark_pairs_1024 scan_only 4
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1024;
.print @@ benchmark_pairs_1024 aggregate_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 GROUP BY src, dst);
.print @@ benchmark_pairs_1024 aggregate_src_list_dst 4
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1024 GROUP BY src);
.print @@ benchmark_pairs_1024 sort_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst);
.print @@ benchmark_pairs_1024 operator 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
.print @@ benchmark_pairs_1024 distinct_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1024 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1024)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_1024 sorted_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst));
.print @@ benchmark_pairs_1024 scan_only 5
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_1024;
.print @@ benchmark_pairs_1024 aggregate_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 GROUP BY src, dst);
.print @@ benchmark_pairs_1024 aggregate_src_list_dst 5
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_1024 GROUP BY src);
.print @@ benchmark_pairs_1024 sort_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst);
.print @@ benchmark_pairs_65536 operator 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536);
.print @@ benchmark_pairs_65536 distinct_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536 sorted_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst));
.print @@ benchmark_pairs_65536 scan_only 1
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536;
.print @@ benchmark_pairs_65536 aggregate_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 GROUP BY src, dst);
.print @@ benchmark_pairs_65536 aggregate_src_list_dst 1
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536 GROUP BY src);
.print @@ benchmark_pairs_65536 sort_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst);
.print @@ benchmark_pairs_65536 operator 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536);
.print @@ benchmark_pairs_65536 distinct_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536 sorted_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst));
.print @@ benchmark_pairs_65536 scan_only 2
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536;
.print @@ benchmark_pairs_65536 aggregate_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 GROUP BY src, dst);
.print @@ benchmark_pairs_65536 aggregate_src_list_dst 2
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536 GROUP BY src);
.print @@ benchmark_pairs_65536 sort_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst);
.print @@ benchmark_pairs_65536 operator 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536);
.print @@ benchmark_pairs_65536 distinct_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536 sorted_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst));
.print @@ benchmark_pairs_65536 scan_only 3
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536;
.print @@ benchmark_pairs_65536 aggregate_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 GROUP BY src, dst);
.print @@ benchmark_pairs_65536 aggregate_src_list_dst 3
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536 GROUP BY src);
.print @@ benchmark_pairs_65536 sort_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst);
.print @@ benchmark_pairs_65536 operator 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536);
.print @@ benchmark_pairs_65536 distinct_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536 sorted_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst));
.print @@ benchmark_pairs_65536 scan_only 4
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536;
.print @@ benchmark_pairs_65536 aggregate_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 GROUP BY src, dst);
.print @@ benchmark_pairs_65536 aggregate_src_list_dst 4
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536 GROUP BY src);
.print @@ benchmark_pairs_65536 sort_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst);
.print @@ benchmark_pairs_65536 operator 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536);
.print @@ benchmark_pairs_65536 distinct_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536 sorted_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst));
.print @@ benchmark_pairs_65536 scan_only 5
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536;
.print @@ benchmark_pairs_65536 aggregate_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 GROUP BY src, dst);
.print @@ benchmark_pairs_65536 aggregate_src_list_dst 5
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536 GROUP BY src);
.print @@ benchmark_pairs_65536 sort_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst);
.print @@ benchmark_pairs_65536_twice operator 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536_twice);
.print @@ benchmark_pairs_65536_twice distinct_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536_twice p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536_twice)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536_twice sorted_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst));
.print @@ benchmark_pairs_65536_twice scan_only 1
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536_twice;
.print @@ benchmark_pairs_65536_twice aggregate_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice GROUP BY src, dst);
.print @@ benchmark_pairs_65536_twice aggregate_src_list_dst 1
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536_twice GROUP BY src);
.print @@ benchmark_pairs_65536_twice sort_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst);
.print @@ benchmark_pairs_65536_twice operator 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536_twice);
.print @@ benchmark_pairs_65536_twice distinct_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536_twice p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536_twice)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536_twice sorted_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst));
.print @@ benchmark_pairs_65536_twice scan_only 2
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536_twice;
.print @@ benchmark_pairs_65536_twice aggregate_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice GROUP BY src, dst);
.print @@ benchmark_pairs_65536_twice aggregate_src_list_dst 2
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536_twice GROUP BY src);
.print @@ benchmark_pairs_65536_twice sort_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst);
.print @@ benchmark_pairs_65536_twice operator 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536_twice);
.print @@ benchmark_pairs_65536_twice distinct_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536_twice p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536_twice)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536_twice sorted_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst));
.print @@ benchmark_pairs_65536_twice scan_only 3
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536_twice;
.print @@ benchmark_pairs_65536_twice aggregate_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice GROUP BY src, dst);
.print @@ benchmark_pairs_65536_twice aggregate_src_list_dst 3
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536_twice GROUP BY src);
.print @@ benchmark_pairs_65536_twice sort_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst);
.print @@ benchmark_pairs_65536_twice operator 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536_twice);
.print @@ benchmark_pairs_65536_twice distinct_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536_twice p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536_twice)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536_twice sorted_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst));
.print @@ benchmark_pairs_65536_twice scan_only 4
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536_twice;
.print @@ benchmark_pairs_65536_twice aggregate_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice GROUP BY src, dst);
.print @@ benchmark_pairs_65536_twice aggregate_src_list_dst 4
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536_twice GROUP BY src);
.print @@ benchmark_pairs_65536_twice sort_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst);
.print @@ benchmark_pairs_65536_twice operator 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536_twice);
.print @@ benchmark_pairs_65536_twice distinct_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536_twice p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536_twice)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_65536_twice sorted_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst));
.print @@ benchmark_pairs_65536_twice scan_only 5
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_65536_twice;
.print @@ benchmark_pairs_65536_twice aggregate_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice GROUP BY src, dst);
.print @@ benchmark_pairs_65536_twice aggregate_src_list_dst 5
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_65536_twice GROUP BY src);
.print @@ benchmark_pairs_65536_twice sort_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 operator 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_all_pairs_s1024_t200);
.print @@ benchmark_pairs_all_pairs_s1024_t200 distinct_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_all_pairs_s1024_t200 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_all_pairs_s1024_t200)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_all_pairs_s1024_t200 sorted_below 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst));
.print @@ benchmark_pairs_all_pairs_s1024_t200 scan_only 1
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_all_pairs_s1024_t200;
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_list_dst 1
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src);
.print @@ benchmark_pairs_all_pairs_s1024_t200 sort_src_dst 1
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 operator 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_all_pairs_s1024_t200);
.print @@ benchmark_pairs_all_pairs_s1024_t200 distinct_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_all_pairs_s1024_t200 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_all_pairs_s1024_t200)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_all_pairs_s1024_t200 sorted_below 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst));
.print @@ benchmark_pairs_all_pairs_s1024_t200 scan_only 2
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_all_pairs_s1024_t200;
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_list_dst 2
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src);
.print @@ benchmark_pairs_all_pairs_s1024_t200 sort_src_dst 2
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 operator 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_all_pairs_s1024_t200);
.print @@ benchmark_pairs_all_pairs_s1024_t200 distinct_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_all_pairs_s1024_t200 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_all_pairs_s1024_t200)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_all_pairs_s1024_t200 sorted_below 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst));
.print @@ benchmark_pairs_all_pairs_s1024_t200 scan_only 3
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_all_pairs_s1024_t200;
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_list_dst 3
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src);
.print @@ benchmark_pairs_all_pairs_s1024_t200 sort_src_dst 3
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 operator 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_all_pairs_s1024_t200);
.print @@ benchmark_pairs_all_pairs_s1024_t200 distinct_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_all_pairs_s1024_t200 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_all_pairs_s1024_t200)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_all_pairs_s1024_t200 sorted_below 4
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst));
.print @@ benchmark_pairs_all_pairs_s1024_t200 scan_only 4
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_all_pairs_s1024_t200;
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_list_dst 4
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src);
.print @@ benchmark_pairs_all_pairs_s1024_t200 sort_src_dst 4
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 operator 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_all_pairs_s1024_t200);
.print @@ benchmark_pairs_all_pairs_s1024_t200 distinct_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_all_pairs_s1024_t200 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_all_pairs_s1024_t200)) r ON p.src = r.src AND p.dst = r.dst;
.print @@ benchmark_pairs_all_pairs_s1024_t200 sorted_below 5
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst));
.print @@ benchmark_pairs_all_pairs_s1024_t200 scan_only 5
SELECT count(*), sum(src), sum(dst) FROM benchmark_pairs_all_pairs_s1024_t200;
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src, dst);
.print @@ benchmark_pairs_all_pairs_s1024_t200 aggregate_src_list_dst 5
SELECT count(*), sum(src), sum(len(dsts)) FROM (SELECT src, list(dst) AS dsts FROM benchmark_pairs_all_pairs_s1024_t200 GROUP BY src);
.print @@ benchmark_pairs_all_pairs_s1024_t200 sort_src_dst 5
SELECT count(*), sum(src), sum(dst) FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst);
.timer off
SET experimental_path_finding_operator_benchmark=true;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1024 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1024)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1024 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1024)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_1024);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_1024 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_1024)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_1024_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_1024 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536_twice);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536_twice p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536_twice)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536_twice);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536_twice p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536_twice)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_65536_twice);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_65536_twice p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_65536_twice)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_65536_twice_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_65536_twice ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_all_pairs_s1024_t200);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_all_pairs_s1024_t200 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_all_pairs_s1024_t200)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_all_pairs_s1024_t200);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_all_pairs_s1024_t200 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_all_pairs_s1024_t200)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst));
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_operator';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM benchmark_pairs_all_pairs_s1024_t200);
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_distinct_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM benchmark_pairs_all_pairs_s1024_t200 p JOIN (SELECT src, dst, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT DISTINCT src, dst FROM benchmark_pairs_all_pairs_s1024_t200)) r ON p.src = r.src AND p.dst = r.dst;
SET experimental_path_finding_operator_benchmark_prefix='/home/dwolde/.claude/jobs/38c43c3d/tmp/probe/sf300/phases/sf300_benchmark_pairs_all_pairs_s1024_t200_sorted_below';
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM benchmark_pairs_all_pairs_s1024_t200 ORDER BY src, dst));
