ATTACH '/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/db/ldbc_sf300.duckdb' AS ldbc (READ_ONLY);
LOAD '/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension';
SET threads=32;
SET experimental_path_finding_operator=true;
SET experimental_path_finding_operator_benchmark=false;
CREATE TEMP TABLE pairs_arrival AS SELECT a.id::BIGINT AS src, b.id::BIGINT AS dst FROM ldbc.benchmark_pairs_65536 p JOIN ldbc.person a ON a.rowid = p.src JOIN ldbc.person b ON b.rowid = p.dst;
CREATE TEMP TABLE pairs AS SELECT src, dst, dense_rank() OVER (ORDER BY src) - 1 AS rnk FROM pairs_arrival;
.print @@ arrival_order
SELECT round(avg((src > prev)::INT), 3) AS ascending_share FROM (SELECT src, lag(src) OVER (ORDER BY rowid) AS prev FROM pairs_arrival) WHERE prev IS NOT NULL;
.print @@ shape
SELECT count(*) AS pairs, count(DISTINCT src) AS sources, (SELECT count(*) FROM ldbc.person) AS vertices FROM pairs;
.print @@ warmup
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM pairs_arrival);
.timer on
.print @@ arrival 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM pairs_arrival);
.print @@ src 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY src));
.print @@ dst 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY dst));
.print @@ hash 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(src)));
.print @@ win1024 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 1024, hash(src)));
.print @@ win4096 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 4096, hash(src)));
.print @@ win16384 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 16384, hash(src)));
.print @@ waves_shuf 1
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(rnk // 256), src));
.print @@ arrival 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM pairs_arrival);
.print @@ src 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY src));
.print @@ dst 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY dst));
.print @@ hash 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(src)));
.print @@ win1024 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 1024, hash(src)));
.print @@ win4096 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 4096, hash(src)));
.print @@ win16384 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 16384, hash(src)));
.print @@ waves_shuf 2
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(rnk // 256), src));
.print @@ arrival 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM pairs_arrival);
.print @@ src 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY src));
.print @@ dst 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY dst));
.print @@ hash 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(src)));
.print @@ win1024 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 1024, hash(src)));
.print @@ win4096 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 4096, hash(src)));
.print @@ win16384 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY rnk // 16384, hash(src)));
.print @@ waves_shuf 3
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(rnk // 256), src));
.timer off
.print @@ deciles
SELECT rnk * 10 // (SELECT max(rnk) + 1 FROM pairs) AS source_decile, count(*) AS pairs, count(len) AS reachable, round(avg(len), 2) AS mean_len, max(len) AS max_len FROM (SELECT rnk, iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM pairs) GROUP BY ALL ORDER BY 1;
SET experimental_path_finding_operator_benchmark=true;
SET experimental_path_finding_operator_benchmark_lane_activity=true;
SET experimental_path_finding_operator_benchmark_prefix='/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/pair-grouping-probe/repro/results/probes_18nov_pair_grouping/control/activity_sf300/arrival/w';
.print @@ diagnosed arrival
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM pairs_arrival);
SET experimental_path_finding_operator_benchmark_prefix='/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/pair-grouping-probe/repro/results/probes_18nov_pair_grouping/control/activity_sf300/src/w';
.print @@ diagnosed src
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY src));
SET experimental_path_finding_operator_benchmark_prefix='/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/pair-grouping-probe/repro/results/probes_18nov_pair_grouping/control/activity_sf300/hash/w';
.print @@ diagnosed hash
SELECT count(*) AS pairs, count(len) AS reachable, sum(len) AS total_len FROM (SELECT iterativelengthoperator(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|ldbc|main|person_knows_person|pathfinding_benchmark_csr') AS len FROM (SELECT src, dst FROM pairs ORDER BY hash(src)));
