LOAD '/Users/dwolde/git/duckdb-worktree/pathfindingoperator-forward-port/build/release/extension/duckpgq/duckpgq.duckdb_extension';
SET active_grammar_extensions = ['duckpgq'];
SET experimental_path_finding_operator = true;
.timer on
SELECT '1-pair first (build)' AS what, count(*) FROM GRAPH_TABLE (g MATCH p = ANY SHORTEST (a:V WHERE a.id = 7)-[x:E]->{1,8}(b:V WHERE b.id = 1999999) COLUMNS (a.id AS s, b.id AS d, path_length(p) AS len));
SELECT '1-pair second (cached)' AS what, count(*) FROM GRAPH_TABLE (g MATCH p = ANY SHORTEST (a:V WHERE a.id = 7)-[x:E]->{1,8}(b:V WHERE b.id = 1999999) COLUMNS (a.id AS s, b.id AS d, path_length(p) AS len));
SELECT '20-source, source-grouped kernel (cached csr)' AS what, count(*), sum(len) FROM GRAPH_TABLE (g MATCH p = ANY SHORTEST (a:V WHERE a.id < 20)-[x:E]->{1,8}(b:V WHERE b.id >= 1999800) COLUMNS (a.id AS s, b.id AS d, path_length(p) AS len));
SELECT 'strategy' AS what, duckpgq_path_finding_strategy();
