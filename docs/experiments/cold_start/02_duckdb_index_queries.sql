LOAD '/Users/dwolde/git/duckdb-worktree/pathfindingoperator-forward-port/build/release/extension/duckpgq/duckpgq.duckdb_extension';
SET active_grammar_extensions = ['duckpgq'];
SET experimental_path_finding_operator = true;
SET experimental_path_finding_operator_source_group_ratio = 0;
.timer on
SELECT 'bind' AS what, duckpgq_csr_index_stats('e', 'e_csr');
SELECT '1-pair first' AS what, count(*) FROM GRAPH_TABLE (g MATCH p = ANY SHORTEST (a:V WHERE a.id = 7)-[x:E]->{1,8}(b:V WHERE b.id = 1999999) COLUMNS (a.id AS s, b.id AS d, path_length(p) AS len));
SELECT '1-pair second' AS what, count(*) FROM GRAPH_TABLE (g MATCH p = ANY SHORTEST (a:V WHERE a.id = 7)-[x:E]->{1,8}(b:V WHERE b.id = 1999999) COLUMNS (a.id AS s, b.id AS d, path_length(p) AS len));
SELECT '20-source first' AS what, count(*), sum(len) FROM GRAPH_TABLE (g MATCH p = ANY SHORTEST (a:V WHERE a.id < 20)-[x:E]->{1,8}(b:V WHERE b.id >= 1999800) COLUMNS (a.id AS s, b.id AS d, path_length(p) AS len));
SELECT '20-source second' AS what, count(*), sum(len) FROM GRAPH_TABLE (g MATCH p = ANY SHORTEST (a:V WHERE a.id < 20)-[x:E]->{1,8}(b:V WHERE b.id >= 1999800) COLUMNS (a.id AS s, b.id AS d, path_length(p) AS len));
SELECT 'strategy' AS what, duckpgq_path_finding_strategy();
