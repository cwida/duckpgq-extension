LOAD '/Users/dwolde/git/duckdb-worktree/pathfindingoperator-forward-port/build/release/extension/duckpgq/duckpgq.duckdb_extension';
SET active_grammar_extensions = ['duckpgq'];
CREATE TABLE v AS SELECT range AS id FROM range(2000000);
CREATE TABLE e AS SELECT (hash(range) % 2000000)::BIGINT AS src, (hash(range * 7 + 3) % 2000000)::BIGINT AS dst FROM range(8000000);
CREATE PROPERTY GRAPH g VERTEX TABLES (v LABEL V) EDGE TABLES (e SOURCE KEY (src) REFERENCES v(id) DESTINATION KEY (dst) REFERENCES v(id) LABEL E);
.timer on
SELECT 'checkpoint without index' AS what;
CHECKPOINT;
CREATE INDEX e_csr ON e USING CSR (src, dst);
SELECT 'checkpoint with index' AS what;
CHECKPOINT;
