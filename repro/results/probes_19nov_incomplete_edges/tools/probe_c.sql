-- Probe (c), part 1: a real FOREIGN KEY on the edge table together with a CSR index. Run on a file database.
SET active_grammar_extensions = ['duckpgq'];
.mode list
.nullvalue NULL
CREATE TABLE pv(id BIGINT PRIMARY KEY);
INSERT INTO pv VALUES (1), (2), (3), (9);
CREATE TABLE pe(src BIGINT REFERENCES pv(id), dst BIGINT REFERENCES pv(id));
INSERT INTO pe VALUES (1, 3), (1, 9), (9, 2);
.print ==== (c) 1. CREATE INDEX ... USING CSR on a table with two foreign keys, and the graph (the check skips its scan)
CREATE INDEX pe_csr ON pe USING CSR (src, dst);
CREATE PROPERTY GRAPH g VERTEX TABLES (pv LABEL PV)
  EDGE TABLES (pe SOURCE KEY (src) REFERENCES pv (id) DESTINATION KEY (dst) REFERENCES pv (id) LABEL PE);
SELECT DISTINCT verified FROM __duckpgq_internal WHERE property_graph = 'g';
SET duckpgq_csr_index_fold_ratio = 0;
SET experimental_path_finding_operator = true;
.print ==== (c) 2. the insert of (a): edges through a key without a vertex row
INSERT INTO pe VALUES (1, 7), (7, 2);
.print ==== (c) 3. the delete of (b): the vertex row of 9 while its edges stay
DELETE FROM pv WHERE id = 9;
.print ==== (c) 4. path query over the index
SELECT src, dst, len, vs, es FROM GRAPH_TABLE (g
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len, vertices(p) AS vs, edges(p) AS es));
SELECT duckpgq_path_finding_strategy() AS strategy;
.print ==== (c) 5. valid changes: a new vertex and its edges in one transaction, then an edge delete; the query
BEGIN;
INSERT INTO pv VALUES (7);
INSERT INTO pe VALUES (3, 7), (7, 2);
COMMIT;
DELETE FROM pe WHERE src = 9 AND dst = 2;
SELECT src, dst, len, vs, es FROM GRAPH_TABLE (g
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len, vertices(p) AS vs, edges(p) AS es));
SELECT duckpgq_csr_index_stats('pe', 'pe_csr') AS stats_before_fold;
.print ==== (c) 6. fold, then the query
SELECT duckpgq_csr_index_fold('pe', 'pe_csr') AS fold;
SELECT duckpgq_csr_index_stats('pe', 'pe_csr') AS stats_after_fold;
SELECT src, dst, len, vs, es FROM GRAPH_TABLE (g
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len, vertices(p) AS vs, edges(p) AS es));
.print ==== (c) 7. removing a vertex properly: its last edge first, then the row
.print -- both in one transaction:
BEGIN;
DELETE FROM pe WHERE src = 1 AND dst = 9;
DELETE FROM pv WHERE id = 9;
COMMIT;
SELECT count(*) AS edges_of_9, (SELECT count(*) FROM pv WHERE id = 9) AS vertex_rows_of_9 FROM pe WHERE src = 9 OR dst = 9;
.print -- in two transactions:
DELETE FROM pe WHERE src = 1 AND dst = 9;
DELETE FROM pv WHERE id = 9;
SELECT count(*) AS edges_of_9, (SELECT count(*) FROM pv WHERE id = 9) AS vertex_rows_of_9 FROM pe WHERE src = 9 OR dst = 9;
.print ==== (c) 8. checkpoint, then the query
CHECKPOINT;
SELECT src, dst, len, vs, es FROM GRAPH_TABLE (g
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len, vertices(p) AS vs, edges(p) AS es));
