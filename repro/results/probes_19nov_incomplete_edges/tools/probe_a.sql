-- Probe (a): after creation, insert a vertex-less key as an intermediate hop.
SET active_grammar_extensions = ['duckpgq'];
.mode list
.nullvalue NULL
CREATE TABLE pv(id BIGINT);
INSERT INTO pv VALUES (1), (2), (3);
CREATE TABLE pe_i(src BIGINT, dst BIGINT);
CREATE TABLE pe_b(src BIGINT, dst BIGINT);
INSERT INTO pe_i VALUES (1, 3);
INSERT INTO pe_b VALUES (1, 3);
CREATE PROPERTY GRAPH g_i VERTEX TABLES (pv LABEL PV)
  EDGE TABLES (pe_i SOURCE KEY (src) REFERENCES pv (id) DESTINATION KEY (dst) REFERENCES pv (id) LABEL PE);
CREATE PROPERTY GRAPH g_b VERTEX TABLES (pv LABEL PV)
  EDGE TABLES (pe_b SOURCE KEY (src) REFERENCES pv (id) DESTINATION KEY (dst) REFERENCES pv (id) LABEL PE);
CREATE INDEX pe_csr ON pe_i USING CSR (src, dst);
-- no automatic fold: the fold below is the explicit one
SET duckpgq_csr_index_fold_ratio = 0;
SELECT property_graph, table_name, verified FROM __duckpgq_internal ORDER BY ALL;
.print ==== (a) 0. complete graph: no path 1 -> 2
.read tools/common_queries.sql
.print ==== (a) 1. after INSERT (1, 9), (9, 2): 9 has no vertex row. The inserts:
INSERT INTO pe_i VALUES (1, 9), (9, 2);
INSERT INTO pe_b VALUES (1, 9), (9, 2);
SELECT duckpgq_csr_index_stats('pe_i', 'pe_csr') AS stats_before_fold;
.read tools/common_queries.sql
.print ==== (a) 2. after an explicit fold
SELECT duckpgq_csr_index_fold('pe_i', 'pe_csr') AS fold;
SELECT duckpgq_csr_index_stats('pe_i', 'pe_csr') AS stats_after_fold;
.read tools/common_queries.sql
.print ==== (a) 3. CREATE OR REPLACE runs the check again and fails; the previous definition stays
CREATE OR REPLACE PROPERTY GRAPH g_i VERTEX TABLES (pv LABEL PV)
  EDGE TABLES (pe_i SOURCE KEY (src) REFERENCES pv (id) DESTINATION KEY (dst) REFERENCES pv (id) LABEL PE);
SELECT src, dst, len FROM GRAPH_TABLE (g_i
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len));
