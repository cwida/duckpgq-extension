-- Probe (b): the same shape reached by deleting the vertex row of m = 9 while its edges stay.
SET active_grammar_extensions = ['duckpgq'];
.mode list
.nullvalue NULL
CREATE TABLE pv(id BIGINT);
INSERT INTO pv VALUES (1), (2), (3), (9);
CREATE TABLE pe_i(src BIGINT, dst BIGINT);
CREATE TABLE pe_b(src BIGINT, dst BIGINT);
INSERT INTO pe_i VALUES (1, 3), (1, 9), (9, 2);
INSERT INTO pe_b VALUES (1, 3), (1, 9), (9, 2);
CREATE PROPERTY GRAPH g_i VERTEX TABLES (pv LABEL PV)
  EDGE TABLES (pe_i SOURCE KEY (src) REFERENCES pv (id) DESTINATION KEY (dst) REFERENCES pv (id) LABEL PE);
CREATE PROPERTY GRAPH g_b VERTEX TABLES (pv LABEL PV)
  EDGE TABLES (pe_b SOURCE KEY (src) REFERENCES pv (id) DESTINATION KEY (dst) REFERENCES pv (id) LABEL PE);
CREATE INDEX pe_csr ON pe_i USING CSR (src, dst);
SET duckpgq_csr_index_fold_ratio = 0;
.print ==== (b) 0. complete graph: 1 -> 9 -> 2 on every route
.read tools/common_queries.sql
.print ==== (b) 1. after DELETE FROM pv WHERE id = 9 (its two edges stay). The delete:
DELETE FROM pv WHERE id = 9;
SELECT duckpgq_csr_index_stats('pe_i', 'pe_csr') AS stats_before_fold;
.read tools/common_queries.sql
.print ==== (b) 2. after an explicit fold (the edge table did not change)
SELECT duckpgq_csr_index_fold('pe_i', 'pe_csr') AS fold;
SELECT duckpgq_csr_index_stats('pe_i', 'pe_csr') AS stats_after_fold;
.read tools/common_queries.sql
.print ==== (b) 3. an edge change and a second fold, so that an epoch is rebuilt after the vertex row is gone
INSERT INTO pe_i VALUES (3, 1);
INSERT INTO pe_b VALUES (3, 1);
SELECT duckpgq_csr_index_fold('pe_i', 'pe_csr') AS fold;
SELECT duckpgq_csr_index_stats('pe_i', 'pe_csr') AS stats_after_second_fold;
.read tools/common_queries.sql
