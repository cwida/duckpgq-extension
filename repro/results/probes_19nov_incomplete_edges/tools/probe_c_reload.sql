-- Probe (c), part 2: the same file in a new process.
SET active_grammar_extensions = ['duckpgq'];
.mode list
.nullvalue NULL
SET experimental_path_finding_operator = true;
SET duckpgq_csr_index_fold_ratio = 0;
.print ==== (c) 9. reload: the graph, the index and the constraints are back
SELECT DISTINCT verified FROM __duckpgq_internal WHERE property_graph = 'g';
SELECT src, dst, len, vs, es FROM GRAPH_TABLE (g
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len, vertices(p) AS vs, edges(p) AS es));
SELECT duckpgq_path_finding_strategy() AS strategy;
SELECT duckpgq_csr_index_stats('pe', 'pe_csr') AS stats;
.print -- still rejected: an edge to a missing vertex, the delete of a vertex with edges
INSERT INTO pe VALUES (1, 99);
DELETE FROM pv WHERE id = 7;
.print -- a valid insert after the reload, a fold and the query
INSERT INTO pe VALUES (1, 2);
SELECT duckpgq_csr_index_fold('pe', 'pe_csr') AS fold;
SELECT src, dst, len, vs, es FROM GRAPH_TABLE (g
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len, vertices(p) AS vs, edges(p) AS es));
