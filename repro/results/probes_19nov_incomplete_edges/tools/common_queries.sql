-- Queries of probes (a) and (b): x = 1, y = 2, m = 9. g_i is the graph over pe_i (CSR index: index route),
-- g_b the same graph over pe_b (no index: the query builds its CSR, build route).
.print -- index route, operator on: length and path 1 -> 2
SET experimental_path_finding_operator = true;
SELECT src, dst, len, vs, es, path FROM GRAPH_TABLE (g_i
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len, vertices(p) AS vs, edges(p) AS es, element_id(p) AS path));
SELECT duckpgq_path_finding_strategy() AS strategy_index_paths;
SELECT src, dst, len FROM GRAPH_TABLE (g_i
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len));
SELECT duckpgq_path_finding_strategy() AS strategy_index_lengths;
.print -- index route: every destination reachable from 1 (9 has no vertex row, so it cannot be a destination)
SELECT src, dst, len FROM GRAPH_TABLE (g_i
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len)) ORDER BY dst;
.print -- build route, operator on: length and path 1 -> 2
SELECT src, dst, len, vs, es, path FROM GRAPH_TABLE (g_b
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len, vertices(p) AS vs, edges(p) AS es, element_id(p) AS path));
SELECT duckpgq_path_finding_strategy() AS strategy_build_paths;
SELECT src, dst, len FROM GRAPH_TABLE (g_b
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len));
SELECT duckpgq_path_finding_strategy() AS strategy_build_lengths;
.print -- build route: every destination reachable from 1
SELECT src, dst, len FROM GRAPH_TABLE (g_b
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len)) ORDER BY dst;
.print -- SQL translation (operator off), on the indexed and then on the unindexed table: length 1 -> 2
SET experimental_path_finding_operator = false;
SELECT src, dst, len FROM GRAPH_TABLE (g_i
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len));
SELECT src, dst, len FROM GRAPH_TABLE (g_b
    MATCH p = ANY SHORTEST (a:PV WHERE a.id = 1)-[e:PE]->*(b:PV WHERE b.id = 2)
    COLUMNS (a.id AS src, b.id AS dst, path_length(p) AS len));
.print -- one-hop pattern (plain MATCH, a join of the edge table with both vertex tables), indexed table
SELECT s, d FROM GRAPH_TABLE (g_i MATCH (a:PV)-[e:PE]->(b:PV) COLUMNS (a.id AS s, b.id AS d)) ORDER BY ALL;
SET experimental_path_finding_operator = true;
