-- Reproducer for path-counting queries that enumerate every path (q8/q9 of the graph benchmark).
-- See FACTORIZED_PATH_COUNTING.md next to this file.
--
-- Run with a locally built DuckPGQ (DuckDB 2.0 / DuckPGQ main):
--   duckdb -unsigned -cmd "LOAD 'build/release/extension/duckpgq/duckpgq.duckdb_extension'; \
--                          SET active_grammar_extensions = ['duckpgq'];" < factorized_join_repro.sql
-- Run with the community extension (DuckDB 1.5.x):
--   duckdb -cmd "INSTALL duckpgq FROM community; LOAD duckpgq;" < factorized_join_repro.sql
--
-- Generates a synthetic social graph with the same shape as the benchmark data:
--   * 10 random FOLLOWS edges per person
--   * 0.5% of persons are hubs, each followed by 0.5%-5% of all persons
-- Adjust the size with the variable below (100000 ~ 2.3M edges, 1000000 ~ 147M edges).
SET VARIABLE n = 100000;

-- Deterministic generation: random() is only reproducible single-threaded.
SET threads = 1;
SELECT setseed(0.42);

CREATE OR REPLACE TABLE Person AS
SELECT i AS id, 18 + (random() * 60)::INT AS age
FROM range(1, getvariable('n') + 1) t(i);

CREATE OR REPLACE TABLE Follows AS
WITH hubs AS (
    SELECT (1 + random() * (getvariable('n') - 1))::BIGINT AS hub,
           (getvariable('n') * 0.005 + random() * getvariable('n') * 0.045)::INT AS n_followers
    FROM range((getvariable('n') * 0.005)::INT)
),
regular AS (
    SELECT (1 + random() * (getvariable('n') - 1))::BIGINT AS src,
           (1 + random() * (getvariable('n') - 1))::BIGINT AS dst
    FROM range(getvariable('n') * 10)
),
hub_edges AS (
    SELECT (1 + random() * (getvariable('n') - 1))::BIGINT AS src, hub AS dst
    FROM hubs, range(hubs.n_followers)
)
SELECT DISTINCT src, dst
FROM (SELECT * FROM regular UNION ALL SELECT * FROM hub_edges)
WHERE src <> dst;

RESET threads;

CREATE OR REPLACE PROPERTY GRAPH g
VERTEX TABLES (Person)
EDGE TABLES (
    Follows SOURCE KEY (src) REFERENCES Person (id)
            DESTINATION KEY (dst) REFERENCES Person (id)
            LABEL Follows
);

SELECT (SELECT count(*) FROM Person) AS persons,
       (SELECT count(*) FROM Follows) AS follows,
       (SELECT max(indeg) FROM (SELECT count(*) AS indeg FROM Follows GROUP BY dst)) AS max_in_degree;

.timer on
-- q8: number of two-hop paths. Enumerates every path.
SELECT count(*) AS q8_paths
FROM GRAPH_TABLE (g
    MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
    COLUMNS (a.id AS a_id)
);

-- q8 as a degree product: sum over b of in-degree(b) * out-degree(b). Linear in edges.
SELECT sum(i.cnt * o.cnt) AS q8_paths_factorized
FROM (SELECT dst AS b, count(*) AS cnt FROM Follows GROUP BY dst) i
JOIN (SELECT src AS b, count(*) AS cnt FROM Follows GROUP BY src) o USING (b);

-- q9: same pattern with filters on the middle and end vertex.
SELECT count(*) AS q9_paths
FROM GRAPH_TABLE (g
    MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
    WHERE b.age < 50 AND c.age > 25
    COLUMNS (a.id AS a_id)
);

-- q9 as a degree product: the c filter moves into the out-degree, the b filter onto b.
SELECT sum(i.cnt * o.cnt) AS q9_paths_factorized
FROM (SELECT dst AS b, count(*) AS cnt FROM Follows GROUP BY dst) i
JOIN (SELECT r2.src AS b, count(*) AS cnt
      FROM Follows r2 JOIN Person c ON c.id = r2.dst
      WHERE c.age > 25 GROUP BY r2.src) o USING (b)
JOIN Person pb ON pb.id = i.b
WHERE pb.age < 50;
.timer off
