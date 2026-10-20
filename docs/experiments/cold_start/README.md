# Cold-start comparison: DuckDB CSR index vs Kuzu vs LadybugDB

Same graph (2M vertices, 8M edges, deterministic hash-based), 18 threads, one fresh process per measurement.

1. `01_duckdb_create_graph.sql` on a file database: creates `v`, `e`, the property graph, and the CSR index, then checkpoints.
2. Export for the graph databases: `COPY (SELECT id FROM v) TO 'pf_v.parquet'; COPY (SELECT src, dst FROM e) TO 'pf_e.parquet';`
3. `python graphdb_cold.py kuzu import <db>` then `python graphdb_cold.py kuzu open <db>` (same for `ladybug`); needs `pip install kuzu ladybug`.
4. `02_duckdb_index_queries.sql` on the file with the index; `03_duckdb_build_queries.sql` on a copy where the index was dropped.

Paths inside the scripts point at /tmp and the local build; adjust before use. Results of 19 Oct 2026 are in docs/CSR_INDEX_DESIGN.md section 5.6.
