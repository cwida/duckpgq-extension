# Neo4j Graphalytics benchmark

This setup uses the official Neo4j Docker image and the Graph Data Science (GDS) plugin.
Docker Desktop must be running.

## Prepare a dataset

```sh
python3 scripts/pathfinding_benchmark.py prepare \
  --system neo4j \
  --graphalytics-datasets wiki-Talk \
  --threads 4
```

The prepare step does these tasks:

1. It converts the Graphalytics Parquet files to the Neo4j import schema.
2. It adds reverse edges for an undirected dataset.
3. It runs `neo4j-admin database import full` with Parquet input.
4. It records input preparation time, import time, row counts, and database size.

The Neo4j files are in
`data/ldbc-pathfinding/systems/neo4j/graphalytics/<dataset>/data`.
The temporary import files are removed after a successful import.

## Run the benchmark

```sh
python3 scripts/pathfinding_benchmark.py run \
  --system neo4j \
  --dataset wiki-Talk \
  --query-pattern graphalytics_bfs \
  --threads 4 \
  --repeats 3 \
  --neo4j-heap 8G \
  --neo4j-pagecache 2G
```

The runner starts one Neo4j container. It projects the native graph into the GDS graph
catalog. It then runs unweighted Delta-Stepping single-source shortest path. The query
returns the same distance aggregates as the Graphalytics BFS reference. The runner stops
if these values do not match the reference.

This is not the `gds.bfs.stream` procedure. That procedure returns traversal paths and
does not return one shortest distance for each reachable vertex. Result files identify
the tested algorithm as `unweighted_delta_sssp`.

The result separates these times:

- `dataset_metadata_system_input_prepare_s`: Parquet conversion.
- `dataset_metadata_system_import_s`: Neo4j native database import.
- `benchmark_system_runtime_version`: GDS version reported by the running server.
- `source_lookup_s`: lookup of the Graphalytics source vertex.
- `graph_projection_s`: client wall time for the GDS projection.
- `graph_projection_reported_s`: projection time reported by GDS.
- `query_s`: Delta-Stepping execution and result aggregation.

The first repeat includes source lookup and graph projection in `setup_s` and `total_s`.
Later repeats reuse the GDS projection.

GDS Community limits algorithm and projection concurrency to four threads. Use thread
counts from one to four for this setup. A larger value requires GDS Enterprise and an
explicit `--neo4j-gds-max-concurrency` value.

## Sweep prepared datasets

```sh
python3 scripts/pathfinding_benchmark.py sweep-graphalytics-bfs \
  --system neo4j \
  --datasets small \
  --threads 1 2 4 \
  --repeats 3 \
  --neo4j-heap 16G \
  --neo4j-pagecache 4G
```

The setup follows the official Neo4j documentation for
[`neo4j-admin database import`](https://neo4j.com/docs/operations-manual/current/import/),
[Docker plugins](https://neo4j.com/docs/operations-manual/current/docker/plugins/),
[native GDS projection](https://neo4j.com/docs/graph-data-science/current/management-ops/graph-creation/graph-project/),
and [Delta-Stepping SSSP](https://neo4j.com/docs/graph-data-science/current/algorithms/delta-single-source/).
