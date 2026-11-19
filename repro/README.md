# Reproducing the path-finding evaluation

Everything the evaluation section reports comes out of four scripts in this directory, run in order on one
machine. The scripts wrap `scripts/pathfinding_benchmark.py` (the harness) and `scripts/plot_sweep_results.py`
(figures and LaTeX tables); the exact command lines are visible in `run_all.sh`.

```
./setup.sh      # toolchain check, Python clients, DuckPGQ build, LDBC generator build, baseline containers
./prepare.sh    # LDBC SNB data at SF10, SF100, SF300, the DuckDB databases, the three Graphalytics graphs and the road network (see below for its edge list)
./run_all.sh    # the full matrix (resumable; ~3.5 days on the paper machine), or: fixed | sweep | graphalytics | road | ldbc | client | native | microbench | updates
./collect.sh    # paper/figures/*.pdf, paper/tables/*.tex, and the results archive in repro/results
```

`./collect.sh archive` renders the figures and tables from the committed CSVs in `repro/results` without
measuring anything. `./environment.sh` prints the machine and version record; `ENVIRONMENT.txt` is the record
of the paper machine.

## Machine used for the paper

AWS m7a.8xlarge (AMD EPYC 9R14, 32 cores, 123 GiB, no swap), Amazon Linux 2023, gp3 EBS volume at about
250 MB/s. The data for all three scale factors needs about 60 GB of disk; Neo4j and Memgraph each hold the SF300
graph in memory, so 64 GiB is the practical minimum for the baselines at SF300.

## Software pins

| Component | Version | How it is pinned |
|---|---|---|
| DuckPGQ | this commit, branch `pathfindingoperator-forward-port` | git; DuckDB as a submodule |
| LDBC SNB generator | `Dtenwolde/ldbc-data-gen` @ `1add85f` | `GENERATOR_COMMIT` in `common.sh` |
| Kuzu | 0.11.3 (embedded) | `requirements.txt` |
| Memgraph | 3.14.0 | `MEMGRAPH_IMAGE` in `common.sh` |
| FalkorDB | 4.2.2 (image `falkordb/falkordb:latest` of 19 Oct 2026) | `FALKORDB_IMAGE`; pin by digest for a frozen rerun |
| Neo4j | 5.26.32 Enterprise, Graph Data Science 2.13.14 | `NEO4J_IMAGE`; GDS via `NEO4J_PLUGINS` |
| Python drivers | neo4j 5.28.8, falkordb 1.2.2 | `requirements.txt` |
| matplotlib | 3.9.4 | `requirements.txt` |

Neo4j runs as the Enterprise edition under its evaluation licence (`NEO4J_ACCEPT_LICENSE_AGREEMENT=yes`), so the
parallel Cypher runtime is available; the GDS plugin is unlicensed and therefore limited to 4 threads. Memgraph
and FalkorDB run with their defaults; FalkorDB has no undirected `shortestPath`, so both edge directions are
loaded for the LDBC queries. The harness waits on the Bolt and Redis ports and loads each baseline from the CSV
export the first time it is needed; that load time is recorded as `load_s` in the CSVs.

## What `run_all.sh` measures

| Step | Harness subcommand | Shapes | Systems |
|---|---|---|---|
| `refresh_indexes_*` (first, in every subset) | `refresh-indexes` | not a measurement: drops every stored CSR index of the database, creates the benchmark index with this build, checkpoints, and fails unless a new read-only process loads the stored index (an index stored by another build is rebuilt by its first reader in every process) | DuckPGQ |
| `sweep_pairs_sf*` | `sweep-pairs` | random pairs 1..65536; 1/16/128/1024 sources x all persons | all seven |
| `ldbc_queries_sf*` | `ldbc-queries` | IC13, BI19, BI20 (hop-count variants) | all seven |
| `*_client32` | the two above with `--client-threads 32` | same | Kuzu, Memgraph, FalkorDB (SF10, SF100) |
| `microbench_sf*` | `microbench` | random pairs, same destination, sources x all with one mechanism switched off at a time | DuckPGQ build and index routes (SF10, SF100) |
| `fixed_costs_*` | `fixed-costs` | database file size, CSR index build time and bytes per edge, every baseline's fresh load time and store size | all |
| `sweep_pairs_cit_patents*`, `sweep_pairs_graph500_24`, `sweep_pairs_datagen_8_4_fb`, `microbench_scheduling_graph500_24` | `sweep-pairs --dataset`, `microbench --dataset` | the sweep shapes on three Graphalytics graphs (W30); pairs sampled from vertices with edges, no hop bound | all on cit-Patents; DuckPGQ and Kuzu on the two large graphs |
| `fixed_costs_osm_oberbayern`, `sweep_pairs_osm_oberbayern*`, `microbench_shape_probe_osm_oberbayern` | `fixed-costs --dataset`, `sweep-pairs --dataset`, `microbench --dataset` | the sweep shapes on a road network, OSM Oberbayern (495,357 vertices, 805,743 directed edges, shortest paths of hundreds of hops); no hop bound; the deep-graph rule on and off | all, and Umbra without and with its navigational index (`umbra`, `umbra_navindex`; the index's creation time is in the fixed costs); the rule on the DuckPGQ index route |
| `native_baselines_sf*` | `sweep-pairs --systems duckpgq_index duckpgq_recursive duckpgq_scalar` | random pairs and sources x all | DuckDB recursive CTE and the 2023 UDF route (SF10, SF100) |
| `update_*`, `concurrent_*` | `update-run`, `update-concurrent`, `update-<system>` | 190 commits of 2000 knows operations with a query after each; the index folds at the extension's default ratio, a thousandth (a tenth in the archives up to rc13) | DuckPGQ build and index routes, Kuzu, Memgraph, FalkorDB, Neo4j |

The road network is not downloaded. Its edge list is `data/oberbayern.tsv` of the Nav-Index reproducibility data
(Reif and Neumann, PVLDB 19(11), 2026; https://db.in.tum.de/~reif/uploads/data.tar.gz, 13 GB), expected as
`~/navindex/data/data/oberbayern.tsv` or under `DUCKPGQ_EDGE_LIST_DIR`:
`curl -L https://db.in.tum.de/~reif/uploads/data.tar.gz | tar -xzO data/oberbayern.tsv > oberbayern.tsv`.

Umbra runs on the road network only, from the binary of the same paper's artifact
(https://gitlab.db.in.tum.de/max.reif/nav-index-reproducibility, `umbra/bin/sql`; the paper's numbers are from
commit 1f341922, Umbra v0.2-1423-g7c87fba6e). The harness expects the artifact at `~/navindex/artifact` or its
`umbra` directory under `DUCKPGQ_UMBRA_ROOT`; `setup.sh` does not fetch it. One `bin/sql` process holds the graph in
memory for the sweep: edges with cost 1, directed, loaded from the same CSV export as the other baselines. Every
count is one statement over a pair table (`shortest_path` per row), timed at the client like the other baselines.
`umbra` has no stored index (its cost model chooses between Dijkstra and a contraction hierarchy built inside the
query); `umbra_navindex` creates the navigational index once after the load. A timeout stops the process, and the
graph is loaded again before the next shape.

Every query has a 600 s timeout; a timed-out point is recorded as `timed_out` and larger points of the same
shape as `timed_out_inferred`. Baselines skip pair lists above 500 M pairs (`exceeds_pair_limit`). Repeats: 3,
the median is reported. All DuckPGQ numbers use 32 threads.

## Results archive

`results/sf{10,100,300}/*.csv` and `results/graphalytics_*/*.csv` are the harness outputs behind the current figures and tables, one file per
(run, scale factor), with the commit hash and machine in the `notes` column. `results/*_table.txt` are the
harness's own text tables over the same files. `collect.sh` adds new CSVs to the archive without overwriting
earlier ones; the plot script takes the newest file per (system, shape).

## Standalone repository

This directory is written so that it can be lifted into a separate repository: it depends on the repository
root only through `common.sh` (`REPO_ROOT`), and `setup.sh` clones and builds the generator itself. For the
artifact, the standalone repository adds this checkout as a submodule and keeps `setup.sh` as the single entry
point.
