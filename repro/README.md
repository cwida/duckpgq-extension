# Reproducing the path-finding evaluation

Everything the evaluation section reports comes out of four scripts in this directory, run in order on one
machine. The scripts wrap `scripts/pathfinding_benchmark.py` (the harness) and `scripts/plot_sweep_results.py`
(figures and LaTeX tables); the exact command lines are visible in `run_all.sh`.

```
./setup.sh      # toolchain check, Python clients, DuckPGQ build, LDBC generator build, baseline containers
./prepare.sh    # LDBC SNB data at SF10, SF100, SF300 and the DuckDB databases
./run_all.sh    # the full matrix (resumable; ~3.5 days on the paper machine), or: sweep | ldbc | client | updates
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
| `sweep_pairs_sf*` | `sweep-pairs` | random pairs 1..65536; 1/16/128/1024 sources x all persons | all seven |
| `ldbc_queries_sf*` | `ldbc-queries` | IC13, BI19, BI20 (hop-count variants) | all seven |
| `*_client32` | the two above with `--client-threads 32` | same | Kuzu, Memgraph, FalkorDB (SF10, SF100) |
| `update_*`, `concurrent_*` | `update-run`, `update-concurrent`, `update-<system>` | 190 commits of 2000 knows operations with a query after each | DuckPGQ build and index routes, Kuzu, Memgraph, FalkorDB, Neo4j |

Every query has a 600 s timeout; a timed-out point is recorded as `timed_out` and larger points of the same
shape as `timed_out_inferred`. Baselines skip pair lists above 500 M pairs (`exceeds_pair_limit`). Repeats: 3,
the mean is reported. All DuckPGQ numbers use 32 threads.

## Results archive

`results/sf{10,100,300}/*.csv` are the harness outputs behind the current figures and tables, one file per
(run, scale factor), with the commit hash and machine in the `notes` column. `results/*_table.txt` are the
harness's own text tables over the same files. `collect.sh` adds new CSVs to the archive without overwriting
earlier ones; the plot script takes the newest file per (system, shape).

## Standalone repository

This directory is written so that it can be lifted into a separate repository: it depends on the repository
root only through `common.sh` (`REPO_ROOT`), and `setup.sh` clones and builds the generator itself. For the
artifact, the standalone repository adds this checkout as a submodule and keeps `setup.sh` as the single entry
point.
