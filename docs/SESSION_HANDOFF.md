# Session handoff (written 21 Oct 2026)

Read this first when continuing the path-finding paper work in a fresh Claude Code session, on any machine.
The durable state lives in git: this file, `docs/PATHFINDING_PAPER_PLAN.md` (work items with Status column,
schedule, progress log in section 11, sweep recipe in 10b) and `docs/CSR_INDEX_DESIGN.md` (index design and
measurements). The private memory directory of the laptop session is not needed; its content is summarised here.

## Where things stand

Branch `pathfindingoperator-forward-port` (DuckDB v2.0 submodule `45f4fdf3`, `.github/duckdb-version` must match).
Paper: VLDB 2027 rolling, 1 February 2027 round (abstract 25 January), complete draft 1 January. Thesis: a loadable
extension gives a relational engine graph-native path finding, including an MVCC-consistent CSR index.

Done (all committed, suite 82 test files green): W1-W6, W8-W11, W14 (prototype persistence and per-connection
cache removed), W26 (point-query kernel: source-grouped frontier kernel on index snapshots, depth bound, bucket
row index), W7 step 1 (shortest paths over the index through the operator), harness index modes.

Open P1 engineering, in recommended order:
1. W15 remaining index tests: restart under load, concurrent writers with a reader mid-query, fold during a long
   snapshot, drop while in use.
2. W7 step 2: paths on the build-per-query route (edge row id through the endpoint STRUCT(src, dst, id) and the
   radix build; `shortestpathoperator` already has the buffered overloads).
3. W12/W13: cost-based index-versus-build choice and the cost model, after the first sweep.
Then W16 (update workload in the harness), W24/W25 if time.

User side: push the branch, DuckDB-only sweep on the r7a.8xlarge (plan 10b), W17 LDBC queries for Kuzu/Neo4j,
W18 Neo4j Enterprise request, W22 outline.

## Working conventions

- No `Co-Authored-By` or "Generated with" trailers in commits (global rule). Plain commit messages.
- Every test file sets `SET active_grammar_extensions = ['duckpgq']` per connection, after `restart`, inside
  `concurrentloop`, and for named connections.
- `make format-fix` reformats ~20 untouched files on this branch; format only edited files with
  `duckdb/.cache/prepare-venv/bin/clang-format -i <files>` (the venv is created by `make format-check` once).
- Build `GEN=ninja make release`; tests `build/release/test/unittest 'test/sql/*'` (full) or
  `'test/sql/path_finding/*'`.
- CLI for ad-hoc runs: `build/release/duckdb -unsigned -no-agent <db>` and `LOAD` the built extension.
- Timing databases used on the laptop (`/tmp/pf_cold.db`, 2M vertices / 8M edges, tables V/E, graph g, index
  `e_csr`) do not exist elsewhere; `docs/experiments/cold_start/` has the scripts to recreate the graph.
- The harness `scripts/pathfinding_benchmark.py` needs the LDBC generator extension from `~/git/ldbc-data-gen`
  for `prepare`; a hand-made tiny database works for smoke tests (see plan section 10b and the harness memory
  note: tables `person(id)`, `person_knows_person(person1id, person2id)`, `benchmark_metadata(key, value)` at
  `data/ldbc-pathfinding/db/ldbc_sf<sf>.duckdb`).

## Pitfalls already met

- `Event::InsertEvent` moves the parents: insert one completion event and add the rest as dependencies.
- Barriers across ExecutorTasks must be abortable (`MakeQueryBarrier`, `BarrierAbortOnUnwind`).
- Never evaluate row visibility at a past bound: DuckDB garbage-collects row versions; the fold stamps epochs with
  the generation and compacts with a live transaction.
- A writing transaction (`transaction.ChangesMade()`) gets a private snapshot; never publish its epoch.
- Scalar functions that throw need `SetFallible()`; the pre-optimizer sees literals as `BoundCastExpression`, fold
  them with `ExpressionExecutor::TryEvaluateScalar`.
- The eager CSR build used `threads_per_batch = 1` to avoid a scheduler deadlock; every kernel scheduler honours
  `GetPathFindingThreadsPerBatch`.
- Any pair with a NULL endpoint used to disable source grouping (288 s single-source query); fixed in `5455ec18`.
- DuckDB v2: `ListVector::GetEntry` is deprecated (use `GetChildMutable`); uncommitted rows have
  transaction-local row ids (~2^55), so tests compare against `rowid` rather than literals.
- zsh: `--include=` globs and `rm -f glob` abort the command; `cd` in one parallel shell call changes the cwd for
  the others, use absolute paths.

## 21 Oct EC2 session (m7a.8xlarge, eu-west-1)

- Sweep done for SF1, 3, 10, 30, 100, 300 (plan 10b plus larger factors); summary tables in `~/sweep/collected*.md`
  on the instance, raw CSVs in `data/ldbc-pathfinding/results/sf*/`. Progress log entry in the plan has the findings.
- Uncommitted working-tree changes (review before committing): pair-column fix in `logical_path_finding_operator.cpp`,
  `physical_path_finding_operator.{cpp,hpp}`, `path_finding_optimizer_rule.cpp` (legacy route now passes the
  function expression); new test `test/sql/path_finding/path_finding_pair_columns.test`; harness
  `index_query_sql` key-pair materialization, `index_operator_warm` mode, verifier skip of `index_build` rows.
  Full suite 83/83 after the changes.
- Index query times in the sf1-sf300 sweep CSVs are cold-process numbers from before the parallel load: the first
  query paid the serial index deserialization (`CSRIndex::LoadFromStorage`, ~42M edges/s, 1.15 s at SF300). The
  load is parallel since commit "Load a persisted CSR index in parallel" (90 ms at SF300, 32 threads). Use
  `index_operator_warm` for resident-index numbers; the plan's progress log has the re-measured SF300 values.
- EBS on this instance is 250 MB/s (measured). The generator repo is `github.com/Dtenwolde/ldbc-data-gen`; `prepare`
  needs a temporary `third_party -> ~/git/ldbc-data-gen/third_party` symlink in the repo root (dictionary paths
  are cwd-relative); remove it afterwards. AWS CLI credentials on the box belong to another account.
- zsh pitfall: `pkill -f '<pattern>'` from a tool shell matches the shell itself; `set -- $var` does not split words.
- W16 harness: `update-run`, `update-concurrent` (driver `scripts/tools/update_concurrent_driver.cpp`, compiled by
  the harness against build/release/src/libduckdb.so), `update-kuzu` (pip `kuzu` 0.11.3 installed for the user).
  Collectors `~/sweep/collect_update.py`, `~/sweep/collect_concurrent.py`. Since 22 Oct the kernels run as event
  chains (`phased_kernel_event.hpp`), there are no barriers across tasks and the driver needs no worker cap.
- Graph database baselines on the EC2 box: containers `pf-memgraph` (Bolt 7687, `data/ldbc-pathfinding/graphdb-export`
  mounted at /import), `pf-falkordb` (6379) and `pf-neo4j` (Enterprise evaluation licence, Bolt 7688, database
  `pathfinding`), started with `docker run` (see the plan's 22 Oct entry); Python
  clients `neo4j` and `falkordb` installed for the user; `~/sweep/collect_systems.py <sf>` prints the cross-system
  table. Kuzu point-to-point and FalkorDB queries are one search per pair, so their point-to-point runs use 20 batches.
- Kernel pattern: a `PhasedKernel` has Start / RunPhase(worker) / AdvancePhase / Finished; serial work goes into
  Start and AdvancePhase (they run on one thread between phases), parallel work into RunPhase; never block inside a
  task.
- The CSR index now stamps change groups and caches the last snapshot (commits f585926a, 3c5b8b78); the design doc's
  "dictionary is a prefix of every later version" is not what the code does (rebuilt in key order at a fold).
- W27/W17 harness (22 Oct): `sweep-pairs --scale-factor <sf>` runs random pairs 1..65536 and k sources x all persons
  over `--systems duckpgq_build duckpgq_index kuzu memgraph falkordb neo4j neo4j_gds` with `--timeout-s` (default 600,
  server-side cancellation, exceeded points recorded), `sweep-pairs-table <csv...>` prints the tables;
  `ldbc-queries --scale-factor <sf>` runs IC13, BI19, BI20 (hop-count variants over the symmetric closure of knows,
  tables `person_knows_person_sym`, `person_city`, `person_work_at`, `ldbc_params_*` added to the benchmark database
  by `ensure_ldbc_attributes`), `ldbc-queries-table` prints it. `pf-neo4j` was recreated with
  `NEO4J_PLUGINS='["graph-data-science"]'` (GDS 2.13, unlicensed: 4 threads, no parallel runtime). Results CSVs
  `sweep_pairs_*.csv` and `ldbc_queries_*.csv` under `data/ldbc-pathfinding/results/sf<sf>/`; logs in `~/sweep/`.
  Neo4j queries bind destination/source lists with UNWIND, never `IN $list` (parallel runtime fan-out, see plan);
  Memgraph multi-source patterns need a `WITH a` barrier before the `*BFS` expansion.
- Reverse orientation on symmetric edge sets (`7fdfe65e`): `gstate.symmetric_edges` is set from an undirected index
  snapshot or a cache key ending in `|undirected` (MATCH sets it for `-[e]-` patterns; the harness passes
  `graph:sweep|undirected` for the LDBC build route); the forward partitions are then also the reverse partitions.
  Test `path_finding_reverse_orientation.test`. The 23 Oct chain `sf300_chain` reruns the SF300 sweep (first run
  died on the 1.2G-pair point) and the DuckPGQ LDBC queries at SF100/SF300; `~/sweep/sf300_chain_done.txt` marks the end.
- Reach mode (W29, 23 Oct): `reachoperator(src, src, struct_pack(src, dst), V, E, key[, max])` or the index form
  `reachoperator(src, src, 0, 0, 'csr-index|...')` returns a `BIGINT[]` [dst, len] per reached vertex; mode string
  `reach`, `PathFindingOperatorMode::REACH`, output in `GetReachData` (parallel source, slices of the distance arrays,
  `source_group_rows` per group, `reach_ungrouped_rows` for sources outside the graph). Test
  `path_finding_reach.test`. The harness's multi-source tables hold the sources only and `DuckPGQSystem.script`
  switches to the reach form for them. A WHERE directly on the source table next to the edge input is not planned
  (filter between scan and cross product); wrap the sources in a subquery, as the harness does.
- Reverse side of the CSR index (W28 steps 1-3, 26 Oct): option `reverse = true` (`CSRIndex::reverse`), epoch
  `csr->reverse_partitions` (no row ids), `reverse_tombstone_positions`, snapshot `reverse_partitions /
  reverse_delta_partitions / reverse_suppressed`, storage format version 2 (version 1 still loads), operator
  `LocalCSRState::reverse_partition_delta_csrs / reverse_partition_suppressed` and the `SearchDeltaCSRs` /
  `SearchSuppressed` helpers. Test `csr_index_reverse.test`. The fold keeps the change log while the previous epoch
  is retained (two epochs), so post-fold stats checks use `LIKE 'epochs=2;%'`.
- MATCH on the reach mode (`9272fcb4`): decided in `MatchBindReplace` after `ProcessPathList`; `PathFindingOperatorResult`
  carries `destination_binding/table/key_column/join_column` and `reach`; the destination alias is skipped in the
  FROM, its predicates and projections move to the outer `reach_sql` query with a join on `result[1]`. Options
  `experimental_path_finding_operator_reach` (default true) and `..._reach_filtered` (default false). Tests
  `path_finding_reach_match.test`; `path_finding_operator_match.test` and `..._multi_batch.test` disable the reach.
- Paths on the build route (W7 step 2, `48a7563a`): `shortestpathoperator(src, dst, struct_pack(src, dst, id), V, E,
  key)`; the `id` field is the edge row id and is required in path mode.
- Many source groups (`8aa7aa32`): groups scheduled together divide the threads (`concurrent_groups` on the
  source-grouped state; a single-worker group runs all levels in one task), destination sets are bitmaps above
  1/64 of the vertices (`SourceGroupTargets`), Finalize materialises the pair chunks with a parallel scan
  (`ParallelFor` in `utils/parallel_for.hpp`). The `pair_analysis_precompute` phase no longer exists. Diagnosis
  recipe that found it: run the query with `SET threads=1` and `=32` (serial if equal), then `perf record -g` on the
  CLI (`perf` is installed on the box, sudo works) and the operator's phase timings
  (`SET experimental_path_finding_operator_benchmark=true; SET ..._benchmark_prefix='/tmp/x'` ->
  `/tmp/x_phase_timing.csv`).
- Queued runs (22 Oct evening, tmux session `w27_chain`, script `~/sweep/run_w27_chain.sh`): DuckPGQ SF10 rerun on
  the current build, GDS SF10, `ldbc-queries` SF10, then `sweep-pairs` and `ldbc-queries` at SF100 and SF300; logs
  `~/sweep/sweep_pairs_sf*.out`, `~/sweep/ldbc_queries_sf*.out`, `~/sweep/chain_done.txt` when finished. Print
  tables with `sweep-pairs-table <csvs>` / `ldbc-queries-table <csvs>` (a system's rows come from the last file
  that has it, so pass the SF10 full-sweep CSV first and the DuckPGQ/GDS rerun CSVs after it).

## Reproduction package (26 Oct)

`repro/` is the archive and the one-script reproduction: `setup.sh`, `prepare.sh`, `run_all.sh`, `collect.sh`,
`environment.sh`, `results/` (CSVs behind every figure). After any new sweep run `./repro/collect.sh` so the
archive and the figures move together, then commit `repro/results` and `paper/`. `./repro/collect.sh archive`
re-renders from the archive alone. Decisions: Neo4j Enterprise evaluation-licence numbers are published (W18);
the archive goes to GitHub, this branch first, then a separate repository (W21).

## Microbenchmarks (26 Oct)

`docs/MICROBENCHMARK_SWITCHES.md` is the inventory and the sweep design. Run `python3 scripts/pathfinding_benchmark.py
microbench --scale-factor SF --threads 32 --repeats 3 --timeout-s 600` and print with `microbench-table`. Configurations
live in `MICROBENCH_CONFIGS` / `MICROBENCH_GROUPS` in the harness; adding one is a dict entry. Results chain on the box:
`~/sweep/run_microbench_chain.sh` (marker `~/sweep/microbench_chain_done.txt`); the first SF10 run wrote `microbenchmark_sf10_threads32_*.csv` under the old name.

## State on 27 Oct (afternoon)

- MS-BFS batch scheduling is adaptive (`grouped_iterative_length_event.*`, `phased_kernel_event.*`): worker groups,
  pooled working sets, window grown on edges swept per second; two decisions from the working-set size (own threads
  vs pooled level tasks: one set <= 32 MB; start wide vs ramp from 16: total <= 512 MB). Per-batch events remain as
  `grouped_batches=false`. Verified equal or better at SF10 / SF100 / SF300 (`microbench_final_sf*.out`).
- Explore phase: frontier summary bitmap (built in the check phase), per-source bitmap test on dense levels when one
  working set > 32 MB (`frontier_bitmap_filter` -1/0/1), row-lookup expansion on sparse levels
  (`sparse_level_divisor` 16), prefetch off. Strategy string records `regime`, `tasks`, `bitmap_filter`,
  `sparse_level_divisor`, `window_trace`.
- Microbenchmark groups: `kernel`, `index`, `reach`, `threads` (= `all`), plus `scheduling`, `memory`, `partitions`
  (run separately). Results in `data/ldbc-pathfinding/results/sf*/microbench_*.csv`; tables with `microbench-table`.
- Headline (index, 65536 random pairs): SF10 0.68 s, SF100 5.20 s, SF300 12.3 s. Every DuckPGQ number in
  `repro/results` is stale until the frozen rerun; the baselines are not.
- Running / next: W32 partition geometry (`microbench --configs partitions`), then freeze (`eval-freeze-rc1`) and the
  frozen rerun via `repro/run_all.sh`; the paper's operator section (merged 27 Oct) predates the adaptive scheduler.
