# Path-finding operator + MVCC-consistent CSR index: paper plan

Status date: 19 October 2026, end of day 1. Branch: `pathfindingoperator-forward-port` (merged with main at
`ea98090f`; head `55429a47`; 91/91 SQL test files pass locally). CI last ran on `7c5db25a` (Linux and macOS build,
Windows MSVC fails on `__builtin_ctz`); nothing after that has been built on Linux yet.

## 1. Target and thesis

**Target venue: PVLDB Vol. 20 / VLDB 2027, 1 February 2027 round (decided 20 Oct; abstract 25 January).** The SIGMOD notes below stay as the fallback if the February round slips.
- PVLDB Vol. 20 / VLDB 2027: rolling deadlines, abstract on the 25th of the month before, paper on the 1st of
  every month until **1 March 2027**; notification by the 15th of the following month; one revision round with
  up to 2.5 months; 12 pages + references; single-blind (names on the paper); code/data in a public archival
  repository required. Realistic rounds: **1 January**, **1 February**, **1 March 2027**. Recommended: aim for
  1 February, treat 1 January as the internal "complete draft with final numbers" milestone, keep 1 March as
  fallback.
- SIGMOD 2028 research track, Round 1 (assumed abstract ~10 January, paper ~17 January 2027; confirm when the
  CFP appears). SIGMOD 2027 Round 4 closed on 17 October 2026. Double-blind, no revision round.

**Thesis.** A loadable extension can give a relational engine graph-native path finding, including an
adjacency (CSR) index that stays transactionally consistent, without forking the engine.

**Contributions.**
1. A batched multi-source BFS path-finding operator that builds a partitioned CSR inside the query
   pipeline from a relational pair input, with statistics-driven orientation and source grouping.
2. An MVCC-consistent CSR index implemented through DuckDB's extension index API: immutable epochs plus an
   append-only delta and tombstones that carry only row ids; a reader resolves them once per query through the
   table's own row versions (`CanFetch`), so it traverses exactly the edges visible at its snapshot. No commit
   timestamps are stored; fold compaction uses epoch generations stamped by the live folding transaction.
3. A cost model for choosing between build-per-query, index traversal, and relational joins, and the
   engine hooks a DBMS must expose for (1) and (2) to live in an extension.
4. Evaluation on LDBC SNB, Graphalytics and graph500 against Kuzu and Neo4j, read-only and under updates,
   including end-to-end SQL/PGQ queries.

## 2. Current state (19 Oct, end of day)

Operator (opt-in via `experimental_path_finding_operator`): sink/source `PhysicalPathFinding`, radix-partitioned
CSR built per query with the sort-and-emit scheme (`csr_build.hpp`, ~0.03-0.06 s for 8M edges), 512-lane MS-BFS
with buffer-managed working sets reused per worker group (grouped scheduling is the default), forward/reverse
orientation, source grouping, pair dedupe. Returns path **lengths** only. Decisions per query are exposed by
`duckpgq_path_finding_strategy()`. Pipeline completion gates on every batch; the worker barrier is abortable.

Planner: the optimizer rule replaces every `iterativelengthoperator` marker in the plan (comma joins, subqueries,
set operations); reverse search falls back to forward when no reverse CSR exists; edges with NULL keys are
tolerated; `path_length(p)` may appear more than once.

Index (`src/core/index/`): `CREATE INDEX ... USING CSR (src, dst) [WITH (undirected = true)]`. Dictionary of
vertex keys (bitmap/identity when compact, sorted otherwise); immutable epochs in the operator's partitioned
layout with the edge row id per position; append-only delta (keys + row id) and tombstones (row ids); per-reader
snapshot = epoch + overlay decided by `CanFetch`; companion delta partitions and suppression bitmaps in the kernel;
private snapshots for writing transactions; fold with two retained epochs and generation-stamped compaction
(`duckpgq_csr_index_fold`, `duckpgq_csr_index_fold_ratio`, `duckpgq_csr_index_stats`); checkpoint + WAL
persistence and reload on bind. MATCH uses a matching index automatically (pairs become keys). Build: 0.10 s for
8M edges; bind from disk 0.03 s; overlay cost unmeasurable up to 50% delta.

Removed 20 Oct (W14): the prototype `__duckpgq_csr_*` tables, triggers, eager build and the per-connection
partitioned CSR cache. Still in the tree: ~7K lines of benchmark and plotting `scripts/` (W21 decides what ships).

Benchmark harness `scripts/pathfinding_benchmark.py`: systems duckpgq/kuzu/neo4j; LDBC SNB SF1-SF100, Graphalytics
S/M/L, graph500-22..26; patterns point_to_point, sssp, all_pairs, graphalytics_bfs. Results so far live on EC2
only. Cold-start comparison scripts for Kuzu/Ladybug in `docs/experiments/cold_start/`. No paper draft in repo.

Key measurements so far are in `docs/CSR_INDEX_DESIGN.md` (sections 5.1, 5.5, 5.6, 8).

## 3. Gaps

### A. Measurement validity and robustness (block any final number)
| # | Gap | Evidence | Fix |
|---|-----|----------|-----|
| A1 (fixed W1) | Per-batch `event.InsertEvent` detaches all but the first batch from pipeline completion; a query can finish before later batches end, under-reporting time and risking torn results | `physical_path_finding_operator.cpp:791,842`; `duckdb/src/parallel/event.cpp:70` moves `parents` | One completion event; `AddDependency` on each batch event |
| A2 (fixed W2) | Blocking `Barrier` hangs on task error, interrupt, `max_execution_time`, `external_threads>1`, concurrent path queries | `duckpgq_barrier.cpp:55-77`, all task files | Abortable barrier with error/interrupt checks, or event-per-phase scheduling |
| A3 (fixed W3, regular kernel) | BFS state for every batch allocated up front, unaccounted (~200 B/vertex/batch; 600 B bidirectional) | `SchedulePathFindingBatches`, `bfs_state.cpp:42-56` | Done for regular MS-BFS: `BFSWorkingSet` from the buffer manager, one per worker group (grouped execution is now the default). Remaining: source-grouped states, CSR arrays |
| A4 (fixed W4) | Strategy choice must be recorded per run (HLL estimates are deterministic for the same pair multiset, so the choice itself is reproducible) | `PathFindingPairStats` | Log orientation/grouping/dedupe decisions per query (profiler or benchmark CSV) |
| A5 (fixed for the default kernel) | Benchmark CSV timing hooks sample clocks in hot paths even when disabled | `duckpgq_barrier.cpp:58-75`, push/pull, bidirectional | Barrier timing removed (W2); default kernel gated. Push/pull and bidirectional variants still sample unconditionally (experimental, not used in sweeps) |

### B. Operator functionality
| # | Gap | Fix |
|---|-----|-----|
| B1 (W7 step 1 done) | No path reconstruction (`shortestpathoperator` throws; task file commented out) | Parent tracking in the per-source kernel; index route done, build route open |
| B2 (fixed W5/W6) | Optimizer rule misses markers (comma join, nested subquery, 3x UNION) -> runtime NotImplemented | Rewrite all markers, drop early `return false`, pre-optimize only |
| B3 (fixed W5/W6) | Reverse orientation chosen when no reverse CSR exists -> NotImplemented at default settings | Fall back to forward; update test asserting the error |
| B4 (fixed W5/W6) | Edge rows with NULL keys / dangling FKs fail every operator query (count mismatch) | Count edges through the same join that produces endpoints |
| B5 (alias fixed; left edges open) | `path_length(p)` twice -> NULL for first; `<-[e]-` unsupported | REPLACE every alias; add left edges (swap src/dst) |
| B6 (fixed W5/W6) | Pair ids outside `[0, V)` index BFS arrays unchecked (OOB write) | Bounds check -> NULL |
| B7 (fixed W14: cache removed) | Stale in-memory cache after count-preserving UPDATE/DELETE+INSERT | Superseded by the index (C) |

### C. Index (replace prototype persistence)
Current prototype must not be the paper's index: four tables created without opt-in, triggers hard-code
catalog name and `duckpgq_mark_csr_invalid` (DML breaks after file rename/ATTACH alias or without the
extension), RENAME blocked, DROP PROPERTY GRAPH impossible once a trigger is missing, "Conflict on
update" for concurrent writers, stale after DROP TABLE+CREATE and EXPORT/IMPORT, eager build runs outside
the caller's transaction.

Needed: a `CREATE INDEX ... USING CSR` extension index type (`IndexType` + `BoundIndex`), built on
DuckDB's commit-time `Append`/`Delete`, checkpoint `SerializeToDisk`, WAL replay via table replay, and the
2.0 checkpoint delta hooks (`SupportsDeltaIndexes`, `MergeCheckpointDelta`). DuckDB indexes are not
snapshot-consistent (ART relies on `DataTable::Fetch` version checks per row id); the CSR must version
itself:
- C1 (done) Design: epoch CSR (immutable, packed) + append-only delta (keys + row id) + tombstones (row ids);
  reader picks the newest retained epoch with bound <= its own and resolves delta/tombstones through `CanFetch`;
  fold builds a new epoch and compacts by epoch generation. Dictionary keyed on the edge table (no dependence on
  vertex rowids). See `docs/CSR_INDEX_DESIGN.md`.
- C2 (done, needs review) Correctness argument in the design doc section 6, plus the generation-stamping
  argument in 5.5 (no visibility evaluated at past bounds).
- C3 (partly) Operator reads the index snapshot whenever a matching index exists; the cost-based choice (C4)
  is open.
- C4 Cost model: pair count, distinct src/dst, V, E, delta size, update rate -> build / index / joins.
- C5 Remove prototype: `InitializePartitionedCSRPersistence`, registry tables, triggers,
  `duckpgq_mark_csr_invalid`, eager build; keep `csr_*.test` ideas as index tests.

### D. Evaluation
| # | Gap | Action |
|---|-----|--------|
| D1 | Neo4j Community caps GDS concurrency at 4 | Enterprise trial, or state the cap in the first figure and report 4-thread parity runs |
| D2 | Only read-only workloads | LDBC SNB insert streams (ldbc-data-gen) as update workload; mixed read/write sweeps; delta-size vs traversal cost; vacuum cost |
| D3 | Kernels only | End-to-end SQL/PGQ: LDBC Interactive 13, 14; BI 19, 20 (filters + joins + path op) |
| D4 | Baselines beatable by construction (recursive CTE strawman) | Keep as context; add rebuild-per-query vs index comparison; optional Teseo/Sortledton or GraphBLAS upper bound |
| D5 | Microbenchmarks are one-off runs with dates in filenames | Systematic sweeps: orientation ratio, source grouping, dedupe, grouped batches, CSR build variants, threads 1..32 |
| D6 | Variance/repeats | 3+ repeats, medians with error bars, cold vs warm separated, chosen strategy logged (A4) |
| D7 | Results only on EC2 | Archive raw CSVs in a results repo; tag the commit per sweep; pin DuckDB SHA, Kuzu, Neo4j 2026.06.0, instance type |

### E. Writing and artifact
- No draft. Need outline (section 8), related work pass (Then et al. MS-BFS; Grail; GRainDB; Umbra graph
  work; Kuzu; Neo4j GDS; LiveGraph/Teseo/Sortledton/Aspen for versioned graphs; DuckPGQ CIDR 2023;
  Ren MSc thesis as prototype origin).
- Artifact repo with scripts, result CSVs, and a run README (VLDB requires a public archival repository;
  SIGMOD would need it anonymised).

## 4. Work items

Effort in person-days (pd). P1 = must for Round 1, P2 = strongly desired, P3 = if time.

| ID | Item | Area | Effort | Prio | Depends on | Status |
|----|------|------|--------|------|-----------|--------|
| W1 | Fix event dependencies (A1) | op | 1 | P1 | — | done 19 Oct, `1729367a` |
| W2 | Abortable barrier + interrupt/error checks (A2) | op | 3 | P1 | — | done 19 Oct, `28a3ee99`; co-scheduling assumption removed 22 Oct: kernels are event chains, no barriers (`864991a1`, `33fd5588`) |
| W3 | Per-running-batch BFS allocation, allocator accounting (A3) | op | 3 | P1 | W1 | done 19 Oct, `e08b95f5`; regular MS-BFS only: source-grouped mode still allocates ~11 B/vertex per distinct source up front, bidirectional/push-pull keep their own arrays, CSR memory itself is still unaccounted |
| W4 | Deterministic strategy choice + logging (A4), gate timing hooks (A5) | op | 1 | P1 | — | done 19 Oct, `879ae6c0`: `duckpgq_path_finding_strategy()` returns mode/pairs/distinct_src/distinct_dst/orientation/scheduling/groups/csr of the last operator; default kernel samples clocks only when benchmark is on (push-pull/bidirectional variants still sample unconditionally) |
| W5 | Optimizer rule: all markers, forward fallback, alias fix, NULL-key edge count (B2–B5) | planner | 3 | P1 | — | done 19 Oct, `e7c36dad`; left edges `<-[e]-` still unsupported (pre-existing, both paths) |
| W6 | Bounds checks on pair ids (B6) | op | 0.5 | P1 | — | done 19 Oct, `578baf3c` |
| W7 | Path reconstruction (B1). Step 1 done 20 Oct (`af26e5ad`): element_id/vertices/edges via the operator over a CSR index; parent vertex + parent edge per vertex in the source-grouped kernel, paths as [key, edge row id, key, ...], delta edges carry their row ids, MATCH derives path_length/vertices/edges from one shortestpathoperator call. 8M edges: 20 sources x 200 destinations with paths 0.24 s vs 0.22 s lengths; 158k paths from one source 0.43 s. Step 2 open: paths on the build-per-query route (endpoint struct with edge row id, row ids through the radix build); MS-BFS kernel stays length-only | op | 5 | P1 | W1–W3 | done: step 2 on 22 Oct (`48a7563a`): MATCH passes the edge row id as the struct field `id`, the radix build keeps row ids per edge position, paths on the build route report vertices as row ids (keys with an index). SF10, 16 threads: 1 source x 1024 destinations 30 ms paths vs 23 ms lengths; 16 sources x all persons 0.34 vs 0.25 s; 1024 random pairs 1.34 s vs 0.075 s (one search per source in path mode) |
| W8 | Index design doc + correctness argument (C1, C2) | index | 4 | P1 | — | draft 19 Oct, `docs/CSR_INDEX_DESIGN.md`; awaiting review (decisions marked **D**) |
| W9 | `IndexType` skeleton: create/bulk build/serialize/load, read-only snapshot (C1) | index | 6 | P1 | W8 | done 19 Oct (`9af62e18`, `16aca50e`, build `337c2ff5`/`6d38d2c9`/`310fb731`; persistence landed in W11) |
| W10 | Commit-time append delta + tombstones, epoch selection by start time | index | 6 | P1 | W9 | done 19 Oct, see git log (overlay via `CanFetch`; companion delta partitions; lazy tombstone positions; readers older than the epoch still error instead of falling back) |
| W11 | Vacuum/epoch fold; checkpoint delta integration; crash-recovery test | index | 5 | P1 | W10 | done 19 Oct: fold (generation-stamped compaction, 2 retained epochs, `duckpgq_csr_index_fold/stats`, ratio setting) and persistence (checkpoint, WAL, restart tests). Not done: checkpoint delta hooks (writes during serialization go to the main index), incremental fold, row-id compression |
| W12 | Operator reads index snapshot; optimizer chooses index vs build (C3) | op+index | 4 | P1 | W9, W5 | partly done 19 Oct: index used whenever a matching bound index exists (pairs become keys, dense-id translation per batch); cost-based choice open |
| W13 | Cost model + calibration (C4) | planner | 3 | P2 | W12, sweeps | open |
| W14 | Remove prototype persistence/eager/triggers (C5) and the per-connection partitioned CSR cache. Done 20 Oct (`a45795db`): -529/+198 lines, 10 prototype tests, 3 benchmark scripts and `docs/CSR_PERSISTENCE.md` removed; without an index every query builds its CSR (`partitioned_csr_build` phase), the cache-only marker form is index-only | index | 2 | P1 | W12 | done |
| W15 | Index tests (snapshot isolation, concurrent writers, rollback, checkpoint, restart, drop) | index | 3 | P1 | W10–W11 | partly done: `csr_index_snapshot.test`, `csr_index_change_groups.test` (21 Oct: commit groups, restart, fold with id shift), `path_finding_concurrent_queries.test_slow` (22 Oct); concurrent writer + readers exercised by the update-concurrent driver (no mismatches at SF10); restart under load and drop while in use open |
| W16 | Update workload: LDBC insert streams into harness; mixed sweeps (D2) | eval | 4 | P1 | W12 | done 21-22 Oct (`update-run`, `update-concurrent`, `update-kuzu`); results SF10/SF100 in the progress log; mixed-sweep matrix for the paper still to run on the frozen commit |
| W17 | End-to-end LDBC IC13/14, BI19/20 in harness for all systems (D3); BI 19/20 double as the predicate-driven multi-source queries of W27 | eval | 4 | P1 | W5 | harness done 22 Oct (`ldbc-queries`, `ldbc-queries-table`): IC13, BI19, BI20 on DuckPGQ build/index, Kuzu, Memgraph, FalkorDB, Neo4j and Neo4j GDS over the symmetric closure of knows (LDBC knows is undirected, the generator stores one direction); BI19/BI20 as hop-count variants (the operator is unweighted; the LDBC versions weight edges by interactions or study years), IC14 omitted (all shortest paths with interaction weights; the operator returns one path). Five deterministic parameter sets per query; SF1 all systems agree. SF10 run pending |
| W18 | Neo4j Enterprise trial or parity protocol (D1) | eval | 1 | P1 | — | decided 26 Oct: publish the Neo4j 5.26 Enterprise numbers measured under the evaluation licence (parallel runtime; GDS 2.13 unlicensed at 4 threads), stated in the setup paragraph; no trial request |
| W19 | Systematic ablation + scaling sweeps on frozen commit (D5, D6). Decided 20 Oct: the first sweep is DuckDB-only. Harness modes added 20 Oct: `index_build`, `index_operator`, composites `index` and `index_comparison` (build-per-query operator vs index) in `scripts/pathfinding_benchmark.py`; smoke-tested on the SNB 0.003 sample | eval | 4 (+machine time) | P1 | W1–W6 | first DuckDB-only sweep done 21 Oct (SF1-SF300, 5 repeats; `~/sweep/collected*.md`); 26 Oct: switch inventory and harness `ablation` subcommand (`docs/ABLATION_SWITCHES.md`), four switches added (snapshot cache, target early termination, sparse frontier divisor, few-sources limit), lane width a build-time define; smoke at SF10 passed, full SF10 and SF100 runs queued; rerun on the frozen commit as `run_all.sh ablation`. Needed for the paper's 'Evolution of the operator' table (26 Oct): the CIDR 2023 UDF route (harness mode `scalar`, the `iterativelength` UDF over a UDF-built CSR) on the same three queries (one pair, 1024 random pairs, one source to all) at SF10 and SF100, 32 threads, next to the switch runs |
| W19 | Systematic microbenchmark + scaling sweeps on frozen commit (D5, D6). Decided 20 Oct: the first sweep is DuckDB-only. Harness modes added 20 Oct: `index_build`, `index_operator`, composites `index` and `index_comparison` (build-per-query operator vs index) in `scripts/pathfinding_benchmark.py`; smoke-tested on the SNB 0.003 sample | eval | 4 (+machine time) | P1 | W1–W6 | first DuckDB-only sweep done 21 Oct (SF1-SF300, 5 repeats; `~/sweep/collected*.md`); 26 Oct: switch inventory and harness `microbench` subcommand (`docs/MICROBENCHMARK_SWITCHES.md`), four switches added (snapshot cache, target early termination, sparse frontier divisor, few-sources limit), lane width a build-time define; smoke at SF10 passed, full SF10 and SF100 runs queued; rerun on the frozen commit as `run_all.sh microbenchmark` |
| W20 | Full read-only system comparison rerun on frozen commit | eval | 2 (+machine time) | P1 | W19 | open |
| W21 | Results archive, version pins, anonymous artifact (D7, E) | eval | 2 | P1 | ongoing | decided 26 Oct: GitHub. First on this branch as `repro/` (setup, prepare, run_all, collect, environment record, pinned versions, the result CSVs behind every figure and table); later a separate reproduction repository with this checkout as a submodule and `setup.sh` as the one entry point. Open: FalkorDB image pinned by tag `latest` (pin by digest for the frozen run), anonymisation for review |
| W22 | Related work + outline + intro draft. Started 22 Oct: `paper/` (PVLDB acmart template, `main.tex`, one file per section with the outline of section 8 as TODO notes, `references.bib` with 35 entries, VERIFY marks on uncertain ones), related work drafted (`paper/sections/related_work.tex`, five paragraphs: graph queries in RDBMSs, path finding, native graph systems, dynamic/versioned graph structures, indexes under MVCC). Intro drafted 22 Oct (`7a706d27`), framed on the CIDR 2023 open problem (tuple-driven parallelism, UDF-built global CSR); style guide `paper/STYLE.md` after Boncz's first-author papers; 'kernel' -> 'traversal'. Open: citation check (VERIFY marks), Figure 1, abstract | writing | 3 | P1 | — | in progress |
| W23 | Full draft, figures, internal review, polish | writing | 10 | P1 | W16–W20 | open |
| W24 | Teseo/Sortledton or GraphBLAS baseline (D4) | eval | 4 | P3 | W16 | open |
| W25 | WCC table function reads the index snapshot (generality argument, build vs index for a whole-graph algorithm) | op+index | 2 | P3 | W12 | open |
| W33 | Lane-width microbenchmark (decided 27 Oct). MS-BFS lanes per wave decide the bytes per vertex of the working set (3 x LANE_LIMIT / 8: 192 B at 512, 96 B at 256, 48 B at 128) against the number of waves per batch. Builds with `-DDUCKPGQ_LANE_LIMIT=128|256|512|1024` kept as separate extension files (`data/ldbc-pathfinding/builds/`), the harness picks one through `DUCKPGQ_EXTENSION`; measured with `microbench` at SF10 and SF100 on random pairs 16384 / 65536 and sources x all 1024, both routes, under the default, grouped-always and per-batch schedules. One row of the microbenchmark table and the memory-boundness argument of the paper (W31). Repro: `run_all.sh lanes` | eval | 1 | P1 | W31 | first probe (256 lanes) running 27 Oct |
| W35 | Where the index matters (author's list, 29 Oct). Throughput of single-pair queries from 1 to 32 clients, index against build-per-query (`throughput` subcommand, driver mode `--mode throughput`, 20 s per point, SF10/100/300); concurrent readers at SF100 and SF300 (1, 4, 16 readers); Section 6 opens with the one-thread result. Fixes found by profiling: parallel and lazy working-set zeroing, tombstones resolved by edge keys, undirected delete suppression | eval, index | 1 | P1 | W16 | in the rc6 matrix; figures `throughput`, `concurrent` |
| W34 | Locality-aware dense ids at CREATE INDEX (retry, decided 27 Oct). Tried before on the old CSR without gain because LDBC knows is close to random at the scale of a cache line; retry on the radix-partitioned CSR, where a partition's destination slice of `next` is already local and the scattered reads are `visit[src]`. Variant: assign dense ids in BFS order from the highest-degree vertex (or degree-sorted), behind an index option (`WITH (ordering = 'bfs')`), dictionary translates both ways so nothing else changes. Measure with perf (cache misses per edge) and the microbench at SF100; expect a modest effect, report it either way as the honest limit of reordering on small-world graphs | index | 1-2 | P2 | W33 | open |
| W31 | Batch scheduling rule (found by the 26 Oct microbenchmarks). Grouped scheduling derives one group of 32 workers, so 65536 pairs run their 32 batches one after another; grouping off or 4 workers per batch is 0.41-0.56x the time at SF10. Study: a two-dimensional sweep groups x workers per batch over the divisors of 32 at 65536 pairs on SF10, SF100, SF300, both routes; derive workers per batch = threads / batches (floored at 1) bounded by a working-set budget (192 B per vertex per group); validate the rule lands at the optimum per scale factor. Paper: a microbenchmark figure (time vs group count, one line per SF, memory column), framed as intra- vs inter-batch parallelism and tied to the baselines needing 32 client threads. Changes the headline numbers, so before the freeze | op | 1 | P1 | W19 | open, next after the SF10 microbenchmark |
| W30 | Graph structure microbenchmark (decided 26 Oct). LDBC knows is the operator's friendliest graph (small world, diameter about 6, mild skew). One figure on LDBC Graphalytics datasets that differ where it matters: graph500-24 (hubs; 8.9M V, 260M E), cit-Patents (long level tail; 3.8M V, 16.5M E), datagen-8_4-fb (same family, other density; 3.8M V, 269M E), optionally dota-league or wiki-Talk. Shapes 65536 random pairs and 1024 sources x all, both DuckPGQ routes plus the fastest-loading baseline (Kuzu or Ladybug), reporting levels reached. Harness has download, materialisation and BFS reference validation (`prepare --graphalytics-datasets`). About a day of machine time; measured on the frozen build. Writing: one paragraph naming where the operator degrades and why | eval | 2 | P1 | W19, W31 | in the rc6 matrix (29 Oct): cit-Patents on every system, graph500-24 and datagen-8_4-fb on DuckPGQ and Kuzu, scheduling microbenchmark on graph500-24; smoke findings in the 29 Oct entries |
| W29 | Reach mode: sources x all vertices without a pair list (done 23 Oct). `reachoperator(src, src, ...)` has the argument forms of `iterativelengthoperator` and yields one row per vertex each source reaches, a list [dst, len] (a struct crashed DuckDB's statistics pass after the unused-columns optimizer pulled the field extraction up). One group per distinct source, no destination sets, the search runs to exhaustion or the pattern's bound; the parallel source emits slices of each group's distance array (65536 vertices per slice), one output chunk per input row with constant pass-through columns, and frees a group's arrays once emitted; a source absent from the graph reaches only itself. SF10, 32 threads, 1024 sources x all persons: build 0.43 s / index 0.22 s (pair mode 4.4 / 4.1 s, Kuzu 4.0 s); 1 source: index 3 ms (Kuzu 7 ms). Open: MATCH planning of an unbound or weakly bound destination onto reach mode (today the harness calls the marker); the cost rule between pair and reach mode belongs to W12 | op | 3 | P1 | W27 | done 26 Oct: MATCH plans an unbound destination onto reach mode (`9272fcb4`); a filtered destination keeps the pair mode by default (option `experimental_path_finding_operator_reach_filtered`), see the 26 Oct entry; cost rule proper is W12 |
| W28 | Reverse direction in the CSR index for directed graphs (decided to look into 23 Oct). Nx1 shapes and the bidirectional kernel need reverse partitions; today only the legacy CSR route builds them, symmetric edge sets reuse the forward partitions (`7fdfe65e`). Plan: one index holding both directions (`WITH (reverse = true)` or similar, one maintenance path, lengths-only reverse side ~5 B/edge), the operator resolving both from one cache key and choosing forward / reverse / bidirectional at runtime from the pair statistics; then bidirectional search over the index for the random-pair shape at scale (SF300: 0.8-1 ms per pair forward MS-BFS vs Neo4j's bidirectional shortestPath). Effort 1-2 days each step | op/index | 3 | P1 | W27 | steps 1-3 done 26 Oct (`WITH (reverse = true)`, storage format 2, reverse companions and suppression, reverse orientation over the index: SF10 directed 2000 sources x 1 destination 5 ms reverse vs 91 ms forward); step 4 (bidirectional over the index) open |
| W27 | Multi-source and all-pairs comparison (decided 22 Oct). Present query shapes by cardinality (1x1 point lookup, 1xN sssp, Nx1, NxM from a predicate on the source table or a join) rather than by pattern name; sweep the pair count 1, 16, 128, 1024, 2048, 16384, 65536 and all pairs on every system with the same pair tables, at SF10, SF100 and SF300 (decided 22 Oct: no reason to stay at small scale factors; DuckPGQ handled 65536 pairs at SF300 in the 21 Oct pair-count axis, build 74 s / index 52 s at 32 threads). Give every baseline a per-query timeout (e.g. 10 minutes) and report a timed-out point as such rather than skipping the point, since the per-pair systems grow linearly (at SF10: Neo4j ~0.7 ms, Kuzu ~6 ms, FalkorDB ~8 ms, Memgraph ~39 ms per pair); add a predicate-driven multi-source query (sources selected by an attribute, as in LDBC BI 19/20) so the many-pair case is not a synthetic pair table; run the systems' set-oriented procedures where they exist (Neo4j GDS single-source and all-pairs, Memgraph MAGE) so no baseline is shown on its slow path, and note that those procedures do not compose with a Cypher predicate in the same query; include all-pairs shortest path as the extreme of the sweep (our source-grouped kernel per source vs their per-pair searches). The absence of a batched, set-oriented path-finding operator is a design limitation of the graph databases to be stated as such, with the sweep as evidence | eval | 4 | P1 | W16, W17 | harness done 22 Oct (`sweep-pairs`, `sweep-pairs-table`): random pairs 1..65536 and k sources x all persons (k = 1, 16, 128, 1024; V^2 all pairs is not materialisable), 600 s timeout per query with server-side cancellation, larger counts after an exceeded one recorded as inferred, cross-system agreement check. SF10 run in progress 22 Oct; SF100 and SF300 next |
| W26 | Point-query kernel: single-pair/single-source queries paid ~0.12 s of dense bitset sweeps over 2M vertices regardless of index or build. Done 20 Oct (`b488486e`, `5455ec18`, `43a37a0f`): source-grouped frontier kernel on index snapshots, hybrid sparse/dense expansion with a per-partition source bucket index, MATCH passes the quantifier upper bound into the operator (both kernels stop at the depth bound), a source group stops once all its destinations are found, up to 16 distinct sources always run source-grouped, pairs with a NULL endpoint no longer force the 512-lane batch path (a single source over all 2M vertices went from 288 s to 0.15 s because 1214 isolated vertices made NULL pairs). 8M edges, index, 18 threads: 1 pair 0.01-0.02 s (Kuzu 0.03-0.05), 20 sources 0.13 s (Kuzu 0.07), 1 source x 2M destinations 0.15 s. Remaining gap on few-source queries is per-group array allocation and worker oversubscription (design doc 5.6); revisit only if the evaluation needs it | op | 3 | P1 | W12 | done |

Done on day 1: W1-W6, W8 (draft), W9, W10, W11, most of W12 and W15. Open P1: W7, W12 (cost choice), W15
(rest), W16-W23. The remaining engineering is roughly 15 pd; evaluation and writing (~25 pd plus machine
time) are now the critical path and depend on EC2 access and the author's hours.

## 5. Schedule (re-planned 19 Oct for a 1 February VLDB submission; 1 January = complete draft)

| Week | Dates | Engineering (Claude) | Evaluation / writing (author) |
|------|-------|----------------------|-------------------------------|
| 1 | 19-25 Oct | done: W1-W6, W8-W11, W14, W26; branch pushed for Linux CI 20 Oct; next: W7 path reconstruction | Venue decided (VLDB); review design doc decisions D2-D5; W18 Neo4j Enterprise request; start W22 outline |
| 2 | 26 Oct-1 Nov | done: W7, W17, W27, W28 steps 1-3, W29, W21 repro package, W19 switches; next: W31 batch scheduling rule, W19 microbenchmarks at SF100 | Decide Ladybug (section 9); W18 and W21 decided 26 Oct |
| 3 | 2-8 Nov | W15 remaining tests (restart under load, concurrent writers); remove dead option `task_size` | Tag frozen commit (`eval-freeze-rc1`); frozen rerun via `repro/run_all.sh` incl. microbenchmarks; then W30 graph structure |
| 4 | 9-15 Nov | W12 cost-based choice + W13 model skeleton; fixes from the sweep | **Checkpoint 13 Nov**: sweep results reviewed; related work (W22) |
| 5 | 16-22 Nov | W16 update workload in the harness (LDBC insert streams) | Intro + operator section draft |
| 6 | 23-29 Nov | W13 calibration from sweep data; W25 if time | Update-workload sweeps on EC2 (W16) |
| 7 | 30 Nov-6 Dec | Bug fixing from sweeps; W24 if time | Index section draft; microbenchmark sweeps (W19 part 2) |
| 8 | 7-13 Dec | Engineering freeze except fixes | **Checkpoint 13 Dec**: update-workload numbers; W20 final comparison rerun |
| 9 | 14-20 Dec | Figures and tables from the archive | Evaluation section draft |
| 10-11 | 21 Dec-3 Jan | Buffer / holidays | **1 Jan: complete draft with final numbers** |
| 12-13 | 4-17 Jan | Fixes only | Internal review, polish; (SIGMOD Round 1 would be ~17 Jan) |
| 14-15 | 18-31 Jan | - | Final pass; **VLDB abstract 25 Jan, paper 1 Feb** |

## 6. Checkpoints and fallbacks

- **13 November (end of week 4):** design doc reviewed (decisions D2-D5 settled); first read-only sweep on
  a frozen commit on EC2 completed and archived; point-query kernel in place. Three of the original four
  criteria were already met on 19 Oct; the sweep is the remaining one. No-go: results at scale contradict the
  local numbers -> re-plan the evaluation before writing.
- **13 December (end of week 8):** update-workload numbers exist and show the index beating
  rebuild-per-query at realistic update rates. If not, the paper narrows its index claim to
  snapshot-consistent reads with rebuild-on-vacuum and reports the trade-off honestly.
- **1 January:** complete draft with final numbers, or move the target from the 1 February to the 1 March
  VLDB round.

## 7. Risks

| Risk | Mitigation |
|------|-----------|
| Barrier still assumes all workers of an event are co-scheduled (two concurrent path queries or `external_threads>1` with idle client threads can stall; now cancellable instead of hanging) | Single-query sweeps are unaffected; restructure as event-per-phase if concurrent-query experiments are needed |
| Index design takes longer than week 4 | Fallback path above; keep epoch design minimal (append + tombstone only; UPDATE = delete + insert) |
| DuckDB index API lacks a hook we need (e.g. snapshot handle, two-table dependency) | Document as a finding; implement minimal addition in the fork only if unavoidable; key dictionary on edge table only |
| Sweeps lost or unrepeatable (results only on EC2) | W21 from week 1: archive after every sweep, tag commits |
| Neo4j comparison attacked as unfair | W18; report 4-thread parity and state limits in figure captions |
| Measurement validity questioned | A1–A5 fixed before any reported run; verify flag on for all configurations |
| Holidays in weeks 10–11 | Scheduled as buffer; machine-time sweeps run unattended |

## 8. Paper outline (12 pages)

1. Introduction (thesis, contributions, 1.5 p)
2. Background: SQL/PGQ in DuckPGQ, DuckDB pipelines, MVCC and indexes, MS-BFS (1 p)
3. Path-finding operator: pair batches, partitioned CSR build, MS-BFS lanes, orientation and grouping,
   planner integration (2.5 p)
4. MVCC-consistent CSR index: epochs, deltas, tombstones, snapshot selection, vacuum, checkpoint and
   recovery, correctness (2.5 p)
5. Choosing a strategy: cost model and the engine hooks an extension needs (1 p)
6. Evaluation: setup; read-only vs Kuzu/Neo4j; updates and delta size; end-to-end queries; microbenchmarks;
   memory and scaling (3 p)
7. Related work (0.75 p)
8. Conclusion (0.25 p)

## 9. Open decisions

- Path reconstruction is in scope (decided 19 Oct): the paper is about paths, not distances. Weighted (cheapest) paths are future work; the index keeps edge row ids so weights can be looked up later.
- Venue: VLDB, 1 February 2027 round (decided 20 Oct). SIGMOD 2028 Round 1 is the fallback.
- Design decisions D2-D5 in `docs/CSR_INDEX_DESIGN.md` (epoch retention without an oldest-active-bound accessor;
  8 B row ids per edge; fold ratio 0.1; undirected as an index option). D1 (keys, not row ids) confirmed 19 Oct.
- Index granularity: per-epoch deltas (implemented) vs per-edge commit stamps (follow-up).
- Remove the in-memory per-connection cache with the prototype persistence (W14): done 20 Oct.
- Native-graph baseline: Kuzu is faster than LadybugDB on the cold-start workload (design doc 5.6); feature Kuzu.
  FalkorDB (RedisGraph successor, GraphBLAS-based, in-memory Redis module) considered and left out (19 Oct): a
  matrix-based data point, if wanted for the kernel discussion after W26, is better taken from GraphBLAS/LAGraph
  directly (W24).
- Staffing: one author with ~1 day/week plus Claude on engineering; the author's time goes to review,
  EC2 sweeps, and writing.

- Ladybug as the maintained successor of Kuzu (raised 26 Oct): Kuzu's last release is 0.11.3 (10 Oct 2025, project archived); Ladybug (PyPI `real_ladybug`, 0.15.3 on 1 Apr 2026) continues it with the same Python API. Proposal: add it as a system next to Kuzu at SF10 and SF100 (about four machine hours after the microbenchmarks), report Ladybug as the embedded baseline if the two agree within noise. Open.
- Terminology (decided 26 Oct): the paper says microbenchmarks, not ablations; harness subcommands `microbench` / `microbench-table` (old names kept as aliases), `docs/MICROBENCHMARK_SWITCHES.md`.

## 10. Evaluation machines (decided 19 Oct, revised 21 Oct)

Sizing is set by the index build and by Neo4j's heap projection, not by queries: peak ~50-60 B/edge during the
build (edge lists 24 B + buckets 16 B + epoch 10-17 B), so datagen-8_9-fb (1.7B edges) needs ~100 GB plus ~27 GB
for the table; Neo4j GDS projects the same graph into 50-85 GB of heap. 64 GB covers only the small/medium
Graphalytics sets (<= ~500M edges); 256 GB covers everything in the harness.

| Phase | Instance | vCPU (= physical cores, no SMT) / RAM | On-demand (approx.) |
|---|---|---|---|
| Primary (started 21 Oct): all datasets up to graph500-24, all systems, thread scaling up to 32 | `m7a.8xlarge` (AMD Genoa) | 32 / 128 GiB | ~$1.85/h |
| Only for datagen-9_4 / graph500-25 class datasets (build peak ~60 GB + Neo4j heap) | `r7a.8xlarge` | 32 / 256 GiB | ~$2.4/h |
| Only if scaling beyond 32 threads is needed | `m7a.16xlarge` | 64 / 256 GiB | ~$3.7/h |

Revision 21 Oct: 128 GiB covers every dataset discussed so far (LDBC SF1-SF100 need < 3 GB; graph500-24 with 260M
edges peaks at ~20 GB; Neo4j heap and page cache 16-32 GB on top). The same Genoa CPU keeps the thread sweep
comparable across the m7a and r7a sizes.

Storage: EBS only. Provision a gp3 volume of 500 GB-1 TB with 1,000 MB/s throughput and 16,000 IOPS (the default
125 MB/s would dominate cold-start and import numbers). Define cold start as "fresh process, warm OS cache", as
measured locally on 19 Oct, and run any dropped-cache experiment separately and labelled. Rules: one instance type
for every system and phase; spot is acceptable since the harness writes each result as it finishes; record
instance type, AMI, kernel, volume type, DuckDB SHA, Kuzu and Neo4j versions in the results archive from the first
run (D7). The August 2026 sweeps did not record their instance type.

## 10b. DuckDB-only sweep recipe (first sweep, decided 20 Oct)

On the r7a.8xlarge, after `GEN=ninja make release` of the frozen commit and a build of the LDBC generator extension
(`~/git/ldbc-data-gen`, used by `prepare`):

```
python3 scripts/pathfinding_benchmark.py prepare --scale-factors 1 3 10 --threads 32
for sf in 1 3 10; do
  for threads in 1 4 8 16 32; do
    python3 scripts/pathfinding_benchmark.py run --scale-factor $sf --threads $threads --repeats 5 \
        --mode index_comparison --pairs 1024 --query-pattern point_to_point
    python3 scripts/pathfinding_benchmark.py run --scale-factor $sf --threads $threads --repeats 5 \
        --mode index_comparison --pairs 1024 --query-pattern sssp
    python3 scripts/pathfinding_benchmark.py run --scale-factor $sf --threads $threads --repeats 5 \
        --mode index_comparison --pairs 64 --query-pattern all_pairs
  done
done
```

`index_comparison` records three rows per repeat: `operator` (CSR built per query), `index_build` (CREATE INDEX
time as `csr_build_s`) and `index_operator` (query over the persisted index). Results land in
`data/ldbc-pathfinding/results/<sf>/` as per-run JSON lines plus `summary_*.csv` and `stats_*.csv`. Record the
instance type and the commit hash in `--notes` (D7). Kuzu and Neo4j follow once W17/W18 are in place.

## 11. Progress log

- 19 Oct: merged main (`ea98090f`), fixed build/tests (`7c5db25a`), release review done. Operator: event
  dependencies `1729367a`, bounds `578baf3c`, abortable barrier `28a3ee99`, planner `e7c36dad`, working sets
  `e08b95f5`, strategy record `879ae6c0`. Index: type `9af62e18`, operator integration `16aca50e`, parallel
  build `79b984eb`, sort-and-emit build `337c2ff5`, sparse dictionary `6d38d2c9`, shared build `310fb731`,
  delta/tombstone overlay `af9d769b`, fold `5db16e4b`, persistence `55429a47`. Design doc and this plan drafted.
  Cold-start comparison vs Kuzu/Ladybug measured (design doc 5.6).
- 20 Oct: W26 done. Source-grouped kernel on index snapshots `b488486e`; depth bound from MATCH into both kernels,
  destination-based early termination, few-sources rule, NULL-pair tolerant source grouping, worker count by edge
  count `5455ec18`; per-partition source bucket index for O(1) row lookup `43a37a0f`. Found and fixed a pathology:
  a single NULL pair (vertex key without edges) disabled source grouping and sent a single-source query over 2M
  destinations through 196 full MS-BFS batches (288 s -> 0.15 s). 8M-edge index: 1 pair 0.01-0.02 s, 20 sources
  0.13 s, 1 source x 2M destinations 0.15 s; single-threaded 0.12 s / 0.87 s. Design doc 5.6 updated. Full suite
  91/91. Branch pushed by the user for Linux CI.
- 20 Oct (later): W14 done. Prototype persistence (`__duckpgq_csr_*` tables, triggers, `duckpgq_mark_csr_invalid`),
  eager build, both settings, the per-connection partitioned CSR cache and the planner's cache lookup removed with
  10 tests, 3 scripts and `docs/CSR_PERSISTENCE.md`. Full suite 81/81. Build-per-query on 8M edges without index:
  1 pair 0.21 s, 20 sources 0.47 s (every query now pays the ~0.15 s build; the index path is unchanged).
  Next: W7 path reconstruction.
- 20 Oct (later): venue decided (VLDB). W7 step 1 (`af26e5ad`): shortest paths over the CSR index through the
  operator with parent pointers in the source-grouped kernel; row ids exposed on epoch and delta partitions; MATCH
  routes element_id/vertices/edges/path_length to one shortestpathoperator call. Suite 82/82. Decision: the first
  sweep is DuckDB-only; next engineering item is the harness index mode, then W7 step 2 (build route).
- 20 Oct (evening): harness index modes (`index_build`, `index_operator`, `index`, `index_comparison`) added and
  smoke-tested; DuckDB-only sweep recipe in section 10b. Next engineering: W7 step 2 (paths on the build route),
  W15 remaining tests, W12 cost choice.
- 21 Oct: machine revised to `m7a.8xlarge` (32 cores, 128 GiB) for all sweeps; `r7a.8xlarge` only for the largest
  Graphalytics sets. Handoff note `docs/SESSION_HANDOFF.md` added for continuing on EC2. Instance started.
- 21 Oct (EC2, m7a.8xlarge, commit `daab1f1c`, uncommitted fixes): first DuckDB-only sweep (10b) on LDBC SF1-SF300,
  5 repeats, no failed runs; results in `data/ldbc-pathfinding/results/sf*/`, run logs in `~/sweep` on the instance.
  Findings: (1) the operator read src/dst from columns 0 and 1 of its pair input and ignored the function
  arguments, so the index route returned reversed pairs when the keys came out of a hash join (591/1024 SF1 pairs
  wrong); fixed by resolving the two arguments in `LogicalPathFindingOperator::ResolveColumnBindings` and
  evaluating them in `SinkPairs` into four leading pair columns (search ids, original keys), test
  `path_finding_pair_columns.test`. (2) A self pair of a vertex without edges returned NULL on the index route and 0
  on the build route; `EmitPathFindingOutput` now answers 0 / [key] for equal non-NULL endpoints on every route.
  (3) The index route's query time was flat across threads (1.3 s at SF300 for every thread count): it is the lazy
  deserialization of the persisted index (1.3 GB, ~42M edges/s, serial, double copy in `LoadFromStorage`) inside
  the first query of each fresh process. Second query in the same process: sssp 30 ms at 32 threads / 87 ms at 1
  thread (build route 0.39 s / 5.3 s), point_to_point 1.04 s / 5.1 s (build route 1.73 s / 13.5 s). Harness mode
  `index_operator_warm` (runs the query twice, times the second) added to `index_comparison`. (4) The index build
  uses under 5 of 32 cores and ~30% of its 4.7 s at SF300 is writing to the 250 MB/s volume (2.8-3.3 s on tmpfs).
  (5) The EBS volume measures 250 MB/s, not the 1,000 MB/s of section 10. Pair-count axis at SF300: the index wins
  at 1 thread for every pair count (4.2x at 1 pair, 1.75x at 8192) and at 32 threads from 8192 pairs upward
  (1.28x; 1.43x at 65536), cold-process numbers. Harness: `index_query_sql` materializes key pairs untimed,
  verifier skips `index_build` rows. Next: faster index load (parallel partition deserialization, no double
  copy), point_to_point kernel scaling over the index (5x on 32 threads), W16.
- 21 Oct (later): parallel index load (`LoadFromStorage`: parallel segment copy into an uninitialized buffer, skip
  pass then parallel partition deserialization, single-copy vector assign). SF300 load 1.15 s -> 0.09 s at 32
  threads (the zero-fill of the 610 MB staging buffer was most of the remaining cost once the copies ran in
  parallel). Harness `index_comparison` at SF300, 3 repeats, cold = first query of a fresh process, warm = second:
  sssp 32 threads operator 0.40 s / index cold 0.17 s / warm 0.024 s; sssp 1 thread 5.3 / 1.30 / 0.086;
  point_to_point 32 threads 1.69 / 1.11 / 0.95; point_to_point 1 thread 13.5 / 6.3 / 4.9. The load follows the
  `threads` setting, so single-threaded runs still pay 1.1 s. Warm single pair over the index is ~0.1 s at every
  thread count (fixed cost, not yet profiled); warm sssp scales 87 -> 27 ms from 1 to 16 threads and is flat after.
- 21 Oct (evening): W16 update workload. Harness `update-run` (sequential: build-per-query, index with automatic
  fold, index without fold, on database copies, exact edge counts from the staged `knows_update_log`),
  `update-concurrent` (C++ driver, one writer + R readers, index and build route in the same read transaction) and
  `update-kuzu` (same batches and pairs on Kuzu 0.11.3). Data: LDBC BI insert/delete batches of Person_knows_Person
  (SF10: 339k inserts, 50k deletes between snapshot persons). Findings and fixes, all committed:
  (1) The reader snapshot cost O(delta + tombstones) row-version checks and two dictionary lookups per edge on
  every query: SF10 sssp grew from 8 ms to 124 ms over 332k pending operations and the index averaged 41 ms against
  26 ms for build-per-query (SF100: 331 vs 132 ms). Fixed by commit groups (one check per Append/TryDelete call),
  stamping deleted delta rows instead of tombstoning them, append-time id resolution (re-resolved at folds: the
  dictionary is rebuilt in key order and is not prefix-stable; a stale-id bug surfaced after the first automatic
  fold) and a one-entry snapshot cache keyed by (epoch generation, newest visible change group). After: SF10 32
  threads sssp build 26 / index_auto 19 / nofold 25 ms (10 queries per commit: 10.4 ms); point_to_point 86 / 76 /
  82 ms; 1 thread sssp 138 / 23 / 22 ms; SF100 sssp 136 / 113 / 112 ms (still rising to 200 ms at 1.35M pending,
  the rebuild after a commit is O(delta) bucketing). Update cost per commit is the same for all routes (+0.3-7 ms).
  (2) Concurrent path-finding queries deadlock: the BFS kernels synchronize worker tasks with barriers, and with 4
  readers at 32 threads the scheduler filled with tasks whose peers could not run (164 threads in futex wait).
  Workaround in the driver: `experimental_path_finding_operator_threads_per_batch = threads / (readers + 1)`. Real
  fix needed: one event per BFS level (W15 item). (3) Two connections initializing DuckPGQ state at once hit a
  catalog write-write conflict on `__duckpgq_internal`. (4) The cache key names the file catalog, not `memory`.
  Concurrent SF10 sssp, writer committing 2000 ops every 200 ms, 190 commits, no mismatches: readers 1/4/8/16 index
  11/18/37/72 ms vs build 23/32/49/84 ms, 25/72/85/88 iterations per second; point_to_point 4 readers 113 vs 143 ms;
  unpaced writer (14 commits/s) 8 readers 39 vs 39 ms. Kuzu: results agree step by step; sssp needs the source as a
  literal (a WITH-bound source ran the recursive join from every vertex: 20 s vs 8 ms at SF1); point_to_point with
  1024 pairs bound through a node table takes 21 s at SF1 (formulation to be revisited, W17). Kuzu SF10 sssp, same
  190 commits, 32 threads, results identical on all 191 steps: query mean 16.6 ms (DuckPGQ index 19 ms, build 26 ms),
  update batch mean 346 ms (DuckDB 8-9 ms; Kuzu via MATCH Op ... CREATE / DELETE, one transaction per batch); Kuzu
  ended with one edge more than expected (2,124,203 vs 2,124,202), results unaffected, cause open.
- 22 Oct: deadlock fixed at the root. Every kernel (source-grouped, MS-BFS iterative length and its grouped
  scheduler, partitioned CSR build, push-pull, bidirectional) now runs as a chain of per-phase events
  (`PhasedKernel`/`PhasedKernelEvent`): the tasks of a phase run independently, the serial step between phases runs
  in `FinishEvent` and inserts the next phase's event. No task waits for another, `duckpgq_barrier` is gone, the
  concurrent driver needs no worker cap: 4/8/16 readers at 32 threads with 32 workers per query run clean
  (78/87/90 iterations per second, no mismatches). Warm SF300 at 32 threads: build route sssp 0.30 s (was 0.39),
  point_to_point 1.61 s (was 1.73); index route unchanged (0.031 s / 0.96 s). Regression test
  `path_finding_concurrent_queries.test_slow` (eight connections, index and build route; the deadlock is timing
  dependent and does not reproduce reliably in it, the driver run is the definitive check).
- 22 Oct (later): overlay rebuild parallelized (counting sort by destination range, ranges sorted and packed in
  parallel, `9c780af3`): SF10 rebuild after a commit 41-51 -> 32-43 ms; SF100 sssp under updates index 95 ms vs
  build 131 ms (was 331 vs 132 before the 21-22 Oct work), still 39 -> 159 ms over 1.35M pending operations with one
  query per commit (the remaining cost is the O(delta) copy and visibility pass; a fold policy belongs to W13).
  SF10 point_to_point under updates with the event-chain kernels: build 76 / index_auto 62 / nofold 66 ms (was
  86 / 76 / 82). Kuzu 0.11.3 on the same batches, results identical on every step: sssp 16 ms per query and 342 ms
  per 2000-op batch (DuckPGQ index 19 ms / 9 ms); point_to_point needs one query per pair (a pair table bound at
  both ends runs the recursive join over far more than the pairs): 6.3 ms per pair, 6.5 s per 1024 pairs at SF10
  against 62-76 ms. Kuzu's MATCH ... DELETE in the same transaction as the CREATE did not see the created
  relationship; the runner commits inserts and deletes separately. lance-graph 0.5.4 checked as a further baseline:
  Cypher over Lance/DataFusion with variable-length patterns but no shortest-path semantics, no mutation statements
  found, no wheel for this box (Rust build needed); related work and at most a read-only bounded-reachability
  comparison (P3).
- 22 Oct (laptop): paper skeleton committed (`db71ccbb`): `paper/` on the PVLDB template (Vol. 20), section stubs
  from the outline, related work draft, bibliography. `paper/` was gitignored before and is tracked now; build
  artifacts are ignored via `paper/.gitignore`. Compile with `cd paper && PATH=/Library/TeX/texbin:$PATH latexmk -pdf
  main.tex`. Decision (user, 22 Oct): the 21-22 Oct sweep numbers are provisional; every number in the paper is
  rerun on one frozen commit once the implementation has settled (before the 13 Dec checkpoint at the latest).
  Tooling on the laptop: TeX Live 2026 and poppler present; Skim not installed despite the attempt.
- 22 Oct (later): `paper/STYLE.md` (analysis of X100, FSST, Teseo, DuckPGQ CIDR, Kersten et al.; ten rules;
  terminology table, 'traversal' not 'kernel'), related work rewritten (`f4af3280`), introduction drafted
  (`7a706d27`): the CIDR 2023 paper's own limitation (morsel-driven parallelism is tuple-driven; CSR as UDF-built
  global state outside the transaction) is the hook, the operator and the index are the answer. The user is
  looking for PDFs of 'Database Architecture Evolution' (VLDB 2009) and 'TPC-H Analyzed' to sharpen the guide.
- 22 Oct (evening): operator section drafted (`70a5d43a`), 'partitioned CSR' named as the layout. Decision (user):
  reverse CSR, push-pull and bidirectional traversals are low priority; they stay in the code and the microbenchmark only,
  and get paper space only if they become faster than the two main traversals.
- 22 Oct (evening): Memgraph (memgraph/memgraph:latest, Bolt) and FalkorDB (falkordb/falkordb:latest) added to the
  update workload (`update-memgraph`, `update-falkordb`, Docker containers on the EC2 box); results identical to
  DuckDB on every step for both patterns. SF10, 190 commits of 2000 operations, one query per commit, query mean /
  update batch mean: DuckPGQ index (auto fold) 19 ms / 9 ms, build per query 26 ms / 8 ms, Kuzu 16 ms / 342 ms,
  Memgraph 41 ms / 26 ms, FalkorDB 9.3 s / 111 ms for sssp (1024 destinations); point_to_point (1024 pairs, 20
  commits for the graph databases): DuckPGQ 62 / 76 ms, Kuzu 6.5 s (one query per pair), Memgraph 40 s (`*BFS` with a
  bound target is one BFS per pair), FalkorDB 8.1 s (one `shortestPath` per pair; its single-source query is the same
  per-destination search, hence 9.3 s). Loads at SF10: Memgraph 6-7 s (LOAD CSV), FalkorDB 85 s (UNWIND batches),
  Kuzu < 1 s (COPY). None of the three has a batched many-pair or many-destination primitive that shares the search;
  the comparison for the paper should lead with single-source (the fair fight) and state every Cypher formulation.
  FalkorDB serializes writes against reads with a per-graph lock (no MVCC), the contrast for the concurrent pass.
- 22 Oct (night): Neo4j 5.26 Enterprise added (`update-neo4j`; Docker `neo4j:5-enterprise` under the evaluation
  licence, `NEO4J_ACCEPT_LICENSE_AGREEMENT=yes`, Bolt 7688, 16 GB heap + 16 GB page cache, Cypher `runtime=parallel`).
  Results identical to DuckDB on every step. SF10, same batches: sssp (1024 destinations) 10.3 s per query (1024
  `shortestPath` searches), point_to_point (1024 pairs, `UNWIND` + `shortestPath`) 0.76 s, update batch 69 ms, load 21 s
  via LOAD CSV. The 13x gap between its two shapes with the same per-pair primitive is unexplained (planner or
  parallel-runtime behaviour with a single source row); try `runtime=pipelined` and GDS before the frozen rerun.
  Licence: evaluation terms only; publication rights must be settled (W18) before these numbers appear in the paper.
  Cross-system table (`~/sweep/collect_systems.py 10`): sssp DuckPGQ index 19 ms / build 26 ms / Kuzu 16 ms /
  Memgraph 41 ms / FalkorDB 9.3 s / Neo4j 10.3 s; point_to_point 62 / 76 ms / 6.5 s / 40 s / 8.1 s / 0.76 s; update
  batch 9 / 8 / 342 / 26 / 111 / 69 ms.
- 22 Oct (observations for the paper, decided with the author): the graph databases answer a set of pairs one
  search at a time, so a predicate on the source table (not a point lookup) or a join turns into N independent
  searches with no shared work; that is a design limitation to be stated as one, quantified by a source-cardinality
  sweep (1, 16, 128, 1024, all pairs) rather than by two query types. For a fair comparison run Neo4j GDS and
  Memgraph MAGE set procedures beside Cypher, and note they do not compose with a predicate in the same query.
  Next engineering/eval items: W17 (LDBC IC13/14, BI19/20 for all systems) together with W27 (multi-source and
  all-pairs sweep), then W7 step 2 and W12/W13. Neo4j's 13x gap between its two 1024-search queries is open.
  Pair-count sweep for W27: 1, 16, 128, 1024, 2048, 16384, 65536, all pairs; scale factors 10, 100, 300; the
  baselines get a timeout per query and a timed-out point is reported, not dropped.
- 22 Oct (late): W27 harness (`sweep-pairs`), W17 harness (`ldbc-queries`), Neo4j GDS, W7 step 2.
  Baseline query findings while smoke-testing, all now in the harness: (1) Neo4j's 13x gap between its two
  1024-search queries was the parallel runtime, not the algorithm: `WHERE b.id IN $dsts` plans a MultiNodeIndexSeek
  that hands the runtime one row, so every shortestPath ran on one worker; `UNWIND $dsts AS d MATCH (b {id: d})`
  fans out and matches the point-to-point time (SF1: 0.77 s -> 0.23 s; the pipelined runtime is 0.75 s for both
  forms). (2) Memgraph's planner puts a source filter after a `*BFS` expansion from every vertex unless a `WITH a`
  barrier precedes the pattern (35 s instead of 10 ms at SF1). (3) Neo4j shortestPath rejects a = b, so the
  all-destinations query excludes the source. (4) GDS 2.13 installed in `pf-neo4j` via `NEO4J_PLUGINS`; unlicensed,
  so concurrency is capped at 4 and its procedures cannot run under the parallel runtime; `neo4j_gds` system uses a
  projected graph (undirected projection for the LDBC queries), delta-stepping single-source and Dijkstra per pair.
  (5) Memgraph MAGE: the `memgraph/memgraph` image only carries the networkx-based `nxalg` procedures; Memgraph's
  native `*BFS` expansion from a bound source is already one BFS per source (SF1: 16 sources x all persons 52 ms,
  DuckPGQ build 45 ms), so no MAGE run. The sources x all persons product is streamed from a cross join on the
  DuckPGQ side (1024 x 1.2M persons at SF300 is not materialisable). DuckPGQ SF10 sweep, 32 threads, first numbers:
  index 2.6 s / build ~3 s for 65536 random pairs; 1024 sources x all persons (22.5M reachable pairs) build 13.1 s,
  index 17.9 s (the index route is slower on the all-destinations shape; key translation and the person join on the
  pair side, to be looked at). Kuzu 65536 random pairs 325 s (5 ms/pair), Memgraph 39 ms/pair (16384 pairs will
  timed out).
- 22 Oct (night): SF10 sweep complete (`sweep_pairs_sf10_threads32_1792681084488488018.csv`, all 11 points agree
  across six systems). Random pairs, 32 threads: 65536 pairs DuckPGQ index 2.6 s / build 3.0 s, Neo4j 11.7 s
  (parallel runtime, UNWIND), Kuzu 330 s, FalkorDB 360 s, Memgraph times out from 16384 pairs
  (40 ms per pair); at 1 pair DuckPGQ build 23 ms / index 4 ms, Kuzu 11 ms, Memgraph 41 ms, FalkorDB 17 ms,
  Neo4j 70 ms. Sources x all persons (1024 sources, 22.5M reachable pairs): Kuzu 4.0 s (one recursive join per
  source), Memgraph 38 s (one `*BFS` per source), DuckPGQ build 13.6 s / index 17.8 s, Neo4j and FalkorDB exceed
  the timeout from 128 and 16 sources (one shortestPath per pair). The DuckPGQ number was investigated the same
  night: the 1024-source query ran at 1 thread in 14.1 s and at 32 threads in 12.2 s, i.e. serially. Three serial
  parts: (1) every source group was a 29-worker BFS with an event per level (65 ms CPU per traversal, 17%
  utilisation); now groups scheduled together divide the threads, and a single-worker group runs its whole search
  in one task (traversal CPU 66 -> 17 s); (2) the scheduler registered 70M destinations into per-group
  `unordered_set`s (3.5 s); groups that target more than 1/64 of the vertices now use a bitmap; (3) Finalize copied
  and analysed the 70M-row pair list in three serial passes (3.5 s); one pass was dead (the endpoint-pair phase
  recomputed statistics the main phase recomputes), the main pass now materialises the chunks with a parallel
  scan and merges per-thread statistics (HLL merge), and the per-group state allocation runs in parallel. Result
  (SF10, 32 threads, same answers): 1024 sources x all 13.8 -> 4.4 s (Kuzu 4.0), 128 sources 1.73 -> 0.59 s
  (Kuzu 0.63), 16 sources 0.245 -> 0.096 s (Kuzu 0.10), 1024 random pairs with paths 1.34 -> 0.54 s; single
  source and random-pair lengths unchanged. What remains on the sources x all shape is the pair-list interface
  itself: the cross product is sunk (0.9 s for 70M rows), grouped (0.5 s serial), scattered (0.75 s); a
  destination-free "all vertices" form of the operator would skip it, noted as future work. SF10 DuckPGQ points
  are rerun on this build before the SF100/SF300 sweeps.
- 22 Oct (night, results): SF10 W27 sweep on the final build (`sweep-pairs-table` over the three SF10 CSVs; 32
  threads, 600 s timeout). Random pairs 1 / 16 / 128 / 1024 / 2048 / 16384 / 65536: DuckPGQ index 4 / 20 / 25 / 50 /
  87 / 658 ms / 2.6 s; build 23 / 45 / 50 / 74 / 119 / 776 ms / 3.1 s; Neo4j 70 / 112 / 552 / 716 / 772 ms / 2.9 /
  11.7 s; Kuzu 11 / 85 / 629 ms / 5.1 / 10.2 / 82 / 330 s; FalkorDB 17 / 92 / 719 ms / 5.7 / 11.5 / 92 / 360 s;
  Memgraph 41 / 459 ms / 5.1 / 40 / 81 s / >600 / >600; Neo4j GDS (4 threads, Dijkstra per pair) 23 / 143 ms / 1.3 /
  9.7 / 19.7 / 152 / 570 s. Sources x all persons 1 / 16 / 128 / 1024: DuckPGQ index 10 / 67 / 527 ms / 4.1 s,
  build 27 / 98 / 579 ms / 4.4 s, Kuzu 7 / 101 / 632 ms / 4.0 s, Memgraph 14 / 573 ms / 4.9 / 38 s, GDS
  (delta-stepping per source) 25 / 350 ms / 3.0 / 22 s, Neo4j 18 s / 179 s / >600 / >600, FalkorDB 297 s / >600 /
  >600 / >600. Reading: with a bound destination set the systems that run one search per pair scale linearly
  (Neo4j is the fast one at 0.2 ms per pair under the parallel runtime, still 4.5x DuckPGQ at 65536 pairs; Kuzu,
  FalkorDB 5 ms per pair; Memgraph 40 ms); on sources x all persons the systems with a single-source primitive
  (Kuzu's recursive join, Memgraph's BFS expansion, GDS) are within 1x-10x and Kuzu matches us, which is the
  honest result for that shape: the work is the traversal, our pair-list interface adds the cross product.
  W17 at SF10 (`ldbc-queries-table`, hop-count variants, 5 parameter sets x 3 repeats, all systems agree):
  IC13 DuckPGQ index 4 ms / build 28 ms / Neo4j 5 ms / Kuzu 8 ms / GDS 64 ms / Memgraph 385 ms; BI19 (149 x 79
  down to 80 x 70 persons of two cities) index 128 ms / build 172 ms / Neo4j 45 ms / Kuzu 1.6 s / GDS 10.2 s /
  Memgraph 41.6 s; BI20 (1277-1339 employees x 1 person) index 25 ms / build 256 ms / Neo4j 31 ms / Memgraph
  540 ms / Kuzu 7.6 s / GDS 58 s. Neo4j's bidirectional shortestPath is very effective when the pairs are close
  (same city, 2-3 hops). FalkorDB does not support undirected shortestPath; its LDBC graph now holds both
  directions and the queries are directed (rerun queued after the SF100/SF300 chain). Loads at SF10: DuckPGQ
  0.2-0.6 s, Kuzu 0.6 s, Memgraph 8 s, Neo4j 23 s, FalkorDB 93 s.
- 23 Oct: SF100 sweep complete (all 11 points agree). Random pairs 65536: DuckPGQ index 16.8 s / build 21.5 s,
  Neo4j 169 s, Kuzu and FalkorDB > 600 s, Memgraph > 600 s from 1024 pairs; 1 pair index 8 ms / build 99 ms,
  Neo4j 14 ms, Kuzu 22 ms. Sources x all (1024): Kuzu 9.4 s, DuckPGQ build 24.5 s / index 26.0 s, GDS 191 s,
  Memgraph, Neo4j, FalkorDB > 600 s. W17 SF100: IC13 index 14 ms / build 182 ms / Neo4j 6 ms / Kuzu 26 ms; BI19
  index 6.0 s / build 8.4 s / Neo4j 4.3 s / Kuzu 63 s; BI20 index 1.2 s / build 3.0 s / Neo4j 127 ms / Memgraph
  6.4 s / Kuzu 165 s. W17 SF300 (all agree): IC13 index 27 ms / build 541 ms / Neo4j 5 ms / Kuzu 53 ms / FalkorDB
  118 ms; BI19 index 44.6 s / build 61 s / Neo4j 48 s / Kuzu 329 s, the rest > 600 s; BI20 index 7.6 s / build
  12.7 s / Neo4j 1.2 s / Memgraph 48 s, the rest > 600 s. The first SF300 sweep died on the 1024 sources x 1.19M
  persons point (1.2G pairs: the DuckDB process was killed, the pair list alone is ~58 GB) after the build route's
  random pairs (65536 in 68.5 s, 1 pair 286 ms) and 128 sources (7.8 s); the harness now records a DuckPGQ failure
  and skips sources x all points above `--max-pairs` (500M); rerun queued (`sf300_chain`).
  Reverse orientation on symmetric edge sets (`7fdfe65e`): Nx1 shapes (BI20) were N forward searches because
  endpoint inputs and index snapshots were forward-only; an undirected index (both directions in the epoch) or an
  undirected MATCH pattern (both directions in the endpoint input) now lets the ratio heuristic search from the
  destination side. SF10 BI20: index 25 -> 6 ms, build 256 -> 32 ms (Neo4j 31 ms); the LDBC harness uses
  `CREATE INDEX ... WITH (undirected = true)` on knows instead of a directed index on the closure table. Still open
  for directed graphs: a reverse direction in the index (W28 below). Storage, measured by checkpointing a copy with
  and without the index: SF10 25.4 MB index vs 12.6 MB knows table (13.8 B/edge), SF100 232 MB vs 123 MB (12.4
  B/edge): 8 B edge row id, 2 B destination, the rest sparse rows, bucket index, dictionary; a lengths-only reverse
  side would add ~5 B/edge. `docs/csr_index_explainer.html`: self-contained explainer of the index.
- 23 Oct (afternoon): reach mode (W29). The sources x all persons shape was the pair-list interface at its worst
  (70M pairs sunk, grouped and scattered at SF10, 1.2G pairs at SF300 not representable). `reachoperator` takes
  the sources only and emits [dst, len] per reached vertex from the per-source distance arrays through a
  parallel source. SF10, 32 threads, both routes, same answers as the pair mode: 1 / 16 / 128 / 1024 sources
  build 22 / 33 / 85 / 430 ms, index 3 / 13 / 42 / 220 ms (pair mode 27 / 98 / 579 ms / 4.4 s build; Kuzu 7 /
  101 / 632 ms / 4.0 s). The harness's multi-source form uses the reach mode; the SF100 and SF300 DuckPGQ
  points are queued for a rerun after the SF300 baseline chain (`after_sf300`). Lesson for the paper: a path-finding
  operator over bound pairs is one shape; "from these sources, everything" needs the destination-free shape, and
  with it the traversal is the cost (the 1024-source point is 1024 BFS runs over 1.8M edges in 0.2 s).
- 23 Oct (evening, final provisional tables; collected 26 Oct): all queued runs done 19:33 UTC, every point agrees
  across the systems that completed it. Files: `sweep_pairs_sf{100,300}_*` (full sweep + DuckPGQ reach rerun),
  `ldbc_queries_sf{100,300}_*` (full + DuckPGQ undirected-index rerun); print with the full CSV first.
  SF300 random pairs 1 / 16 / 128 / 1024 / 2048 / 16384 / 65536: DuckPGQ index 9 / 106 / 514 / 984 ms / 1.9 /
  14.0 / 55.2 s; build 288 ms / 1.0 / 1.0 / 1.6 / 2.8 / 17.9 / 70.3 s; Neo4j 15 / 515 ms / 24 / 39 / 43 / 192 s /
  > 600 (its per-pair cost grew from 0.18 ms at SF10 to 12 ms at SF300: the bidirectional search is not scale-free,
  the worry that it overtakes us at scale is settled); Kuzu 33 / 647 ms / 5.8 / 46 / 96 s / > 600 / > 600; FalkorDB
  113 / 927 ms / 8.3 / 69 / 137 s / > 600 / > 600; Memgraph 2.4 / 27 / 307 s / > 600 ...; GDS 79 ms / 3.4 / 49 /
  404 s / > 600 .... SF300 sources x all 1 / 16 / 128 / 1024: DuckPGQ index (reach mode) 9 / 135 / 657 ms / 4.2 s,
  build 282 / 542 ms / 1.7 / 9.7 s; Kuzu 3 / 151 ms / 2.0 s / not run (above the 500M-pair limit); Memgraph 1 ms /
  17 / 284 s / not run; GDS 51 ms / 4.1 / 57 s / not run; Neo4j 119 ms then > 600; FalkorDB > 600. SF100 sources x
  all with reach mode: index 15 / 91 / 383 ms / 2.3 s (Kuzu 13 / 148 ms / 1.2 / 9.4 s). SF100 random pairs on the
  final build: index 8 / 62 / 157 / 303 / 543 ms / 4.3 / 16.9 s.
  W17 on the undirected index with the reverse orientation: SF100 IC13 14 ms / BI19 5.9 s / BI20 25 ms (Neo4j 6 ms /
  4.3 s / 127 ms; Kuzu 26 ms / 63 s / 165 s); SF300 IC13 27 ms / BI19 43.7 s / BI20 55 ms (Neo4j 5 ms / 48 s / 1.2 s;
  Kuzu 53 ms / 329 s / > 600). BI20 went from 7.6 s to 55 ms at SF300 with the reverse orientation. What is left:
  BI19 (persons of city 1 x persons of city 2, about 1500 x 800 at SF300) runs as a pair list with 1500 sources;
  the reach mode through MATCH with the destination predicate applied after the reach is the planned fix (W29 step
  2); IC13, a point lookup, is 27 ms against Neo4j's 5 ms at SF300, the per-query fixed cost of the index route.
  Loads at SF300: Memgraph 279 s, Neo4j 841 s, FalkorDB 2597 s, Kuzu and DuckPGQ under a second (persisted).
- 26 Oct: paper draft has all technical sections (intro, related work, operator, index with timeline figure, section 5
  interface table, evaluation with figures and tables from the 22-26 Oct sweeps); 12 pages before background,
  cost-model text, abstract and conclusion, so a cut pass is due. Pending final edits recorded in `paper/NOTES.md`
  (thesis sentence without the four interface names; three headline numbers instead of eight). Figure 1 is a
  column-width TikZ figure on page 1; captions follow the rule in `paper/STYLE.md`.
- 26 Oct: W29 step 2, MATCH on the reach mode (`9272fcb4` and the follow-up). Decided after the whole pattern is
  known: a path-finding destination that no condition references, with no path projected and not the source
  itself, is served by `reachoperator`; the destination leaves the candidate query and, when its columns are
  projected or filtered, the destination table joins back on the reached vertex (key over an index, rowid on the
  build route) in the outer query, which keeps the COLUMNS order. A destination pinned by key (`b.id = ...`,
  `b.id IN (...)`, a further edge) keeps the pair mode. A destination with a non-key predicate (BI19's
  `b.cityId = c2`) also keeps the pair mode by default, measured: SF100, city 1425 to city 1302 through MATCH on
  the undirected index, pair mode 6.9 s (884 source groups, 420k pairs, every search stops once its destinations
  are found) against reach mode 7.8 s (884 full traversals, 401M distances produced, then filtered). The reach
  mode wins when the destination set is a large fraction of the vertices; a selective predicate favours early
  termination. `experimental_path_finding_operator_reach_filtered` (default false) enables the reach for filtered
  destinations; the proper rule needs a selectivity estimate at plan time and belongs to W12. The unbound query
  "everyone city 1425 reaches" (1095 sources x 473k persons) takes 7.8 s through MATCH, 51M distances per second.
  Options: `experimental_path_finding_operator_reach` (default true) returns every pattern to the pair mode; two
  tests that count pair-mode batches set it. Fixed the same day: `{1,1}` with `path_length(p)` or `element_id(p)` projected dereferenced a
  NULL WHERE clause when the operator option was on (the named-subpath pass is deferred and ran after the pattern
  pass had moved the element WHERE clauses into the main conditions; the SQL-fallback builder now skips moved
  clauses and `CreateWhereClause` skips nulls).
  Client-side concurrency (`--client-threads`, `d99fed4e`) measured 26 Oct: Memgraph gains 13-24x, FalkorDB 4x,
  Kuzu 11x on 65536 pairs (SF10: 110 s, 83 s, 29 s against the index's 2.6 s) and 6.5x on 1024 sources x all
  (0.62 s against 0.22 s); SF100 Kuzu@32 170 s on 65536 pairs, level with Neo4j's parallel runtime, 4.3 s on
  1024 sources x all against 2.3 s. Figures in `paper/figures/` (`scripts/plot_sweep_results.py`): random pairs,
  sources x all, LDBC, speedup, scaling, throughput, first query, updates, client threads; LaTeX tables with
  every system in `paper/tables/`.
- 26 Oct (later): W28 steps 1-3. `CREATE INDEX ... USING CSR (src, dst) WITH (reverse = true)` stores the edges a
  second time by source range in the same epoch, without row ids (the forward side keeps them for paths); the
  bulk build buckets (dst, src) in the same pass, the storage stream is version 2 with the reverse partitions
  after the forward ones (version 1 streams still load, forward only; a reverse index whose stream predates the
  option rebuilds from the table at the first read), a tombstone's reverse position is found from its forward
  position (source row by binary search over the row offsets, then the destination's row in the reverse
  partition of the source's range), and the snapshot carries reverse partitions, reverse delta companions
  (visible delta edges packed a second time with the endpoints swapped) and reverse suppression. An undirected
  index aliases its forward vectors as the reverse side, so `undirected = true` ignores `reverse`. The operator
  takes the reverse side from the snapshot into `reverse_partition_csrs` with its own companions and suppression
  (`SearchDeltaCSRs`/`SearchSuppressed` pick by orientation) and `CanSearchInReverse` is true when the snapshot
  has one. Test `csr_index_reverse.test`: reverse orientation, forward agreement, committed insert and delete on
  both sides, a private snapshot with uncommitted changes, fold, checkpoint and restart, undirected + reverse.
  SF10 directed knows, 2000 random sources to the person with the most incoming edges, index route: reverse
  orientation 5 ms, forward 91 ms (18x); index build with the reverse side 0.18 s (1.84M edges, 32 threads).
  Open: step 4, the bidirectional traversal over the index for random pairs at scale, and the storage cost
  measurement of the reverse side (about 5 B per edge by construction).
- 26 Oct (afternoon): reproduction package `repro/` (W21). `setup.sh` (toolchain check, pinned Python clients,
  `GEN=ninja make release`, generator clone at `1add85f` and build, containers), `containers.sh` (the three
  baseline servers with the flags they ran with: Memgraph 3.14.0 without edge properties, FalkorDB 4.2.2, Neo4j
  5.26.32 Enterprise 16G heap/16G page cache with GDS 2.13.14), `prepare.sh`, `run_all.sh` (the matrix in the
  order it was run, resumable per step through `logs/<step>.done`, subsets sweep/ldbc/client/updates),
  `collect.sh` (figures, tables, archive refresh; `archive` mode renders from the committed CSVs alone),
  `environment.sh` (machine and version record, `ENVIRONMENT.txt`). The harness reads
  `DUCKPGQ_GENERATOR_ROOT`, the plot script `DUCKPGQ_RESULTS_DIR`. `repro/results/` holds the 173 CSVs behind
  the current figures and tables (20 MB) plus the harness text tables; rendering from the archive reproduces
  `paper/tables/*.tex` byte for byte. Decisions recorded in W18 and W21.
- 26 Oct (late afternoon): microbenchmark switches (W19). Inventory of every mechanism that affects query time in
  `docs/MICROBENCHMARK_SWITCHES.md`: about twenty switchable, about twenty hard-coded. Added as `SET` options with the
  measured behaviour as default: `experimental_path_finding_operator_snapshot_cache` (true),
  `..._target_early_termination` (true), `..._sparse_frontier_divisor` (64; 0 always sweeps),
  `..._few_sources_limit` (16; 0 off); `LANE_LIMIT` overridable with `cmake -DDUCKPGQ_LANE_LIMIT`. The `run`
  subcommand's `--grouped-batches` default now follows the engine default (true); the 21 Oct DuckDB-only sweep ran
  with grouped batches off. Harness `microbench` / `microbench-table`: 23 configurations in groups kernel, index, reach,
  threads over random pairs, same destination and sources x all, both routes, per-point timeout, agreement check;
  `DuckPGQSystem` takes settings, an index WITH clause (separate index per clause) and a pair-list form of the
  sources x all shape. Test `path_finding_microbenchmark_switches.test`. Smoke at SF10 (1 repeat, points 1/128/1024
  pairs and 1/128 sources): all 8 points agree. Already visible: pair list instead of reach for 128 sources 7.0x
  (build) and 9.5x (index) slower; reverse side for 1024 sources to one destination 43 ms to 5 ms (0.12x); forcing
  dense or sparse expansion 1.3-1.5x slower at 128 sources; `threads_per_batch=4` 2-3x slower on random pairs;
  source grouping off 2x (build) to 4x (index) slower for a single pair. Oddity to check in the full run: on the
  index route 2 threads is slower than 1 thread (167 vs 98 ms at 1024 pairs). Dead option `task_size` to remove
  before the freeze. Full runs queued: SF10 then SF100, 3 repeats, all configurations and shapes.
- 26 Oct (evening): SF10 microbenchmarks complete (`ablation_sf10_threads32_1793027665795144714.csv`, 3 repeats,
  600 s timeout, 23 configurations x both routes x 11 points, all points agree; tables in
  `~/sweep/microbench_sf10_tables.md`). Findings beyond the smoke: (1) batch scheduling, 65536 random pairs:
  grouped off 0.56x (build, 3.04 to 1.69 s) and 0.40x (index, 2.61 to 1.04 s); 4 workers per batch 0.43x / 0.41x;
  the default derives one group of 32 workers (W31). (2) Thread scaling index route 65536 pairs: 1 thread 5.8 s,
  2 threads 10.2 s, 4 threads 6.3 s, 8 threads 4.1 s, 16 threads 3.1 s, 32 threads 2.6 s; the 2-thread point is
  the same scheduler issue. Sources x all 1024 scales 9.5x from 1 to 32 threads on the index route (2.19 s to
  0.23 s), 6.2x on the build route. (3) Source group ratio 1 (group everything): 5.0x slower at 65536 pairs; ratio
  16 equals 4. (4) Per-source kernel for one pair: 4.4-5.2x faster than lanes on the index route, 2x on build.
  (5) Reach vs pair list, 1024 sources x all: 11.0x (build) and 14.6x (index). (6) Reverse side, 65536 sources to
  one destination, index: 2.62 s to 12 ms; forward-only with the reverse side present equals the baseline. (7) The
  reverse side costs the forward sources x all shape 1.44x on the index route (231 to 332 ms at 1024 sources) and
  1.25x at 128 sources; random pairs unaffected. To investigate (W28): what the forward search pays for the
  reverse partitions being present (snapshot packing or partition loading). (8) Forcing dense or sparse frontier
  expansion: 1.5-1.8x slower for sources x all; no effect on pairs. (9) Doubling partitions: 1.3x slower on
  sources x all, flat on pairs. (10) Within noise: dedupe, early termination on these shapes, snapshot cache with an
  empty change log. Scheduling study configs added to the harness (`microbench --configs scheduling`: workers per
  batch 1..16, groups 2..16 at one worker, grouped off).
- 26 Oct (evening): W31 scheduling sweep at SF10, random pairs, 3 repeats (`microbench --configs scheduling`).
  Workers per batch w with 32/w groups; baseline is w=32 (one group). Build route, 2048 / 16384 / 65536 pairs:
  w=32 0.12 / 0.78 / 3.03 s; w=16 0.14 / 0.51 / 1.91; w=8 0.20 / 0.42 / 1.48; w=4 0.32 / 0.40 / 1.32; w=2 0.49 /
  0.61 / 1.23; w=1 0.57 / 0.79 / 1.23; grouping off 0.12 / 0.46 / 1.72. Index route: w=32 0.09 / 0.66 / 2.63 s;
  w=4 0.26 / 0.32 / 1.09; w=1 0.42 / 0.59 / 0.80; grouping off 0.09 / 0.31 / 1.07. Capping the groups at w=1
  (2/4/8/16 groups at 65536 pairs: 8.9 / 4.8 / 2.7 / 1.7 s build) shows the cost is linear in the batches a group
  runs in sequence. The optimum is w = threads / batches at every point (1 batch: 32; 8 batches: 4; 32 batches: 1),
  and grouped with the rule beats grouping off (0.80 vs 1.07 s on the index at 65536 pairs: one working set per
  group instead of one per batch). Rule implemented: `GetGroupedWorkersPerBatch` takes the batch count, groups are
  bounded by a working-set budget of a quarter of the buffer manager's memory (192 B per vertex per group), spare
  threads go back to the batches when the budget binds. Headline effect at SF10, 65536 pairs: index 2.6 s to about
  0.8 s, build 3.0 s to 1.2 s; SF100 and SF300 to be re-measured.
- 26 Oct (evening): reverse-side forward cost (finding 7 above) not reproducible with freshly built indexes. Both
  SF10 indexes dropped and recreated, index route, 3 repeats: 128 sources x all baseline 45 ms vs reverse 45 ms;
  1024 sources 232 ms vs 232 ms. The 1.44x row came from a reverse index created while the 22 Oct baseline index
  (storage format 1) coexisted; the forward-index rows measured in the same window were normal, so the cause is
  not the second index as such. Unexplained; watch the SF100 microbenchmark, which recreates the situation. Rule for
  the frozen rerun: every index built fresh (the repro scripts start from an empty data directory).
- 26 Oct (evening): W31 rule verified at SF10 (`microbench_rule_sf10.out`): the new default equals the best explicit
  setting at every point (build 0.12 / 0.40 / 1.22 s, index 0.09 / 0.32 / 0.80 s at 2048 / 16384 / 65536 pairs;
  grouping off now 1.3-1.4x slower at 65536). Thread scaling on the index at 65536 pairs is monotonic again: 12.6 /
  6.3 / 3.2 / 1.7 / 1.1 / 0.80 s for 1 / 2 / 4 / 8 / 16 / 32 threads (15.7x), build 6.5 / 3.6 / 2.1 / 1.5 / 1.2 /
  1.2 s. The 1-thread index points are 2-3x slower than in the morning run (12.6 vs 5.8 s; sources x all 1024: 6.5
  vs 2.2 s) because the SF10 index was rebuilt fresh in between: the partition count is fixed at CREATE INDEX from
  the creating session's threads (64 partitions at 32 threads), and a vertex's adjacency is spread over every
  partition (partitioned by destination range), so a per-source expansion pays one lookup per partition per
  frontier vertex. Check: an index built with `SET threads=4` (8 partitions) answers the 1-thread sources x all
  query in 2.96 s against 6.47 s with 64 partitions; both 0.23 s at 32 threads. So the 22 Oct index had fewer
  partitions than today's (built with an older geometry), which also explains the morning's "reverse side costs
  1.44x" row (fresh 64-partition index vs old index). Consequences: (a) the thread-scaling microbenchmark on the
  index route measures the geometry of the index as much as the kernel; report it with the partition count and
  build indexes with the thread count they will be queried with, or (b, W32 candidate) decouple the index geometry
  from the creating session (e.g. partitions from the vertex count and the machine's core count) and give the
  per-source kernel a per-vertex partition bitmap so sparse expansion touches only partitions that hold edges of
  the vertex. SF100 indexes dropped so the SF100 microbenchmarks build them fresh.
- 27 Oct (morning): overnight chain done (`microbench_sf100.out`, `microbench_sched_sf100.out`,
  `microbench_sched_sf300.out`; tables in `~/sweep/microbench_*_tables.md`; all points agree). SF100
  microbenchmarks (fresh indexes, 3 repeats, build / index): (1) scheduling, 65536 pairs: grouped (one group of 32
  under the old default; the same time as 32 one-worker groups under the 26 Oct rule) 24.1 / 13.6 s, per-batch
  events 15.5 / 7.5 s (0.64x / 0.55x); the SF10 result inverts at SF100 and SF300 (index 40.8 s grouped vs 23.3 s
  per-batch events at SF300). Capping the groups at one worker shows why: 2 groups answer 65536 pairs in 11.7 s on
  the index and 32 groups in 13.5 s, so 32 concurrent single-worker sweeps gain nothing over 2: each group streams
  its own visit array (V x 64 B: 4.7 MB at SF10, 29 MB at SF100, 75 MB at SF300) and 32 of them exceed the 128 MB
  L3; per-batch events let the threads of one level share one working set. Rule now: grouped (w = threads /
  batches) while min(batches, threads) x 192 B x V <= `experimental_path_finding_operator_grouped_batches_budget`
  (default 512 MB: SF10 32 x 14 MB = 450 MB grouped, SF100 32 x 86 MB and SF300 32 x 225 MB per-batch events;
  16384 pairs at SF100 8 x 86 MB = 690 MB per-batch events, 1.9 vs 3.2 s), strategy string records
  `grouped_working_sets_mb`. For the paper figure: time vs scheduling at SF10 / SF100 / SF300 shows the crossover
  between per-level barrier cost (small graphs) and working-set locality (large graphs). (2) Reach vs pair list,
  1024 sources x all: 6.0x (build, 4.1 vs 24.8 s) and 10.0x (index, 2.45 vs 24.6 s). (3) Reverse side, 65536
  sources to one destination, index: 13.5 s to 27 ms; no forward cost (sources x all 2.42 vs 2.45 s), so the SF10
  1.44x row did not recur with fresh indexes. (4) Per-source kernel for one pair over the index: 8 ms vs 87-96 ms
  with lanes (11-12x); build 110 vs 215 ms. (5) Source group ratio 1 at 65536 pairs: out of memory (65536 groups x
  450K x 8 B distance arrays); the ratio of 4 is what prevents that; 16 equals 4. (6) Forced dense or sparse
  expansion: 1.4-1.5x slower on sources x all, flat on pairs. (7) Doubling partitions on the build route: 1.3x
  slower at 65536 pairs and on sources x all. (8) Threads 1 to 32, index: 65536 pairs 139 to 13.6 s (10.2x),
  sources x all 1024 52.5 to 2.45 s (21x, the 1-thread point carrying the 64-partition lookup cost of the 26 Oct
  geometry note); build 102 to 24 s and 47 to 4.1 s. (9) Within noise: dedupe, early termination, snapshot cache
  (empty log), threads_per_batch=4 at 65536 pairs (0.87-0.90x, consistent with the per-batch-events finding).
- 27 Oct (afternoon): W31 step 2, the adaptive batch scheduler. Measurements first (`~/sweep/sched_probe.txt`,
  perf stat on the 65536-pair index query, SF100): K independent single-worker streams scale linearly to 8 (141.9
  s at K=1, 19.4 s at K=8), bend at 16 (11.5 s) and hit a wall at 32 (10.4 s) where cache misses jump from 10.5 G
  to 25 G and IPC from 1.15 to 0.53; misses stay flat up to 16 streams although 16 visit arrays are 464 MB (not an
  L3 capacity cliff but the memory system saturating). All 32 threads busy: 1x32 19.0 s with 22% more instructions
  (barrier spin: 46% of cycles in RunPhase outside Explore), 4x8 10.9 s, 32x1 10.4 s, per-batch events 7.7 s with
  as many misses as 32x1 but IPC 0.69 (pooled partition tasks keep every core productive). SF10: per-batch events
  have 3x the misses of 32x1 (2.7 vs 0.9 G), the cost of allocating and zeroing a working set per batch. Lane
  width 256 (W33 first row): identical times and miss counts to 512 at SF10 and SF100, so bytes per vertex are not
  the lever; the traffic is the edge sweep and one visit line per source. SF1 / SF3 65536 pairs: grouped 0.30 /
  0.23 s vs per-batch 0.29 / 0.41 s (index). Design implemented (`grouped_iterative_length_event.*`,
  `phased_kernel_event.*`): worker groups take batches from a queue and share pooled, reusable working sets; the
  window of live groups is an admission controller fed by edges swept per second (one shared increment per worker
  per phase): grow by doubling while the rate improves > 10% (measured only after every live group finished a batch
  under the window and >= 4 graph sweeps), step back once on a > 10% drop, else hold; groups spawn further groups as
  new event chains (a runtime-added event is only scheduled through a completing dependency, so a spawned event
  depends on its spawner; the first version without that dependency hung, found with gdb). Two regimes by the
  working-set budget (512 MB): cache resident starts wide with one worker per group (SF10 65536 pairs 0.67 s, better
  than the 0.73-0.80 s before because the counter contention is gone); memory bound starts at 4 groups with the
  level tasks of all groups pooled over every thread (`batch_oversubscription`), traces SF100 `4 > 8 > 16 hold`,
  SF300 `4 > 8 hold`. 3 repeats, index, 65536 pairs: SF100 8.03 s vs 7.65-7.74 s per-batch events, SF300 20.4 vs
  20.1 s, holding 16 / 8 working sets instead of 32 (1.4 / 1.8 GB instead of 2.8 / 7.2 GB). Own threads per group
  above the budget stays at 11 s (SF100) whatever the window: pooling the tasks is what closes the gap. Default now:
  the grouped scheduler in both regimes; per-batch events remain the microbenchmark switch
  (`grouped_batches=false`). Strategy string: `regime`, `grouped_working_sets_mb`, `window_trace`. Open: the
  learned window per graph (store in the cache entry), and whether the budget constant can be replaced by starting
  wide and shrinking on a measured drop.
- 27 Oct (midday): adaptive scheduler default verified against per-batch events, 3 repeats, both routes
  (`microbench_default_sf*.out`). Index 65536 pairs: SF10 0.75 vs 1.23 s, SF100 7.97 vs 7.64 s, SF300 20.3 vs 20.1
  s; build 65536: SF10 1.00 vs 1.92 s, SF100 12.5 vs 12.2 s, SF300 39.8 vs 39.7 s; sources x all unchanged (the
  per-source kernel). Weak point found: SF100 16384 pairs (8 batches, memory-bound regime) 2.60 vs 1.96 s on the
  index and 4.67 vs 3.24 s on the build route, because the ramp from 4 groups spends half the query's batches at
  window 4. Fix: the ramp starts at 8 groups (SF300's knee; below 8 the window is never the bottleneck). Frontier
  summary bitmap and visit-line prefetch written (W31 step 3, memory traffic of the explore phase), with the
  microbenchmark group `memory`; lane-activity instrumentation now records the vertices with any lane active per
  level.
- 27 Oct: paper Sections 3 and 4 audited against the code (26 Oct, two claim-by-claim passes; eleven corrections
  applied, `44aa6ccd`, `74534530`). Decision: repeat the audit on the frozen commit after the memory-bandwidth work on
  the multi-source traversal and the other open optimizations have settled; recorded in `paper/NOTES.md`.
- 27 Oct (midday): frontier bitmap and prefetch measured (`microbench_memory_sf10/100.out`, 3 repeats). SF100, the
  per-source bitmap test on every level: index 65536 pairs 6.35 s vs 7.76 s without (1.22x), 16384 pairs 1.91 vs
  2.62 s (1.37x); build 9.46 vs 12.87 s (1.36x) and 2.94 vs 4.63 s (1.57x). SF10: the test costs 10% at 65536
  pairs (0.83 vs 0.75 s) and gains 5-8% at 2048 / 16384. Prefetch (4 / 8 / 16 rows ahead): within 3% everywhere,
  default off. The bitmap test follows the scheduler's regime (`frontier_bitmap_filter` -1: on when memory bound,
  off when cache resident; 0 / 1 force). Activity profile of one SF100 batch (eight waves): levels 1-2 and 8-18
  have 0.0-13% of vertices with any active lane (one lane each at the tail: straggler pairs at distance 10-18),
  levels 3-7 46-97%; a wave spends 11 of 18 levels sweeping the CSR for a handful of vertices. Sparse-level path
  written: a level with active x divisor < V (divisor 64, the per-source kernel's) expands the active list (from
  the bitmap) by row lookup per partition instead of the sweep; first level of every wave included. Lunch chain
  (`run_lunch_chain.sh`) measures it. Note: the author merged paper commits (`4dd78ba8`, operator section
  corrected against the code at `64bc8f54`); the scheduling text there predates the adaptive scheduler and the
  regime rules and needs a pass once the defaults settle.
- 27 Oct (early afternoon): sparse levels measured (`microbench_memory2_sf*.out`, 3 repeats, `memory` group).
  Index 65536 pairs, plain explore -> bitmap filter + sparse levels: SF10 0.78 -> 0.68 s (1.14x), SF100 7.9 -> 5.8 s
  (1.37x), SF300 20.7 -> 12.3 s (1.68x); build SF100 12.6 -> 8.6 s (1.47x). Split: the per-source bitmap test is the
  larger part at scale (SF300 65536: 13.3 s without it), the row-lookup sparse levels add 5-13% (SF10 65536 773 ->
  684 ms, SF300 13.2 -> 12.3 s). Divisor 16 (active < V/16 takes the sparse path) beats 64 by 5-10% at the
  16384-pair points, 256 is worse: adopted as its own option `sparse_level_divisor` (the per-source kernel keeps
  `sparse_frontier_divisor` = 64). Prefetch neutral (default 0). The bitmap test pays even for a single SF100 batch
  (2048 pairs: 526 vs 554 ms), so its rule is the size of one working set (> budget / 16 = 32 MB) rather than the
  scheduler's regime; recorded as `bitmap_filter` and `sparse_level_divisor` in the strategy string. Headline after
  W31 steps 2-3 (index, 65536 random pairs): SF10 0.68 s (was 2.6 s on 25 Oct), SF100 5.8 s (was 16.9 s), SF300
  12.3 s (was 55 s). Pending: ramp-start check and activity profiles at SF10 / SF300 (lunch chain); commit.
- 27 Oct (noon): ramp-start check after the explore changes (`microbench_ramp8_sf100/300.out`, 3 repeats): SF300
  adaptive default = per-batch events on both routes (index 12.25 vs 12.13 s, build 20.3 vs 20.3 s at 65536 pairs);
  SF100 per-batch ahead again (index 5.33 vs 5.86 s, 16384 pairs 1.58 vs 1.93 s; build 8.13 vs 8.60, 2.43 vs 3.46
  s). With 8 batches both schedulers run all 8 concurrently, so the 25% gap at 16384 pairs is not the window; to
  isolate (fixed 8x32 grouped vs per-batch, phase timings). Activity profiles: SF10 waves reach level 17, SF300
  level 26 (19 of 26 levels under 2% active); dense core at levels 3-7 at every scale factor.
- 27 Oct (early afternoon): the SF100 scheduler gap isolated by alternating the variants at 16384 pairs: adaptive
  default 1.69 / 1.76 s, fixed 8 groups x 32 pooled tasks 1.50 / 1.53 s, per-batch events 1.57 / 1.52 s. The
  default's strategy string showed 8 working sets of 41 MB (332 MB, under the 512 MB budget), so the regime rule
  said cache resident and gave each group its own 4 threads; pooling is what the point wanted. The two decisions
  are now separate: own threads only while one working set is <= budget / 16 (32 MB; SF10's 13 MB), pooled tasks
  otherwise (`tasks` in the strategy string); the window starts at all groups while the total fits the budget and
  ramps from 16 otherwise (fixed 8 / 16 / 32 groups are within 2% at SF100 and SF300 after the explore changes;
  the ramp from 8 cost 5%). Fixed 32 x 32 equals per-batch events exactly (5.16 s), so the grouped mechanism has no
  inherent cost. Working-set sizes as the engine computes them from the index dictionary (vertices with edges):
  SF10 13 MB, SF100 41 MB, SF300 about 110 MB per group; earlier notes quoting 86 / 225 MB used person counts
  (68.7K / 473K / 1.19M persons; 1.84M / 18.7M / 55.7M knows edges).
- 27 Oct (afternoon): final scheduler default verified (`microbench_final_sf*.out`, 3 repeats, both routes, 2048 /
  16384 / 65536 pairs, every point agreeing, no errors). Default vs per-batch events, index: SF10 0.68 vs 1.12 s
  (1.64x), SF100 5.20 vs 5.20 s, SF300 12.33 vs 12.37 s; build: SF10 0.91 vs 1.56 s (1.70x), SF100 8.24 vs 8.09 s
  (0.98x), SF300 20.3 vs 20.3 s; 2048 and 16384 pairs within 3% everywhere. W31 steps 2 and 3 closed: one
  scheduler at every scale, equal to the best fixed shape where it is not better. Headline, index route, 65536
  random pairs: SF10 0.68 s, SF100 5.20 s, SF300 12.3 s (25 Oct archive: 2.6 / 16.9 / 55 s). Next: W32 partition
  geometry microbenchmark (option `partition_vertices`, harness group `partitions`, one index per geometry).
- 27 Oct (afternoon): W32 partition geometry measured (`microbench_partitions_sf10/100.out`, 3 repeats, option
  `partition_vertices`, one index per geometry). Partitions at SF10 / SF100: default 2 x threads = 64 / 64;
  4096-vertex 17 / 116; 8192 9 / 58; 16384 5 / 29; 32768 3 / 15; 65535 2 / 8. Index route, 65536 pairs: SF10 675
  -> 387 ms (65535-wide, 1.74x), SF100 5.28 -> 3.45 s (1.53x); sources x all 1024: SF10 228 -> 79 ms (2.9x), SF100
  2.37 -> 1.11 s (2.1x); build route the same direction (SF100 8.28 -> 4.30 s, 4.09 -> 2.18 s). The cost: a
  single-batch lane query gets its parallelism from the partitions, so 2048 pairs go SF10 103 -> 262 ms, SF100 528
  -> 1100 ms (2-2.5x slower); 4096-vertex partitions are slower than the default everywhere at SF100 (more visit
  re-reads, no more parallelism). Both routes fix the geometry before the pair shape is known (the build route
  radix-partitions edges as they arrive, the index at CREATE INDEX), so the choice cannot follow the query. W32
  step 2: decouple a batch's parallelism from the partition count with (partition, destination sub-range) explore
  and check tasks when a level has fewer partitions than workers, then default to the coarsest geometry (65535
  vertices per partition). Default unchanged until then.
- 27 Oct (late afternoon): W32 step 2. Destination sub-range tasks: when a query's batches x partitions leave
  threads idle (the scheduler's hint threads / (partitions x batches)), each partition's destination range is split
  into sub-ranges and explore / check tasks are (partition, sub-range) pairs with disjoint writes; the build's radix
  sort now orders every row's destinations (two LSD passes on the 16-bit local destination before the source
  passes) so a sub-range task binary-searches its slice of a row instead of scanning the row. Two bugs on the way: the
  group scheduled its own worker count while the batch had sized its arrays for the partition count (out-of-bounds
  writes, one failed test), and the first rule split every batch of a 32-batch query. SF100, 1 repeat, 8 partitions
  vs the 64 of the thread rule: index 2048 pairs 531 -> 816 ms (1.54x slower; 1.09 s without sub-ranges), 16384
  pairs 1.42 -> 0.99 s, 65536 5.12 -> 3.34 s, sources x all 2.27 -> 1.05 s; build 682 -> 927 ms, 2.25 -> 1.34 s,
  8.11 -> 4.28 s, 3.91 -> 2.14 s. The single-batch penalty that remains is inherent to the geometry (every task
  reads every row header and visit line of its partition); it is the price of 1.5-2.2x on the set-oriented shapes
  and the per-source kernel, which is the paper's thesis. Default now 65535 vertices per partition
  (`partition_vertices`; <= 0 is the thread rule, microbenchmark `threads_rule_partitions`). Verification at SF10 /
  SF100 / SF300 with fresh indexes follows.
- 27 Oct (end of afternoon): W32 closed. Default geometry: 65535 vertices per partition with a floor of eight
  partitions (SF10 8, SF100 8, SF300 19 instead of 64); the per-source kernel's dense sweep splits a partition's
  rows between workers when partitions < workers (discovery is a compare-and-swap, so row ranges are safe), the
  lane kernel splits destination sub-ranges with rows sorted by destination. Verification vs the thread rule
  (`microbench_floor_sf10/100.out`, `microbench_coarse_sf300.out`, 3 repeats), "faster" / "slower" of the default:
  index 65536 pairs SF10 1.48x faster (446 vs 658 ms), SF100 1.51x (3.43 vs 5.18 s), SF300 1.21x (10.0 vs 12.2 s);
  16384 pairs 1.23x / 1.53x / 1.16x faster; 1024 sources x all 2.07x / 2.09x / 1.28x faster (112 ms, 1.11 s, 3.97
  s); 1 pair and 1 source level (3-10 ms, 18 vs 15 ms at SF100); 128 and 2048 pairs (one batch) 1.3-1.6x slower
  (SF100 2048: 831 vs 507 ms). Build route: 65536 pairs 1.64x / 1.92x / 1.56x faster, sources x all 1.78x / 1.84x /
  1.45x faster, 2048 pairs 1.25-1.4x slower, 1 pair and 1 source 1.1-1.35x slower because the per-query CSR build
  sorts one task per partition (W32 step 3, open: a parallel per-partition sort would remove that). The single-batch
  penalty is inherent to coarse partitions (every task reads every row header and visit line of its partition) and
  is the price of the set-oriented gains; the `partitions` group documents the trade-off. Headline, index route:
  SF10 65536 pairs 0.45 s, 1024 sources x all 0.11 s; SF100 3.43 s and 1.11 s; SF300 10.0 s and 3.97 s.
- 27 Oct (evening): freeze preparation. Dead option `task_size` removed (`1cc48dc2`); `repro/environment.sh` records
  every operator option default from the built extension (`repro/ENVIRONMENT.txt`); switches document gets a terms
  section (shape classes by counts and kernel; every ratio with its direction). Soak of today's changes on the shapes
  the microbenchmarks did not cover, DuckPGQ only, SF10 and SF100 (`~/sweep/run_soak.sh`, log `~/sweep/soak.log`):
  sweep-pairs and ldbc-queries with the counts compared against the archived CSVs (`compare_counts.py`), the index
  group (reverse side, same destination), update-run sssp and point-to-point (190 x 2000) and update-concurrent with 4
  readers. Tag `eval-freeze-rc1` and the frozen rerun follow a clean soak. Left for after the paper's numbers, with
  reasons: W32 step 3 (parallel per-partition sort; build route single searches 1.1-1.35x slower than the thread
  rule), the learned scheduler window per graph (changes no number), W34 locality ordering (unmeasured), W28 step 4.
- 27 Oct (evening): soak clean (`~/sweep/soak.log`, build `1cc48dc2`, 9 minutes): SF10 and SF100 sweep-pairs (44
  points each) and ldbc-queries (60 points each) agree with the archived counts, 0 mismatches; index group (reverse
  side, same destination) 6 points agree; update-run sssp and point-to-point, 190 x 2000, verified with 0 mismatches
  on build, index_auto and index_nofold; update-concurrent 4 readers 7696 events, 190 commits, 0 mismatches, 0
  errors (index query mean 15.5 ms, build 25 ms, commit 32 ms). The repro matrix's microbench step now also runs the
  `memory`, `partitions` and `scheduling` groups (SF300 scheduling on the index route). Ready to tag
  `eval-freeze-rc1`.
- 27 Oct 15:07 UTC: tag `eval-freeze-rc1` (`7e9778c0`); frozen rerun started (`repro/run_all.sh`, log
  `repro/logs/run_all.out`, one `.done` marker per step). Every benchmark index was dropped beforehand so the run
  builds them under the tagged geometry; containers up (Memgraph 3.14.0, FalkorDB 4.2.2, Neo4j 5.26.32 Enterprise
  with GDS 2.13.14). Expected about 3.5 days; nothing else runs on the machine meanwhile. Afterwards:
  `./repro/collect.sh`, commit `repro/results` and `paper/`, compare with the 25 Oct archive.
- 27 Oct (paper): after the frozen sweep on eval-freeze-rc1, verify every number in the paper against the sweep's
  tables, figures and CSVs and make the text cite its source; replace the laptop numbers in Section 3. Checklist in
  `paper/NOTES.md` under "TODO after the frozen-commit sweep".
- 28 Oct: frozen run steps 1-6 done (sweeps and LDBC at all three scale factors, 20 h, no errors, every point
  agreeing). Decided: the LDBC table and figure report medians (the baselines' first repeats are cold: Kuzu IC13 at
  SF300 mean 186 ms vs median 51 ms, Memgraph BI20 at SF10 578 vs 275 ms; DuckPGQ medians equal its means because
  every query runs once untimed in its process). To do after the run, together with `collect.sh`: `ldbc-queries-table`
  and `ldbc_figure` on medians, raw rows unchanged.
- 28 Oct (afternoon): the author's audit of the frozen commit found index bugs (tests in `4bad0c97`, merged here as
  `05f80f7f`, failing on rc1): (1) fold order, an older transaction folding after a newer one installs the older
  epoch as newest and compaction drops entries only the newer one covered; (2) row-id reuse after a failed commit,
  a reverted append's log entry becomes visible once the row id is reused by a committed row; (3) two readers that
  differ only in a fully deleted change group share a cached snapshot (the cache key was the newest visible group,
  and a fully deleted group is decided per row); plus two performance notes, duplicate overlay edges for a snapshot
  on the newest epoch while the older one is retained, and a tombstone for a row still in the log. Decision: the
  rc1 run continues (none of these touch the read path with an empty log); fixes written in the working tree,
  unbuilt until the run ends: Fold skips when the newest epoch is not older than the folder's view; Append marks a
  reused row id's previous entry DEAD (invisible, excluded from folds, dropped by compaction and serialisation);
  the snapshot skips log entries its epoch already contains; the cache key hashes every per-entry visibility
  decision. Then build, suite incl. the three new tests, tag `eval-freeze-rc2`, rerun the full matrix (about 1.5
  days); rc1 serves as the validation set. The author's paper commits (sections audited against the frozen commit)
  came in with the same merge.
- 29 Oct (morning): rc1 matrix finished 28 Oct about 23:00 UTC (35 steps, 32.4 h of step time, every step exit 0,
  every point agreeing); `collect.sh` run, archive, figures and tables committed (`7c65bed0`). Index fixes built and
  tested: 95 test cases pass including the three audit tests (`2517ebc2`); soak on the fixed build: update-run
  (190 x 2000, 0 mismatches on all three configs, index query mean 18-23 ms, commit 15 ms), update-concurrent 4
  readers (7834 events, 0 mismatches), read-only spot check equal to the rc1 counts. Tag `eval-freeze-rc2`; the full
  matrix reruns on it (rc1 is the validation set: every read-only number should reproduce within noise).
- 29 Oct: two DuckDB-native baselines added to the matrix (author's request): `duckpgq_recursive` (WITH RECURSIVE ...
  USING KEY, shortest lengths by keyed minimum, depth cap 1000) and `duckpgq_scalar` (the CIDR 2023 UDF route:
  create_csr_edge / create_csr_vertex + iterativelength per pair). Driver `scripts/native_baselines_sweep.py` on top
  of the harness (kept outside `pathfinding_benchmark.py` while the rc2 matrix re-reads it per step; to be folded in
  after the run), same pair tables, repeats, timeout and CSV as `sweep-pairs`, the index route in the same sweep for
  the agreement check, no warm-up query for the native routes. Smoke at SF1: all 5 points agree across the three
  routes. Queued as `repro/run_native_baselines.sh`: waits for "matrix finished", then SF10 and SF100, random pairs
  and sources x all, 32 threads, 600 s, 3 repeats. Report: time per point and the first timed-out point per shape.
- 29 Oct: teaser figure gains DuckDB's recursive CTE (author's request), drawn in ink rather than a system colour
  as the in-engine reference, labelled at its timeout triangle ("DuckDB recursive CTE: > 600 s at 1k pairs" on the
  SF10 probe, 4 threads during the matrix: 128 pairs 15.8 s, 1024 pairs over 120 s; sources x all 128 in 13.6 s vs
  0.11 s on the index). The SF100 line appears once the native-baselines step has run after the rc2 matrix;
  `collect.sh` re-renders it. Caption in `paper/sections/introduction.tex` needs the recursive CTE sentence then
  (author's file). DuckDB version under test: v2.0.0-dev1 at submodule `45f4fdf3`, to be cited as the commit.
- 29 Oct (late morning): rc2 run stopped at 1.6 h (author's check): the rc1 SF300 single-source sample had no
  outgoing edge, so that point measured nothing (Memgraph 2 ms, Neo4j 141 ms there); the SF10 single pair has no
  path (valid, the text says so); the LDBC table and figure now use medians (the author implemented it); result files
  are ordered by the run time in the file name, not by modification time (the author's fix; the last such sort in the
  updates figure fixed here). Changes for the restart: multi-source samples require an outgoing edge
  (`ensure_multi_source_table`, deterministic hash order kept), the multi-source tables, their exports and every
  benchmark index dropped at all scale factors; the two DuckDB-native baselines are regular harness systems
  (`duckpgq_recursive`, `duckpgq_scalar`, class `DuckPGQNativeSystem`) with a `native` step in `run_all.sh` (SF10 and
  SF100 with the index route for the agreement check); table names "DuckDB recursive CTE" and "DuckPGQ 2023 UDFs".
  Smoke at SF1: all points agree across the three routes. Sources identical to rc2; tag `eval-freeze-rc3` on the
  harness state; full matrix restarted.
- 29 Oct (midday): rc3 stopped minutes in for the author's list. In the tree now: medians in the sweep tables, the
  figures and the teaser (`load()` summarises repeats by the median; the harness printers too; the LDBC table stays
  median per parameter set, mean over sets); `fixed-costs` subcommand and matrix step (database file size, CSR index
  build time x3 and bytes per edge from the new `duckpgq_csr_index_size`, every baseline's fresh load time and store
  size: Kuzu database file, Memgraph process resident memory, FalkorDB graph memory, Neo4j store directory after a
  checkpoint; SF1 smoke: index 11.9 B/edge, Kuzu 104, FalkorDB 42, Neo4j 33 B/edge); per-row visibility switch
  `duckpgq_csr_index_change_groups` (false decides every logged row by its own row version) with the update-run
  configuration `index_per_row` for the per-row cost claim; the unpaced concurrent writer step (4 readers, pause 0);
  Section 3 switches as the microbenchmark group `section3` (1 pair with and without the per-source kernel, 20
  sources x 200 destinations as the `all_pairs` shape with source grouping on and off, paths vs lengths through
  `shortestpathoperator`, all agreeing at SF1). MAGE: the Memgraph image's query modules (community detection,
  PageRank, node2vec, text and vector search) include no shortest-path procedure; the built-in BFS the harness uses
  is Memgraph's path operator. Sources: one new option and one new function (95 tests pass). Tag `eval-freeze-rc4`,
  full matrix restarted.
- 29 Oct (afternoon): lane-per-source MS-BFS waves (author's diagnosis of BI19: 1.4K sources x 1.7K destinations at
  SF300 ran 1.4K single-source searches, Neo4j's work in parallel; one lane per pair would need 4.6K waves). New
  kernel `LaneWaveState` on the lane kernel's explore and check phases (virtual hooks `DetectReached`,
  `MarkUnreachable`, `StartNextWave` in `IterativeLengthState`; finished lanes are masked out of `next` in the check
  phase, Then et al.): a wave is up to 512 source groups, one lane each, with the group's destination list; after
  every check phase a lane records the level of newly reached destinations and stops when none is left; groups in
  lane mode keep a destination-distance map instead of the per-vertex arrays. Rule: lengths, more groups than the
  few-sources limit (`experimental_path_finding_operator_lane_waves`, default true; `source_group_kernel` and
  `lane_waves` in the strategy string). 96 tests pass (new `path_finding_lane_waves.test`). BI19 on the index, all
  counts equal to the rc1 archive: SF10 61 -> 15 ms (4x faster), SF100 3.6 s -> 252 ms (14x faster), SF300 30.6 s
  -> 1.6 s (19x faster; Neo4j 46 s, so 29x faster than Neo4j instead of 1.5x); build route SF300 39.4 -> 2.6 s.
  Section 3 microbenchmarks (`section3` group, 1 repeat): 20 sources x 200 destinations on the index SF10 20 ms lane
  waves vs 13 ms per-source searches (1.5x slower at this size) vs 86 ms lane-per-pair; SF100 79 vs 91 ms (1.15x
  faster) vs 509 ms; one pair on the index: per-source kernel 4 / 9 ms vs lanes 13 / 45 ms (SF10 / SF100); paths
  vs lengths at 2048 random pairs: 3.6x / 4.5x slower (index), 3.9x / 5.3x (build). Tag `eval-freeze-rc5`, matrix
  restarted.
- 29 Oct (evening): W30 brought forward (author's request before the rc5 matrix restarted; rc5 stopped after its
  three fixed-costs steps). Three Graphalytics graphs join the matrix: cit-Patents (3.77M vertices, 16.5M edges,
  directed, 55% of the vertices have an outgoing edge, 86% an incoming one), graph500-24 (8.87M vertices, 521M
  directed edges after symmetrising the undirected generator output, hubs) and datagen-8_4-fb (3.81M vertices, 539M
  directed edges, dense LDBC family); download and DuckDB materialisation took four minutes in total
  (`prepare --graphalytics-datasets`, `repro/prepare.sh`). Harness: a benchmark target is an LDBC scale factor or a
  Graphalytics dataset (`--dataset` on `sweep-pairs`, `microbench`, `fixed-costs`; `sf_name`/`db_path` dispatch,
  the Graphalytics databases carry the LDBC table names so every driver and baseline loader runs unchanged, an empty
  update log stands in for the LDBC one). Sampling (author's rule, all targets): sources are drawn from the vertices
  with an outgoing edge and destinations from those with an incoming edge, deterministic positions in the candidate
  lists, so the LDBC random-pair tables change too (15% of SF10 persons have no outgoing knows edge); the cached pair
  exports and Kuzu snapshots were removed and rebuild on first use. Hop bounds: LDBC keeps `..30`; on the
  Graphalytics graphs the Cypher systems run unbounded and Kuzu's cap (a binder error above 30) is lifted per
  connection with `CALL var_length_extend_max_depth=1000` (`kuzu_connect`). `sweep-pairs` prints the graph's
  sampling populations and the reachable fraction per point; the table printer adds a reachable-pairs row and
  vertices reached per source. Matrix subset `graphalytics` in `run_all.sh`: fixed costs and the full sweep plus the
  32-client pass on cit-Patents for every system (16.5M edges, every baseline loads in minutes: Kuzu 2 s, Neo4j 282 s),
  DuckPGQ and Kuzu on the two large graphs, the scheduling microbenchmark on graph500-24 (index route; one wave's
  working set, 192 B x 8.87M vertices = 1.7 GB, is three times the 512 MiB budget, so the adaptive scheduler starts
  in its memory-bound regime). Figure `graphalytics` (plot script, two shapes x three graphs) and the archive
  directories `repro/results/graphalytics_*`.
  Smoke on cit-Patents (1 repeat, 300 s timeout): DuckPGQ index and Kuzu agree on all 5 points; 1024 random pairs
  index 0.60 s vs Kuzu 40.1 s (67x slower than the index); 16 sources x all: index 46 ms, Kuzu 76 ms. Two findings
  for the writing: (1) even with the edge-aware sample 0.0% of the random pairs are reachable on cit-Patents (a
  citation DAG: a source reaches about 2200 vertices, 0.06% of the graph; LDBC random pairs are 34-38% reachable at
  every SF), so the random-pairs shape there measures exhaustive searches without early termination; the sources x
  all shape does reach the deep levels (16 sources: deepest level 19, median 7, p99 16). (2) The lane kernel has a
  fixed cost per batch proportional to the vertex count: 128 unreachable pairs take 0.29 s on the index (strategy
  `grouped_working_sets_mb=346`, user 0.35-0.5 s and sys 0.2 s for a 0.29 s wall, so a serial allocation and first
  touch of the working set, not memory bandwidth; the same with sparse levels or the frontier bitmap off), Neo4j's
  bidirectional shortestPath answers the same 128 pairs in 24 ms; with one thread the 128 pairs take the same
  0.33 s, and 1024 pairs 0.60 s (one batch, user 1.8 s). Neo4j's
  sources x all form (one shortestPath per destination over 3.77M vertices) takes 30 s per source and times out at
  16 sources. Candidate operator fix after the freeze: pool or lazily touch the per-batch working sets (W31 follow-up).
- 29 Oct (late evening): the author's second list before the relaunch: measure where the index matters, two code
  changes, and two lane-wave points. Done in the tree:
  (1) Lane waves through the worker-group controller (author's point 1): `AppendGroupedKernelEvents` builds the
  controller for a list of lane-kernel states, the MS-BFS batches of a pair list or the waves of a source-grouped
  query, so the window bounds live working sets and the workers per group the tasks (strategy keys `wave_*`;
  `grouped_batches=false` keeps one event per wave as the microbenchmark baseline). BI19 index: SF10 15 -> 17 ms
  (noise), SF300 1.74 s before, see (3) for after. The serial destination check (point 2) is 0.08% of the BI19 SF300
  samples, about 23 ms per run; left as is.
  (2) Single pair at SF300 over the index: the harness's 9-10 ms is the search, not the setup. In one process the
  time per pair ranges 3-17 ms by pair (a 3-4 hop frontier expansion on a 1.19M-vertex, 55M-edge graph; profile:
  Relax 25%, ExplorePartition 21%, ExpandVertex 18%, frontier emplace_back 16%, memset 2%, Clear 2%), so reusing the
  per-source arrays would gain a few percent; Neo4j's 2 ms comes from its bidirectional search. An *empty* search (a
  source without outgoing edges: no dictionary entry, distinct sources 0, so the MS-BFS lane kernel ran) cost 66 ms:
  a serial memset of the 229 MB working set at allocation and again in `Clear()` at the end of the batch; on
  cit-Patents (724 MB) that was the 0.29 s fixed cost of 128 pairs. Fixed: `BFSWorkingSet::Zero` zeroes in parallel
  (one thread per 8 MB), the last wave of a batch marks the set dirty instead of clearing it and the next batch that
  attaches it zeroes it (`HasNextWave`), `FillSeen` fills the seen mask in parallel. Index route after: SF300 empty
  pair 66 -> 6 ms (11x faster), 65536 random pairs 9.2 s (rc1 10.1 s), BI19 1.74 -> 1.26-1.30 s (1.35x faster);
  cit-Patents 128 pairs 0.29 -> 0.056 s (5x faster), 1024 pairs 0.60 -> 0.115 s; SF10 65536 pairs 0.33-0.43 s.
  (3) One snapshot per commit (author's point 2): the profile of 16 readers at SF10 shows BuildSnapshot at 1.4%
  and the DuckDB scheduler and mutexes at about 45% (short queries, CPU contention); at SF300 the duplicated work is
  elsewhere: `ResolveTombstonePositions` scanned all 55M epoch row ids after every commit with deletes, outside the
  lock, so every reader repeated it: 46% of all CPU with 16 readers. Tombstones now carry the deleted edge's keys
  (TryDelete evaluates the key expressions as Append does) and resolve by lookup: dictionary ids, the destination's
  partition, the source's sparse row, the row id among its edges; entries loaded from storage keep the scan. SF300,
  16 readers, 20 commits, index route: mean query 166 -> 69 ms (2.4x faster), p99 359 -> 122 ms, commit 178 -> 101 ms;
  tombstone and snapshot work 0.19% of the CPU. update-run SF10 (30 batches, 4 configs) agrees on every step.
  (4) Found on the way: an undirected index suppressed only one direction of a deleted epoch edge (the old scan kept
  one position per row id; the mirrored edge stayed traversable). Fixed with `mirror_tombstone_positions`; new test
  `csr_index_undirected_delete.test`.
  (5) Measurements added to the matrix: `throughput` subcommand and `run_all.sh throughput` (driver mode
  `--mode throughput`: C clients run single-pair queries round robin over the 1024 random pairs for 20 s, index
  against build, clients 1 2 4 8 16 32, SF10/100/300; W35); SF10 smoke with 4 clients: index 1594 q/s at 2.4 ms
  median vs build 169 q/s at 23 ms (9.4x more queries per second). Concurrent readers at SF100 (100 commits) and
  SF300 (60 commits) with 1, 4 and 16 readers (the SF300 update log is staged from the generator's batches). Figures
  `concurrent` (readers vs latency per SF) and `throughput` (clients vs queries per second per SF) in the plot script.
  Section 6 should open with the one-thread result (index 6x faster under updates), the author's text.
- 29 Oct (night): simplification of the kernel choice (author's trace of both routes; "section 3 is hard to read
  because of the different modes and schedulers"). Lengths now always group by source: a few distinct sources (the
  few-sources limit, 16) run the per-source frontier kernel, more run as MS-BFS waves of one lane per source with
  its destination list, scheduled by the worker-group controller like any batch list. The ratio threshold
  (`source_group_ratio`: pairs >= 4 x distinct sources) is gone; `source_group_ratio <= 0` keeps the former
  one-lane-per-pair batches as the microbenchmark baseline (`no_source_grouping`; the push-pull and bidirectional
  variants still use them). The group settings are read once per query (`SourceGroupedIterativeLengthState::Settings`,
  65536 groups used to cost about 300,000 setting lookups). Measured first (operator phase timings): the serial
  grouping pass is 6 ms for 65536 random pairs at SF10 and 27-32 ms for BI19's 2.56M pairs at SF300, so it stays
  serial; the result scatter of the grouped path, however, ran on one thread (`MaxThreads` 1 for every mode but
  reach): 406 ms of BI19's 1.3 s were 2.56M distance-map lookups. The pair modes' result scan is now parallel (the
  source threads claim output chunks with `fetch_add`). A/B on the index, 65536 random pairs, grouped waves vs the
  former lanes per pair: SF10 0.40-0.46 vs 0.44-0.46 s (neutral), SF100 3.02 vs 3.28 s (1.09x faster), SF300 9.2 vs
  9.65 s (1.05x faster); build route SF10 0.55 vs 0.56-0.59 s. With the parallel scatter: BI19 SF300 1.3 -> 0.92-1.07 s
  (1.3x faster), 65536 pairs SF10 0.44-0.46 s, SF300 9.15-9.29 s. Section 3 microbenchmark at SF10 (1 repeat, index):
  2048 random pairs 57 ms grouped waves vs 115 ms lanes per pair (2x faster) vs 386 ms per-source searches; 20 x 200
  all pairs 16 ms (waves) vs 62 ms (lanes per pair) vs 13 ms (per-source); every configuration agrees. 97 tests pass;
  update-run SF10 (20 batches, 4 configs) agrees on every step. Scheduler switches act on the waves (`wave_*` keys).
  The matrix is NOT restarted: the author asked to hold the sweep until told, clean up, and use rc1's results
  meanwhile. Tag `eval-freeze-rc7` marks the state.
