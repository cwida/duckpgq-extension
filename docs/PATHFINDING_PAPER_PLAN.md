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
| D5 | Ablations are one-off runs with dates in filenames | Systematic sweeps: orientation ratio, source grouping, dedupe, grouped batches, CSR build variants, threads 1..32 |
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
| W7 | Path reconstruction (B1). Step 1 done 20 Oct (`af26e5ad`): element_id/vertices/edges via the operator over a CSR index; parent vertex + parent edge per vertex in the source-grouped kernel, paths as [key, edge row id, key, ...], delta edges carry their row ids, MATCH derives path_length/vertices/edges from one shortestpathoperator call. 8M edges: 20 sources x 200 destinations with paths 0.24 s vs 0.22 s lengths; 158k paths from one source 0.43 s. Step 2 open: paths on the build-per-query route (endpoint struct with edge row id, row ids through the radix build); MS-BFS kernel stays length-only | op | 5 | P1 | W1–W3 | step 1 done, step 2 open |
| W8 | Index design doc + correctness argument (C1, C2) | index | 4 | P1 | — | draft 19 Oct, `docs/CSR_INDEX_DESIGN.md`; awaiting review (decisions marked **D**) |
| W9 | `IndexType` skeleton: create/bulk build/serialize/load, read-only snapshot (C1) | index | 6 | P1 | W8 | done 19 Oct (`9af62e18`, `16aca50e`, build `337c2ff5`/`6d38d2c9`/`310fb731`; persistence landed in W11) |
| W10 | Commit-time append delta + tombstones, epoch selection by start time | index | 6 | P1 | W9 | done 19 Oct, see git log (overlay via `CanFetch`; companion delta partitions; lazy tombstone positions; readers older than the epoch still error instead of falling back) |
| W11 | Vacuum/epoch fold; checkpoint delta integration; crash-recovery test | index | 5 | P1 | W10 | done 19 Oct: fold (generation-stamped compaction, 2 retained epochs, `duckpgq_csr_index_fold/stats`, ratio setting) and persistence (checkpoint, WAL, restart tests). Not done: checkpoint delta hooks (writes during serialization go to the main index), incremental fold, row-id compression |
| W12 | Operator reads index snapshot; optimizer chooses index vs build (C3) | op+index | 4 | P1 | W9, W5 | partly done 19 Oct: index used whenever a matching bound index exists (pairs become keys, dense-id translation per batch); cost-based choice open |
| W13 | Cost model + calibration (C4) | planner | 3 | P2 | W12, sweeps | open |
| W14 | Remove prototype persistence/eager/triggers (C5) and the per-connection partitioned CSR cache. Done 20 Oct (`a45795db`): -529/+198 lines, 10 prototype tests, 3 benchmark scripts and `docs/CSR_PERSISTENCE.md` removed; without an index every query builds its CSR (`partitioned_csr_build` phase), the cache-only marker form is index-only | index | 2 | P1 | W12 | done |
| W15 | Index tests (snapshot isolation, concurrent writers, rollback, checkpoint, restart, drop) | index | 3 | P1 | W10–W11 | partly done: `csr_index_snapshot.test`, `csr_index_change_groups.test` (21 Oct: commit groups, restart, fold with id shift), `path_finding_concurrent_queries.test_slow` (22 Oct); concurrent writer + readers exercised by the update-concurrent driver (no mismatches at SF10); restart under load and drop while in use open |
| W16 | Update workload: LDBC insert streams into harness; mixed sweeps (D2) | eval | 4 | P1 | W12 | done 21-22 Oct (`update-run`, `update-concurrent`, `update-kuzu`); results SF10/SF100 in the progress log; mixed-sweep matrix for the paper still to run on the frozen commit |
| W17 | End-to-end LDBC IC13/14, BI19/20 in harness for all systems (D3) | eval | 4 | P1 | W5 | open |
| W18 | Neo4j Enterprise trial or parity protocol (D1) | eval | 1 | P1 | — | open |
| W19 | Systematic ablation + scaling sweeps on frozen commit (D5, D6). Decided 20 Oct: the first sweep is DuckDB-only. Harness modes added 20 Oct: `index_build`, `index_operator`, composites `index` and `index_comparison` (build-per-query operator vs index) in `scripts/pathfinding_benchmark.py`; smoke-tested on the SNB 0.003 sample | eval | 4 (+machine time) | P1 | W1–W6 | first DuckDB-only sweep done 21 Oct (SF1-SF300, 5 repeats; `~/sweep/collected*.md` on the EC2 box); to be rerun on the frozen commit after W7 step 2 and W12 |
| W20 | Full read-only system comparison rerun on frozen commit | eval | 2 (+machine time) | P1 | W19 | open |
| W21 | Results archive, version pins, anonymous artifact (D7, E) | eval | 2 | P1 | ongoing | open |
| W22 | Related work + outline + intro draft | writing | 3 | P1 | — | open |
| W23 | Full draft, figures, internal review, polish | writing | 10 | P1 | W16–W20 | open |
| W24 | Teseo/Sortledton or GraphBLAS baseline (D4) | eval | 4 | P3 | W16 | open |
| W25 | WCC table function reads the index snapshot (generality argument, build vs index for a whole-graph algorithm) | op+index | 2 | P3 | W12 | open |
| W26 | Point-query kernel: single-pair/single-source queries paid ~0.12 s of dense bitset sweeps over 2M vertices regardless of index or build. Done 20 Oct (`b488486e`, `5455ec18`, `43a37a0f`): source-grouped frontier kernel on index snapshots, hybrid sparse/dense expansion with a per-partition source bucket index, MATCH passes the quantifier upper bound into the operator (both kernels stop at the depth bound), a source group stops once all its destinations are found, up to 16 distinct sources always run source-grouped, pairs with a NULL endpoint no longer force the 512-lane batch path (a single source over all 2M vertices went from 288 s to 0.15 s because 1214 isolated vertices made NULL pairs). 8M edges, index, 18 threads: 1 pair 0.01-0.02 s (Kuzu 0.03-0.05), 20 sources 0.13 s (Kuzu 0.07), 1 source x 2M destinations 0.15 s. Remaining gap on few-source queries is per-group array allocation and worker oversubscription (design doc 5.6); revisit only if the evaluation needs it | op | 3 | P1 | W12 | done |

Done on day 1: W1-W6, W8 (draft), W9, W10, W11, most of W12 and W15. Open P1: W7, W12 (cost choice), W15
(rest), W16-W23. The remaining engineering is roughly 15 pd; evaluation and writing (~25 pd plus machine
time) are now the critical path and depend on EC2 access and the author's hours.

## 5. Schedule (re-planned 19 Oct for a 1 February VLDB submission; 1 January = complete draft)

| Week | Dates | Engineering (Claude) | Evaluation / writing (author) |
|------|-------|----------------------|-------------------------------|
| 1 | 19-25 Oct | done: W1-W6, W8-W11, W14, W26; branch pushed for Linux CI 20 Oct; next: W7 path reconstruction | Venue decided (VLDB); review design doc decisions D2-D5; W18 Neo4j Enterprise request; start W22 outline |
| 2 | 26 Oct-1 Nov | W7 path reconstruction | W17 end-to-end LDBC queries into the harness; W21 archive setup |
| 3 | 2-8 Nov | W7 done; W15 remaining tests (restart under load, concurrent writers) | Tag frozen commit; first read-only sweep on EC2 (W19 part 1) |
| 4 | 9-15 Nov | W12 cost-based choice + W13 model skeleton; fixes from the sweep | **Checkpoint 13 Nov**: sweep results reviewed; related work (W22) |
| 5 | 16-22 Nov | W16 update workload in the harness (LDBC insert streams) | Intro + operator section draft |
| 6 | 23-29 Nov | W13 calibration from sweep data; W25 if time | Update-workload sweeps on EC2 (W16) |
| 7 | 30 Nov-6 Dec | Bug fixing from sweeps; W24 if time | Index section draft; ablation sweeps (W19 part 2) |
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
6. Evaluation: setup; read-only vs Kuzu/Neo4j; updates and delta size; end-to-end queries; ablations;
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
