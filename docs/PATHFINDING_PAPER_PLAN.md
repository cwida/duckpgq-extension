# Path-finding operator + MVCC-consistent CSR index: paper plan

Status date: 19 October 2026. Branch: `pathfindingoperator-forward-port` (merged with main at `ea98090f`,
build/test fixes in `7c5db25a`; 83/83 SQL test files pass locally, Linux and macOS CI build).

## 1. Target and thesis

**Target venue.** SIGMOD 2028 research track, Round 1. SIGMOD 2027 Round 4 closed on 17 October 2026.
Assumed Round 1 dates by the usual cadence: abstract ~10 January 2027, paper ~17 January 2027
(12 pages + references). Confirm as soon as the 2028 CFP is posted. Fallback: Round 2 (~April 2027).

**Thesis.** A loadable extension can give a relational engine graph-native path finding, including an
adjacency (CSR) index that stays transactionally consistent, without forking the engine.

**Contributions.**
1. A batched multi-source BFS path-finding operator that builds a partitioned CSR inside the query
   pipeline from a relational pair input, with statistics-driven orientation and source grouping.
2. An MVCC-consistent CSR index implemented through DuckDB's extension index API: epoch snapshots plus
   commit-stamped deltas and tombstones, so a reader traverses exactly the edges visible at its snapshot.
3. A cost model for choosing between build-per-query, index traversal, and relational joins, and the
   engine hooks a DBMS must expose for (1) and (2) to live in an extension.
4. Evaluation on LDBC SNB, Graphalytics and graph500 against Kuzu and Neo4j, read-only and under updates,
   including end-to-end SQL/PGQ queries.

## 2. Current state

What exists and works (opt-in via `experimental_path_finding_operator`):
- Operator: sink/source `PhysicalPathFinding`, radix-partitioned local CSR build, 512-lane MS-BFS,
  forward/reverse orientation, source grouping, pair dedupe, grouped batch scheduling. Returns path
  **lengths** only.
- Planner: optimizer rule replaces the `iterativelengthoperator` marker emitted by `match.cpp`.
- In-memory partitioned CSR cache per connection; prototype persistence in `__duckpgq_csr_*` tables with
  trigger-based invalidation (`experimental_persist_csr`), eager build on CREATE PROPERTY GRAPH.
- Benchmark harness `scripts/pathfinding_benchmark.py`: systems duckpgq/kuzu/neo4j; LDBC SNB SF1–SF100,
  Graphalytics S/M/L, graph500-22..26; patterns point_to_point, sssp, all_pairs, graphalytics_bfs; pair
  shapes random/same_src/same_dst; recursive-CTE and SQL-rewrite baselines; verify flag; result CSVs.
- Results so far live on EC2 only (local `data/ldbc-pathfinding/results` is empty). No paper draft in repo.

## 3. Gaps

### A. Measurement validity and robustness (block any final number)
| # | Gap | Evidence | Fix |
|---|-----|----------|-----|
| A1 | Per-batch `event.InsertEvent` detaches all but the first batch from pipeline completion; a query can finish before later batches end, under-reporting time and risking torn results | `physical_path_finding_operator.cpp:791,842`; `duckdb/src/parallel/event.cpp:70` moves `parents` | One completion event; `AddDependency` on each batch event |
| A2 | Blocking `Barrier` hangs on task error, interrupt, `max_execution_time`, `external_threads>1`, concurrent path queries | `duckpgq_barrier.cpp:55-77`, all task files | Abortable barrier with error/interrupt checks, or event-per-phase scheduling |
| A3 | BFS state for every batch allocated up front, unaccounted (~200 B/vertex/batch; 600 B bidirectional) | `SchedulePathFindingBatches`, `bfs_state.cpp:42-56` | Done for regular MS-BFS: `BFSWorkingSet` from the buffer manager, one per worker group (grouped execution is now the default). Remaining: source-grouped states, CSR arrays |
| A4 | Strategy choice must be recorded per run (HLL estimates are deterministic for the same pair multiset, so the choice itself is reproducible) | `PathFindingPairStats` | Log orientation/grouping/dedupe decisions per query (profiler or benchmark CSV) |
| A5 | Benchmark CSV timing hooks sample clocks in hot paths even when disabled | `duckpgq_barrier.cpp:58-75`, push/pull, bidirectional | Barrier timing removed (W2); default kernel gated. Push/pull and bidirectional variants still sample unconditionally (experimental, not used in sweeps) |

### B. Operator functionality
| # | Gap | Fix |
|---|-----|-----|
| B1 | No path reconstruction (`shortestpathoperator` throws; task file commented out) | Implement parent tracking + reconstruction, or scope paper to distances/reachability |
| B2 | Optimizer rule misses markers (comma join, nested subquery, 3x UNION) -> runtime NotImplemented | Rewrite all markers, drop early `return false`, pre-optimize only |
| B3 | Reverse orientation chosen when no reverse CSR exists -> NotImplemented at default settings | Fall back to forward; update test asserting the error |
| B4 | Edge rows with NULL keys / dangling FKs fail every operator query (count mismatch) | Count edges through the same join that produces endpoints |
| B5 | `path_length(p)` twice -> NULL for first; `<-[e]-` unsupported | REPLACE every alias; add left edges (swap src/dst) |
| B6 | Pair ids outside `[0, V)` index BFS arrays unchecked (OOB write) | Bounds check -> NULL |
| B7 | Stale in-memory cache after count-preserving UPDATE/DELETE+INSERT | Superseded by the index (C); until then validate on table version or disable cache |

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
- C1 Design: epoch CSR (immutable, packed) + append delta (edges with commit id) + tombstones (row ids
  with commit id); reader picks newest epoch <= its start time and filters delta/tombstones by commit id;
  vacuum folds deltas into a new epoch. Own key->dense-id dictionary keyed on the edge table (no
  dependence on vertex rowids). Snapshot handle pinned for the query.
- C2 Correctness argument: visible-edge set equals edges whose insert commit <= start and no delete commit
  <= start; behaviour at checkpoint and crash recovery.
- C3 Operator reads the index: `cached_partitioned_csr_input` path replaced by an index snapshot;
  optimizer rule picks index vs build-per-query via the cost model (C4).
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
- Anonymous artifact repo with scripts, result CSVs, and a run README (SIGMOD asks for a link at
  submission).

## 4. Work items

Effort in person-days (pd). P1 = must for Round 1, P2 = strongly desired, P3 = if time.

| ID | Item | Area | Effort | Prio | Depends on | Status |
|----|------|------|--------|------|-----------|--------|
| W1 | Fix event dependencies (A1) | op | 1 | P1 | — | done 19 Oct, `1729367a` |
| W2 | Abortable barrier + interrupt/error checks (A2) | op | 3 | P1 | — | done 19 Oct, `28a3ee99`; M3 co-scheduling assumption remains (see risks) |
| W3 | Per-running-batch BFS allocation, allocator accounting (A3) | op | 3 | P1 | W1 | done 19 Oct, `e08b95f5`; regular MS-BFS only: source-grouped mode still allocates ~11 B/vertex per distinct source up front, bidirectional/push-pull keep their own arrays, CSR memory itself is still unaccounted |
| W4 | Deterministic strategy choice + logging (A4), gate timing hooks (A5) | op | 1 | P1 | — | done 19 Oct, `879ae6c0`: `duckpgq_path_finding_strategy()` returns mode/pairs/distinct_src/distinct_dst/orientation/scheduling/groups/csr of the last operator; default kernel samples clocks only when benchmark is on (push-pull/bidirectional variants still sample unconditionally) |
| W5 | Optimizer rule: all markers, forward fallback, alias fix, NULL-key edge count (B2–B5) | planner | 3 | P1 | — | done 19 Oct, `e7c36dad`; left edges `<-[e]-` still unsupported (pre-existing, both paths) |
| W6 | Bounds checks on pair ids (B6) | op | 0.5 | P1 | — | done 19 Oct, `578baf3c` |
| W7 | Path reconstruction (B1): per-level frontier bitsets or reverse BFS from destinations; return edge row ids from the packed CSR | op | 5 | P1 | W1–W3 | open |
| W8 | Index design doc + correctness argument (C1, C2) | index | 4 | P1 | — | draft 19 Oct, `docs/CSR_INDEX_DESIGN.md`; awaiting review (decisions marked **D**) |
| W9 | `IndexType` skeleton: create/bulk build/serialize/load, read-only snapshot (C1) | index | 6 | P1 | W8 | in progress 19 Oct (build: 8M edges 0.10 s dense / 0.155 s sparse keys, shared with the operator's build): `CREATE INDEX ... USING CSR` builds an epoch, MATCH uses it (`csr=index`), changes invalidate and the next reader rebuilds at its snapshot, writing transactions get private epochs; persistence (SerializeToDisk) still empty |
| W10 | Commit-time append delta + tombstones, epoch selection by start time | index | 6 | P1 | W9 | open |
| W11 | Vacuum/epoch fold; checkpoint delta integration; crash-recovery test | index | 5 | P1 | W10 | open |
| W12 | Operator reads index snapshot; optimizer chooses index vs build (C3) | op+index | 4 | P1 | W9, W5 | partly done 19 Oct: index used whenever a matching bound index exists (pairs become keys, dense-id translation per batch); cost-based choice open |
| W13 | Cost model + calibration (C4) | planner | 3 | P2 | W12, sweeps | open |
| W14 | Remove prototype persistence/eager/triggers (C5) | index | 2 | P1 | W12 | open |
| W15 | Index tests (snapshot isolation, concurrent writers, rollback, checkpoint, restart, drop) | index | 3 | P1 | W10–W11 | open |
| W16 | Update workload: LDBC insert streams into harness; mixed sweeps (D2) | eval | 4 | P1 | W12 | open |
| W17 | End-to-end LDBC IC13/14, BI19/20 in harness for all systems (D3) | eval | 4 | P1 | W5 | open |
| W18 | Neo4j Enterprise trial or parity protocol (D1) | eval | 1 | P1 | — | open |
| W19 | Systematic ablation + scaling sweeps on frozen commit (D5, D6) | eval | 4 (+machine time) | P1 | W1–W6 | open |
| W20 | Full read-only system comparison rerun on frozen commit | eval | 2 (+machine time) | P1 | W19 | open |
| W21 | Results archive, version pins, anonymous artifact (D7, E) | eval | 2 | P1 | ongoing | open |
| W22 | Related work + outline + intro draft | writing | 3 | P1 | — | open |
| W23 | Full draft, figures, internal review, polish | writing | 10 | P1 | W16–W20 | open |
| W24 | Teseo/Sortledton or GraphBLAS baseline (D4) | eval | 4 | P3 | W16 | open |
| W25 | WCC table function reads the index snapshot (generality argument, build vs index for a whole-graph algorithm) | op+index | 2 | P3 | W12 | open |
| W26 | Point-query kernel: single-pair/single-source queries pay ~0.12 s of dense bitset sweeps over 2M vertices regardless of index or build; allow the source-grouped kernel (or a sparse-frontier kernel) on index snapshots so point lookups cost milliseconds. Affects every Neo4j/Kuzu comparison | op | 2 | P1 | W12 | open, noted 19 Oct |

Total P1 ≈ 65 pd, P2 ≈ 3 pd. With two people in parallel (operator/eval track and index track) this fits
12 weeks with slack for one slipped week.

## 5. Schedule (weeks start Monday)

| Week | Dates | Operator / eval track | Index track | Writing |
|------|-------|-----------------------|-------------|---------|
| 1 | 19–25 Oct | W1, W4, W6; start W2 | W8 design doc | Outline (W22) |
| 2 | 26 Oct–1 Nov | W2, W5 | W8 done; start W9 | Related work |
| 3 | 2–8 Nov | W3; W18 | W9 | Intro draft |
| 4 | 9–15 Nov | W17 harness queries | W9 done; W10 | **Checkpoint 13 Nov** (see §6) |
| 5 | 16–22 Nov | W7 path reconstruction; W19 sweeps begin on frozen commit | W10 | Sec 3 (operator) |
| 6 | 23–29 Nov | W7 done; W19 | W11 | Sec 4 (index design) |
| 7 | 30 Nov–6 Dec | W12 (joint) | W12, W15 | Sec 5 (cost model) |
| 8 | 7–13 Dec | W16 update workload | W14 remove prototype; W15 | Sec 6 eval skeleton |
| 9 | 14–20 Dec | W16 sweeps, W20 rerun | W13 cost model calibration | Figures |
| 10 | 21–27 Dec | Buffer / holidays; machine time for sweeps | Buffer | — |
| 11 | 28 Dec–3 Jan | Finish sweeps; W21 archive | Bug fixing from eval | Full draft (W23) |
| 12 | 4–10 Jan | Final numbers | — | Internal review; **abstract ~10 Jan** |
| 13 | 11–17 Jan | — | — | Polish; **paper ~17 Jan** |

## 6. Checkpoints and fallbacks

- **13 November (end of week 4):** index design doc and correctness argument reviewed; `IndexType`
  skeleton builds and serves a read-only snapshot; operator bugs A1–A4 fixed and a first sweep on the
  frozen commit is clean. Go: continue to Round 1. No-go: index not stable -> switch to Round 2 (April),
  or submit the operator-only paper with the cost-model framing to a systems venue and keep the index
  for a follow-up.
- **13 December (end of week 8):** update-workload numbers exist and show the index beating
  rebuild-per-query at realistic update rates. If not, the paper narrows its index claim to
  snapshot-consistent reads with rebuild-on-vacuum and reports the trade-off honestly.
- **3 January:** full draft exists or we move to Round 2.

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
- Index granularity: per-epoch deltas (plan) vs per-edge commit stamps (defer to follow-up).
- Whether to keep the in-memory per-connection cache at all once the index exists (recommend: no).
- Two people or one? The schedule assumes two parallel tracks.
