# Notes for the paper from the 21-22 Oct dev-desk work

Extracted from the plan progress log, the design doc and the handoff after the merge of 22 Oct. Every number is
provisional until the frozen-commit rerun. "SF" = LDBC SNB scale factor, knows graph; machine m7a.8xlarge unless noted.

## Introduction / framing

- Bulk claim, stated the desk's way: the native systems answer a set of pairs one search at a time. A predicate on the
  source table or a join turns into N independent searches with no shared work. Quantify with a source-cardinality
  sweep (1, 16, 128, 1024, 2048, 16384, 65536, all pairs; SF10, 100, 300; W27), not with two query types. Single
  source is the fair fight; many pairs is where the operator's design pays.
- Cross-system table at SF10, 190 commits of 2000 operations, one query per commit (query mean / update batch mean):
  DuckPGQ index 19 ms / 9 ms; build per query 26 ms / 8 ms; Kuzu 16 ms / 342 ms; Memgraph 41 ms / 26 ms;
  FalkorDB 9.3 s / 111 ms; Neo4j 5 Enterprise 10.3 s / 69 ms (sssp, 1024 destinations). Point-to-point, 1024 pairs:
  DuckPGQ 62-76 ms; Kuzu 6.5 s (one query per pair); Memgraph 40 s; FalkorDB 8.1 s; Neo4j 0.76 s.
- Results identical to DuckDB on every step for all four systems (the correctness story of the update workload).
- Neo4j numbers are under an evaluation licence; W18 must settle publication rights before they appear.

## Operator section (already drafted; add)

- Per-phase events (author's decision 22 Oct: the barrier deadlock that motivated them is an implementation detail
  and stays out of the paper). State the design positively: no task waits for a task, the operator shares the fixed
  thread pool like DuckDB's own operators; concurrent driver runs 4/8/16 readers at 32 workers per query
  (78/87/90 iterations per second).
- Phased events also sped up the build route at SF300, 32 threads, warm: sssp 0.39 -> 0.30 s, point-to-point
  1.73 -> 1.61 s; the index route unchanged (0.031 s / 0.96 s).
- Scaling over the index: warm sssp 87 -> 27 ms from 1 to 16 threads, flat after; warm single pair ~0.1 s at every
  thread count (fixed cost, not profiled yet). Candidate for the ablation and for the "what remains" paragraph.
- SF300 cold vs warm (fresh process, first query vs second): sssp 32 threads build 0.40 s / index cold 0.17 s / warm
  0.024 s; 1 thread 5.3 / 1.30 / 0.086 s; point-to-point 32 threads 1.69 / 1.11 / 0.95 s; 1 thread 13.5 / 6.3 / 4.9 s.
  Index load follows the `threads` setting (1.1 s single-threaded).

## Index section (to write)

- Change groups: one Append or TryDelete call = the rows of one commit chunk of one transaction = one visibility
  decision; readers check one row per group, O(commits since fold) instead of O(delta + tombstones). Per-row checks
  cost 3-4x the whole query at SF10 with 300k pending operations (sssp 8 -> 124 ms).
- Commits are serialized, so visible groups form a prefix of the group sequence; the newest visible group is the key
  of a one-entry snapshot cache: all readers between two commits share one snapshot.
- Delete handling: a delta row is stamped with the deleting group; an epoch row becomes a tombstone (row, group) whose
  epoch position the first reader that needs it resolves. A reverted commit calls Delete after Append; the group is
  never visible, so the stamp never applies.
- Dictionary per epoch, rebuilt in key order at a fold, not prefix-stable: ids resolved at append time are tagged
  with the epoch generation and re-resolved at a fold (bug found: 411 vs 394 reachable pairs after the first automatic
  fold). Unknown keys resolve at read time as extra vertices.
- Overlay build per query: bitmap over epoch positions for applied tombstones + small adjacency over visible delta
  edges, bucketed by destination range like the epoch partitions, built in parallel since 22 Oct (counting sort by
  range; SF10 rebuild after a commit 41-51 -> 32-43 ms). No edge of the epoch is examined. Remaining cost is O(delta)
  copy and visibility pass: SF100 sssp 39 -> 159 ms over 1.35M pending operations with one query per commit; a fold
  policy belongs to the strategy section (W13).
- Rows loaded from storage (committed before a restart) have no group and are decided per row until the next fold.
- Parallel load: SF300 index load 1.15 -> 0.09 s at 32 threads (parallel segment copy into an uninitialized buffer,
  parallel partition deserialization, single-copy vector assign; the zero-fill of a 610 MB staging buffer was most of
  the remaining cost).
- Under updates, SF10 32 threads, sssp: build 26 / index with automatic fold 19 / index without fold 25 ms (10 queries
  per commit: 10.4 ms); point-to-point 76 / 62 / 66 ms (phased events); 1 thread sssp 138 / 23 / 22 ms; SF100 sssp
  136 / 113 / 112 ms before the parallel overlay, 131 / 95 after. Update cost per commit is the same for all routes
  (+0.3-7 ms).
- Concurrent SF10 sssp, writer committing 2000 ops every 200 ms, 190 commits, no mismatches: readers 1/4/8/16 index
  11/18/37/72 ms vs build 23/32/49/84 ms; point-to-point 4 readers 113 vs 143 ms; unpaced writer (14 commits/s),
  8 readers 39 vs 39 ms.
- Engine facts for the "what the interface lacks" list: the index is told which rows changed, not which transaction;
  row versions are garbage-collected, so visibility at past bounds is unavailable; two connections initializing the
  extension state at once hit a catalog write-write conflict (fixed on our side); the cache key must name the file
  catalog, not `memory`.

## Evaluation section

- Baseline formulations to state verbatim: Kuzu sssp needs the source as a literal (a WITH-bound source ran the
  recursive join from every vertex, 20 s vs 8 ms at SF1); Kuzu point-to-point with a pair table bound at both ends
  runs far more than the pairs, so one query per pair (6.3 ms per pair); Kuzu's MATCH ... DELETE in the same
  transaction as the CREATE did not see the created relationship, so inserts and deletes commit separately; Kuzu
  ended with one edge more than expected (2,124,203 vs 2,124,202), results unaffected, cause open.
- Memgraph `*BFS` with a bound target is one BFS per pair; FalkorDB `shortestPath` per pair, and its single-source
  query is the same per-destination search (hence 9.3 s); FalkorDB serializes writes against reads with a per-graph
  lock (no MVCC): the contrast for the concurrent pass. Neo4j: 1024 `shortestPath` searches 10.3 s single-source vs
  0.76 s point-to-point with UNWIND, a 13x gap with the same primitive, unexplained; try `runtime=pipelined` and GDS.
- Loads at SF10: Kuzu < 1 s (COPY), Memgraph 6-7 s (LOAD CSV), FalkorDB 85 s (UNWIND batches), Neo4j 21 s (LOAD CSV).
- Fair-comparison rule decided with the author: run Neo4j GDS and Memgraph MAGE set procedures beside Cypher, and
  note that they do not compose with a predicate in the same query. Baselines get a time budget per query; an
  exceeded budget is reported, not dropped.
- Update workload definition: LDBC BI insert/delete batches of Person_knows_Person (SF10: 339k inserts, 50k deletes
  between snapshot persons), exact edge counts from the staged `knows_update_log`; sequential runs on database copies
  (build per query, index with automatic fold, index without fold) and a concurrent C++ driver (one writer, R readers,
  index and build route in the same read transaction).
- lance-graph 0.5.4: Cypher over Lance/DataFusion, variable-length patterns but no shortest-path semantics, no
  mutation statements found; related work at most, or a read-only bounded-reachability comparison (P3).

## Open items that the paper depends on

- Frozen commit and full rerun (all numbers above).
- W27 cardinality sweep; W17 end-to-end LDBC IC13/14, BI19/20; W18 Neo4j publication rights; Neo4j 13x gap.
- W7 step 2 (paths on the build route), W12/W13 (strategy choice and fold policy) for the strategy section.
