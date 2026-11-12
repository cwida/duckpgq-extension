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

## Pending edits (decided with the author, 26 Oct)

Introduction, when the frozen-commit numbers are in:
- Thesis sentence: stop enumerating the four interfaces by name ("a parser extension, an optimizer extension, a custom
  physical operator and a custom index type"); say "the interfaces a modern engine already offers to extensions" and
  let Section 5 list them.
- Headline numbers: replace the TODO wall of eight figures by three, one per claim: single-pair time over the index
  (vs Kuzu/Neo4j), many-pair scaling against the native systems (the 1024- or 65 536-pair point), and the update cost
  per commit (index vs Kuzu). Written once, from the frozen rerun.
- Keep the register of the rewritten paragraphs: ideas in the introduction, mechanism in Sections 3 and 4.

## Implementation gaps found by the Section 4 code audit (26 Oct); the paper now states the code's behaviour

- A reader whose bound is older than every retained epoch gets an error ("rerun the query"), not a build-per-query
  fallback. Fix before the frozen commit or keep the sentence.
- `CSRIndex::Vacuum` is a no-op and there is no checkpoint hook: the only fold triggers are the explicit function and
  the automatic check in Snapshot. The paper no longer claims VACUUM/checkpoint folds.
- The fold's table scan (`RebuildFromTable`) is single-threaded; only the build after it is parallel.
- A writing transaction's private snapshot is a full CSR rebuild from the table, not epoch + overlay.
- The snapshot cache is checked after the per-group visibility pass, so a cache hit still costs O(groups) CanFetch.
- WAL: `SerializeToWAL` writes the full epoch + log into the CREATE INDEX record (not just create/drop).
- Rows replayed from the WAL carry no group id and are decided per row until the next fold.
- No vertex-existence filter in MATCH; endpoints come from the vertex tables, intermediate deleted vertices are
  traversed. Documented as a semantic difference.
- Epoch memory is plain heap, not buffer-managed (only the BFS working sets and partitions in the operator are).
- Key limits: two integer columns fitting BIGINT, no UNIQUE/PK, < 2^32 vertices, NULL-keyed rows skipped.
- Reverse side of the index (W28, `WITH (reverse = true)`): expected to stay in the frozen commit; call it after the
  full sweep. If it stays: rewrite the Section 3.3 sentence that demotes reverse/push-pull/bidirectional to the
  ablation (reverse orientation is 18x faster for many sources to one destination, the BI20 shape), add the reverse
  partitions to Section 4 (stored without row ids, same epoch and change groups, undirected aliases forward), and
  state its cost (~5 B per edge). Push/pull and bidirectional stay out unless the sweep says otherwise.
- Word choice: "ablation" is not used. The design-choice subsection is "Evolution of the operator" (after FSST's
  "Evolution of FSST"): one table, one row per step from the CIDR UDF design to the index and the reverse side, the
  same three queries per row. "Microbenchmark" is the prose word for narrow measurements (thread scaling, snapshot
  cost), as in Kersten et al.

## Section 3 code audit (26 Oct): corrections applied and facts to keep straight

- The optimizer rule does not replace the projection; it swaps the marker for a column reference and puts the
  operator beneath the projection. Pre-optimize for MATCH; the post-optimize pass serves only the legacy global-CSR form.
- MS-BFS batches are 2048 pairs in input order run as waves of 512 lanes; there is no grouping by source. A level is
  two phases (explore, check). Scheduling since W31: workers per batch = threads / batches, concurrent groups bounded
  by a working-set budget (3 bitsets x 512 lanes = 192 B/vertex per group; a quarter of the buffer manager's memory).
- Dense sweep of the single-source traversal uses a per-vertex level stamp, not a membership byte.
- Reverse orientation: distinct sources >= 4 x distinct destinations AND reverse partitions available (undirected
  pattern, or index with reverse side); the on-the-fly build never builds reverse partitions.
- Push/pull and bidirectional exist only for the legacy global-CSR route (3/4-arg markers), unreachable from MATCH:
  removed from the paper text.
- The on-the-fly build is not a PhasedKernel: Sink/Combine partition, one build event sorts and emits, FinishEvent
  publishes. "No task waits" holds.
- Reach mode (unbound destination -> reachoperator, one traversal per source, destination table joined back) exists
  since W29 and is what sources x all runs; now described in 3.3.
- Limits to state somewhere: vertex ids fit uint32 and the radix build allows at most 4096 x 65535 (~268M) vertices;
  build-route vertex ids are row ids up to the next row id (deleted rows count as vertices); pairs are not
  deduplicated by default; per-group memory of the single-source traversal 13 B/vertex (+12 with parents), all groups
  live until the scatter; NULL pairs count toward the ratio rule and disable the single-source zero-copy shortcut.
- Re-audit Sections 3 and 4 against the frozen commit (decided 27 Oct). The desk is working on the memory bandwidth
  of the multi-source traversal and other optimizations whose fate is open; once the dust settles and the frozen commit
  is tagged, run the claim-by-claim code audit again (same procedure as 26 Oct: each mechanism and number in 3.x and
  4.x checked against the code with file:line verdicts) and apply the corrections before the final numbers go in.
  Sections most exposed: 3.3 (traversal costs, batch scheduling, thresholds), 3.4 (phases), 4.2-4.4 (snapshot cost,
  fold trigger, load), the Evolution table.
- Every number in Sections 3 and 4 must come from the evaluation setup or say where it comes from (author, 27 Oct).
  Section 3 now states its source in the opening and cites Tables 2 and 3 for the build cost. Four passages still
  carry laptop numbers from the synthetic 2M-vertex, 8M-edge graph (build time and bytes per edge, the 0.12 s sweep
  for one pair, the single-source switch 0.11 -> 0.01 s and 1.5 -> 0.13 s, paths 0.24 vs 0.22 s and 0.43 s), marked
  red; replace them from the frozen rerun or move them into the Evolution table. Check Section 4 in the same pass.

## TODO after the frozen-commit sweep (eval-freeze-rc1)

- Verify every number in the paper against the frozen sweep, not only Sections 3 and 4: abstract, introduction,
  every section's prose, every caption, Tables 1-5, the figures, and the appendix. For each number: find the table,
  figure or result CSV it comes from, check the value, and make the text cite it. Numbers that come from no table
  are either added to one or removed.
- In particular the build-cost sentence of Section 3.2 (282 ms vs 9 ms and 1.6 s vs 0.98 s at SF300, 97% and two
  fifths), the "a fifth" of Sections 4 and 6.2, and the per-row check cost (8 to 124 ms at SF10) in Section 4.2.
- Replace the laptop numbers on the synthetic 8M-edge graph (four passages in Section 3, marked red).
- Then the code re-audit of Sections 3 and 4 (listed above), the two pending introduction edits, and the abstract's
  provisional numbers.

## Frozen-commit audit (28 Oct, eval-freeze-rc1 = 7e9778c0)

Text and figures of Sections 3, 4 and 5, the background, the abstract and Figures 1 to 3 now state the frozen code's
behaviour. Probes are in /tmp/foldbug/test (sqllogictests, two connections); none is in the repo.

Code defects in the CSR index, found by the audit; the paper carries red TODOs for each:
- Fold race, reproduced. Fold does not compare its bound with epochs.back()->bound. An older transaction folding
  after a newer one installs the older epoch as the newest; compaction drops entries only the newer epoch covered;
  a new transaction misses a committed edge. Automatic folds by concurrent readers can take this path, and every
  reader that saw the log too long folds again after the first (serial redundant rebuilds). Fix: recheck under
  fold_lock and skip when the newest epoch's bound is not older than the folder's.
- Row-id reuse after a failed commit, reproduced. A unique-key conflict at commit reverts an append the CSR index
  already logged; the next insert gets the same row id and the failed commit's edge becomes visible. Not tested: a
  checkpoint dropping a fully deleted trailing row group.
- Duplicate overlay edges, by reading. BuildSnapshot ignores covered_epoch, so a snapshot of the newest epoch re-adds
  log entries that epoch contains while the older epoch is retained. Lengths unaffected.
- Not reproduced: the cache-key collision through fully deleted groups (probe passed).
- Unexplained: a delete of a log row also records a tombstone (stats count it); every answer in six probes was right,
  so the log entry is stamped as well. Fully deleted groups are never compacted (by reading).

Decide: fix the code (breaks the freeze; the sequential update workload has no failed commits and no concurrent
folds, so its numbers stand, but the concurrent-reader run can hit the race) or qualify Section 4.6.
Also not in the paper: the FactorizedJoinOptimizer is registered by the extension and unmentioned in Section 5.

## Numbers checked against eval-freeze-rc1 (29 Oct)

Every number in the abstract, introduction, Sections 3, 4 and 6 and the captions now comes from repro/results
(rc1), checked point by point; recheck against rc2 when it lands. Still pre-freeze and marked red: the fixed-costs
table (create, load, size), the per-row visibility number (8 to 124 ms, Sections 4, 6, related work), the unpaced
concurrent writer, the laptop passages of Section 3 (single pair 0.12 s, the traversal switch, paths), the CSR bytes
per edge. Findings from the check:
- The single sampled source at SF300 has no outgoing edge (reaches 1 vertex), so sources x all at k=1 on SF300 is a
  fixed-cost query. Noted in the figure and table captions; consider another source sample for rc2/rc3.
- The single random pair at SF10 has no path (reachable 0).
- The LDBC table and figure now use medians per parameter set (plan decision of 28 Oct); the sweep tables use means.
- The plot script now orders result files by the run timestamp in the file name, not by mtime (copies and checkouts
  reset mtimes); the tables came out identical.
- All seven update configurations agree on all 191 steps; one automatic fold, in the query after commit 94 (162 ms).

29 Oct: the fold race, the row-id reuse after a failed commit, the duplicate overlay edges and the cache key are fixed
in 2517ebc2 (rc2 runs on it); Section 4 and the background now describe the fixed behaviour. Open: a reused id of an
epoch row (only possible if a checkpoint that drops a fully deleted trailing row group hands its ids out again,
untested) would resurrect the deleted edge, because the dead marking covers log entries only.

## Lanes per source (8286e9ea), read 29 Oct

Integrated into Sections 3, 5 and 6 (red TODOs where numbers are single runs before rc5). Two implementation
observations, neither a correctness problem:
- The waves of a lane-per-source query are scheduled all at once as independent events, outside the worker-group
  scheduler and its 512 MiB budget; each allocates its 192 B/vertex working set when it starts, with num_threads
  tasks each. Fine for BI19 (3 waves, 4.7 GiB at SF300); a query with W waves holds W working sets at once, which is
  W x 1.7 GB on graph500-24. Routing the waves through the grouped scheduler would bound both memory and tasks.
- DetectReached runs in the serial completion step after every check phase and scans every pending destination of
  every active lane (a 64-byte bit-set row per lookup): O(pending) serial work per level, about 0.8 M lookups per
  wave and level on BI19 at SF300. Unmeasured; a profile of the serial step would show whether it matters.

4 Nov: the fold's serial table scan is an implementation shortcut (single TableScanState in BuildEpochFromTable),
not a design constraint; the desk confirmed visibility, locking and the build side all permit a parallel scan
(about forty lines, per-thread edge lists into BuildEpoch). Pending: a FoldStats timing field and three SF300 folds to
measure the scan's share (expected a quarter to a third of 3 s); then parallelize or state the share in 4.4.

4 Nov: moved out of the paper text (Section 4.6), kept here as our record: the untested corner of row-id reuse after a
checkpoint drops a fully deleted trailing row group; rc1's 15,765 compared concurrent transactions with no mismatch;
the fourteen regression test files; CREATE INDEX rejects non-integer or more than two key columns and UNIQUE, and a
build fails above 2^32-1 vertices. The concurrent-comparison count goes into Section 6.3 from rc10's check step.

4 Nov: the Section 6 tables and figures were still means. paper/tables/random_pairs.tex and sources_all.tex were
written by 7c65bed0 (29 Oct 08:56 UTC, caption "mean seconds over three runs"); the plot script switched to medians in
2620a9b6 (10:25 UTC); 0e4c9287 regenerated the figures at 09:59 UTC, before the switch, and only the teaser was
regenerated after it. Regenerated everything from the rc1 archive (DUCKPGQ_RESULTS_DIR=repro/results, /tmp/plotvenv).
Changes: DuckPGQ values move by a few ms (one pair at SF300 274 -> 278 ms on the build route, so the build cost
derived from the one-pair gap is 22/128/268 ms, not 264; updated in 4.7, 5 and 6.2). Baseline single-pair values lose
their cold first run (Neo4j 14 -> 2 ms at SF300, Kuzu 96 -> 30 ms), and Neo4j is now the fastest single-pair system
at SF100 (3 ms) and SF300 (2 ms) against 9 and 10 ms over the index; 6.2's "fastest at every scale" rewritten, TODO
left. The sources_all caption no longer carries the degenerate-SF300-source note (the script's caption dropped it);
the figure caption still explains it. The script also writes figures/concurrent.{pdf,png}, not yet referenced;
not added.

4 Nov: the 65,536-pair gap between the routes at SF300 is 2.8 s (12.9 against 10.1 s), ten times the 268 ms build,
and it grows along the pair axis (398 ms at 16 pairs, 550 at 1024, 900 at 8192). The build route pays something
beyond the build per traversal; candidates: CSR without source index or reverse partitions on the build route,
id assignment (vertex row numbers vs dictionary order) and locality, per-pair key mapping. 4.7 and 6.2 no longer
attribute the gap to the build; TODOs ask the desk to profile before the rerun's numbers go in.

4 Nov: Section 4 storyline pass after the proofreading rounds: the opening states the build cost the index saves;
the paths-carry-row-ids sentence moved from 4.3 to 4.2; updates of a key column named as delete plus insert; the
cache-key sentence split; 4.6 uses "build route"; 4.7 counts five costs (retained epoch, private epoch added) and
separates commit work (15 ms) from the contention of the concurrent run (158/604 ms). Row-id stability stays a fact
in 4.1 and implementation behaviour in 4.6 (both true; Section 5's hook table rests on the distinction).

5 Nov: desk status on the final sweep (parts of the message arrived truncated; marked ?). Build: eval-freeze-rc13 =
cf4d88ce, 256 lanes, x86-64-v3. Three system changes since the 3 Nov dry run: (1) reach results stored densely, a
source's result starts as a list of entries and switches to one byte per vertex once it has reached more than 1/64 of
the graph; the 1024-source point on graph500-24 used to be killed and now completes, peak memory about 8x lower.
Results are still materialised per query before emission, which bounds the reach set by memory; streaming the output
is future work (for Section 3's reach description and the conclusion). (2) The fold scans the table in parallel,
claiming row groups per thread as CREATE INDEX does; SF300 fold about 2.5x faster; fold statistics record the scan's
share. 4.4's serial remark dropped (this commit); 4.7's fold latency still a TODO. (3) A memory budget for reach
results: exceeding memory_limit minus the working-set cap fails with a DuckDB out-of-memory error instead of a kernel
kill; no point in the matrix hits it with dense storage.
Benchmark decisions: FalkorDB stays; cit-Patents runs sources-to-all only (random pairs on the citation DAG are
unreachable by construction); the harness pair limit applies only to DuckPGQ's pair-list formulation in the
microbenchmark, every baseline runs every point under 600 s; every number from one run on one commit, nothing older.
Done: SF10 matrix (35 steps) on the 3 Nov dry run with reach rows re-measured on rc13 (author's view 5 Nov: nothing changed between the dry run and the sweep start, so SF10 counts as the same
sweep; my objection: the desk lists the parallel fold among the three changes made after the dry run, and the SF10
update stream measures fold latency); Graphalytics
(cit-Patents, datagen-8_4-fb with DuckPGQ and Kuzu, scheduling microbenchmark on graph500-24) on rc13; SF100/SF300
fixed costs and pair sweeps with all seven systems. Running: LDBC queries, then concurrency, microbenchmarks,
throughput, update and concurrent-reader experiments at SF100/SF300; finish expected early 6 Nov; no numbers into the
text before the collector runs.
Qualitative (resent in full): operator time grows slowly with pairs and sources on LDBC and Graphalytics, every
per-pair baseline grows linearly and times out at the large points; at the largest SF300 point no baseline finishes;
on graph500-24 Kuzu completes the 1024-source reach, streaming its result per source, and the index route is about 1.5
orders of magnitude faster on it rather than failing. Baselines lead at the single pair and the single source: a
per-source engine starts answering at once, the operator pays a fixed cost of a few to a few tens of ms for snapshot
and working set; on cit-Patents (a source reaches ~2000 vertices) Kuzu and Memgraph win the one-source point outright,
on datagen it is a tie; state plainly: shared traversal pays from a handful of sources on. Two baseline failure modes:
per-pair engines (Neo4j Cypher, FalkorDB, Memgraph) time out; per-source engines (Kuzu, Neo4j GDS) run longer but
complete more; Neo4j Cypher shortestPath times out on every sources-to-all point, a single source included, because it
enumerates every destination. DuckDB-native: the recursive CTE scales with pairs and times out from the tens of
thousands; the CIDR 2023 UDF route holds on random pairs about an order of magnitude behind the operator and collapses
on sources-to-all. Index vs build: both routes complete every point; the index leads by the build cost on single
queries and by a smaller margin on large batches (so the rc1 2.8 s gap at 65,536 pairs may have been an rc1 artefact,
512 lanes and the old scheduler; keep the 4.7/6.2 TODOs until the numbers land); the strong case is concurrency and
updates: throughput with many clients, latency under a writer, commit cost, memory, reported with medians and p99;
reader-paid fold latency should drop with the parallel scan. Scheduling: at SF10 the window of concurrently running
batches matters a lot; on graph500-24 the window shapes land within a few percent because a wave's working set is
memory-bound and saturates the machine; the one-event-per-batch ablation runs out of memory on graph500-24 at 65,536
pairs, the memory argument for grouped scheduling in one sentence. Fixed costs: index 11-13 B/edge on every graph, 17
on sparse cit-Patents; builds at roughly ten million edges per second; smaller than Kuzu's store on the two large
Graphalytics graphs.
Open: the few-sources threshold is a default whose ideal is graph-dependent (4 datagen to 256 cit-Patents), present it
as a default; peak memory of the largest reach point is several times the result (allocator/buffer, unexplained), no
precise memory claims for that point.

5 Nov: the index's case at SF300, tentative (desk, 2 Nov runs on the rc9-era build, same index and scheduler as rc13
apart from the faster fold; single runs; rc13's last block, due early 6 Nov, replaces them). SF300, 32 threads, index
route vs build route: 16 readers under a writer, query median/p99 67/116 ms vs 3606/5183 ms; the writer's commit
117/165 vs 582/962 ms; with folds every 0.05% of edges 66/268/3685 ms (median/p99/max, 3 folds; the max is the reader
that triggered a fold, expected ~1.3 s with the parallel scan); single-pair throughput 1 client 62 q/s (p99 30 ms) vs
4 q/s (p99 294 ms), 32 clients 342 q/s (p99 267 ms) vs 6 q/s (p99 11 s); peak memory at 32 clients 1.6 GiB index,
build route not recorded at SF300 (2.0 GiB at SF10). Why SF10 shows near parity: a build is ~18 ms for 1.9 M edges
there, so 16 readers rebuilding still fit the machine; at SF300 16 concurrent builds each wanting 32 threads degrade
to ~3.4 s per query and the build route's throughput stays at 5-6 q/s however many clients. At SF10 the commit side
favours the build route, 86 vs 176 ms, because every commit appends to the index and invalidates the shared snapshot.
Consequences for the text once rc13 confirms: 4.7 leads with concurrency and updates (one to two orders), then the
read-only saving (the build cost, a fifth on heavy queries); the abstract's index numbers move from the SF10 update
stream (18 ms, 15 ms commits) to SF300 concurrency; "Commits are not slowed" gets the qualification that under
concurrent readers at SF10 the index route's commit is slower (176 vs 86 ms) and at SF300 faster (117 vs 582);
the fold max latency is stated plainly as one query per fold. Reviewer question to pre-empt in 4.7 or the opening:
why not a CSR cached across queries and dropped at every commit? Under a writer committing every 200 ms it would be
rebuilt about as often as it is used (a build is 270 ms at SF300), and transactions with different snapshots could
not share it. Ask the desk to record the build route's peak memory at SF300 in rc13 (memory is one of the four
index arguments).

5 Nov: read the rc13 pair sweeps directly on the desk (ssh devdesk2, files under
data/ldbc-pathfinding/results/sf{100,300}/sweep_pairs_*_1793828631973236496.csv and _1793841659700078410.csv, commit
cf4d88ce, 3 repeats each, tight). Medians, build route / index route, ms:
  SF300 pairs 1: 285/22  16: 472/139  128: 459/142  1024: 498/209  2048: 714/345  16384: 3629/2777  65536: 12809/10011
  SF100 pairs 1: 141/15  16: 223/63   128: 207/65   1024: 242/110  2048: 283/132  16384: 1213/877   65536: 4134/3251
  Gap build-index at SF300: 263, 333, 317, 289, 369, 852, 2798 ms; at SF100: 126, 160, 142, 132, 151, 336, 883 ms.
Two findings. (1) The 65,536-pair gap reproduces on rc13 (2.8 s at SF300, 0.9 s at SF100), so it is not an rc1
artefact: up to 2048 pairs the gap is the build plus ~50-100 ms, from 16,384 pairs it grows about linearly, ~38 us per
pair at SF300 and ~12 us at SF100. Something on the build route costs per pair (or per source) what the index route
does not; the desk's "smaller margin on large batches" is the relative share (22% vs 92%), the absolute gap is ten
times the build. 4.7/6.2 TODOs stay; the desk should profile the build route at 65,536 pairs (candidates: the
dense-id mapping of the pairs through the vertex table, a CSR without source index or reverse partitions on the
build route, id assignment and locality). Peak RSS at 65,536 pairs SF300: build 6.7-6.8 GiB, index 4.4-5.4 GiB.
(2) The index route's single-pair latency rose since rc1: SF300 10 -> 22 ms, SF100 9 -> 15 ms, SF10 unchanged (3-4
ms); the build route did not move (278 -> 285). A fixed cost proportional to the vertex count appeared, about 4 ns
per vertex, the size of zeroing or page-faulting a fresh per-vertex working set (13 B x 2.9 M vertices = 38 MB at
SF300); suspect the working-set cache behaviour after dff0c0eb (512 MiB budget became the cache-regime threshold).
It matters for the "baselines lead at the single pair" sentence (Neo4j 2 ms, Kuzu 30 ms at SF300; the index is now
22 ms, not 10) and for 4.7's 96% share (now 92%). Ask the desk what added it and whether the working set is still
reused across queries.

5 Nov, desk's answers (freeze kept). Q4 memory: throughput rows carry peak memory for both routes since 2 Nov
(SF10 build route 2.0 GiB at 32 clients), concurrent-reader rows per row (build route 2.3 GiB at 16 readers, SF10);
the SF100/SF300 steps write the same columns. Q6: the empty SF10 index cells came from a partial newest file; the 4
Nov DuckPGQ-only rerun is now the newest and complete; the desk confirms on the printed table. Q2 single-pair 10 -> 22
ms at SF300: working-set reuse is out (allocated per batch through the buffer manager since 19 Oct in rc1 and rc13).
Since 29 Oct lengths always group by source, so one pair runs the per-source frontier kernel, which allocates and
fills its per-vertex arrays per query: int64 distances, a seen byte, a frontier stamp, 13 B/vertex, 38 MB at SF300,
23 MB at SF100, filled serially; in rc1 a single pair was one lane of a batch whose working set is zeroed in parallel
slices. Phase columns to confirm after the sweep. Possible later fix (not in rc13): send a single pair to the lane
kernel, or narrower arrays. Text: 3.3/3.4 should say the frontier kernel's fixed cost grows with the graph (its
arrays are filled per query), which is why a single pair on a large graph costs tens of ms. Q1 65,536-pair gap:
both routes build partitions with the same radix sort and emit function (layout identical); pairs enter both as
plain tables, the index's key pairs materialised untimed; what differs is the id space: build route = person table
row ids, index = dense ids in sorted key order. The gap is better described per 256-lane batch, 11-13 ms at SF300 and
3-5 ms at SF100, which points at traversal locality. Experiment after the sweep: a CSR index on row-id keys puts both
routes on one id space; if the gap closes, the paper says the build route pays for the table's row order and the
index gets its locality from sorted keys (a second index benefit beyond the build, for 4.7 and 6.2). Q3 (fold
numbers) and Q5 (SF10 reruns) wait for the last block.

9 Nov: Section 6 written in one pass from the rc13 archive (repro/results, commit 551b224c). Floats: Figure 6
scaling grid (2x3, every system in its fastest formulation, DuckDB-native routes dashed), Table 2 Graphalytics
(sources x all, 65,536 pairs on the two large graphs), Table 3 LDBC (generated as before), Table 4 fixed costs,
Table 5 update stream vs baselines, Figure 7 concurrency (throughput over clients; readers under a writer), Table 6
ablation (SF100, index route, rc13 microbench files only), plus Figure 4 crossover at SF100 in Section 3.4. Dropped
from the paper: the random-pairs and sources-all tables, the throughput-by-pairs, client-threads, speedup,
thread-scaling, first-query and updates figures, and the Evolution plan (the ablation table carries what Sections 3
and 5 refer to). The plot script gained fastest_path (min of single and 32-connection rows), scaling_grid_figure,
graphalytics_table, fixed_costs_table, update_stream_table, concurrency_figure, crossover_sf100_figure,
ablation_table; BI19_OVERRIDE_S is empty. Numbers replaced outside Section 6: abstract (BI19 0.77 s vs 47 / 332 s;
stream 15 / 19 ms vs Kuzu 17 / 335), introduction (22 vs 4 ms single pair; BI19; commits), 3.3 (lane-width
microbench claim removed, no rc13 data), 3.5 (one-batch TODO removed, 9x / 3.9x confirmed: 3.54 vs 0.38 s SF10,
38.8 vs 10.0 s SF300), 4 opening and 4.7 (22 / 126 / 263 ms; 92% at one pair; 329 vs 5 q/s; 82 ms vs 3.9 s; 153 vs
647 ms; 11-13 B/edge, 17 cit-Patents; per-row 15 -> 35 ms; fold 1.6-1.8 s), 4.5 (4.5 s), 5 (build cost; 16 readers
at SF10 42 vs 98 ms). Facts worth remembering: the sweep's index has no reverse partitions (harness CREATE INDEX
without WITH (reverse)); the ablation's 'with reverse partitions' row is the only measurement of them (116 -> 6 ms
for 1024 sources to one destination at SF100); the SF100 single random pair is unreachable; the correctness oracle ran
at SF10 with 4 readers, 4584 queries on both routes under 190 commits, 0 mismatches (repro/logs_rc13); fold at SF300
1.6-1.8 s with a 120-190 ms scan; the 16-source points on datagen and graph500-24 are slower than 128 (few-sources
rule). Still open: the index load time after a restart was not measured on rc13 (4.5 keeps the pre-freeze TODO); the
65,536-pair gap attribution (desk's row-id-key index experiment; TODO in 6.3); the Graphalytics citation's author
list (VERIFY in the bib); introduction, abstract and conclusion still to be realigned as text, Section 5 to shrink;
the unused figures and tables in paper/figures and paper/tables to be removed with the appendix move.

9 Nov (desk's post-sweep checks, commits 409890e3 and aace9efc on the desk, not yet pushed; full numbers in the
plan's "9 Nov (2)" entry). Oracle at scale, 16 readers under the writer, 20 commits: SF100 354 queries compared, SF300
218, SF300 with a fold during the comparison 220, all 0 mismatches / 0 errors; files under sf100/oracle and
sf300/oracle (kept out of the concurrent-table printer's glob). Row-id-keyed index (both routes number vertices by
row order): 65,536 pairs 3.97 s at SF100 and 12.07 s at SF300 against the id-keyed index's 3.28 / 9.80 s and the
sweep's build route 4.13 / 12.81 s, so build minus row-id index is the per-query build (0.16 / 0.74 s) and the rest
of the gap, 0.7 / 2.3 s, is the vertex numbering: sorted keys give locality, row order does not. 6.3 and 4.7 state it
as measured. Index load on first use after a restart (first query minus warm second, medians): warm (page cache)
SF10 16 ms, SF100 60, SF300 117, cit-Patents 54, datagen 0.86 s, graph500-24 0.94 s; cold (file evicted with dd
iflag=nocache, EBS ~270 MB/s) 23 ms, 0.29 s (0.17-0.63), 2.1 s, 0.84 s, 23.1 s, 24.4 s. Table 4 has a load column
(warm) read from repro/results/probes_9nov/load_after_restart.txt (copied here ahead of the pull, identical bytes);
4.5 updated, TODO gone. Single pair at SF300 (22 ms): the sampled pair is five hops; the five levels take 18-20 ms,
snapshot + per-source arrays + output 2-4 ms, array fills about 1 ms (perf: Relax 25%, ExplorePartition 25%,
frontier emplace_back 7%); the 5 Nov array-fill hypothesis is wrong and 6.1 now says the traversal of five levels.
High-diameter graph: DIMACS USA-road offline, SuiteSparse 403 from the desk, SNAP roadNet-CA/TX/PA (1-2 M V, 3-5.5 M
E, diameter 500-850) feasible in an afternoon; caveats: the dense reach form holds distances up to 254 (fallback to
8-byte entries; 1024 sources x all on an 11.5 M-vertex road graph would need 94 GB, a 16-bit dense form is not in
rc13), and thousands of levels cost 0.1-0.3 ms of scheduling each. Decided 9 Nov (author): no road graph for this submission; 6.1 names the gap. LadybugDB
0.15.3 (Kuzu's fork) probed at SF10 only: 8 ms / 548 s at 1 / 65,536 pairs, 6 ms / 9.6 s at 1 / 1024 sources, LDBC
23 ms / 4.7 s / 19.7 s, slower than Kuzu 0.11.3 everywhere; kept out of the tables (EXCLUDED in the plot script),
footnote in the setup (added 9 Nov): not faster than Kuzu 0.11.3, 1.1 to 2.9 times slower, so we report Kuzu. Trailer commits on the desk: 13, 7c65bed0 through d9701272.

9 Nov: Neo4j memory check (desk, interim): SF300 with a 31 GB heap and a 64 GB page cache against the sweep's 16 / 16 GB.
BI19 parameter sets 1422/1301: 51.6 / 52.1 s against 52.0 / 52.9 / 52.5; 1423/1302: 50.2 / 50.0 / 50.0 against 50.7 / 50.8
/ 51.1; 1427/1304: 26.4 / 26.3 / 26.3 against 27.0 / 26.8 / 27.0; 16,384 random pairs 250.5 / 250.8 s against 248.
Within 1-2% everywhere; the store (2.3 GB at SF300) already fit the 16 GB page cache. The 65,536-pair and one-source
points (timeouts in the sweep) were still running; the setup's clause assumes they stay timeouts. Baseline containers
return to the sweep configuration after the run. Neo4j's own guidance caps the heap at 31 GB (compressed pointers).

9 Nov: Section 5 dissolved. Its paragraph became the first paragraph of "Lessons and Conclusion" (three additions a
host could make, the smaller gaps, the two unpromised behaviours); the interface table (tab:hooks) is dropped from the
paper and its inventory (parser extension, bind replacement, scalar functions, optimizer extension, operator, index
type, client context state, settings, buffer manager; used for / lacked) goes to the artifact README. Labels
sec:strategy and sec:hooks now sit on the final section. Contribution 3 says "the gaps a host would have to close".
sections/strategy.tex removed (in git history up to 4f3a4c20).

11 Nov: the SF10 commit "reversal" explained by the desk (measured on the frozen build with a scratch copy of the
driver; archived in repro/results/probes_11nov_writer_lock, desk commit 8ab7922f). With 16 readers the writer's transaction at SF10 reproduces at 162 ms under the index
route's readers and 78 ms under the build route's (sweep 166 / 83). Per statement, medians, index readers / build
readers: INSERT 1.5 / 1.6 ms, DELETE 5.8 / 48.1 ms, COMMIT 153.5 / 27.4 ms. Under index readers the wait for the exclusive lock of the index entry is 90% of all COMMIT time, median 155.6 ms per commit, measured on 12 Nov with a shim around StorageLock::GetExclusiveLock (callers IndexEntry::RemoveFromIndex and IndexEntry::Append; about 3100 exclusive acquisitions per commit); under build readers it is 0.1 ms. (An earlier version of this entry quoted "85 of 95 stack samples"; the desk withdrew that figure, the samples are a lower bound and not a time share.) the readers meanwhile were in CSRIndex::Resolve -> BuildSnapshot, mostly in
DataTable::CanFetch; Resolve holds the entry's read handle across the build (src/core/index/csr_index.cpp:1640). The
wait grows with the pending log, not with the graph: median COMMIT 40 ms in the first 19 commits, 208 ms before the
fold, 91 ms after it, 294 ms at the end; the readers' median follows (11 -> 115 -> 35 -> 163 ms). The driver's
"commit" is the whole write transaction. Index route 166 / 214 / 153 ms at SF10 / SF100 / SF300, build route 83 / 348
/ 647 ms: SF10 is the scale where builds are cheap enough to undercut a roughly constant lock wait (SF100 and SF300
not profiled; inference from the numbers). Consequences in the text: 5.4 states the cause; 4.3 says a transaction
holds the index's lock shared while it builds its snapshot; 4.7's "Commits are not slowed" was wrong under concurrent
readers and is now a sixth cost (a commit does no more work but waits, 150-210 ms under 16 readers against 19 ms on
its own); abstract, introduction and conclusion no longer put the 19 ms of the sequential stream into the
sixteen-reader sentence. Only archive numbers are in the text (19; 153; 150-210; 83 / 348 / 647).

12 Nov: why the lock is held across the snapshot build (desk, from the code and the stack samples; not built or
tested). It does not have to be. CSRIndex::Resolve takes the entry's read handle and runs Snapshot -> BuildSnapshot
under it (src/core/index/csr_index.cpp:1640); BuildSnapshot copies the log prefix under the index's own mutex (:995)
and then works on private copies, on the epoch (shared_ptr) and on row versions through DataTable::CanFetch (table
locks, not the entry lock), touching the index again only for the snapshot-cache lookup and store. Over the long part
the handle only keeps the index object from being rebound or dropped. RunFold already releases the handle for the
rebuild and re-takes it for the install (:1415, 2 Nov), for the same reason. Unproven: that a commit's append and
removal reach the log before the commit is visible to any reader; a test would have to confirm it. Two details: the
hold is long because of contention, about nine in ten sampled reader stacks in the build are locking row-group mutexes
in RowGroupCollection::CanFetch, so sixteen readers serialise on the table's mutex while each holds the index lock
shared; and the commit takes the exclusive lock once for the appended chunk and once per flushed batch of deletes
(IndexDataRemover::PushDelete), waiting for the readers each time. Decision (author, 12 Nov): an implementation
detail; the paper states the cost (4.3, 4.7, 5.4) and says no more, no fix promised, no confirmation build; this
entry is the answer if a reviewer asks.

12 Nov: the "one lane per pair" row of the ablation table and line of the crossover figure are gone. Desk, from the
code and the strategy strings of the SF100 runs: with distinct sources both paths run the same 256-lane kernel with one
lane per pair; the lane-per-pair path (source_group_ratio=0) runs the 256-lane waves of a batch of up to 2048 pairs
one after another on a single working set (BFSState::AssignLanes, IterativeLengthState::BeginWave), the default packs
the source groups into waves that each have their own working set and run side by side
(physical_path_finding_operator.cpp:1722). 128 pairs: 1 wave either way, 64 / 63 ms; 1024 sources to one destination:
4 waves concurrently vs in sequence, 116 / 284 ms; 2048 pairs: 8 waves, 135 / 520 ms; 65,536 pairs: 32 groups either
way, 3301 / 3249 ms. So the row measured the scheduling of an older code path, not deduplication, and was removed
rather than explained; what deduplication saves is stated in 3.3 by count (1.4 K searches in six batches against 9.4 K
batches for BI19) and its effect on sources x all by the lengths-mode row.

12 Nov: paper/ holds only what the build reads. Removed: 19 unused figure PDFs, all PNG previews, tables random_pairs
and sources_all; sections/appendix_queries.tex moved to repro/package/queries.tex. scripts/plot_sweep_results.py
writes the paper's four data figures and six tables by default; DUCKPGQ_EXTRA_DIR=<dir> also writes the diagnostic
figures and the old table layouts there, DUCKPGQ_PREVIEW_DIR=<dir> writes PNG previews.

12 Nov: spread of the three repeats over the pair sweeps of all six graphs (newest file per system, shape and
count; spread = (max - min) / median). DuckPGQ index and build, 132 points: median 3.5%, 90th percentile 9.3%, max 33%
(a 3 ms query at timer resolution); points of 1 s and more, 49: median 1.7%, max 14.8%. Other systems, 262 points:
median 1.9%, 90th percentile 39%, max 2476% (Kuzu, one pair on graph500-24: 9.875 s cold, then 0.383 and 0.382 s);
points of 1 s and more, 159: median 1.1%, max 20.4%. The large spreads are cold first runs, which the median does not
follow. The setup states the 1 s figures.

## Reproduction package (decided 4 Nov, deferred until rc10 lands)

Separate private repository with its own history, populated once from the frozen commit by a copy script under
repro/package/: README (machine, setup, one command per matrix step, how to regenerate figures and tables), repro/,
scripts/pathfinding_benchmark.py, scripts/plot_sweep_results.py, scripts/tools/, repro/results from rc10,
ENVIRONMENT.txt, and the query appendix as markdown once it leaves the paper. Paper sources, plan, NOTES and the LDBC
data stay out. First commit message names cwida/duckpgq-extension and the eval-freeze tag. Private for submission
(single-blind), public at camera-ready.
