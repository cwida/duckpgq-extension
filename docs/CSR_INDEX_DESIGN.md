# CSR index: a snapshot-consistent adjacency index as a DuckDB extension index

Status: draft for review, 19 October 2026 (plan item W8). Everything in section 2 was checked against the DuckDB
submodule at `45f4fdf3` (v2.0); file references point there. Decisions marked **D** are proposals to review.

## 1. Goal

Replace the prototype persistence (registry tables, triggers on user tables, eager build on CREATE PROPERTY GRAPH)
with an index type registered through DuckDB's extension index API:

```sql
CREATE INDEX knows_csr ON knows USING CSR (src, dst);              -- directed
CREATE INDEX knows_csr ON knows USING CSR (src, dst) WITH (undirected = true);
```

Requirements:
1. **Snapshot consistency.** A query traverses exactly the edges visible to its transaction, including under
   concurrent inserts and deletes, explicit transactions, and rollback. DuckDB's own indexes do not have this
   property (section 2.4); it is the research contribution.
2. **Maintained by the storage layer.** No triggers, no registry tables, no second connection. Appends and deletes
   reach the index through the hooks every DuckDB index gets; checkpoint persists it; WAL replay rebuilds it; `DROP
   TABLE` and `DROP INDEX` remove it.
3. **Readable by the operator at CSR speed.** A reader obtains a snapshot in time proportional to the changes since
   the last fold, not to the graph, and traverses the packed CSR with at most one extra bitmap check per edge.
4. **Bounded memory.** Buffer-manager owned, so it counts towards `memory_limit` (same as the BFS working sets).

Out of scope for the paper: weighted edges, composite keys (section 9), per-edge version stamps.

## 2. What DuckDB v2.0 gives us (verified)

### 2.1 Extension index type
`IndexType` (`src/include/duckdb/execution/index/index_type.hpp:190`) is registered on the `IndexTypeSet` of the
database (`index_type_set.cpp:21`). It carries a `name`, `create_instance` (`CreateIndexInput` with
`context`, `table_io_manager`, `db`, `name`, `column_ids`, `unbound_expressions`, `storage_info`, `options`), the
bulk-build callbacks `build_bind/sort/global_init/local_init/sink/combine/finalize` (parallel sink over the table's
key columns and row ids), and `create_plan` as an escape hatch. The `vss` HNSW and `spatial` R-tree indexes use this
API.

### 2.2 Maintenance hooks (`BoundIndex`, `bound_index.hpp`)
`Append(IndexLock&, DataChunk &keys, Vector &row_ids)`, `Insert`, `Delete`/`TryDelete(keys, row_ids)`, `Vacuum`,
`MergeIndexes`, `ResetStorage`, `SerializeToDisk`, `SerializeToWAL`, `CreateEmptyCopy`, and the checkpoint delta
hooks `SupportsDeltaIndexes`/`MergeCheckpointDelta` with `IndexDeltaType {DELETED_ROWS_IN_USE,
ADDED_DATA_DURING_CHECKPOINT, REMOVED_DATA_DURING_CHECKPOINT}`. All calls hold the index lock, so they are
serialised.

When they fire:
- **Appends** reach the global index while the writer commits: `DuckTransaction::Commit` assigns `commit_id`
  (`duck_transaction.cpp:271`) and then flushes local storage, which calls `AppendToIndexes` before the rows are
  merged into the table (`local_storage.cpp:575-605`). Uncommitted appends never reach the index (local storage
  keeps private `append_indexes` only for constraint indexes, `local_storage.cpp:30`). A failed commit reverts
  with `RemoveFromIndexes`.
- **Deletes** reach the index at commit through `CommitState` (`commit_state.cpp:124`). UPDATE is delete + insert.
- The hooks receive **row ids**, never a commit id or transaction.

### 2.3 Row visibility
`DataTable::CanFetch(DuckTransaction&, row_t)` (`data_table.cpp:554`) answers, for one row id, whether the row is
visible to the transaction, through the row group's `RowVersionManager`. A transaction's snapshot is its
`view.visibility_bound` (`VisibilityBound`, `common/constants.hpp:67`); the transaction manager tracks the
`lowest_visibility_bound` over active transactions (`duck_transaction_manager.hpp:134`, private), the point before
which versions can be cleaned up.

### 2.4 Why ART does not need any of this
An ART returns row ids and `DataTable::Fetch` filters them by visibility; the index may contain rows a reader must
not see. A BFS never goes back to the table per edge, so the CSR must carry visibility itself.

### 2.5 Row id stability
Checkpoint vacuum only remaps or rebuilds row ids when every index on the table is a bound ART
(`row_group_collection.cpp:40-60`, `CanVacuumRemap`/rebuild policy). With a CSR index present, row ids are stable.
Consequence to document: a table with a CSR index keeps delete gaps until the index is dropped.

### 2.6 Persistence and recovery
`SerializeToDisk` returns an `IndexStorageInfo` (`index_storage_info.hpp:48`: `root`, `options`,
`allocator_infos`, `buffers`) that the checkpoint writes; WAL only records create/drop of the index
(`WriteCreateIndex`/`WriteDropIndex`). Replay recreates an `UnboundIndex` from the last checkpoint's storage info
(`wal_replay.cpp:1225`), binds it on first use (`DataTableInfo::BindIndexes`), and replays inserts/deletes through
the normal table path, i.e. through `Append`/`Delete`. An index created after the last checkpoint has
`!storage_info.IsValid()` and must come up empty and rebuild itself (section 5.7).

### 2.7 Discovery from the planner
`DataTableInfo::GetIndexes().IndexEntries()` enumerates `IndexEntry`s with `GetIndexType()`, `GetBindState()`,
and typed `GetReadHandle<T>()`/`GetWriteHandle<T>()` (`table/index_entry.hpp:178`).

## 3. Design in one paragraph

The index stores edges by **edge-table row id** and vertices by **key** (not by vertex-table row id; confirmed 19 Oct: the edge index never learns vertex row ids at commit, vertex row ids are not stable without an index on the vertex table, and edge row ids still allow `edges(p)` to return edge rows). It keeps an
immutable, packed, partitioned **epoch** CSR that equals the edge set visible at the epoch's visibility bound, an
append-only **delta** of edges committed after that bound, and append-only **tombstones** for epoch edges deleted
after that bound. A reader with visibility bound `b >= bound(epoch)` sees exactly `epoch ∪ {d ∈ delta : CanFetch(b,
d.row)} \ {t ∈ tombstones : ¬CanFetch(b, t.row)}`. The index never stores commit ids: DuckDB's row version
information decides visibility for the (small) delta and tombstone sets, once per query. A **fold** builds a new epoch
from the visible set at a fresh bound and resets delta and tombstones; old epochs stay until no active transaction
can still need them.

## 4. Data structures

```
CSRIndex (BoundIndex)
├── options: undirected (bool), key type
├── dictionary        key (int64) -> dense vertex id (uint32), append-only; prefix of every later version
├── epochs[]          shared_ptr<Epoch>, newest last, at least the newest kept
│   └── Epoch
│       ├── bound                 VisibilityBound of the builder transaction
│       ├── vertex_count          dictionary size at build time
│       ├── partitions[]          packed LocalCSR partitions (same layout the operator uses today:
│       │                         (source_vertices, row_offsets) + uint16 local destinations), forward [+ reverse]
│       ├── edge_row_ids[]        row_t per epoch edge position (8 B/edge; see 9.2 for compression)
│       └── position_of_row       hash row_t -> epoch position, used by Delete to turn a row id into a tombstone
├── delta             append-only (src_id, dst_id, row_t) in commit order; length is the publish point
├── tombstones        append-only (epoch_position, row_t); only for rows present in the newest epoch
└── lock              the DuckDB IndexLock; readers copy lengths under it, then read without it
```

Sizes for E edges, V vertices, D delta edges, T tombstones: epoch ≈ 2 B (dst) + 8 B (row id) per edge + offsets;
delta 16 B per edge; tombstones 12 B each; dictionary 12 B per vertex. **D**: start with 8-byte row ids in the
epoch; compress later (9.2).

All arrays are buffer-manager allocations (`MemoryTag::EXTENSION`), persisted through the checkpoint (section 5.6).

## 5. Operations

### 5.1 CREATE INDEX (bulk build)
Implemented 19 Oct (`csr_index.cpp`): per-thread edge lists from the parallel sink; dictionary as a presence
bitmap with rank counts when the key range is at most ~16x the edge count (identity when the range is fully
populated), sorted keys otherwise; one parallel pass maps ids and buckets edges by destination partition into
thread-local buckets; each partition is then radix-sorted by source in cache and emitted as sparse rows, so no
per-vertex array exists during the build. 8M edges / 2M vertices on 18 threads: 0.10 s directed, 0.11 s
undirected, 0.155 s with sparse keys (keys bucketed by top offset bits into 1024 ordered buckets, sorted and
deduplicated per bucket, concatenated; no merge pass). The
operator's per-query build uses the same helpers (`csr_build.hpp`) since 19 Oct: ~0.03-0.06 s inside a query.
The finalize step uses std::thread; a DuckDB-task version via `create_plan` is a later cleanup.

Through the standard build pipeline (`build_sink` receives key chunks + row ids in parallel, `build_finalize`
produces the `BoundIndex`): assign dense ids, radix-partition by destination, pack partitions, record row ids. The
build runs in the creating transaction, so the epoch bound is that transaction's visibility bound, and rows
appended by concurrent committers arrive through `Append` into the delta as usual.

### 5.2 Append (writer commit)
Under the index lock: for each (src_key, dst_key, row_id): look up or assign dense ids; push (src, dst, row) to the
delta (both directions when undirected). No visibility information is recorded. Constant work per edge.

### 5.3 Delete (writer commit)
Under the index lock: for each row id: if `position_of_row` finds it in the newest epoch, push (position, row) to
tombstones; otherwise it is a delta row and nothing is recorded, because readers decide delta visibility by
`CanFetch` anyway. A commit that is reverted after `Append` calls `Delete` for the same rows; those are delta rows,
so nothing happens, and `CanFetch` will report the row invisible.

### 5.4 Reader snapshot
`CSRIndex::Snapshot(DuckTransaction &t)`:
0. If `t` has uncommitted changes (`DuckTransaction::ChangesMade()`), it gets a private snapshot: its own writes
   are visible to it alone, so neither the shared epoch nor a shared overlay can serve it, and nothing it builds may
   be published. (Found by test: a rebuild from a writing transaction's table view leaked a rolled-back edge.)
1. Under the lock: pick the newest epoch with `bound(epoch) <= t.visibility_bound`; copy `delta.size()` and
   `tombstones.size()` as the publish lengths; take a `shared_ptr` to the epoch and the dictionary length.
2. Without the lock: build the **overlay**: a bitmap over epoch positions with bit set for every tombstone whose row
   is **not** visible (`!CanFetch(t, row)`), and a small adjacency (sorted by source, forward [+ reverse]) over the
   delta entries whose row **is** visible. Both are O(D + T) `CanFetch` calls; no edge of the epoch is examined.
3. Return `{epoch, overlay, dictionary_view}`, pinned for the duration of the query.

A reader whose bound is older than every retained epoch gets no snapshot and the operator falls back to building the
CSR for the query (today's path). This is rare (a long transaction that started before the last two folds).

### 5.5 Fold (new epoch)
Triggered by `Vacuum`, by `CHECKPOINT`, or when `D + T > fold_ratio * E` at the end of a query (**D**: default 0.1).
Runs in its own transaction with bound `b_f` and no local changes (so the scan sees committed rows only):
1. visible set = epoch edges whose position is not suppressed under `b_f` ∪ delta edges with `CanFetch(b_f, row)`;
2. build the new epoch from it (same code as the bulk build; dictionary may be compacted, producing a new version);
3. under the lock: install the new epoch; the new delta = entries whose row is **not** visible under `b_f`
   (committed after the fold's snapshot, or appended concurrently during the fold); tombstones = ∅.
Old epochs are retained while a transaction with bound `< b_f` may still start reading; released when
`lowest_visibility_bound >= b_f` (9.1) or, failing a public accessor, by keeping the previous epoch until the next
fold and treating older readers by fallback (5.4).

Implemented 19 Oct: `Fold` rebuilds the epoch from a table scan at the folding transaction's bound (the
incremental epoch-plus-delta build is an optimisation for later), `MAX_RETAINED_EPOCHS = 2`, triggered by
`duckpgq_csr_index_fold(table, index)` or automatically in `Snapshot` when `delta + tombstones >
duckpgq_csr_index_fold_ratio * epoch edges` (default 0.1). **Compaction does not evaluate visibility at past
bounds.** The first attempt did (`CanFetch` with a synthetic transaction view at the oldest epoch's bound) and
was wrong: DuckDB drops row version information once no active transaction can observe the pre-commit state, after
which every row looks visible at every bound, so entries an older epoch's readers still needed were dropped.
Instead each change-log entry is stamped with the generation of the first epoch that accounts for it (delta: the
epoch contains the row; tombstone: the epoch excludes it), decided by the *live* folding transaction at fold time;
compaction drops entries whose generation is at most the oldest retained epoch's. Entries are therefore kept while
an older epoch that lacks them is retained, e.g. after one fold with two retained epochs the delta is unchanged.

### 5.6 Checkpoint
`SerializeToDisk` writes dictionary, newest epoch, delta, tombstones as blocks and returns their locations in
`IndexStorageInfo` (through `FixedSizeAllocator` infos like ART, or raw block pointers under `options`). With
`SupportsDeltaIndexes() = true`, DuckDB routes commits that happen during serialisation into delta indexes and
hands them back via `MergeCheckpointDelta`; for this index those are plain delta/tombstone appends, so merging is
appending. Older epochs are not persisted: after a restart no transaction predates the checkpoint.

Implemented 19 Oct: one byte stream (newest epoch + change-log entries it does not account for) in one-block
fixed-size allocator segments; segment list and byte count in `IndexStorageInfo::options`; `SerializeToWAL`
uses the same stream for indexes created after the last checkpoint. Loaded epochs get the default (always
visible) bound. Each checkpoint rewrites the whole index (no incremental persistence yet); `SupportsDeltaIndexes`
is false, so writes during serialization go straight to the main index under its lock.

Cold start, 8M edges / 2M vertices: binding the index from disk 0.038 s; first single-pair query through the
loaded index 0.142 s versus 0.213 s when the CSR has to be built from the table. The persisted stream is about
140 MB (17.5 B/edge: 8 B row id, 2 B destination, sparse-row offsets); the database file grew by 282 MB because
CREATE INDEX first lands in the WAL (converted to blocks at the next checkpoint) and the checkpoint then writes a
fresh copy. Incremental persistence and row-id compression (9.2) would halve both.

Cold start versus native graph databases (19 Oct, same 2M-vertex / 8M-edge graph exported to Parquet, fresh process
per engine, 18 threads, machine moderately loaded by another session, load ~10; scripts in
`docs/experiments/cold_start/`):

| | DuckDB + CSR index | DuckDB build per query | Kuzu 0.11.3 | LadybugDB 0.21.2 |
|---|---|---|---|---|
| graph structure load | 0.10 s index build | per query | 1.16 s import | 1.21 s import |
| open + bind | 0.031 s | - | 0.013 s | 0.016 s |
| 1 pair, first / second | 0.131 / 0.110 s (19 Oct); 0.021 / 0.010 s (20 Oct) | 0.214 / 0.113 s; 0.104 / 0.021 s | 0.051 / 0.032 s | 0.237 / 0.121 s |
| 20 sources x 200 destinations, depth <= 8 | 1.50 s (19 Oct, dense MS-BFS); 0.145 / 0.128 s (20 Oct) | 0.48 s; 0.25 s | 0.073 s | 0.548 s |
| 1 source x all 2M destinations, depth <= 8 | 288 s (19 Oct); 0.15 s (20 Oct) | - | - | - |

All engines agree on the 20-source answer (159 paths, total length 1226); the single-source answer (158,396
reachable vertices, 118,534 at depth 8) is unchanged between the two DuckDB measurements. Single-threaded, the
index answers 1 pair in 0.12 s and 20 sources in 0.87 s (20 Oct), so the remaining parallel speed-up on few-source
queries is about 7x on 18 threads. On the on-disk database the index build takes 0.16 s and the bind after
reopening 0.043 s (the bucket index below is rebuilt at load, +0.01 s).

The 19 Oct numbers were taken with the dense per-vertex MS-BFS sweep, which costs ~0.12 s per 512-lane batch on 2M
vertices regardless of how many pairs the batch holds. The 20 Oct numbers (plan item W26, commits `b488486e`,
`502bb577`, `96fbc7b4`) come from five changes: (1) the source-grouped frontier kernel is allowed on index
snapshots and runs whenever there are at most 16 distinct sources, (2) MATCH passes the upper bound of the path
quantifier into the operator and both kernels stop at that depth, (3) a source group stops as soon as all of its
destinations are discovered, (4) pairs with a NULL endpoint no longer disable source grouping, (5) every sparse
partition carries a bucket index over source ids (about two rows per bucket, ~2 B per row) so a frontier vertex
is looked up with one bucket read instead of a binary search per partition. Point (4) explains the 288 s:
1,214 vertices of the generated graph have no edges, so they are absent from the index dictionary and their
pairs carry a NULL position; one NULL pair used to send the whole query through 196 full 512-lane batches.
What remains versus Kuzu on the 20-source query (0.13 s vs 0.07 s) is the per-group fixed cost: each group
allocates and clears its own distance, seen and frontier arrays over all 2M vertices (~20 MB) and the 20 groups
share 18 threads with 18 workers each; a shared working set per worker and fewer workers per group are the next
steps if that gap matters in the evaluation.

### 5.7 Recovery and bind
On open, the index is an `UnboundIndex` with the checkpoint's storage info; WAL replay re-applies inserts and
deletes through `Append`/`Delete` once bound. If the storage info is empty (index created after the last
checkpoint, or legacy file), the index comes up empty and marks itself `needs_rebuild`; the first `Snapshot` (or a
`Vacuum`) performs the bulk build at that transaction's bound. Until then readers fall back to build-per-query.

### 5.8 Drop
`DROP INDEX`/`DROP TABLE` go through the catalog; `CommitDrop` releases buffers. Nothing else to clean up.

### 5.9 Implementation notes (19 Oct)
- The kernel clears and checks the frontier per partition *destination range*; two partitions over the same
  range would wipe each other's frontier. Delta edges therefore live in *companion* partitions aligned with the
  epoch partitions and are explored right after them, never as separate ranges. Vertices beyond the epoch get
  empty cover partitions so every range up to the snapshot's vertex count is checked.
- Suppression is a per-partition bitmap over edge positions held by the snapshot, tested in the sparse-row loop;
  the epoch partitions themselves stay shared and immutable.
- Tombstone positions are not resolved at Delete time (no row-id index yet, 9.2); the first snapshot that sees
  new tombstones scans the epoch's row ids once (parallel) and caches the positions on the index.
- `Append` evaluates the key expressions itself (`ExecuteExpressions`) and reads integer keys without a
  ClientContext, so UBIGINT/HUGEINT keys are rejected at CREATE INDEX.
- Delta edges carry keys, not ids: a delta-only vertex is assigned an id above the epoch's count per snapshot.

## 6. Correctness

Let `Vis(b)` be the set of edge rows visible to a transaction with visibility bound `b` (DuckDB's definition via
`CanFetch`). Let the epoch `E` be built by a transaction with bound `b_e`, so `E = Vis(b_e)` as a set of row ids
(the builder reads through its own snapshot).

**Claim.** For a reader with bound `b >= b_e`: `Vis(b) = E ∪ Δ_b \ T_b`, where
`Δ_b = {row ∈ delta : CanFetch(b, row)}` and `T_b = {row ∈ tombstones : ¬CanFetch(b, row)}`.

**Proof sketch.**
- *Rows not in E and not in delta.* Every row appended to the table after the epoch was built reached the index
  through `Append` at commit (2.2), so it is in the delta; rows committed before `b_e` and visible then are in `E`;
  rows committed before `b_e` but already deleted before `b_e` are in neither, and DuckDB never resurrects a deleted
  row id (2.5). So `Vis(b) ⊆ E ∪ delta`.
- *Delta rows.* `row ∈ delta ∩ Vis(b)` iff `CanFetch(b, row)`, by definition of `Vis`. This also covers rows
  inserted and deleted after `b_e`, and rows whose commit was reverted (never visible).
- *Epoch rows.* `row ∈ E` is in `Vis(b)` unless deleted with a commit `<= b`. Every delete of an epoch row after
  `b_e` reached `Delete` at commit (2.2) and became a tombstone (5.3). `¬CanFetch(b, row)` iff that delete is visible
  to `b`. Deletes before `b_e` are already excluded from `E`.
- *Concurrency.* Delta and tombstones are append-only and the reader copies their lengths under the lock. An entry
  appended after the copy belongs to a commit that is in flight or later; its commit id exceeds `b`... (strictly:
  a commit whose entries are not yet published cannot be visible to a reader that already holds `b`, because
  `Append` runs during that commit, before the rows become visible). Entries copied but committed later than `b` are
  excluded by `CanFetch`.
- *Fold.* The new epoch is `Vis(b_f)` by the same argument applied to the old epoch at bound `b_f`; the retained
  delta is exactly `delta \ Vis(b_f)`, so the claim holds for the new epoch with `b_e := b_f`. ∎

The claim does not hold for readers with `b < b_e`; they are refused a snapshot (5.4). It relies on (i) hooks being
called at every commit, (ii) row ids never being reused while the index exists (2.5), (iii) `CanFetch` implementing
`Vis`. All three are DuckDB invariants, not ours.

## 7. Operator integration

- **Pairs become keys.** `match.cpp` emits the vertex key column (`a.id`) instead of `rowid` for the pair source and
  destination when an index is used; the operator maps keys to dense ids through the snapshot's dictionary (one hash
  lookup per pair). Unknown key → unreachable (NULL), consistent with today's bounds handling.
- **Planner.** The optimizer rule looks for a bound `CSR` index on the edge table whose key columns match the
  property graph's edge keys and whose direction matches the pattern; if found, `LogicalPathFindingOperator` is
  planned with `index_input` (no endpoint child), else build-per-query as today. The cost model (W13) later decides
  between index, build, and joins instead of "index if present".
- **Kernel.** `Explore` iterates the epoch partition; when the overlay bitmap is non-empty it skips suppressed
  positions (one bit test per edge, branch predicted away when empty); then explores the delta adjacency of the
  vertex. Delta edges to vertices beyond the epoch's `vertex_count` are valid: the working set is sized by the
  snapshot's dictionary length.
- **Path reconstruction (W7)** records predecessor edge positions; an epoch position maps to a row id via
  `edge_row_ids`, a delta edge carries its row id, so `edges(p)` can fetch edge rows by row id.
- **Undirected.** Index option; a directed index is not used for `-[e]-` patterns.
- **Memory.** Epoch, delta, overlay, and working sets are all buffer-manager allocations.

## 8. What to measure (feeds the cost model and the evaluation)

First numbers (19 Oct, 8M edges / 2M vertices, 18 threads, single-pair query, whose BFS alone is ~0.12 s):
unchanged index 0.124 s; after 100K committed inserts (delta overlay, 100K `CanFetch`) 0.111-0.121 s; after
~10K deletes the first snapshot resolves tombstone positions with one scan of the epoch row ids: 0.131 s, the
next one 0.109 s. The overlay is within measurement noise at these sizes. Fold policy measurement (20K pairs, depth <= 8, folding
disabled, inserts of 1%, 10%, 50% of the epoch): 1.51 s at 0%, 1.42 s at 1%, 1.67 s at 10%, 2.12 s at 50%; after
folding the 50% delta into a 12M-edge epoch the same query takes 2.17 s, i.e. traversing delta edges through
companion partitions costs the same as traversing them in the epoch, and the larger result set explains the
growth. The fold itself (scan + build of 12M edges) took 0.42 s. Folding is therefore about memory (24 B per delta
edge plus O(D) snapshot work), not traversal speed; a default ratio of 0.1 is conservative.


- Snapshot cost vs `D + T` (CanFetch per row), compared to build-per-query vs `E`.
- Traversal slowdown vs suppressed fraction `T/E` and delta fraction `D/E`.
- Fold cost vs `E` and the fold ratio; steady-state throughput under the LDBC SNB insert stream at several rates.
- Writer overhead of `Append`/`Delete` (dictionary lookup, delta push) per committed edge.
- Break-even: update rate at which build-per-query beats the index, and query rate at which the index wins.

## 9. Open points

1. **Epoch retention needs the oldest active bound.** `lowest_visibility_bound` is private on
   `DuckTransactionManager`. Options: a public getter in the fork (document as a required hook), or keep the previous
   epoch until the next fold and fall back for older readers (5.4). **D**: start with the fallback.
2. **Row ids in the epoch.** 8 B/edge dominates. Row ids within a row group are dense, so a per-partition
   (row_group_start, uint32 offset) encoding or a positional bitmap of deleted gaps would cut this to ~4 B. Defer.
3. **Composite and non-integer keys.** Hash to 64 bits in the dictionary; collisions must be detected (store the key).
   Single integer keys first.
4. **`Delete` without key values.** The hook passes keys too, but we only need the row id; fine.
5. **Dictionary growth across versions.** Append-only within an epoch generation keeps "epoch dictionary is a prefix"
   true; a compacting fold changes ids, so a snapshot must carry its dictionary version. Simplest: never compact
   until a configurable ratio of deleted vertices is reached.
6. **Checkpoint storage format.** Decide between `FixedSizeAllocator` (ART-style, many small buffers, WAL-friendly)
   and large raw blocks referenced from `options`. Prototype with raw blocks; switch if `SerializeToWAL` is needed.
7. **Two tables, one index.** The vertex table never touches the index. If a vertex row is deleted while edges
   remain, the SQL path drops those edges (join), the index keeps them. **D**: document as a semantic difference, or
   make `MATCH` add a vertex-existence filter on endpoints when it matters.
8. **Interaction with the existing per-connection cache.** Remove it once the index exists (plan W14).

## 10. Implementation steps (maps to plan W9–W15)

1. W9: `IndexType "CSR"`, bulk build to an epoch, `Snapshot` without delta/tombstones, operator `index_input`,
   `CREATE INDEX`/`DROP INDEX`, read-only tests. Checkpoint of the epoch; empty-storage rebuild.
2. W10: `Append`/`Delete` into delta/tombstones, overlay in `Snapshot`, snapshot-isolation tests (two connections,
   rollback, update = delete + insert).
3. W11: fold with retention, checkpoint delta hooks, crash/restart tests through WAL replay.
4. W12: planner selection; W14: remove prototype persistence; W15: full test matrix.
