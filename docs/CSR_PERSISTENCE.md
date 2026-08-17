# Experimental CSR persistence contract

This document defines the lifecycle contract for the experimental path-finding CSR cache. The implementation is
intentionally opt-in while the persisted format and invalidation behavior are being developed.

## Supported index

The first persisted format covers the forward, directed, unweighted CSR used by unweighted BFS (`iterativelength` and
compatible shortest-path execution). An eligible edge table must reference the same DuckDB vertex table at both
endpoints. Each eligible edge table in a property graph owns an independent CSR artifact.

The following are outside the first format:

- edge tables whose source and destination reference different vertex tables;
- reverse and pull CSR capabilities;
- weighted traversal state; and
- tables that are not DuckDB internal tables.

Unsupported graph shapes are skipped by eager construction and are never represented as a compatible persisted
artifact.

## Option interaction

Both options default to `false` and can be enabled independently.

| `experimental_build_csr_on_create` | `experimental_persist_csr` | Contract |
| --- | --- | --- |
| `false` | `false` | Build lazily and keep completed indexes in memory only. |
| `true` | `false` | Build eligible indexes during `CREATE PROPERTY GRAPH` and keep them in memory only. |
| `false` | `true` | Try a valid persisted generation on a memory miss; lazily build and persist on a miss. |
| `true` | `true` | Build eligible indexes during `CREATE PROPERTY GRAPH`, then persist complete generations. |

Disabling persistence stops durable reads and writes. It does not delete artifacts that were created while the option
was enabled. Dropping a property graph removes its artifacts and dependency metadata regardless of the current option
value.

## Publication and compatibility

A completed CSR is immutable after publication. Persistence writes a versioned generation transactionally, and a
reader publishes it to the in-memory cache only after validating the format version, capabilities, counts, partition
ranges, array bounds, and total edge count.

An incompatible, stale, partial, or corrupt generation is never published. If the graph input is available, normal
query execution treats it as a cache miss and rebuilds the CSR. A cache-only execution path reports an error instead
of using an invalid artifact. A cancelled or failed write cannot replace the last complete generation.

## Format version 1

Version 1 uses four reserved DuckDB tables:

- `__duckpgq_csr_registry` identifies the active immutable generation and records validity, format version, counts,
  capabilities, layout, and payload cardinalities;
- `__duckpgq_csr_segments` stores ordered forward-CSR partition payloads, including partition ranges, sparse source
  rows, row offsets, local destinations, and optional streaming segments; and
- `__duckpgq_csr_dependencies` maps artifacts to physical vertex and edge tables plus their structural key columns;
  and
- `__duckpgq_csr_trigger_owners` records the exact invalidation triggers owned by DuckPGQ. Ownership is checked before
  a trigger is reused, changed, or removed; a colliding or modified trigger fails closed and is never overwritten.

All payload rows and the active registry update are written in one transaction. Payload rows from a generation that
is not named by the registry are not visible to the codec. Version 1 persists only the forward capability, even when
the in-memory index also contains reverse or pull data.

## Thread-count invariant

CSR identity and correctness are independent of the number of threads used to build, load, or traverse it. Thread
count and thread-derived partition geometry are physical execution details and must not be part of the persisted
logical identity. Vertex and edge counts are also generation metadata rather than identity, so cardinality-changing
DML invalidates and advances the same artifact. For every supported graph and source vertex, changing `threads` may
change timing but must not change the reached vertices or distances.

Persisted construction uses a deterministic adaptive policy. It targets 22 destination partitions to retain useful
parallel traversal without the locality and allocation cost of the former fixed-256 target. The target was selected
by the reproducible local 1/4/8/16-thread sweep as the smallest geometry that kept KGS warm traversal within 15% of
the thread-tuned layout across that matrix. Tiny graphs naturally materialize fewer logical vertex ranges, while
larger graphs increase beyond 22 whenever
`ceil((vertex_count + 2) / UINT16_MAX)` requires it to keep each local destination within `uint16`. The policy never
reads the build or query thread count. The current 12-bit radix limit permits at most 4,096 physical partitions;
graphs beyond that representation fail explicitly.

Persisted CSR storage remains experimental. No migration guarantee is provided for artifacts created by earlier
geometry or format revisions; they may be discarded and rebuilt. P9 changes only construction, so registry
`partition_count` plus each segment's `start_vertex` and `end_vertex` still describe the geometry of newly built
artifacts.

## Invalidation and transaction lifecycle

Each physical table has one owned `AFTER INSERT`, `AFTER DELETE`, and `AFTER UPDATE OF <structural keys>` trigger,
regardless of how many persisted artifacts depend on it. Edge dependencies track the source and destination foreign
keys; vertex dependencies track the referenced vertex keys. Insert and delete transition tables suppress invalidation
when a statement affects no rows. DuckDB does not permit `UPDATE OF` together with transition tables, so a statement
that names a structural key conservatively invalidates even if it affects no rows or assigns the old value. Updates
that name only property columns do not invalidate.

The trigger marks every dependent registry row invalid, advances its generation token in the same transaction as the
table mutation, and evicts the affected key from all connection-local caches. Commit exposes the invalid registry
state; rollback restores the prior valid generation. Builders capture the generation token before construction and
publish only if it is still current, so a mutation racing a build cannot publish a stale snapshot. Concurrent builders
for the same identity serialize the final write; a superseded builder keeps its immutable CSR query-local. A query in
an explicit transaction may build a CSR from its own snapshot for that query, but it cannot load a committed CSR or
publish/persist the snapshot-local build. This prevents rollback from leaking a CSR made from uncommitted rows and
prevents an older read snapshot from revalidating data after a newer commit.

Every persisted in-memory index carries its registry generation. Both optimization and physical lookup validate that
generation, validity flag, format, layout, counts, and capabilities before treating it as a cache hit. A mismatch is a
cache miss, never a best-effort read. Dropping a property graph removes all of its registry generations, payload rows,
dependencies, in-memory entries, and any owned table triggers whose dependency refcount reaches zero.

## Benchmark timing phases

When `experimental_path_finding_operator_benchmark=true`, the phase timing CSV separates persistence lifecycle work
from traversal work:

- `partitioned_csr_metadata_lookup` measures the in-memory identity/generation lookup;
- `partitioned_csr_deserialize_load` measures a persisted-generation load attempt, including validation;
- `partitioned_csr_rebuild` measures construction after a cache miss and before persistence;
- `partitioned_csr_serialize_write` measures the transactional durable generation write; and
- `partitioned_csr_invalidation` measures the synchronous trigger action.

The existing `partitioned_csr_cache_hit`, `partitioned_csr_cache_miss`, and `partitioned_csr_cache_publish` phases remain
available for end-to-end cache accounting. Persistence-disabled paths never emit deserialize/load or serialize/write
phases.

## Local performance sweep

`scripts/csr_persistence_benchmark.py` runs the P7 comparison without EC2 or downloads. Every trial clones an existing
prepared DuckDB database, eagerly creates the property graph in one process, and reopens the database in a second
process for the first and warm BFS. Because the existing benchmark files predate trigger-capable storage, the runner
first copies the two graph tables once into a v2.0.0 benchmark base and leaves the source files untouched. This one-time
conversion is recorded but excluded from CSR timings. It pairs two modes:

- `persisted_canonical` builds the canonical layout, serializes it, and deserializes it after process restart; and
- `transient_thread_tuned` disables persistence, so the restarted process builds the thread-tuned layout on its first
  BFS.

The paper-facing columns have non-overlapping meanings: `Load` is persisted metadata/deserialization or transient CSR
construction, `Cold` is the remainder of the first end-to-end BFS, and `Warm` is a repeated BFS in the same process.
`cold_total_s` remains available as `Load + Cold` without phase subtraction. Raw results also contain eager creation,
serialization, phase counts, partition counts, process peak RSS, used database blocks, correctness checksums, and
per-trial disk estimates. The generated environment file records the commit, dirty-tree hash, binary/extension hashes,
machine resources, options, datasets, repetitions, and thread counts.

The default local sweep covers SNB SF1 plus `wiki-Talk` and `kgs` at 1, 4, 8, and 16 threads with three repetitions:

```shell
python3 scripts/csr_persistence_benchmark.py
```

Results are written below the ignored `data/ldbc-pathfinding/results/csr_persistence` directory. Guardrail results for
load-versus-rebuild time, warm traversal, disk amplification, and query RSS are recorded without failing the exploratory
run; pass `--strict-guardrails` when those thresholds should gate automation.
