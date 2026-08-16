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
logical identity. For every supported graph and source vertex, changing `threads` may change timing but must not change
the reached vertices or distances.

Persisted construction uses a canonical target of 256 destination partitions; tiny graphs naturally materialize fewer
logical vertex ranges. This is a target rather than a cap: the partition count increases as needed to keep each local
destination within `uint16`. The current 12-bit radix limit permits at most 4,096 physical partitions; graphs beyond
that representation fail explicitly.

## Invalidation and transaction lifecycle

Each physical table has one owned `AFTER INSERT`, `AFTER DELETE`, and `AFTER UPDATE OF <structural keys>` trigger,
regardless of how many persisted artifacts depend on it. Edge dependencies track the source and destination foreign
keys; vertex dependencies track the referenced vertex keys. Insert and delete transition tables suppress invalidation
when a statement affects no rows. DuckDB does not permit `UPDATE OF` together with transition tables, so a statement
that names a structural key conservatively invalidates even if it affects no rows or assigns the old value. Updates
that name only property columns do not invalidate.

The trigger marks every dependent registry row invalid in the same transaction as the table mutation and evicts the
affected key from all connection-local caches. Commit exposes the invalid registry state; rollback restores the prior
valid generation. A query in an explicit transaction may build a CSR from its own snapshot for that query, but it
cannot load a committed CSR or publish/persist the snapshot-local build. This prevents rollback from leaking a CSR
made from uncommitted rows and prevents an older read snapshot from revalidating data after a newer commit.

Every persisted in-memory index carries its registry generation. Both optimization and physical lookup validate that
generation, validity flag, format, layout, counts, and capabilities before treating it as a cache hit. A mismatch is a
cache miss, never a best-effort read. Dropping a property graph removes all of its registry generations, payload rows,
dependencies, in-memory entries, and any owned table triggers whose dependency refcount reaches zero.
