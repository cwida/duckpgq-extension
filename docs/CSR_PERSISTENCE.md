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

Version 1 uses three reserved DuckDB tables:

- `__duckpgq_csr_registry` identifies the active immutable generation and records validity, format version, counts,
  capabilities, layout, and payload cardinalities;
- `__duckpgq_csr_segments` stores ordered forward-CSR partition payloads, including partition ranges, sparse source
  rows, row offsets, local destinations, and optional streaming segments; and
- `__duckpgq_csr_dependencies` maps artifacts to their physical vertex and edge tables. Dependency population and DML
  invalidation are implemented in the subsequent lifecycle phases.

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
