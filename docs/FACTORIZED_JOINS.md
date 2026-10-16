# Factorized joins

DuckPGQ registers an optimizer extension, `FactorizedJoinOptimizer`
(`src/core/optimizer/factorized_join_optimizer.cpp`), that keeps aggregates over n:m joins from
enumerating every joined row. It applies to every SQL query on a connection that has the extension
loaded, not only to `GRAPH_TABLE`, and can be switched off with

```sql
SET duckpgq_factorized_joins = false;
```

## What it does

Counting two-hop paths expands the pattern into a self-join of the edge table on the middle vertex:

```sql
SELECT count(*) FROM GRAPH_TABLE (g
    MATCH (a:Person)-[r1:Follows]->(b:Person)-[r2:Follows]->(c:Person)
    COLUMNS (a.id AS a_id));
```

Without the rewrite DuckDB builds a hash table on one `Follows` and emits one row per path before
counting them. The number of paths grows much faster than the number of edges when the graph has
hubs. The rewrite notices that nothing above the join uses any column of either side except the join
key, replaces each side by `GROUP BY key, count(*)`, and turns `count(*)` into
`sum(count_left * count_right)`:

```
Projection          CAST(COALESCE(sum, 0) AS BIGINT)
└─ Aggregate        sum(cnt_l * cnt_r)
   └─ Hash Join     (b = b)
      ├─ Aggregate  GROUP BY r1.dst, count(*)    over  Follows r1 ⋈ Person b
      └─ Aggregate  GROUP BY r2.src, count(*)    over  Follows r2 ⋈ Person c ⋈ Person a
```

This is the degree product `sum over b of in_degree(b) * out_degree(b)`, which is linear in the number
of edges. The build side of the join now stores one count per key, the same information the cached
chain-head aggregates of [Groß, ten Wolde and Boncz, CIDR 2025](https://www.vldb.org/cidrdb/papers/2025/p21-gro.pdf)
keep inside the hash table.

The rewrite is general. Starting from an aggregate it walks down through projections, filters and
inner equi-joins. A join side that contributes no column needed above the join is pre-aggregated by
its join keys and yields a multiplicity column; a side that is needed is kept and only forwards the
multiplicities of the joins below it. Longer chains therefore factorize per vertex, and grouped
aggregates keep their grouping columns.

| Aggregate over the joined rows | Factorized form                                   |
| ------------------------------ | ------------------------------------------------- |
| `count(*)`                     | `sum(m)`                                          |
| `count(x)`                     | `sum(CASE WHEN x IS NULL THEN 0 ELSE m END)`      |
| `sum(x)`                       | `sum(x * m)` for integer and floating-point `x`   |

For floating-point `x` the products are summed in a different order than the flattened rows would be,
so the last bits of the result can differ from the un-factorized plan, as they already do between
DuckDB's parallel aggregation orders.
| `min(x)`, `max(x)`             | unchanged                                         |

where `m` is the product of the multiplicities. An ungrouped count is wrapped in `COALESCE(..., 0)`
because a sum over no rows is `NULL`. Everything else leaves the plan alone: `DISTINCT` aggregates,
`avg`, aggregates with `FILTER` or `ORDER BY`, grouping sets, outer joins, joins with non-equality
conditions, and queries that return columns of both sides.

## The FACTORIZED_GROUP_JOIN operator

When both sides of a join collapse, the pass does not emit two aggregates and a hash join but a single
`LogicalFactorizedGroupJoin` (`src/core/operator/`), modelled on the factorized GroupJoin of
[kryonix/duckdb](https://github.com/kryonix/duckdb/tree/duckdb-factorized-groupjoin) and the cached
chain-head aggregates of the CIDR 2025 paper:

```
Aggregate                     sum(cnt_l * cnt_r)
└─ FACTORIZED_GROUP_JOIN      keys, cnt_l, cnt_r         one row per key found on both sides
   ├─ Follows r1 ⋈ Person b                               left factor
   └─ Follows r2 ⋈ Person c                               right factor
```

The physical operator owns one `RadixPartitionedHashTable` keyed by the join keys with two aggregates,
`count_star()` (or `sum(weight)` when the side carries multiplicities from a join below it) for the left
and for the right side. Each side is sunk by a helper operator that updates only its own aggregate, both
sides run as concurrent child pipelines with per-thread tables that are merged on `Combine`, and the
second side to finish finalizes the table. The operator itself is the source: it scans the table in
parallel and drops keys that only one side produced. Rows with a NULL key are dropped before they reach
the table, so NULL never matches NULL.

Compared to the aggregate-plus-join plan this removes the third hash table and the materialised
aggregate outputs, which by itself is worth little (at 300K persons q8 went from 66 ms to 53 ms):
hashing the edge keys is the cost, not the join.

### Directory lookup

What does pay off is not hashing at all. When the join has a single integer key and the statistics
of both sides give its bounds, the operator counts into a dense array indexed by `key - min`
(`Lookup: DIRECTORY` in EXPLAIN) instead of the hash table. Vertex ids are dense, so this is the normal
case for graph patterns. Every sinking thread holds private counters, because hub vertices receive
tens of thousands of increments and shared counters contend on them (a first version with shared
atomics was twice as slow as the hash table); the private arrays are merged once on `Combine`. The
directory is used when the key range is at most 2^23 values and the per-thread arrays fit comfortably
in memory, otherwise the hash table is used. Keys outside the statistics bounds fail the query rather
than producing a wrong count. Weighted multiplicities are accumulated in BIGINT: they are partial path
counts, so they can only overflow when the final BIGINT count would overflow as well.
`SET duckpgq_factorized_join_directory = false;` disables it.

In EXPLAIN the two build sides appear as children of the operator (`FACTORIZED_GROUP_JOIN_LEFT` and
`_RIGHT`), so EXPLAIN ANALYZE attributes the build time to them.

| Persons | q8 logical rewrite | q8 operator, hash | q8 operator, directory | q9 directory | 3-hop directory |
| ------- | ------------------ | ----------------- | ---------------------- | ------------ | --------------- |
| 300K    | 58 ms              | 51 ms             | 14 ms                  | 15 ms        | 104 ms          |
| 1M*     | 490 ms             | 417 ms            | 160 ms                 | 150 ms       | 1,174 ms        |

\* 1M persons from a file-backed database, where the two scans of the 145M-row edge table are now the
largest cost together with the three edge-to-vertex label joins. The three-hop count gains less because
only its top join uses the operator; the inner one-sided collapse still goes through a hash aggregate.

The operator can be switched off with `SET duckpgq_factorized_join_operator = false;`, which falls
back to the plan above. Plans containing it cannot be serialized.

## When it fires

A side is only pre-aggregated when the cardinality estimates say the join is n:m: both inputs must
have at least twice as many rows as the estimated number of matched join keys, the same test DuckDB's
own partial aggregate pushdown uses. A one-sided test is not enough: an edge table joined to a handful
of filtered vertices has many rows per matched key, but grouping it is far slower than probing the small
hash table, and a side whose key is unique gains nothing from being grouped (this cost the benchmark's
q5 about 1.5 ms before the rule became two-sided). Without estimates the rewrite is applied, because
the pre-aggregation costs about as much as the hash join build it replaces.

The pass runs after DuckDB's own optimizers. DuckDB 2.0 already pushes partial aggregates below a join
when the aggregate has `GROUP BY` columns (`PartialAggregatePushdown`); this pass covers the ungrouped
case and chains of joins, and leaves plans that DuckDB rewrote alone.

## Measured effect

Synthetic social graph with 10 follows per person plus hubs (`test/sql/factorized_path_count.test`
uses the 10K-person variant), M5 Pro, 18 threads:

| Persons | Edges | Two-hop paths  | `count(*)` before | `count(*)` after | Hand-written degree product |
| ------- | ----- | -------------- | ----------------- | ---------------- | --------------------------- |
| 100K    | 2.3M  | 54,211,695     | 121 ms            | 13 ms            | 10 ms                       |
| 300K    | 15.5M | 751,536,843    | 2,266 ms          | 14 ms            | 74 ms                       |

The three-hop count at 300K persons (37.6 billion paths) takes 0.1 s. The remaining gap to systems with
adjacency storage (Ladybug counts two-hop paths at 1M persons in 18 ms against our ~0.4 s) is that every
query still passes over the edge table once per side; closing it needs degrees that persist across
queries, which is the planned next step.
