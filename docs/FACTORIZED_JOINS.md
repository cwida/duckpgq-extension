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
| `min(x)`, `max(x)`             | unchanged                                         |

where `m` is the product of the multiplicities. An ungrouped count is wrapped in `COALESCE(..., 0)`
because a sum over no rows is `NULL`. Everything else leaves the plan alone: `DISTINCT` aggregates,
`avg`, aggregates with `FILTER` or `ORDER BY`, grouping sets, outer joins, joins with non-equality
conditions, and queries that return columns of both sides.

## When it fires

A side is only pre-aggregated when the cardinality estimates say the join is n:m on that side: the
side must have at least twice as many rows as the estimated number of distinct join keys. Joins from
an edge table to its vertex table on a unique key are therefore left alone. Without estimates the
rewrite is applied, because the pre-aggregation costs about as much as the hash join build it replaces.

The pass runs after DuckDB's own optimizers. DuckDB 2.0 already pushes partial aggregates below a join
when the aggregate has `GROUP BY` columns (`PartialAggregatePushdown`); this pass covers the ungrouped
case and chains of joins, and leaves plans that DuckDB rewrote alone.

## Measured effect

Synthetic social graph with 10 follows per person plus hubs (`test/sql/factorized_path_count.test`
uses the 10K-person variant), M5 Pro, 18 threads:

| Persons | Edges | Two-hop paths  | `count(*)` before | `count(*)` after | Hand-written degree product |
| ------- | ----- | -------------- | ----------------- | ---------------- | --------------------------- |
| 100K    | 2.3M  | 54,211,695     | 121 ms            | 12 ms            | 10 ms                       |
| 300K    | 15.5M | 751,536,843    | 2,266 ms          | 89 ms            | 74 ms                       |

The three-hop count at 300K persons (37.6 billion paths) takes 0.3 s.
