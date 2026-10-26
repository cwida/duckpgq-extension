# Planning MATCH onto the reach mode (W29, step 2)

Status: design note, 23 October 2026. The operator side of the reach mode is done (`bf149024`): `reachoperator(src,
src, ...)` yields one `[dst, len]` row per vertex a source reaches, on the build route and over a CSR index. This
note describes how `MATCH` should choose it, grounded in the current `match.cpp` (functions `AddPathFinding`,
`CreatePathFindingEndpointSubquery`, the result assembly around `PATH_FINDING_RESULT_PREFIX`).

## 1. What the pair mode does today

For a pattern `(a)-[e:KNOWS]->{l,u}(b)` MATCH builds one SELECT in three layers:

```
outer:      SELECT <projections, path functions derived from the result column>
            FROM pairs WHERE len BETWEEN l AND u
operator:   SELECT candidate.*, iterativelengthoperator(src, dst, <edges>, V, E, key) AS __duckpgq_result_0
            FROM candidate [, endpoint input]
candidate:  SELECT a.rowid AS src, b.rowid AS dst, <a.*, b.*, other elements>
            FROM a, b, <other pattern elements> WHERE <all predicates on a, b, ...>
```

The candidate query is a cross product of the source and destination vertex tables restricted by every WHERE
predicate of the pattern. When `b` carries no predicate, the candidate query is `|a| x |b|` rows and the operator
sinks, groups and scatters that list although the search would discover every destination by itself (plan, 23 Oct
entries: 70M pairs at SF10, 1.2G pairs at SF300 for 1024 sources). The reach mode exists to skip the list.

## 2. Recognising the shape

MATCH knows, for the destination element `b`, (i) whether the pattern's WHERE mentions it (the predicates are
collected per element into `conditions`; `PGQNormalizeGraphElementRefs` rewrites the references), (ii) whether its
columns are projected, and (iii) whether it is bound to another pattern element by a second edge.

Rule, in the order checked:

1. **Unbound destination**: `b` has no predicate in the WHERE and is the last element of the path pattern (nothing
   continues from it). Use reach mode. Its projected columns are fetched by a join on the result's `dst`.
2. **Weakly bound destination**: `b` has a predicate but the predicate is not a key lookup (`b.id = c` or `b.id IN
   (...)`), for example `b.cityId = 42` or `b.name LIKE 'A%'`. Use reach mode and apply the predicate after the join
   on `dst`. This is the LDBC BI19 shape (every person of city 1 to every person of city 2). The alternative, the pair
   list restricted by the predicate, costs the cross product of the source set with the selected destinations plus a
   traversal that cannot stop early; reach mode costs the traversal plus a join. Reach mode wins unless the selected
   destinations are very few; that threshold is the cost rule of W12 (section 5), and the default here is reach.
3. **Bound destination**: `b.id = c`, `b.id IN (...)`, or `b` joined to another element. Pair mode as today.
4. **Path projections** (`element_id(p)`, `vertices(p)`, `edges(p)`): pair mode, because paths are reconstructed per
   (source, destination) pair with parent pointers; reach mode returns distances only. `path_length(p)` alone is
   served by reach mode (it is `len`).

The source side is unchanged: `a` keeps its predicates in the candidate query, which now has only `a`'s rows.

## 3. The rewrite

```
outer:      SELECT <projections>, r[2] AS path_length
            FROM reached JOIN b_table b ON b.<key or rowid> = reached.dst
            WHERE r[2] BETWEEN l AND u AND <predicates on b>
reached:    SELECT candidate.*, reachoperator(src, src, <edges>, V, E, key[, u]) AS r
            FROM candidate [, endpoint input]
candidate:  SELECT a.rowid AS src, <a.*, other elements> FROM a, <other elements> WHERE <predicates on a, ...>
```

Details:

- `reachoperator` repeats the source as its second argument (the operator ignores it; the argument forms stay
  those of `iterativelengthoperator`, so the optimizer rule and both input forms are reused unchanged).
- `r[1]` is a vertex **row id** on the build route and a vertex **key** over an index, exactly as `vertices(p)`
  behaves today; the join uses `b.rowid` or `b.<key>` accordingly. MATCH already knows which route it planned
  (`use_index` in the result assembly).
- The upper bound `u` is passed as the operator's `max_depth` as today; the lower bound stays a filter. A `{0,u}`
  quantifier keeps the source's own row (`len = 0`); `{1,u}` filters it.
- Predicates on `b` move from the candidate query to the outer query. They must not reference `e` (the edge of a
  variable-length pattern has no single row), which MATCH already enforces for the pair mode.
- The outer join on `dst` is a hash join of `|reached|` rows against the vertex table; at SF10 for 1024 sources that
  is 22.5M probes, about 0.3 s, against 0.2 s for the traversal. When no column of `b` is projected and `b` has no
  predicate, the join is skipped entirely (count and sum queries, the sweep).
- Pass-through columns: the operator repeats the source row's columns once per reached vertex as constant vectors,
  so projecting `a.*` costs nothing extra in the operator and the usual amount in the join above it.

## 4. Where it lives in `match.cpp`

- `AddPathFinding(...)`: after `return_path` is known, compute `destination_kind` (unbound / weak / bound) from
  `conditions` for `next_binding` and from whether `next_vertex_element` is the last element. Push a
  `PathFindingOperatorResult` with a new field `reach = true`; its `destination_expression` is unused.
- Result assembly (the block that builds `path_length_sql`): emit `reachoperator(` with the source expression
  twice when `reach`; the outer query derives `path_length` as `r[2]` and adds the join on `r[1]` when `b` is
  projected or has a predicate. `projected_aliases` already carries which path functions are requested; `reach`
  rejects `element_id`, `vertices`, `edges` (rule 4 above sends those to pair mode before this point).
- `CreatePathFindingEndpointSubquery` is unchanged; the undirected pattern's both-directions input and the
  `|undirected` cache key (reverse orientation) work as they do now, although reach mode is forward only.
- The CSR index route needs nothing new: `CSRIndex::FindForEdgeTable` picks the index as today and the operator
  translates the source keys through the snapshot's dictionary.

## 5. Cost rule (W12 hook)

Inputs available at plan time: `|a_selected|` (unknown, estimate from the predicate), `V`, `E`, whether `b` is
bound. Reach mode costs `|a_selected|` traversals plus `sum(reachable)` output rows; pair mode costs the same
traversals (one per source, or one multi-source traversal per 512 sources when the destinations are few) plus the
pair list `|a_selected| x |b_selected|`. The switch point is where `|b_selected|` falls below the average reachable
set, roughly `V / 4` for LDBC knows. A first rule that needs no estimates: reach mode when `b` is unbound or weakly
bound, pair mode when `b` is bound by key; the exact threshold can follow once W12 has the estimates.

## 6. Tests to add

- `(a WHERE a.id IN (...))-[e:PE]->{1,4}(b)` with `b.id` projected: rows equal the pair-mode query over `a x pv`,
  strategy string shows `mode=reach`.
- Weakly bound `b` (`b.id > 2`): same rows filtered.
- `{0,u}` keeps `len = 0` rows, `{1,u}` drops them.
- Undirected pattern `-[e:PE]-` on both routes.
- `element_id(p)` projected: strategy string shows `mode=shortestpath` (pair mode kept).
- Over a CSR index with a source key absent from the graph: the source appears once with `len = 0`.

## 7. What this changes in the paper

The sweep's sources x all persons column becomes a MATCH query rather than a marker call, and BI19 can be written
as the natural pattern `(a WHERE a.cityId = c1)-[:knows]-{1,30}(b WHERE b.cityId = c2)` on every system, including
ours, which is the predicate-driven multi-source query W27 asks for.
