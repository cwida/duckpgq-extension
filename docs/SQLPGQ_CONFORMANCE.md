# SQL/PGQ conformance status

This document records how far `GRAPH_TABLE` in DuckPGQ conforms to the pattern-matching semantics of
ISO/IEC 9075-16:2023 (SQL/PGQ). Feature IDs are the optional-feature identifiers of Annex D; clause
numbers refer to the same part. Features marked **mandatory** are required by clause 18.1 for any
conformance claim.

The conformance tests live in `test/sql/conformance/`, one file per feature group, named after the
feature IDs they exercise. Each test file states the clause it is derived from.

## Implementation-defined choices

| Item | Choice |
|---|---|
| Default match mode (ID022) | `REPEATABLE ELEMENTS`. Vertices and edges may repeat within a match. |
| Element identity | A graph element is a row of one vertex or edge table; `rowid` is its identity. Every element pattern therefore binds to exactly one table. |
| Element pattern without a label | Standard: matches any element. DuckPGQ: a named element whose variable is a registered label uses that label (historical reading, `(a)` is `(a:a)`); otherwise the table is inferred when exactly one vertex table, or exactly one edge table connecting the endpoint tables in the pattern's direction, exists. Anything else is a binder error. |
| Tie-breaking in `ANY SHORTEST` | Whichever path the breadth-first search reaches first; not stable across runs. |
| Edge tables with a `source` equal to the `destination` table | Undirected patterns `-[e]-` match each edge in both directions. |

## Deviations and extensions

| Item | Status |
|---|---|
| `vertices(p)`, `edges(p)`, `element_id(p)` on a path variable | DuckPGQ extensions returning lists of `rowid`s. Part 16 only has `PATH_LENGTH`. |
| `OPTIONAL MATCH`, `CHEAPEST` | Not in Part 16. Not implemented. |

## Feature inventory

Status values: **yes** (implemented and tested), **partial** (see notes), **parse** (accepted by the
grammar, rejected by the binder with a clear error), **no**.

| Feature ID | Feature | Status | Notes |
|---|---|---|---|
| **G900** | `GRAPH_TABLE` | yes | |
| **G920, G924, G925** | Property graph DDL with key, label and properties clauses | yes | |
| **G000, G008** | Graph pattern with `WHERE` | yes | |
| **G001** | `REPEATABLE ELEMENTS` match mode | yes | Default; not spelled out in the grammar yet |
| G002, G003 | `DIFFERENT EDGES`, explicit `REPEATABLE ELEMENTS` | no | |
| **G034** | Path concatenation | yes | |
| **G040** | Vertex pattern | yes | Anonymous `()` binds to the only vertex table; with several vertex tables a label is required |
| **G042** | Basic full edge patterns `-[]->`, `<-[]-`, `-[]-` | yes | Anonymous `-[]->` binds to the only edge table that connects its endpoints in that direction |
| G043 | Complete full edge patterns `<-[]->`, `~[]~`, `<~[]~`, `~[]~>` | partial | `<-[]->` only, and not with a quantifier |
| G044, G045 | Abbreviated edge patterns `->`, `<-`, `-`, `<->` | no | |
| G046, G047 | Relaxed topological consistency | no | |
| **G070** | Label disjunction `\|` | no | mandatory |
| G071, G072, G074, G075 | Label `&`, `!`, `%`, parentheses | no | |
| **G073** | Individual label name | yes | Sublabels via `LABEL x IN column (a, b)` |
| **G090** | Property reference | partial | Only bare column references are validated |
| G004 | Path variables | yes | |
| G005 | Path search prefix in a path pattern | partial | See selectors |
| G006, G007 | `KEEP` | no | |
| G010 | `WALK` | yes | |
| G011 to G013 | `TRAIL`, `SIMPLE`, `ACYCLIC` | parse | Need path enumeration |
| G014 | `PATH` / `PATHS` keywords | no | |
| G015 | `ALL` path search | partial | Single-edge patterns are correct. With a quantifier the result is one row per endpoint pair, filtered on the shortest length |
| G016 | `ANY [k]` | no | |
| G017 | `ALL SHORTEST` | parse | |
| G018 | `ANY SHORTEST` | partial | Correct without bounds; with bounds the shortest path is selected before the bounds are applied (9.8 requires the opposite order) |
| G019 | `SHORTEST k` | parse | |
| G020 | `SHORTEST k GROUP` | no | |
| G030 to G033 | Path alternation `\|`, `\|+\|` | no | |
| G035, G036 | Quantified paths and edges | partial | Edges only; bounds are a post-filter on the shortest length |
| G037 | Questioned path primary `?` | partial | Evaluated as `{0,1}` including the zero-length path; variables are not exposed as conditional singletons |
| G038, G048 to G051 | Parenthesised path patterns, subpath variables and modes | parse | |
| G060, G061 | Bounded / unbounded quantifiers | yes | 10.7 SR 3 bounds checks: upper bound > 0, lower <= upper (issue #9) |
| G039, G080 to G082 | Simplified path pattern expression `-/ /->` | no | |
| G100 | `ELEMENT_ID` | partial | Path variables only |
| G800 to G803 | `PATH_NAME`, `ELEMENT_NUMBER`, `PATH_LENGTH`, `MATCHNUM` | partial | `PATH_LENGTH` only |
| G110 to G113 | `IS DIRECTED`, `IS LABELED`, `IS SOURCE OF`, `IS DESTINATION OF` | no | |
| G114, G115 | `ALL_DIFFERENT`, `SAME` | no | |
| G810, G811 | `IS BOUND` | no | |
| G120, G840 | Within-match aggregates | no | |
| G901 to G903, G910 | `ONE ROW PER MATCH / VERTEX / STEP` | no | |
| G904, G905 | `v.*`, optional `COLUMNS` | yes | |
| G906 to G909 | `EXPORT` clause | no | |

## Progress log

Most recent first. Each entry names the commit's scope and the feature IDs it touched.

- Anonymous vertex and edge patterns `()`, `(:L)`, `-[]->`, `-[:L]->` (10.6, G040, G042; issue #195). The label of an anonymous element is inferred when exactly one table fits; otherwise the binder names the candidates. Adds `G040_anonymous_elements.test`.
- `?`, `{0,1}` and `{,1}` include the zero-length path. They were evaluated as a plain edge join, so `(a)-[e]->?(b)` never returned `a` itself. G037, G060.
- Left-pointing edges `<-[e]-` with a quantifier are evaluated along the edge direction (4.7.2); previously a NotImplementedException. Adds `G042_edge_direction.test`, which also pins the directed semantics of `-[e]->` with quantifiers (issue #284).
- Quantifier bounds (10.7 SR 3): `{0,0}`, `{0}`, `{,0}` and `{3,1}` are syntax errors. G060, G061; closes #9.
- Added this document and the `test/sql/conformance/` layout.
