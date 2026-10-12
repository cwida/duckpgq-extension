# SQL/PGQ grammar extension

DuckPGQ adds its SQL/PGQ syntax through the `GrammarExtension` API of DuckDB v2.0-cyanoptera: `CREATE`, `DROP`,
`DESCRIBE`/`SHOW`, and `SUMMARIZE PROPERTY GRAPH`, and `GRAPH_TABLE`. DuckDB itself is not patched, no parser code is
vendored, and the submodule points at upstream `duckdb/duckdb`.

## Activation

Loading DuckPGQ registers the `duckpgq` grammar extension. Activation is explicit and connection-local:

```sql
LOAD duckpgq;
SET active_grammar_extensions = ['duckpgq'];

CREATE TABLE vertices(id BIGINT, name VARCHAR);
CREATE PROPERTY GRAPH example VERTEX TABLES (vertices);
```

If other grammar extensions are in use, include their names in the same list. `RESET active_grammar_extensions`
restores the base grammar. Loading DuckPGQ no longer changes `allow_parser_override_extension` or intercepts every
SQL query. A new connection must activate the grammar independently.

Execute activation before sending queries containing PGQ syntax: DuckDB parses a query batch before executing
its `SET` statement. SQL `PREPARE` is currently unavailable for extension statements.

## Supported syntax

### CREATE PROPERTY GRAPH

- `CREATE PROPERTY GRAPH`, `CREATE OR REPLACE PROPERTY GRAPH`, and `CREATE PROPERTY GRAPH IF NOT EXISTS`.
  `OR REPLACE` and `IF NOT EXISTS` cannot be combined.
- A required non-empty `VERTEX TABLES (...)` clause and an optional non-empty `EDGE TABLES (...)` clause.
- Quoted and qualified table names; `AS` table aliases; explicit labels and discriminator sublabels.
- Omitted properties, `ALL PROPERTIES`, `NO PROPERTIES`, `PROPERTIES [ARE] ALL COLUMNS`,
  `PROPERTIES [ARE] ALL COLUMNS EXCEPT (...)`, and explicit property lists with optional `AS` column aliases.
- Explicit source/destination key references, including composite keys, or short references inferred from table
  primary/foreign-key constraints.
- Existing create-graph binding, validation, metadata storage, and conflict policies.

As before, the property-graph registry is keyed by the graph's unqualified name. Table/reference qualification is
preserved. Temporary property graphs, vertex-key declarations, and arbitrary property expressions are not supported.

Edge metadata stores resolved physical vertex names and qualification rather than statement-local aliases, so
new connections can reconstruct graphs created with aliased references.

### GRAPH_TABLE

- `GRAPH_TABLE (...)` and `GRAPH TABLE (...)` as a table reference anywhere DuckDB accepts one: FROM lists, joins,
  CTEs, subqueries, views, `CREATE TABLE AS`, `INSERT ... SELECT`, `COPY`, `EXPLAIN`, and prepared statements.
- Path patterns with vertex and edge elements, `:label` or `IS label`, element-local and pattern-level `WHERE`, and
  an optional `COLUMNS (...)`. Without `COLUMNS`, every vertex binding's columns are exposed.
- Edge directions `-[e]->`, `<-[e]-`, `-[e]-`, and the spaced spellings the old parser accepted.
- Quantifiers `*`, `+`, `?`, `{n}`, `{m,n}`, `{m,}`, `{,n}`, including the forms the tokenizer fuses with the arrow
  (`->*`, `->?`, `-*`, `-?`).
- Path variables, `ANY SHORTEST`, `ALL SHORTEST`, `SHORTEST k`, and the `WALK`/`TRAIL`/`SIMPLE`/`ACYCLIC` modes.
  Which of these the match rewrite can execute is decided in the rewrite, not the grammar.

### Graph-management statements

- `DROP PROPERTY GRAPH [IF EXISTS] name` transforms to the existing `DropPropertyGraphInfo` inside an
  `ExtensionStatement`, planned onto the `drop_property_graph` table function like CREATE.
- `DESCRIBE`, `SHOW`, and `SUMMARIZE PROPERTY GRAPH name` join DuckDB's native `DescribeStatement` family and rewrite
  to `SELECT * FROM describe_property_graph('name')` or `summarize_property_graph('name')`. They therefore also work
  as subqueries, e.g. `FROM (DESCRIBE PROPERTY GRAPH g)`. `describe_property_graph` now takes the graph name as its
  argument instead of reading parser state.

The GRAPH_TABLE transform builds the existing `MatchExpression` and returns a native `TableFunctionRef` to
`duckpgq_match`.
The pattern travels in `TableFunctionRef::bind_info`, DuckDB's process-local slot that survives statement copies,
so it is never bound as a scalar argument and prepared statements can be rebound. `duckpgq_match` takes no
arguments and its `bind_replace` reads the pattern from `bind_input.ref.bind_info`, rewriting a copy of it.
The old plan-time interception of SELECT statements and the operator-extension bind hook are removed.

## Integration

The `.gram` files under `src/core/parser/grammar/` define the namespaced `DuckPGQ*` rules, one complete rule per
non-comment line. CMake embeds each file in a generated header under the build directory (`duckpgq_embed_grammar`
in `src/core/parser/CMakeLists.txt`) and reconfigures when a `.gram` file changes. No generation script is required.

`create_property_graph_grammar.cpp`, `graph_table_grammar.cpp`, and `graph_management_grammar.cpp` register their
rules, attach handwritten transform callbacks with `SetTransformProcess`, and prepend their entry rules to the native
`Statement`, `InnerTableRef`, and `DescribeStatement` choices. `duckpgq_grammar_extension.cpp` combines them into the
single `duckpgq` grammar extension. Built-in
`QualifiedName`, `BaseTableName`, `TableAlias`, `ColId`, `Identifier`, `NumberLiteral`, `WhereClause`, `TargetList`,
`Parens`, and `List` rules/transforms are reused rather than copied. Keyword literals that are not DuckDB keywords
match by text, so no reserved words are added; the create-graph words are registered as unreserved keywords.

The rule transforms to the existing `CreatePropertyGraphInfo`, wrapped in a native `ExtensionStatement` carrying
`DuckPGQParseData`. The existing parser-extension plan callback invokes the create-graph table function. The parser
extension is retained for planning only: no parser override or parse fallback is registered. DuckDB handles
tokenization, statement boundaries, native SQL transforms, and syntax-error locations.

## Verification

Build static/loadable extensions, the shell, and the test runner, then run:

```sh
./build/release/test/unittest 'test/sql/grammar_extension/*'
```

The focused suite verifies activation/reset and connection isolation, ordinary SQL coexistence, mixed statements,
quoted/schema-qualified names, table/property aliases, property modes, labels, inferred/explicit references,
conflict policies, the SQL `PREPARE` limitation, GRAPH_TABLE in every table-reference position, the graph-management
statements, and syntax/semantic errors.

The full suite (`./build/release/test/unittest 'test/sql/*'`) passes. Every legacy test activates the grammar right
after its `require` lines, once per connection it uses, because activation is connection-local. Expected
parser-error messages in `parser_arrow_kleene.test` no longer pin the offending token, since DuckDB's tokenizer
decides token boundaries (for example `/->` or `<->` are single operator tokens).
