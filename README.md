# DuckPGQ

DuckPGQ is a DuckDB extension for graph workloads with support for SQL/PGQ.

For user documentation, examples, and background, see [duckpgq.org](https://duckpgq.org/).

[![Discord](https://discordapp.com/api/guilds/1225369321077866496/widget.png?style=banner3)](https://discord.gg/8X95XHhQB7)

## Status

DuckPGQ is a research project and a work in progress. Feedback, bug reports, and contributions are welcome.

The SQL/PGQ syntax (`CREATE`, `DROP`, `DESCRIBE` and `SUMMARIZE PROPERTY GRAPH`, and `GRAPH_TABLE` queries) is
provided through DuckDB's composable `GrammarExtension` API, so DuckPGQ needs no patched or vendored parser.
See [the grammar notes](docs/GRAMMAR_EXTENSION.md) for activation, supported syntax, and how the grammar is wired in.

## Installation

DuckPGQ is available as a DuckDB community extension:

```sql
INSTALL duckpgq FROM community;
LOAD duckpgq;
```

See the [DuckPGQ community extension page](https://community-extensions.duckdb.org/extensions/duckpgq.html) for package availability.

## Quick Example

```sql
SET active_grammar_extensions = ['duckpgq'];

CREATE TABLE Person(id BIGINT, name VARCHAR);
CREATE TABLE Person_knows_Person(person1id BIGINT, person2id BIGINT);

INSERT INTO Person VALUES
    (1, 'Alice'),
    (2, 'Bob');

INSERT INTO Person_knows_Person VALUES
    (1, 2);

CREATE PROPERTY GRAPH social
VERTEX TABLES (
    Person
)
EDGE TABLES (
    Person_knows_Person
        SOURCE KEY (person1id) REFERENCES Person (id)
        DESTINATION KEY (person2id) REFERENCES Person (id)
        LABEL Knows
);

SELECT *
FROM GRAPH_TABLE (snb
    MATCH (a:Person)-[k:Knows]->(b:Person)
    COLUMNS (a.id AS person, b.id AS friend)
);
```

## Building From Source

Initialize submodules before building:

```sh
git submodule update --init --recursive
```

Build the extension and the bundled DuckDB shell:

```sh
make release GEN=ninja
```

For a debug build:

```sh
make debug GEN=ninja
```

Build artifacts are written under `build/<build-type>/`:

| Artifact | Release path | Debug path |
| --- | --- | --- |
| DuckDB shell | `build/release/duckdb` | `build/debug/duckdb` |
| Test runner | `build/release/test/unittest` | `build/debug/test/unittest` |
| Loadable extension | `build/release/extension/duckpgq/duckpgq.duckdb_extension` | `build/debug/extension/duckpgq/duckpgq.duckdb_extension` |

DuckPGQ uses the DuckDB extension build system. If dependency resolution fails, install and configure [vcpkg](https://vcpkg.io/en/getting-started), then set `VCPKG_TOOLCHAIN_PATH` to your vcpkg toolchain file.

## Running a Local Build

Start the locally built DuckDB shell:

```sh
./build/release/duckdb
```

The extension is linked into this shell.

Enable the SQL/PGQ grammar on each connection with `SET active_grammar_extensions = ['duckpgq'];`.

## Testing

Run the SQL test suite:

```sh
make test_release
```

Every test activates the grammar explicitly, including on each named connection it uses. The focused grammar
tests live under `test/sql/grammar_extension/`.

Run a single SQL logic test:

```sh
make test T=test/sql/pattern_matching/undirected_edges.test
```

Run clang-tidy checks:

```sh
make tidy-check
```

## Repository Notes

- SQL tests live in `test/sql`.
- Extension source lives in `src`.
- Grammar integration lives under `src/core/parser` and extends DuckDB's native grammar without a vendored parser.
- Maintenance notes for updating DuckDB or parser dependencies are in [docs/UPDATING.md](docs/UPDATING.md).
- Aggregates over n:m joins are rewritten into factorized form by an optimizer extension, see
  [docs/FACTORIZED_JOINS.md](docs/FACTORIZED_JOINS.md). Disable it with `SET duckpgq_factorized_joins = false;`,
  or only its `FACTORIZED_GROUP_JOIN` operator with `SET duckpgq_factorized_join_operator = false;`, or
  the operator's dense-array counting with `SET duckpgq_factorized_join_directory = false;`.
- `CREATE PROPERTY GRAPH` verifies that every edge key references an existing vertex row and fails with an
  "Incomplete edge" error otherwise. This lets the `MATCH` translation leave out vertex tables that a query only
  uses for their unique key, joining the incident edge tables to each other instead, see
  [docs/VERTEX_JOIN_ELIMINATION.md](docs/VERTEX_JOIN_ELIMINATION.md). Disable the rewrite with
  `SET duckpgq_vertex_join_elimination = false;`.
