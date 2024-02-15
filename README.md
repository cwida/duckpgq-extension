# DuckPGQ
A DuckDB extension for graph workloads that supports the SQL/PGQ standard. 

# WIP Disclaimer
This repository is currently a research project and a work in progress. Feel free to play around with it and give us feedback. NOTE: Currently any query containing SQL/PGQ syntax requires a `-` at the start of the query (else you will get a segmentation fault). 

## Getting started
To get started, first clone this repository:
```sh
git clone --recurse-submodules git@github.com:cwida/duckpgq-extension.git
```
Note that `--recurse-submodules` will ensure the correct version of DuckDB is pulled allowing you to get started right away.

This repository uses a modified version of DuckDB (currently a modified v0.8.1) and is not yet easily installed from a standard DuckDB (e.g. `pip install duckdb`) installation. 
If you want to use the SQL/PGQ syntax, you will have to build this repository from the source. 
In the future, we aim to have an easily installed and loaded DuckDB extension. 

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
make
```
Or if you have ninja installed: 
```sh
make GEN=ninja
```

The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/duckpgq/duckpgq.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `duckpgq.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

Now we can use the features from the extension directly in DuckDB. To verify that the extension works correctly you can run the following query: 
```
D select duckpgq('Jan');
┌───────────────────┐
│ duckpgq('Jan')    │
│      varchar      │
├───────────────────┤
│ Duckpgq Jan 🐥    │
└───────────────────┘
```

## SQL/PGQ
SQL/PGQ is a graph query language built on top of SQL, bringing graph pattern matching capabilities to existing SQL users as well as to new users who are interested in graph technology but who do not have an SQL background.

SQL/PGQ is standardized by the International Organization for Standardization (ISO). It provides a declarative language for querying property graphs, which are a type of graph data model that stores nodes, edges, and properties on both nodes and edges.

SQL/PGQ queries use a visual graph syntax that is similar to that of Cypher, another popular graph query language. However, SQL/PGQ also supports traditional SQL syntax, which makes it easy for SQL users to get started with graph querying.

SQL/PGQ can be used to query property graphs for a variety of purposes, including:

- Finding paths between nodes
- Finding nodes that match certain criteria
- Finding the shortest path between two nodes
- Finding the most connected nodes in a graph
- Finding the most influential nodes in a graph
SQL/PGQ is a powerful tool for querying graph data, and it is becoming increasingly popular as graph databases become more widely adopted.


## Example
First load some data into DuckDB. The following loads the LDBC Social Network Benchmark dataset
```sql
import database 'duckdb-pgq/data/SNB0.003';
```

The first step of SQL/PGQ is to register a property graph: 
```sql
-CREATE PROPERTY GRAPH snb
VERTEX TABLES (
    Person LABEL Person
    )
EDGE TABLES (
    person_knows_person     SOURCE KEY ( person1id ) REFERENCES Person ( id )
                            DESTINATION KEY ( person2id ) REFERENCES Person ( id )
                            LABEL Knows
    );
```

Here is a simple example of a SQL/PGQ query:
```sql
-SELECT *
FROM GRAPH_TABLE (snb
    MATCH (p:Person)-[k:knows]->(f:Person)
    WHERE p.firstname = 'Jan'
    COLUMNS(f.firstname)
) x;
```

This query will find all of Jan's friends and return their first names.

SQL/PGQ is a complex language, but there are resources available to help users get started. 
- [DuckPGQ: Efficient Property Graph Queries in an analytical RDBMS](https://www.cidrdb.org/cidr2023/papers/p66-wolde.pdf)

## Contributing
Want to contribute to the project? Great! Please refer to DuckDB's own [development](https://github.com/duckdb/duckdb#development) and [contribution](https://github.com/duckdb/duckdb/blob/main/CONTRIBUTING.md) guides which we tend to follow to see how you can help us out. If you are unsure, do not hesitate to reach out. 

For development, you generally want to build using debug mode:
```sh
make debug
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
[Currently not possible] To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL duckpgq
LOAD duckpgq
```

## Setting up CLion 

### Opening project
Configuring CLion with the extension template requires a little work. Firstly, make sure that the DuckDB submodule is available. 
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

### Debugging
To set up debugging in CLion, there are two simple steps required. Firstly, in `CLion -> Settings / Preferences -> Build, Execution, Deploy -> CMake` you will need to add the desired builds (e.g. Debug, Release, RelDebug, etc). There's different ways to configure this, but the easiest is to leave all empty, except the `build path`, which needs to be set to `../build/{build type}`. Now on a clean repository you will first need to run `make {build type}` to initialize the CMake build directory. After running make, you will be able to (re)build from CLion by using the build target we just created.

The second step is to configure the unittest runner as a run/debug configuration. To do this, go to `Run -> Edit Configurations` and click `+ -> Cmake Application`. The target and executable should be `unittest`. This will run all the DuckDB tests. To specify only running the extension specific tests, add `--test-dir ../../.. [sql]` to the `Program Arguments`. Note that it is recommended to use the `unittest` executable for testing/development within CLion. The actual DuckDB CLI currently does not reliably work as a run target in CLion.
