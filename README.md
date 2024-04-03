# DuckPGQ
A DuckDB extension for graph workloads that supports the SQL/PGQ standard. For more information, please see the [documentation page](https://duckpgq.notion.site/duckpgq/b8ac652667964f958bfada1c3e53f1bb?v=3b47a8d44bdf4e0c8b503bf23f1b76f2).

## WIP Disclaimer
This repository is currently a research project and a work in progress. Feel free to play around with it and give us feedback. NOTE: Currently any query containing SQL/PGQ syntax requires a `-` at the start of the query (else you will get a segmentation fault). 

---

This repository uses a modified version of DuckDB (Currently up-to-date with v0.10.1) and is not yet easily installed from a standard DuckDB (e.g. `pip install duckdb`) installation. 
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

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `duckpgq()` that takes a string arguments and returns a string:
```
D select duckpgq('Jan') as result;
┌───────────────┐
│    result     │
│    varchar    │
├───────────────┤
│ Duckpgq Jan 🐥│
└───────────────┘
```

## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
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
