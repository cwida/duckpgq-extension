# DuckPGQ
A DuckDB extension for graph workloads that supports the SQL/PGQ standard. For more information, please see the [documentation page](https://duckpgq.org/).

[![Discord](https://discordapp.com/api/guilds/1225369321077866496/widget.png?style=banner3)](https://discord.gg/8X95XHhQB7)
## WIP Disclaimer
This repository is currently a research project and a work in progress. Feel free to play around with it and give us feedback.

---

## Loading DuckPGQ 
Since DuckDB v1.0.0, DuckPGQ can be loaded as a community extension without requiring the `unsigned` flag. From any DuckDB instance, the following two commands will allow you to install and load DuckPGQ:
```sql
install duckpgq from community;
load duckpgq;
```
See the [DuckPGQ community extension page](https://community-extensions.duckdb.org/extensions/duckpgq.html) for more information.

For older DuckDB versions there are two ways to install the DuckPGQ extension. 
Both ways require DuckDB to be launched in the `unsigned` mode.
The first way is by setting the `custom_extension_repository` command (see below). The other way is by directly downloading the extension file for your OS + architecture (see the [DuckPGQ availibility section](#duckpgq-extension-availability))

For CLI: 
```bash
duckdb -unsigned
```
```bash
set custom_extension_repository = 'http://duckpgq.s3.eu-north-1.amazonaws.com';
force install 'duckpgq';
load 'duckpgq';
```

For Python: 
```python
import duckdb

conn = duckdb.connect(config = {"allow_unsigned_extensions": "true"})
conn.execute("set custom_extension_repository = 'http://duckpgq.s3.eu-north-1.amazonaws.com';")
conn.execute("force install 'duckpgq';")
conn.execute("load 'duckpgq';")
```

## Direct download
To use the extension, check the direct download links below. To install and load the extension, launch DuckDB in unsigned mode and execute the commands:
```sql
force install 'path/to/duckpgq_extension';
load 'duckpgq'; 
```

## DuckPGQ Extension Availability

<details>
<summary>Version v1.1.3</summary>

### Linux

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [linux_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/linux_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [linux_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/linux_arm64/duckpgq.duckdb_extension.gz>) |

### Osx

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [osx_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/osx_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/osx_arm64/duckpgq.duckdb_extension.gz>) |

### Wasm

| Architecture | Download Link |
|--------------|---------------|
| eh        | [wasm_eh](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/wasm_eh/duckpgq.duckdb_extension.wasm>) |
| mvp        | [wasm_mvp](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/wasm_mvp/duckpgq.duckdb_extension.wasm>) |
| threads        | [wasm_threads](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/wasm_threads/duckpgq.duckdb_extension.wasm>) |

### Windows

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [windows_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/windows_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_rtools        | [windows_amd64_rtools](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.3/windows_amd64_rtools/duckpgq.duckdb_extension.gz>) |

</details>

<details>
<summary>Version v1.1.2</summary>

### Linux

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [linux_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/linux_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [linux_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/linux_arm64/duckpgq.duckdb_extension.gz>) |

### Osx

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [osx_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/osx_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/osx_arm64/duckpgq.duckdb_extension.gz>) |

### Wasm

| Architecture | Download Link |
|--------------|---------------|
| eh        | [wasm_eh](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/wasm_eh/duckpgq.duckdb_extension.wasm>) |
| mvp        | [wasm_mvp](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/wasm_mvp/duckpgq.duckdb_extension.wasm>) |
| threads        | [wasm_threads](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/wasm_threads/duckpgq.duckdb_extension.wasm>) |

### Windows

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [windows_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/windows_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_rtools        | [windows_amd64_rtools](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.2/windows_amd64_rtools/duckpgq.duckdb_extension.gz>) |

</details>

<details>
<summary>Version v1.1.1</summary>

### Linux

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [linux_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/linux_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [linux_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/linux_arm64/duckpgq.duckdb_extension.gz>) |

### Osx

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [osx_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/osx_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/osx_arm64/duckpgq.duckdb_extension.gz>) |

### Wasm

| Architecture | Download Link |
|--------------|---------------|
| eh        | [wasm_eh](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/wasm_eh/duckpgq.duckdb_extension.wasm>) |
| mvp        | [wasm_mvp](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/wasm_mvp/duckpgq.duckdb_extension.wasm>) |
| threads        | [wasm_threads](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/wasm_threads/duckpgq.duckdb_extension.wasm>) |

### Windows

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [windows_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/windows_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_rtools        | [windows_amd64_rtools](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.1/windows_amd64_rtools/duckpgq.duckdb_extension.gz>) |

</details>

<details>
<summary>Version v1.1.0</summary>

### Linux

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [linux_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/linux_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [linux_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/linux_arm64/duckpgq.duckdb_extension.gz>) |

### Osx

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [osx_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/osx_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/osx_arm64/duckpgq.duckdb_extension.gz>) |

### Wasm

| Architecture | Download Link |
|--------------|---------------|
| eh        | [wasm_eh](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/wasm_eh/duckpgq.duckdb_extension.wasm>) |
| mvp        | [wasm_mvp](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/wasm_mvp/duckpgq.duckdb_extension.wasm>) |
| threads        | [wasm_threads](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/wasm_threads/duckpgq.duckdb_extension.wasm>) |

### Windows

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [windows_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/windows_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_rtools        | [windows_amd64_rtools](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.1.0/windows_amd64_rtools/duckpgq.duckdb_extension.gz>) |

</details>

<details>
<summary>Version v1.0.0</summary>

### Linux

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [linux_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/linux_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_gcc4        | [linux_amd64_gcc4](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/linux_amd64_gcc4/duckpgq.duckdb_extension.gz>) |
| arm64        | [linux_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/linux_arm64/duckpgq.duckdb_extension.gz>) |

### Osx

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [osx_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/osx_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/osx_arm64/duckpgq.duckdb_extension.gz>) |

### Wasm

| Architecture | Download Link |
|--------------|---------------|
| eh        | [wasm_eh](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/wasm_eh/duckpgq.duckdb_extension.wasm>) |
| mvp        | [wasm_mvp](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/wasm_mvp/duckpgq.duckdb_extension.wasm>) |
| threads        | [wasm_threads](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/wasm_threads/duckpgq.duckdb_extension.wasm>) |

### Windows

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [windows_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/windows_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_rtools        | [windows_amd64_rtools](<https://duckpgq.s3.eu-north-1.amazonaws.com/v1.0.0/windows_amd64_rtools/duckpgq.duckdb_extension.gz>) |

</details>

<details>
<summary>Version v0.10.3</summary>

### Linux

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [linux_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.3/linux_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_gcc4        | [linux_amd64_gcc4](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.3/linux_amd64_gcc4/duckpgq.duckdb_extension.gz>) |
| arm64        | [linux_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.3/linux_arm64/duckpgq.duckdb_extension.gz>) |

### Osx

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [osx_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.3/osx_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.3/osx_arm64/duckpgq.duckdb_extension.gz>) |

### Wasm

| Architecture | Download Link |
|--------------|---------------|
| eh        | [wasm_eh](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.3/wasm_eh/duckpgq.duckdb_extension.wasm>) |
| mvp        | [wasm_mvp](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.3/wasm_mvp/duckpgq.duckdb_extension.wasm>) |
| threads        | [wasm_threads](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.3/wasm_threads/duckpgq.duckdb_extension.wasm>) |

</details>

<details>
<summary>Version v0.10.2</summary>

### Linux

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [linux_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.2/linux_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_gcc4        | [linux_amd64_gcc4](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.2/linux_amd64_gcc4/duckpgq.duckdb_extension.gz>) |
| arm64        | [linux_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.2/linux_arm64/duckpgq.duckdb_extension.gz>) |

### Osx

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [osx_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.2/osx_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.2/osx_arm64/duckpgq.duckdb_extension.gz>) |

### Wasm

| Architecture | Download Link |
|--------------|---------------|
| eh        | [wasm_eh](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.2/wasm_eh/duckpgq.duckdb_extension.wasm>) |
| mvp        | [wasm_mvp](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.2/wasm_mvp/duckpgq.duckdb_extension.wasm>) |
| threads        | [wasm_threads](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.2/wasm_threads/duckpgq.duckdb_extension.wasm>) |

</details>

<details>
<summary>Version v0.10.1</summary>

### Linux

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [linux_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.1/linux_amd64/duckpgq.duckdb_extension.gz>) |
| amd64_gcc4        | [linux_amd64_gcc4](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.1/linux_amd64_gcc4/duckpgq.duckdb_extension.gz>) |
| arm64        | [linux_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.1/linux_arm64/duckpgq.duckdb_extension.gz>) |

### Osx

| Architecture | Download Link |
|--------------|---------------|
| amd64        | [osx_amd64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.1/osx_amd64/duckpgq.duckdb_extension.gz>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.1/osx_arm64/duckpgq.duckdb_extension.gz>) |

### Wasm

| Architecture | Download Link |
|--------------|---------------|
| eh        | [wasm_eh](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.1/wasm_eh/duckpgq.duckdb_extension.wasm>) |
| mvp        | [wasm_mvp](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.1/wasm_mvp/duckpgq.duckdb_extension.wasm>) |
| threads        | [wasm_threads](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.1/wasm_threads/duckpgq.duckdb_extension.wasm>) |

</details>

<details>
<summary>Version v0.10.0</summary>

### Osx

| Architecture | Download Link |
|--------------|---------------|
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.0/osx_arm64/duckpgq.duckdb_extension>) |
| arm64        | [osx_arm64](<https://duckpgq.s3.eu-north-1.amazonaws.com/v0.10.0/osx_arm64/duckpgq.duckdb_extension.gz>) |

</details>

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
Checkout git submodules:
```sh
git submodule update --init --recursive
```
Now to build the extension, run:
```sh
make <debug> GEN=ninja
```
The location of the binaries depends on the `BUILD_TYPE` flag (`release` or `debug`) specified during the build process. By default, the binaries are organized as follows:

### For a **release build** (`make release`):
- DuckDB binary: `./build/release/duckdb`
- Unit tests: `./build/release/test/unittest`
- DuckPGQ extension: `./build/release/extension/duckpgq/duckpgq.duckdb_extension`

### For a **debug build** (`make debug`):
- DuckDB binary: `./build/debug/duckdb`
- Unit tests: `./build/debug/test/unittest`
- DuckPGQ extension: `./build/debug/extension/duckpgq/duckpgq.duckdb_extension`

Ensure you specify the appropriate `BUILD_TYPE` flag when configuring the build to place binaries in the corresponding directory.
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `duckpgq.duckdb_extension` is the loadable binary as it would be distributed.

## Running the extension
To run the extension code, simply start the shell with `./build/release/duckdb`.

> [!CAUTION]
> Any query containing SQL/PGQ syntax requires a `-` at the start of the query when building the extension from the source, otherwise, you will experience a segmentation fault. This is not the case when loading the extension from DuckDB.

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
