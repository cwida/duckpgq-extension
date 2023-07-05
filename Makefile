.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJ_DIR := $(dir $(MKFILE_PATH))

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif
ifeq (${STATIC_LIBCPP}, 1)
	STATIC_LIBCPP=-DSTATIC_LIBCPP=TRUE
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

ifndef ($(DUCKDB_VERSION))
	DUCKDB_VERSION_FLAG=$(DUCKDB_VERSION)
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_PARQUET_EXTENSION=1 ${OSX_BUILD_UNIVERSAL_FLAG} ${STATIC_LIBCPP}

CLIENT_FLAGS :=

# These flags will make DuckDB build the extension
EXTENSION_FLAGS=-DDUCKDB_OOT_EXTENSION_NAMES="duckpgq" -DDUCKDB_OOT_EXTENSION_DUCKPGQ_PATH="$(PROJ_DIR)" -DDUCKDB_OOT_EXTENSION_DUCKPGQ_SHOULD_LINK="TRUE" -DDUCKDB_OOT_EXTENSION_DUCKPGQ_INCLUDE_PATH="$(PROJ_DIR)duckpgq/include"

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	cd duckdb-pgq && make clean

# Main build
debug:
	mkdir -p  build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} -S ./duckdb-pgq/ -B build/debug && \
	cmake --build build/debug --config Debug

release:
	mkdir -p build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) $(EXTENSION_FLAGS) ${CLIENT_FLAGS} -DCMAKE_BUILD_TYPE=Release ${BUILD_FLAGS} -S ./duckdb-pgq/ -B build/release && \
	cmake --build build/release --config Release

# Client build
debug_js: CLIENT_FLAGS=-DBUILD_NODE=1 -DBUILD_JSON_EXTENSION=1
debug_js: debug

debug_r: CLIENT_FLAGS=-DBUILD_R=1
debug_r: debug

debug_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
debug_python: debug

release_js: CLIENT_FLAGS=-DBUILD_NODE=1 -DBUILD_JSON_EXTENSION=1
release_js: release

release_r: CLIENT_FLAGS=-DBUILD_R=1
release_r: release

release_python: CLIENT_FLAGS=-DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
release_python: release

# Main tests
test: test_release

test_release: release
	./build/release/test/unittest --test-dir . "[sql]"

test_debug: debug
	./build/debug/test/unittest --test-dir . "[sql]"

# Client tests
test_js: test_debug_js
test_debug_js: debug_js
	cd duckdb-pgq/tools/nodejs && npm run test-path -- "../../../test/nodejs/**/*.js"

test_release_js: release_js
	cd duckdb-pgq/tools/nodejs && npm run test-path -- "../../../test/nodejs/**/*.js"

test_python: test_debug_python
test_debug_python: debug_python
	cd test/python && python3 -m pytest

test_release_python: release_python
	cd test/python && python3 -m pytest

format:
	find duckpgq/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt

update:
	git submodule update --remote --merge

test_release_stock_duckdb:
	rm -rf duckdb
	git clone -b $(DUCKDB_VERSION_FLAG) --single-branch --depth 1 https://github.com/duckdb/duckdb.git
	cd duckdb; make release;
	python scripts/copy_tests.py --mode release
	cd duckdb; ./build/release/test/unittest "test/extension/duckpgq/*"

test_debug_stock_duckdb:
	rm -rf duckdb
	git clone -b $(DUCKDB_VERSION_FLAG) --single-branch --depth 1 https://github.com/duckdb/duckdb.git
	cd duckdb; make debug;
	python scripts/copy_tests.py --mode debug
	cd duckdb; ./build/debug/test/unittest "test/extension/duckpgq/*"
