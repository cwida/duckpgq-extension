PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckpgq
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

ifneq (,$(filter windows_amd64 windows_arm64,$(DUCKDB_PLATFORM)))
	TEST_PATH=test/Release/unittest.exe
else
	TEST_PATH=test/unittest
endif
