PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckpgq
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

define RUN_EXTENSION_TEST
	@set -e; \
	test_binary=$$(find ./build/$1 -type f \( -name unittest -o -name unittest.exe \) | head -n 1); \
	if [ -z "$$test_binary" ]; then \
		echo "Could not find unittest binary under ./build/$1"; \
		find ./build/$1 -maxdepth 4 -type f \( -name '*unittest*' -o -name '*.exe' \) -print; \
		exit 127; \
	fi; \
	echo "$$test_binary" "$(TESTS_BASE_DIRECTORY)*"; \
	ls -l "$$test_binary"; \
	case "$(DUCKDB_PLATFORM)" in \
		windows_amd64|windows_arm64) \
			win_test_binary=$$(cygpath -w "$$test_binary" 2>/dev/null || echo "$$test_binary"); \
			cmd.exe /c "$$win_test_binary" "$(TESTS_BASE_DIRECTORY)*"; \
			;; \
		*) \
			"$$test_binary" "$(TESTS_BASE_DIRECTORY)*"; \
			;; \
	esac
endef

test_release_internal:
	$(call RUN_EXTENSION_TEST,release)

test_debug_internal:
	$(call RUN_EXTENSION_TEST,debug)

test_reldebug_internal:
	$(call RUN_EXTENSION_TEST,reldebug)
