#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd )"

DUCKDB_DIR="${DUCKDB_DIR:-${REPO_ROOT}/duckdb}"
VENDOR_DIR="${DUCKPGQ_PEG_VENDOR_DIR:-${REPO_ROOT}/third_party/duckdb_peg_parser}"
UPSTREAM_DIR="${VENDOR_DIR}/upstream"
PATCH_DIR="${VENDOR_DIR}/patches"
BASE_FILE="${VENDOR_DIR}/DUCKDB_PEG_BASE"

if [ ! -d "${DUCKDB_DIR}/src/parser/peg" ]; then
	echo "DuckDB PEG parser directory not found: ${DUCKDB_DIR}/src/parser/peg" >&2
	exit 1
fi

if [ ! -d "${DUCKDB_DIR}/src/include/duckdb/parser/peg" ]; then
	echo "DuckDB PEG parser include directory not found: ${DUCKDB_DIR}/src/include/duckdb/parser/peg" >&2
	exit 1
fi

if [ ! -d "${DUCKDB_DIR}/scripts/parser" ]; then
	echo "DuckDB parser scripts directory not found: ${DUCKDB_DIR}/scripts/parser" >&2
	exit 1
fi

mkdir -p "${VENDOR_DIR}"
mkdir -p "${PATCH_DIR}"

rm -rf "${UPSTREAM_DIR}"
mkdir -p "${UPSTREAM_DIR}/src/parser"
mkdir -p "${UPSTREAM_DIR}/src/include/duckdb/parser"
mkdir -p "${UPSTREAM_DIR}/scripts"

cp -R "${DUCKDB_DIR}/src/parser/peg" "${UPSTREAM_DIR}/src/parser/peg"
cp -R "${DUCKDB_DIR}/src/include/duckdb/parser/peg" "${UPSTREAM_DIR}/src/include/duckdb/parser/peg"
cp -R "${DUCKDB_DIR}/scripts/parser" "${UPSTREAM_DIR}/scripts/parser"

git -C "${DUCKDB_DIR}" rev-parse HEAD > "${BASE_FILE}"

python3 "${SCRIPT_DIR}/wrap_duckdb_peg_namespace.py" "${UPSTREAM_DIR}"

if compgen -G "${PATCH_DIR}/*.patch" >/dev/null; then
	git -C "${REPO_ROOT}" apply --3way "${PATCH_DIR}"/*.patch
	echo "Applied PEG parser patches from ${PATCH_DIR}"
else
	echo "No PEG parser patches found in ${PATCH_DIR}"
fi

echo "Synced DuckDB PEG parser from $(cat "${BASE_FILE}")"
