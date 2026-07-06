#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." >/dev/null 2>&1 && pwd )"
UPSTREAM_DIR="${REPO_ROOT}/third_party/duckdb_peg_parser/upstream"
VENDORED_INCLUDE_DIR="${UPSTREAM_DIR}/src/include/duckpgq/third_party/duckdb_peg_parser/peg"
DUCKDB_INCLUDE_DIR="${UPSTREAM_DIR}/src/include/duckdb/parser/peg"
PYTHON="${PYTHON:-python3}"

if [ -x "${REPO_ROOT}/.venv/bin/python" ]; then
	PYTHON="${REPO_ROOT}/.venv/bin/python"
fi

if [ ! -d "${UPSTREAM_DIR}" ]; then
	echo "Vendored DuckDB PEG parser not found: ${UPSTREAM_DIR}" >&2
	exit 1
fi

rm -rf "${UPSTREAM_DIR}/src/include/duckdb"
mkdir -p "$(dirname "${DUCKDB_INCLUDE_DIR}")"
cp -R "${VENDORED_INCLUDE_DIR}" "${DUCKDB_INCLUDE_DIR}"

(
	cd "${UPSTREAM_DIR}"
	"${PYTHON}" scripts/parser/inline_grammar.py --grammar-file
	"${PYTHON}" scripts/parser/inline_grammar.py
	"${PYTHON}" scripts/parser/generate_transformer.py --write
)

rm -rf "${VENDORED_INCLUDE_DIR}"
mkdir -p "$(dirname "${VENDORED_INCLUDE_DIR}")"
cp -R "${DUCKDB_INCLUDE_DIR}" "${VENDORED_INCLUDE_DIR}"
rm -rf "${UPSTREAM_DIR}/src/include/duckdb"

"${PYTHON}" "${SCRIPT_DIR}/wrap_duckdb_peg_namespace.py" "${UPSTREAM_DIR}"

echo "Regenerated vendored DuckDB PEG parser"
