#!/usr/bin/env bash
# One-time setup: toolchain check, Python clients, DuckPGQ build, LDBC generator build, containers.
# Tested on Amazon Linux 2023 (gcc 11.5, cmake 3.22, ninja 1.10, python 3.9, docker 25).
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "== toolchain"
for tool in gcc cmake ninja python3 git docker; do
  command -v "$tool" >/dev/null || { echo "missing: $tool (dnf install gcc gcc-c++ cmake ninja-build python3 python3-pip git docker)"; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "docker daemon not reachable (sudo systemctl start docker; usermod -aG docker \$USER)"; exit 1; }

echo "== python clients (versions used for the paper)"
python3 -m pip install --user --quiet -r "${REPRO_DIR}/requirements.txt"

echo "== DuckPGQ (this checkout, submodules pinned by the repository)"
cd "${REPO_ROOT}"
git submodule update --init --recursive
GEN=ninja make release
test -f build/release/extension/duckpgq/duckpgq.duckdb_extension

echo "== LDBC SNB generator at ${DUCKPGQ_GENERATOR_ROOT} (${GENERATOR_REPO} @ ${GENERATOR_COMMIT})"
if [ ! -d "${DUCKPGQ_GENERATOR_ROOT}/.git" ]; then
  git clone "${GENERATOR_REPO}" "${DUCKPGQ_GENERATOR_ROOT}"
fi
git -C "${DUCKPGQ_GENERATOR_ROOT}" checkout --quiet "${GENERATOR_COMMIT}"
git -C "${DUCKPGQ_GENERATOR_ROOT}" submodule update --init --recursive
(cd "${DUCKPGQ_GENERATOR_ROOT}" && GEN=ninja make release)
test -f "${DUCKPGQ_GENERATOR_ROOT}/build/release/extension/ldbc_data_gen/ldbc_data_gen.duckdb_extension"

echo "== baseline servers"
"${REPRO_DIR}/containers.sh" start

echo "== environment record"
"${REPRO_DIR}/environment.sh" > "${REPRO_DIR}/ENVIRONMENT.txt"
echo "setup complete; next: ./prepare.sh then ./run_all.sh"
