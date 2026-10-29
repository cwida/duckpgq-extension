#!/usr/bin/env bash
# The two DuckDB-native baselines (recursive CTE with USING KEY, CIDR 2023 scalar UDFs) in the pair / source sweep at
# SF10 and SF100, next to the operator's index route for the agreement check. Runs as an extra step after the matrix:
# it waits for run_all.sh to print "matrix finished" so the measurements never overlap.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "${REPO_ROOT}"
while ! grep -q "matrix finished" "${LOG_DIR}/run_all.out" 2>/dev/null; do sleep 60; done
while pgrep -f "repro/run_all.sh" > /dev/null; do sleep 30; done
NOTE="repro $(git rev-parse --short HEAD) $(hostname)"
for sf in 10 100; do
  step "native_baselines_sf${sf}" python3 "${REPO_ROOT}/scripts/native_baselines_sweep.py" --scale-factor "$sf" \
    --threads "${THREADS}" --repeats "${REPEATS}" --timeout-s "${TIMEOUT_S}" \
    --systems duckpgq_index duckpgq_recursive duckpgq_scalar --notes "native baselines SF${sf} ${NOTE}"
done
echo "native baselines finished" >> "${LOG_DIR}/run_all.out"
