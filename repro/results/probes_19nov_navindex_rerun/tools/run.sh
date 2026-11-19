#!/usr/bin/env bash
# The Nav-Index comparison of probes_18nov_navindex again, with the build of this worktree (shape probe and the
# search from both ends, default settings) instead of rc13, and Umbra measured in the same run.
# Usage, from the worktree root: tools/run.sh <graph>... (oberbayern twitter)
# Needs ~/navindex/work_head/<graph>.duckdb (probes_19nov_shape_probe/tools/run.sh prepare <graph>).
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "${HERE}/../../.." && pwd)"
export NAV_OUT="${HERE}" NAV_WORK="${HOME}/navindex/work_head"
export NAV_SYSTEM="duckpgq-branch"
export NAV_CLI="${ROOT}/build/release/duckdb" NAV_EXT="${ROOT}/build/release/extension/duckpgq/duckpgq.duckdb_extension"
# default: the build as it is. one_end: no search from both ends. no_shape_probe: lanes above 16 sources whatever the
# graph. single_source_one_end: rc13's forced setting (probes_18nov_navindex "single_source")
export NAV_CONFIGS="${NAV_CONFIGS:-default one_end no_shape_probe single_source_one_end}"
for graph in "$@"; do
    cp "${ROOT}/repro/results/probes_19nov_shape_probe/prepare_${graph}.txt" "${HERE}/"
    echo "$(date -u +%FT%TZ) commit=$(git -C "${ROOT}" rev-parse --short HEAD)" >> "${HERE}/run.log"
    "${ROOT}/repro/results/probes_18nov_navindex/tools/run.sh" "${graph}" duckpgq umbra
    python3 "${ROOT}/repro/results/probes_18nov_navindex/tools/navprobe.py" check "${graph}" > /dev/null
done
