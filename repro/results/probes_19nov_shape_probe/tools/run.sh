#!/usr/bin/env bash
# The graph shape probe (my-beans-83ld) on the two graphs of the Nav-Index paper: the build of this worktree with
# the probe (default), without it (no_shape_probe, the rule of rc13) and with single-source searches forced.
# Usage: tools/run.sh prepare|time <graph>...   (graphs: oberbayern twitter; run from the worktree root)
# Uses the driver of repro/results/probes_18nov_navindex; databases go to ~/navindex/work_head (the index format of
# this build differs from rc13's, whose databases stay in ~/navindex/work).
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "${HERE}/../../.." && pwd)"
DRIVER="${ROOT}/repro/results/probes_18nov_navindex/tools/navprobe.py"
export NAV_OUT="${HERE}" NAV_WORK="${HOME}/navindex/work_head"
export NAV_CLI="${ROOT}/build/release/duckdb" NAV_EXT="${ROOT}/build/release/extension/duckpgq/duckpgq.duckdb_extension"
export NAV_CONFIGS="${NAV_CONFIGS:-default no_shape_probe single_source}"
LOG="${HERE}/run.log"
ACTION="$1"; shift
for graph in "$@"; do
    echo "$(date -u +%FT%TZ) ${ACTION} graph=${graph} threads=$(nproc) commit=$(git -C "${ROOT}" rev-parse --short HEAD)" \
         "extension_sha256=$(sha256sum "${NAV_EXT}" | cut -c1-16) load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "${LOG}"
    if [ "${ACTION}" = prepare ]; then
        python3 "${DRIVER}" prepare "${graph}" > "${HERE}/prepare_${graph}.out" 2>&1
    else
        python3 "${DRIVER}" duckpgq "${graph}" > "${HERE}/duckpgq_${graph}.out" 2>&1
    fi
    echo "$(date -u +%FT%TZ) end ${ACTION} graph=${graph} exit=$? load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "${LOG}"
done
