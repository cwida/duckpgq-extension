#!/usr/bin/env bash
# The larger road graphs of the Nav-Index archive (maps/germany.tsv, maps/europe.tsv; hop counts, costs ignored).
# Usage, from the worktree root: tools/large.sh germany_operator | germany_umbra | europe_operator
# Queries take minutes here: no warm-up, one timed run, a timeout per process; results under large/.
# Needs ~/navindex/work_head/{germany,europe}.duckdb (tools/run.sh prepare germany europe).
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "${HERE}/../../.." && pwd)"
DRIVER="${ROOT}/repro/results/probes_18nov_navindex/tools/navprobe.py"
OUT="${HERE}/large"
mkdir -p "${OUT}"
cp "${HERE}"/prepare_germany.txt "${HERE}"/prepare_europe.txt "${OUT}/"
export NAV_OUT="${OUT}" NAV_WORK="${HOME}/navindex/work_head" NAV_SYSTEM="duckpgq-branch"
export NAV_CLI="${ROOT}/build/release/duckdb" NAV_EXT="${ROOT}/build/release/extension/duckpgq/duckpgq.duckdb_extension"
export NAV_WORKLOADS="w2" NAV_ROUTES="index" NAV_REPEATS=1 NAV_WARMUP=0 NAV_DUMP=0
PART="$1"
echo "$(date -u +%FT%TZ) large ${PART} commit=$(git -C "${ROOT}" rev-parse --short HEAD)" \
     "extension_sha256=$(sha256sum "${NAV_EXT}" | cut -c1-16) load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "${HERE}/run.log"
case "${PART}" in
    germany_operator)
        # default (probe: deep, one search per source, from both ends), lanes (no_shape_probe), one end
        NAV_CONFIGS="default no_shape_probe one_end" NAV_PAIR_COUNTS="${NAV_PAIR_COUNTS:-1 10 100}" \
            NAV_TIMEOUT="${NAV_TIMEOUT:-600}" python3 "${DRIVER}" duckpgq germany > "${OUT}/duckpgq_germany.out" 2>&1 ;;
    germany_umbra)
        NAV_UMBRA_MODES="${NAV_UMBRA_MODES:-dijkstra table_index}" NAV_PAIR_COUNTS="${NAV_PAIR_COUNTS:-1 10 100}" \
            NAV_TIMEOUT="${NAV_TIMEOUT:-900}" python3 "${DRIVER}" umbra germany > "${OUT}/umbra_germany.out" 2>&1 ;;
    europe_operator)
        NAV_CONFIGS="default" NAV_PAIR_COUNTS="${NAV_PAIR_COUNTS:-1 17}" \
            NAV_TIMEOUT="${NAV_TIMEOUT:-1200}" python3 "${DRIVER}" duckpgq europe > "${OUT}/duckpgq_europe.out" 2>&1 ;;
    *) echo "unknown part ${PART}" >&2; exit 2 ;;
esac
echo "$(date -u +%FT%TZ) end large ${PART} exit=$? load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "${HERE}/run.log"
