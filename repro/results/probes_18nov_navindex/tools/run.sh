#!/usr/bin/env bash
# Timing run of the Nav-Index comparison (my-beans-24hi). Usage: tools/run.sh oberbayern|twitter [duckpgq umbra community]
# One graph per invocation (a slot of about 20 minutes on a quiet machine); the systems run one after the other.
# Needs: ~/navindex/{artifact,data/data,duckdb-1.4.1} and prepare_<graph>.txt (navprobe.py prepare <graph>).
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
GRAPH="$1"; shift
SYSTEMS="${*:-duckpgq umbra community}"
LOG="${HERE}/run.log"
EXT="${NAV_EXT:-/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension}"
export NAV_OUT="${HERE}" NAV_EXT="${EXT}"

step() {
    local name="$1"; shift
    echo "$(date -u +%FT%TZ) start ${name}" >> "${LOG}"
    "$@" > "${HERE}/${name}.out" 2>&1
    echo "$(date -u +%FT%TZ) end   ${name} exit=$?" >> "${LOG}"
}

echo "$(date -u +%FT%TZ) graph=${GRAPH} threads=$(nproc) extension_sha256=$(sha256sum "${EXT}" | cut -c1-16)" \
     "cli_sha256=$(sha256sum "${NAV_CLI:-/local/home/dwolde/bootstrap/duckpgq-extension/build/release/duckdb}" | cut -c1-16)" \
     "umbra=$("${HOME}/navindex/artifact/umbra/bin/sql" -version 2>&1 | head -1) load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "${LOG}"

for system in ${SYSTEMS}; do
    if [ "${system}" = umbra ] && [ "${GRAPH}" = twitter ]; then
        # A forced ad-hoc contraction hierarchy is rebuilt by every execution (minutes on Twitter): left out there
        NAV_UMBRA_MODES="dijkstra adaptive table_index" step "umbra_${GRAPH}" python3 "${HERE}/tools/navprobe.py" umbra "${GRAPH}"
    else
        step "${system}_${GRAPH}" python3 "${HERE}/tools/navprobe.py" "${system}" "${GRAPH}"
    fi
done
echo "$(date -u +%FT%TZ) done graph=${GRAPH} load=$(cut -d' ' -f1-3 /proc/loadavg)" >> "${LOG}"
