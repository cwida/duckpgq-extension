# Shared settings for the reproduction scripts. Source, do not execute.
# Every value can be overridden from the environment before calling a script.

REPRO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${REPRO_DIR}/.." && pwd)"

# LDBC SNB data generator (Dtenwolde/ldbc-data-gen). The harness reads DUCKPGQ_GENERATOR_ROOT.
: "${DUCKPGQ_GENERATOR_ROOT:=${HOME}/git/ldbc-data-gen}"
: "${GENERATOR_REPO:=https://github.com/Dtenwolde/ldbc-data-gen}"
: "${GENERATOR_COMMIT:=1add85f}"
export DUCKPGQ_GENERATOR_ROOT

# Scale factors and thread count used in the paper
: "${SCALE_FACTORS:=10 100 300}"
: "${GRAPHALYTICS_DATASETS:=cit-Patents graph500-24 datagen-8_4-fb}"
: "${THREADS:=$(nproc)}"
: "${REPEATS:=3}"
: "${TIMEOUT_S:=600}"
: "${CLIENT_THREADS:=32}"

# Baseline servers (Docker). Host ports match the harness defaults.
: "${MEMGRAPH_IMAGE:=memgraph/memgraph:3.14.0}"
: "${FALKORDB_IMAGE:=falkordb/falkordb:latest}"
: "${NEO4J_IMAGE:=neo4j:5.26.32-enterprise}"
: "${NEO4J_HEAP:=16G}"
: "${NEO4J_PAGECACHE:=16G}"
: "${MEMGRAPH_PORT:=7687}"
: "${NEO4J_PORT:=7688}"
: "${FALKORDB_PORT:=6379}"
EXPORT_DIR="${REPO_ROOT}/data/ldbc-pathfinding/graphdb-export"

# Where the harness writes CSVs and where the archive lives
RESULTS_DIR="${REPO_ROOT}/data/ldbc-pathfinding/results"
ARCHIVE_DIR="${REPRO_DIR}/results"
LOG_DIR="${REPRO_DIR}/logs"
mkdir -p "${LOG_DIR}"

HARNESS="python3 ${REPO_ROOT}/scripts/pathfinding_benchmark.py"
PLOT="python3 ${REPO_ROOT}/scripts/plot_sweep_results.py"

# step NAME CMD...: run CMD unless logs/NAME.done exists; log to logs/NAME.out
step() {
  local name="$1"; shift
  if [ -f "${LOG_DIR}/${name}.done" ]; then
    echo "[skip] ${name} (done)"
    return 0
  fi
  echo "[run ] ${name}: $*"
  local start; start=$(date +%s)
  if "$@" > "${LOG_DIR}/${name}.out" 2>&1; then
    echo "exit=0 seconds=$(( $(date +%s) - start ))" > "${LOG_DIR}/${name}.done"
    echo "[done] ${name} in $(( $(date +%s) - start )) s"
  else
    echo "[FAIL] ${name} (see ${LOG_DIR}/${name}.out)"
    return 1
  fi
}
