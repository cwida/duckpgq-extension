#!/usr/bin/env bash
# Generate LDBC SNB person/knows data and build the DuckDB databases for every scale factor, then download
# and materialise the three Graphalytics graphs (W30). SF10 takes minutes, SF100 about half an hour, SF300
# about two hours on the paper machine; the Graphalytics download and build took four minutes.
# The baseline loads (Kuzu database, CSV export for Memgraph/FalkorDB/Neo4j) happen lazily on
# first use by run_all.sh and are counted as load time there.
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "${REPO_ROOT}"
# shellcheck disable=SC2086
step "prepare_sf$(echo ${SCALE_FACTORS} | tr ' ' '_')" ${HARNESS} prepare --scale-factors ${SCALE_FACTORS} --threads "${THREADS}"
# shellcheck disable=SC2086
step "prepare_graphalytics" ${HARNESS} prepare --graphalytics-datasets ${GRAPHALYTICS_DATASETS} --threads "${THREADS}"
