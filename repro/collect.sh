#!/usr/bin/env bash
# Render the paper's figures and LaTeX tables, and refresh the committed results archive.
#   ./collect.sh            figures/tables from the live results in data/ldbc-pathfinding/results, then archive
#   ./collect.sh archive    figures/tables from repro/results only (no measurement needed)
set -euo pipefail
source "$(dirname "$0")/common.sh"
cd "${REPO_ROOT}"
mode="${1:-live}"
if [ "$mode" = archive ]; then
  DUCKPGQ_RESULTS_DIR="${ARCHIVE_DIR}" ${PLOT}
else
  ${PLOT}
  for sf in ${SCALE_FACTORS}; do
    mkdir -p "${ARCHIVE_DIR}/sf${sf}"
    cp -n "${RESULTS_DIR}/sf${sf}/"*.csv "${ARCHIVE_DIR}/sf${sf}/" 2>/dev/null || true
  done
  # One text table per scale factor (the table printers take the newest file per system and shape)
  : > "${ARCHIVE_DIR}/sweep_pairs_table.txt"; : > "${ARCHIVE_DIR}/ldbc_queries_table.txt"
  for sf in ${SCALE_FACTORS}; do
    ${HARNESS} sweep-pairs-table  "${ARCHIVE_DIR}"/sf${sf}/sweep_pairs_*.csv  >> "${ARCHIVE_DIR}/sweep_pairs_table.txt"
    ${HARNESS} ldbc-queries-table "${ARCHIVE_DIR}"/sf${sf}/ldbc_queries_*.csv >> "${ARCHIVE_DIR}/ldbc_queries_table.txt"
  done
fi
echo "figures: paper/figures/*.pdf   tables: paper/tables/*.tex   archive: ${ARCHIVE_DIR}"
