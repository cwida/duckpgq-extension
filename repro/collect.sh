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
  for dir in "${RESULTS_DIR}"/graphalytics_*/; do
    [ -d "$dir" ] || continue
    name="$(basename "$dir")"
    mkdir -p "${ARCHIVE_DIR}/${name}"
    cp -n "${dir}"*.csv "${ARCHIVE_DIR}/${name}/" 2>/dev/null || true
  done
  # One text table per scale factor (the table printers take the newest file per system and shape, in the order
  # given: files of the matrix's thread count only, sorted by their timestamp suffix; a probe at another thread count
  # such as sweep_pairs_sf10_threads4_* sorts after threads32 by name and would otherwise pass for the newest file)
  : > "${ARCHIVE_DIR}/sweep_pairs_table.txt"; : > "${ARCHIVE_DIR}/ldbc_queries_table.txt"
  for sf in ${SCALE_FACTORS}; do
    ${HARNESS} sweep-pairs-table  $(ls "${ARCHIVE_DIR}"/sf${sf}/sweep_pairs_sf${sf}_threads${THREADS}_*.csv | sort -t_ -k5,5n)  >> "${ARCHIVE_DIR}/sweep_pairs_table.txt"
    ${HARNESS} ldbc-queries-table $(ls "${ARCHIVE_DIR}"/sf${sf}/ldbc_queries_sf${sf}_threads${THREADS}_*.csv | sort -t_ -k5,5n) >> "${ARCHIVE_DIR}/ldbc_queries_table.txt"
  done
  : > "${ARCHIVE_DIR}/crossover_table.txt"
  for sf in ${SCALE_FACTORS}; do
    ls "${ARCHIVE_DIR}"/sf${sf}/microbench_*.csv >/dev/null 2>&1 && ${HARNESS} crossover-table "${ARCHIVE_DIR}"/sf${sf}/microbench_*.csv >> "${ARCHIVE_DIR}/crossover_table.txt"
  done
  : > "${ARCHIVE_DIR}/throughput_table.txt"
  for sf in ${SCALE_FACTORS}; do
    ls "${ARCHIVE_DIR}"/sf${sf}/throughput_*.csv >/dev/null 2>&1 && ${HARNESS} throughput-table "${ARCHIVE_DIR}"/sf${sf}/throughput_*.csv >> "${ARCHIVE_DIR}/throughput_table.txt"
  done
  : > "${ARCHIVE_DIR}/concurrent_table.txt"
  for sf in ${SCALE_FACTORS}; do
    ls "${ARCHIVE_DIR}"/sf${sf}/concurrent_*.csv >/dev/null 2>&1 && ${HARNESS} concurrent-table "${ARCHIVE_DIR}"/sf${sf}/concurrent_*.csv >> "${ARCHIVE_DIR}/concurrent_table.txt"
  done
  for dir in "${ARCHIVE_DIR}"/graphalytics_*/; do
    [ -d "$dir" ] || continue
    ls "${dir}"sweep_pairs_*.csv >/dev/null 2>&1 && ${HARNESS} sweep-pairs-table "${dir}"sweep_pairs_*.csv >> "${ARCHIVE_DIR}/sweep_pairs_table.txt"
  done
fi
echo "figures: paper/figures/*.pdf   tables: paper/tables/*.tex   archive: ${ARCHIVE_DIR}"
