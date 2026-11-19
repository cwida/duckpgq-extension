#!/usr/bin/env bash
# Benchmark data for the worktree: private copies of the databases the harness writes to (the build re-creates
# their CSR indexes), symlinks for the read-only inputs. One cp process; about 30 GB. `setup_data.sh remove` deletes
# the copies again.
set -eu
MAIN=/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding
HERE="$(cd "$(dirname "$0")/../../../.." && pwd)/data/ldbc-pathfinding"
if [ "${1:-}" = remove ]; then
    rm -f "${HERE}"/db/ldbc_sf*.duckdb* "${HERE}"/graphalytics/db/*.duckdb*
    exit 0
fi
mkdir -p "${HERE}/db" "${HERE}/graphalytics/db" "${HERE}/results" "${HERE}/work"
for name in sources graphdb-export; do
    [ -e "${HERE}/${name}" ] || ln -s "${MAIN}/${name}" "${HERE}/${name}"
done
for name in sources references; do
    [ -e "${HERE}/graphalytics/${name}" ] || ln -s "${MAIN}/graphalytics/${name}" "${HERE}/graphalytics/${name}"
done
for file in db/ldbc_sf10.duckdb db/ldbc_sf100.duckdb db/ldbc_sf300.duckdb graphalytics/db/cit-Patents.duckdb \
            graphalytics/db/datagen-8_4-fb.duckdb graphalytics/db/graph500-24.duckdb; do
    [ -e "${HERE}/${file}" ] || cp "${MAIN}/${file}" "${HERE}/${file}"
done
du -sh "${HERE}/db" "${HERE}/graphalytics/db"
