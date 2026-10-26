#!/usr/bin/env bash
# Start, stop or inspect the three baseline servers exactly as they ran for the paper.
#   ./containers.sh start|stop|status|rm
set -euo pipefail
source "$(dirname "$0")/common.sh"

start() {
  mkdir -p "${EXPORT_DIR}"
  docker start pf-memgraph 2>/dev/null || docker run -d --name pf-memgraph \
    -p "${MEMGRAPH_PORT}:7687" \
    -v "${EXPORT_DIR}:/import:ro" \
    "${MEMGRAPH_IMAGE}" --log-level=WARNING --storage-snapshot-on-exit=false --storage-properties-on-edges=false
  docker start pf-falkordb 2>/dev/null || docker run -d --name pf-falkordb \
    -p "${FALKORDB_PORT}:6379" \
    "${FALKORDB_IMAGE}"
  # Neo4j Enterprise under its evaluation licence (NEO4J_ACCEPT_LICENSE_AGREEMENT=yes) with the
  # Graph Data Science plugin (unlicensed: 4 threads, no parallel runtime).
  docker start pf-neo4j 2>/dev/null || docker run -d --name pf-neo4j \
    -p "${NEO4J_PORT}:7687" -p 7474:7474 \
    -v "${EXPORT_DIR}:/import:ro" \
    -e NEO4J_AUTH=none \
    -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
    -e NEO4J_server_memory_heap_initial__size="${NEO4J_HEAP}" \
    -e NEO4J_server_memory_heap_max__size="${NEO4J_HEAP}" \
    -e NEO4J_server_memory_pagecache_size="${NEO4J_PAGECACHE}" \
    -e NEO4J_server_directories_import=/import \
    -e 'NEO4J_PLUGINS=["graph-data-science"]' \
    -e 'NEO4J_dbms_security_procedures_unrestricted=gds.*' \
    "${NEO4J_IMAGE}"
  echo "waiting for bolt ports"
  for port in "${MEMGRAPH_PORT}" "${NEO4J_PORT}" "${FALKORDB_PORT}"; do
    for _ in $(seq 1 120); do
      if (echo > "/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then break; fi
      sleep 1
    done
  done
  # Neo4j accepts TCP before it accepts queries; the harness retries, but give it a head start.
  sleep 10
  status
}

status() { docker ps -a --filter name=pf- --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'; }
stop()   { docker stop pf-memgraph pf-falkordb pf-neo4j 2>/dev/null || true; }
rm_all() { stop; docker rm pf-memgraph pf-falkordb pf-neo4j 2>/dev/null || true; }

case "${1:-status}" in
  start) start ;; stop) stop ;; status) status ;; rm) rm_all ;;
  *) echo "usage: $0 start|stop|status|rm"; exit 2 ;;
esac
