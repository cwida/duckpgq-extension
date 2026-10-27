#!/usr/bin/env bash
# Print the facts that go next to every result: hardware, OS, toolchain, container and client versions.
source "$(dirname "$0")/common.sh" 2>/dev/null
cd "${REPO_ROOT}" 2>/dev/null || true
echo "date:        $(date -u +%Y-%m-%dT%H:%MZ)"
echo "instance:    $(curl -s -m 1 http://169.254.169.254/latest/meta-data/instance-type 2>/dev/null || echo 'not EC2')"
echo "cpu:         $(lscpu | awk -F: '/Model name/{gsub(/^ +/,"",$2); print $2}')  cores=$(nproc)"
echo "memory:      $(free -g | awk '/Mem:/{print $2}') GiB"
echo "disk:        $(df -h / | awk 'NR==2{print $2" "$1}')  $(lsblk -d -o NAME,MODEL,SIZE 2>/dev/null | sed -n 2p)"
echo "kernel:      $(uname -r)  $(. /etc/os-release && echo "$PRETTY_NAME")"
echo "toolchain:   gcc $(gcc -dumpfullversion 2>/dev/null) cmake $(cmake --version | head -1 | awk '{print $3}') ninja $(ninja --version 2>/dev/null) python $(python3 --version | awk '{print $2}')"
echo "docker:      $(docker --version | awk '{print $3}' | tr -d ,)"
echo "duckpgq:     $(git rev-parse --short HEAD) ($(git rev-parse --abbrev-ref HEAD))  duckdb submodule $(git -C duckdb rev-parse --short HEAD 2>/dev/null)"
echo "generator:   $(git -C "${DUCKPGQ_GENERATOR_ROOT}" rev-parse --short HEAD 2>/dev/null) at ${DUCKPGQ_GENERATOR_ROOT}"
echo "python pkgs: $(python3 -m pip list 2>/dev/null | grep -iE '^(kuzu|neo4j|falkordb|matplotlib) ' | awk '{printf "%s %s  ", $1, $2}')"
for c in pf-memgraph pf-falkordb pf-neo4j; do
  echo "container:   $c $(docker inspect "$c" --format '{{.Config.Image}} {{.Image}}' 2>/dev/null | cut -c1-60)"
done
echo "memgraph:    $(docker exec pf-memgraph /usr/lib/memgraph/memgraph --version 2>/dev/null | head -1)"
echo "falkordb:    module version $(docker exec pf-falkordb redis-cli MODULE LIST 2>/dev/null | sed -n 4p) (4.2.2 = 42202)"
echo "neo4j:       $(docker exec pf-neo4j neo4j --version 2>/dev/null) enterprise, GDS $(docker exec pf-neo4j cypher-shell --format plain 'RETURN gds.version()' 2>/dev/null | tail -1 | tr -d '\"')"
echo "duckpgq operator option defaults (the measured configuration):"
"${REPO_ROOT}/build/release/duckdb" -unsigned -no-agent -csv -c "LOAD '${REPO_ROOT}/build/release/extension/duckpgq/duckpgq.duckdb_extension'; SELECT name, value FROM duckdb_settings() WHERE name LIKE 'experimental_path_finding%' OR name LIKE 'duckpgq%' ORDER BY name;" 2>/dev/null | tail -n +2 | sed 's/^/  /'
