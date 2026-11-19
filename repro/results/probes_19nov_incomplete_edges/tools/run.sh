#!/usr/bin/env bash
# Runs the three probes with one build: run.sh <duckdb cli> <duckpgq extension> [output suffix]
# Writes probe_a<suffix>.out, probe_b<suffix>.out, probe_c<suffix>.out next to tools/. The `.read` lines of the probe
# files are expanded first: the CLI stops reading an included file at its first error, and errors are results here.
set -uo pipefail
CLI="$1"; EXT="$2"; SUFFIX="${3:-}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
cd "${HERE}"
expand() {
  python3 - "$1" <<'EOF'
import sys
for line in open(sys.argv[1]):
    if line.startswith(".read "):
        sys.stdout.write(open(line.split()[1]).read())
    else:
        sys.stdout.write(line)
EOF
}
run() { "${CLI}" -no-agent -unsigned -cmd "LOAD '${EXT}'" "$@" 2>&1; }
expand tools/probe_a.sql | run > "probe_a${SUFFIX}.out"
expand tools/probe_b.sql | run > "probe_b${SUFFIX}.out"
expand tools/probe_c.sql | run "${TMP}/probe_c.duckdb" > "probe_c${SUFFIX}.out"
ls -l "${TMP}" | sed 's/^/-- files after the first process: /' >> "probe_c${SUFFIX}.out"
expand tools/probe_c_reload.sql | run "${TMP}/probe_c.duckdb" >> "probe_c${SUFFIX}.out"
rm -rf "${TMP}"
