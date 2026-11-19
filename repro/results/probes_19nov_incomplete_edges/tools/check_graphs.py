#!/usr/bin/env python3
"""CREATE PROPERTY GRAPH on every benchmark database with a given build of the extension.

For each database: the number of edge rows per endpoint whose key matches no vertex row (counted directly, with an
anti-join, independent of the extension), then CREATE PROPERTY GRAPH with the benchmark's own statement, timed
(`--repeats` processes, each one loads the extension, attaches the database read-only and creates the graph in the
in-memory catalog, as the harness does). Nothing is written to the databases.

    check_graphs.py --cli <duckdb> --extension <duckpgq.duckdb_extension> --label new --out timings.csv [--only sf300]
"""
import argparse
import csv
import re
import subprocess
import time
from pathlib import Path

MAIN = Path("/local/home/dwolde/bootstrap/duckpgq-extension/data/ldbc-pathfinding")
NAV = Path("/local/home/dwolde/navindex/work")
LDBC = ("person", "person_knows_person", "id", "person1id", "person2id")
NAVT = ("nodes", "edges", "id", "src", "dst")
DATABASES = [
    ("sf10", MAIN / "db" / "ldbc_sf10.duckdb", LDBC),
    ("sf100", MAIN / "db" / "ldbc_sf100.duckdb", LDBC),
    ("sf300", MAIN / "db" / "ldbc_sf300.duckdb", LDBC),
    ("cit-Patents", MAIN / "graphalytics" / "db" / "cit-Patents.duckdb", LDBC),
    ("graph500-24", MAIN / "graphalytics" / "db" / "graph500-24.duckdb", LDBC),
    ("datagen-8_4-fb", MAIN / "graphalytics" / "db" / "datagen-8_4-fb.duckdb", LDBC),
    ("navindex-oberbayern", NAV / "oberbayern.duckdb", NAVT),
    ("navindex-twitter", NAV / "twitter.duckdb", NAVT),
]


def run(cli, script):
    result = subprocess.run([cli, "-unsigned", "-csv"], input=script, text=True, capture_output=True)
    return result.stdout, result.stderr


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", required=True)
    parser.add_argument("--extension", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--only", nargs="*")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--no-counts", action="store_true")
    args = parser.parse_args()
    rows = []
    for name, path, (vertex, edge, key, src, dst) in DATABASES:
        if args.only and name not in args.only:
            continue
        if not path.exists():
            print(f"{name}: {path} missing, skipped")
            continue
        attach = f"LOAD '{args.extension}';\nATTACH '{path}' AS ldbc (READ_ONLY);\n"
        counts = ""
        if not args.no_counts:
            out, err = run(args.cli, attach + f"""
SELECT (SELECT count(*) FROM ldbc.{vertex}) AS vertices, (SELECT count(*) FROM ldbc.{edge}) AS edges,
       (SELECT count(*) FROM ldbc.{edge} e WHERE e.{src} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ldbc.{vertex} v WHERE v.{key} = e.{src})) AS incomplete_source,
       (SELECT count(*) FROM ldbc.{edge} e WHERE e.{dst} IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ldbc.{vertex} v WHERE v.{key} = e.{dst})) AS incomplete_destination,
       (SELECT count(*) FROM ldbc.{edge} e WHERE e.{src} IS NULL OR e.{dst} IS NULL) AS null_keys,
       (SELECT count(*) FROM duckdb_constraints() WHERE database_name = 'ldbc' AND table_name = '{edge}' AND constraint_type = 'FOREIGN KEY') AS foreign_keys;
""")
            counts = out.strip().splitlines()[-1] if out.strip() else "ERROR " + err.strip()
            print(f"{name}: vertices,edges,incomplete_source,incomplete_destination,null_keys,foreign_keys = {counts}")
        create = f"""SET active_grammar_extensions = ['duckpgq'];
.timer on
CREATE PROPERTY GRAPH pathfinding_benchmark_pg
VERTEX TABLES (ldbc.{vertex} PROPERTIES ({key}) LABEL Person)
EDGE TABLES (ldbc.{edge} SOURCE KEY ({src}) REFERENCES ldbc.{vertex} ({key})
             DESTINATION KEY ({dst}) REFERENCES ldbc.{vertex} ({key}) LABEL Knows);
.timer off
SELECT 'registered', count(*) FROM __duckpgq_internal WHERE property_graph = 'pathfinding_benchmark_pg';
"""
        for repeat in range(args.repeats):
            start = time.time()
            out, err = run(args.cli, attach + create)
            wall = time.time() - start
            timer = re.search(r"Run Time \(s\): real ([0-9.]+)", out + err)
            registered = re.search(r"registered,(\d+)", out)
            outcome = "ok" if registered and registered.group(1) == "2" and not err.strip() else "FAILED: " + " ".join(err.split())[:300]
            seconds = timer.group(1) if timer else ""
            print(f"{name}: {args.label} run {repeat + 1}: CREATE PROPERTY GRAPH {outcome}, {seconds} s (process {wall:.2f} s)")
            rows.append({"database": name, "build": args.label, "run": repeat + 1, "create_s": seconds,
                         "process_s": f"{wall:.3f}", "outcome": outcome, "counts": counts})
    with open(args.out, "a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["database", "build", "run", "create_s", "process_s", "outcome", "counts"])
        if handle.tell() == 0:
            writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
