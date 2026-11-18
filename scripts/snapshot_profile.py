#!/usr/bin/env python3
"""Where the first query after a bulk change spends its time (bean my-beans-p65v, option D).

Loads the edges of an LDBC database into an in-memory table with one CSR index, applies one batch of changes
(deletes of random edges, inserts between random vertices, or both, each one statement), and runs a single-pair
query, which has to build the snapshot of the whole batch. Reports the wall time of that query, the phases of
the snapshot from duckpgq_csr_index_snapshot_stats, and, with --perf, a flat perf profile of the process taken
while the query runs. Automatic folding is off, so the reader pays the snapshot and not a fold.

Usage: snapshot_profile.py --duckdb build/release/duckdb --extension .../duckpgq.duckdb_extension
         --db ldbc_sf300.duckdb --label sf300 --kind uniform --fraction 0.1 --out profile.csv [--perf perf.txt]
"""
import argparse
import csv
import os
import re
import signal
import subprocess
import sys
import tempfile
import time


def wait_for(path, process, timeout=120):
    start = time.time()
    while not os.path.exists(path):
        if process.poll() is not None:
            raise SystemExit("duckdb exited early")
        if time.time() - start > timeout:
            process.kill()
            raise SystemExit(f"timed out after {timeout} s waiting for {path}; duckdb killed")
        time.sleep(0.05)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--extension", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--kind", choices=["uniform", "deletes", "inserts"], default="uniform")
    parser.add_argument("--fraction", type=float, default=0.1)
    parser.add_argument("--out", required=True)
    parser.add_argument("--perf", help="write a flat perf report of the first query to this file")
    parser.add_argument("--threads", type=int, default=0)
    parser.add_argument("--perf-cached", help="write a flat perf report of the queries after the first one, which "
                                              "find their snapshot in the cache, to this file")
    parser.add_argument("--cached-queries", type=int, default=15)
    parser.add_argument("--stat-cached", help="write perf stat (page faults, task clock) of the cached queries here")
    parser.add_argument("--set", action="append", default=[], help="a setting, name=value, applied before the load")
    args = parser.parse_args()

    # Sizes first, as constants: a scalar subquery inside a join condition makes the insert a nested-loop join
    sizes = subprocess.run([args.duckdb, "-no-agent", "-unsigned", "-csv", "-noheader", ":memory:", "-c", f"""
ATTACH '{args.db}' AS ldbc (READ_ONLY);
SELECT count(*), (SELECT count(*) FROM (SELECT person1id FROM ldbc.person_knows_person UNION SELECT person2id FROM ldbc.person_knows_person))
FROM ldbc.person_knows_person;"""], capture_output=True, text=True)
    edges, vertices = (int(x) for x in sizes.stdout.strip().splitlines()[-1].split(","))
    work = tempfile.mkdtemp(prefix="snapshot_profile_")
    ready, done = os.path.join(work, "ready.csv"), os.path.join(work, "done.csv")
    log = open(os.path.join(work, "out.txt"), "w+")
    process = subprocess.Popen([args.duckdb, "-no-agent", "-unsigned", ":memory:"], stdin=subprocess.PIPE,
                               stdout=log, stderr=subprocess.STDOUT, text=True)

    def send(sql):
        process.stdin.write(sql + "\n")
        process.stdin.flush()

    spec = "'csr-index|memory|main|knows|idx'"
    query = (f"SELECT 'ROWS|' || count(len) FROM (SELECT iterativelengthoperator(s, d, 0::BIGINT, 0::BIGINT, {spec}) "
             f"AS len FROM pairs);")
    send(f"""
.mode csv
.headers off
LOAD '{args.extension}';
{f'SET threads = {args.threads};' if args.threads > 0 else ''}
SET experimental_path_finding_operator = true;
SET duckpgq_csr_index_fold_ratio = 0;
{" ".join(f"SET {item.split('=')[0]} = {item.split('=')[1]};" for item in args.set)}
ATTACH '{args.db}' AS ldbc (READ_ONLY);
CREATE TABLE knows AS SELECT person1id AS src, person2id AS dst FROM ldbc.person_knows_person;
DETACH ldbc;
CREATE TABLE keys AS SELECT k, (row_number() OVER (ORDER BY k) - 1)::BIGINT AS rn
  FROM (SELECT src AS k FROM knows UNION SELECT dst FROM knows);
CREATE TABLE pairs AS SELECT a.k AS s, b.k AS d FROM range(1) r
  JOIN keys a ON a.rn = (hash(r.range, 7) % {vertices})::BIGINT
  JOIN keys b ON b.rn = (hash(r.range, 8) % {vertices})::BIGINT;
CREATE INDEX idx ON knows USING CSR (src, dst);
SELECT delete_csr(2147483647);
.timer on
SELECT 'MARK|query_before';
{query}
.timer off
""")
    changes = max(2, int(edges * args.fraction))
    if args.kind in ("uniform", "deletes"):
        share = args.fraction / 2 if args.kind == "uniform" else args.fraction
        send(f"DELETE FROM knows WHERE hash(rowid, 1) % {max(1, int(1 / share))} = 0;")
    if args.kind in ("uniform", "inserts"):
        count = changes // 2 if args.kind == "uniform" else changes
        send(f"""
INSERT INTO knows
SELECT a.k, b.k FROM range({count}) r
JOIN keys a ON a.rn = (hash(r.range, 1, 1) % {vertices})::BIGINT
JOIN keys b ON b.rn = (hash(r.range, 1, 2) % {vertices})::BIGINT;""")
    send(f"SELECT 'LOG|' || duckpgq_csr_index_stats('knows', 'idx');")
    send(f"COPY (SELECT 1) TO '{ready}';")
    wait_for(ready, process)

    perf = None
    perf_data = os.path.join(work, "perf.data")
    if args.perf:
        perf = subprocess.Popen(["perf", "record", "-F", "999", "-p", str(process.pid), "-o", perf_data],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.5)
    send(f"""
.timer on
SELECT 'MARK|query_first';
{query}
.timer off
SELECT 'SNAPSHOT|' || duckpgq_csr_index_snapshot_stats('knows', 'idx');
COPY (SELECT 1) TO '{done}';""")
    wait_for(done, process)
    if perf:
        perf.send_signal(signal.SIGINT)
        perf.wait()
    done2 = os.path.join(work, "done2.csv")
    perf_cached_data = os.path.join(work, "perf_cached.data")
    perf = None
    if args.perf_cached:
        perf = subprocess.Popen(["perf", "record", "-F", "1999", "-p", str(process.pid), "-o", perf_cached_data],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.5)
    elif args.stat_cached:
        perf = subprocess.Popen(["perf", "stat", "-e", "page-faults,task-clock", "-p", str(process.pid), "-o",
                                 args.stat_cached], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.5)
    for _ in range(args.cached_queries):
        send(f"""
.timer on
SELECT 'MARK|query_cached';
{query}
.timer off""")
    send(f"""
SELECT 'SNAPSHOT2|' || duckpgq_csr_index_snapshot_stats('knows', 'idx');
COPY (SELECT 1) TO '{done2}';""")
    wait_for(done2, process)
    if perf:
        perf.send_signal(signal.SIGINT)
        perf.wait()
    send(".quit")
    process.stdin.close()
    process.wait()
    log.seek(0)
    output = log.read()
    if process.returncode != 0 or "Error" in output:
        sys.stderr.write(output[-3000:])
        raise SystemExit("duckdb failed")

    row = {"dataset": args.label, "kind": args.kind, "fraction": args.fraction, "edges": edges, "vertices": vertices,
           "settings": " ".join(args.set)}
    mark, since = None, 0
    cached = []
    for line in output.splitlines():
        line = line.strip().strip('"')
        if line.startswith("MARK|"):
            mark, since = line.split("|")[1], 0
        elif line.startswith("Run Time") and mark:
            since += 1
            if since == 2:
                seconds = float(re.search(r"real ([0-9.]+)", line).group(1))
                if mark == "query_cached":
                    cached.append(seconds)
                else:
                    row[mark + "_s"] = seconds
                mark = None
        elif line.startswith("LOG|"):
            row["log"] = line.split("|", 1)[1]
        elif line.startswith("SNAPSHOT|"):
            for item in line.split("|", 1)[1].split(";"):
                key, value = item.split("=")
                if not key.startswith("hit_") and key != "fast_hits":
                    row[key] = value
        elif line.startswith("SNAPSHOT2|"):
            for item in line.split("|", 1)[1].split(";"):
                key, value = item.split("=")
                if key.startswith("hit_") or key in ("cache_hits", "fast_hits"):
                    row[key] = value
    cached.sort()
    row["query_cached_median_s"] = cached[len(cached) // 2]
    row["query_cached_min_s"] = cached[0]
    row["query_cached_max_s"] = cached[-1]
    with open(args.out, "a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if handle.tell() == 0:
            writer.writeheader()
        writer.writerow(row)
    print(row)
    if args.perf_cached:
        report = subprocess.run(["perf", "report", "-i", perf_cached_data, "--stdio", "--sort", "dso,symbol",
                                 "--percent-limit", "0.7"], capture_output=True, text=True)
        with open(args.perf_cached, "w") as handle:
            handle.write(report.stdout)
    if args.perf:
        report = subprocess.run(["perf", "report", "-i", perf_data, "--stdio", "--sort", "dso,symbol", "--percent-limit",
                                 "0.7"], capture_output=True, text=True)
        with open(args.perf, "w") as handle:
            handle.write(report.stdout)


if __name__ == "__main__":
    main()
