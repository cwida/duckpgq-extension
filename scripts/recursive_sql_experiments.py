#!/usr/bin/env python3
import argparse
import csv
import json
import re
import subprocess
import time
from pathlib import Path

from pathfinding_benchmark import (
    BENCH_DUCKDB,
    DATA_ROOT,
    PAIR_SHAPES,
    db_path,
    ensure_pair_table,
    pair_table_name,
    read_pair_profile,
    sf_name,
)


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_duckdb_timed(sql, timeout_s):
    script = ".timer on\n" + sql.strip() + "\n"
    result = subprocess.run(
        [str(BENCH_DUCKDB), "-unsigned", "-csv"],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        input=script,
        capture_output=True,
        timeout=timeout_s,
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        raise RuntimeError(output)
    timers = []
    blocks = []
    current = []
    for line in output.splitlines():
        match = re.match(r"Run Time \(s\): real ([0-9.]+)", line)
        if match:
            timers.append(float(match.group(1)))
            block = "\n".join(current).strip()
            if block:
                blocks.append(block)
            current = []
        elif line.strip():
            current.append(line)
    if not blocks or not timers:
        raise RuntimeError(output)
    rows = list(csv.DictReader(blocks[-1].splitlines()))
    if len(rows) != 1:
        raise RuntimeError(blocks[-1])
    return rows[0], timers[-1]


def setup_sql(scale_factor, threads):
    return f"""
SET threads={threads};
ATTACH {sql_string(db_path(scale_factor))} AS ldbc;
"""


def edges_cte():
    return """
edges AS (
    SELECT a.rowid::BIGINT AS src, c.rowid::BIGINT AS dst
    FROM ldbc.person_knows_person k
    JOIN ldbc.person a ON a.id = k.person1id
    JOIN ldbc.person c ON c.id = k.person2id
)
"""


def current_reverse_sql(pair_table, max_depth):
    return f"""
WITH RECURSIVE
pairs AS (
    SELECT rowid::BIGINT AS pair_id, src, dst
    FROM ldbc.{pair_table}
),
targets AS (
    SELECT DISTINCT dst
    FROM pairs
),
{edges_cte()},
dvr(here, there, len) USING KEY (here, there) AS (
    SELECT edges.src, edges.dst, 1::BIGINT AS len
    FROM edges
    JOIN targets ON targets.dst = edges.dst
    UNION ALL (
        SELECT edges.src, dvr.there, dvr.len + 1 AS len
        FROM dvr
        JOIN edges ON edges.dst = dvr.here
        LEFT JOIN recurring.dvr rec ON rec.here = edges.src AND rec.there = dvr.there
        WHERE edges.src <> dvr.there
          AND dvr.len < {max_depth}
          AND dvr.len + 1 < coalesce(rec.len, 9223372036854775807)
        ORDER BY len DESC
    )
),
lengths AS (
    SELECT pairs.pair_id,
           CASE WHEN pairs.src = pairs.dst THEN 0 ELSE dvr.len END AS len
    FROM pairs
    LEFT JOIN dvr ON dvr.here = pairs.src AND dvr.there = pairs.dst
)
SELECT 'current_reverse' AS variant, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM lengths;
"""


def pair_forward_sql(pair_table, max_depth, stop_on_found):
    found_join = ""
    found_filter = ""
    variant = "pair_forward_stop" if stop_on_found else "pair_forward"
    if stop_on_found:
        found_join = "LEFT JOIN recurring.walk found ON found.pair_id = walk.pair_id AND found.here = pairs.dst"
        found_filter = "AND found.pair_id IS NULL"
    return f"""
WITH RECURSIVE
pairs AS (
    SELECT rowid::BIGINT AS pair_id, src, dst
    FROM ldbc.{pair_table}
),
{edges_cte()},
walk(pair_id, here, len) USING KEY (pair_id, here) AS (
    SELECT pair_id, src AS here, 0::BIGINT AS len
    FROM pairs
    UNION ALL (
        SELECT walk.pair_id, edges.dst AS here, walk.len + 1 AS len
        FROM walk
        JOIN pairs ON pairs.pair_id = walk.pair_id
        JOIN edges ON edges.src = walk.here
        LEFT JOIN recurring.walk rec ON rec.pair_id = walk.pair_id AND rec.here = edges.dst
        {found_join}
        WHERE walk.here <> pairs.dst
          AND walk.len < {max_depth}
          {found_filter}
          AND walk.len + 1 < coalesce(rec.len, 9223372036854775807)
        ORDER BY len DESC
    )
),
lengths AS (
    SELECT pairs.pair_id, walk.len
    FROM pairs
    LEFT JOIN walk ON walk.pair_id = pairs.pair_id AND walk.here = pairs.dst
)
SELECT '{variant}' AS variant, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM lengths;
"""


def pair_reverse_sql(pair_table, max_depth, stop_on_found):
    found_join = ""
    found_filter = ""
    variant = "pair_reverse_stop" if stop_on_found else "pair_reverse"
    if stop_on_found:
        found_join = "LEFT JOIN recurring.walk found ON found.pair_id = walk.pair_id AND found.here = pairs.src"
        found_filter = "AND found.pair_id IS NULL"
    return f"""
WITH RECURSIVE
pairs AS (
    SELECT rowid::BIGINT AS pair_id, src, dst
    FROM ldbc.{pair_table}
),
{edges_cte()},
walk(pair_id, here, len) USING KEY (pair_id, here) AS (
    SELECT pair_id, dst AS here, 0::BIGINT AS len
    FROM pairs
    UNION ALL (
        SELECT walk.pair_id, edges.src AS here, walk.len + 1 AS len
        FROM walk
        JOIN pairs ON pairs.pair_id = walk.pair_id
        JOIN edges ON edges.dst = walk.here
        LEFT JOIN recurring.walk rec ON rec.pair_id = walk.pair_id AND rec.here = edges.src
        {found_join}
        WHERE walk.here <> pairs.src
          AND walk.len < {max_depth}
          {found_filter}
          AND walk.len + 1 < coalesce(rec.len, 9223372036854775807)
        ORDER BY len DESC
    )
),
lengths AS (
    SELECT pairs.pair_id, walk.len
    FROM pairs
    LEFT JOIN walk ON walk.pair_id = pairs.pair_id AND walk.here = pairs.src
)
SELECT '{variant}' AS variant, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM lengths;
"""


VARIANTS = {
    "current_reverse": current_reverse_sql,
    "pair_forward": lambda pair_table, max_depth: pair_forward_sql(pair_table, max_depth, False),
    "pair_forward_stop": lambda pair_table, max_depth: pair_forward_sql(pair_table, max_depth, True),
    "pair_reverse": lambda pair_table, max_depth: pair_reverse_sql(pair_table, max_depth, False),
    "pair_reverse_stop": lambda pair_table, max_depth: pair_reverse_sql(pair_table, max_depth, True),
}


def main():
    global BENCH_DUCKDB

    parser = argparse.ArgumentParser()
    parser.add_argument("--scale-factor", default="1")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--pairs", type=int, nargs="+", default=[1, 8, 32, 128])
    parser.add_argument("--pair-table")
    parser.add_argument("--pair-shape", choices=PAIR_SHAPES, default="random")
    parser.add_argument("--max-depth", type=int, default=6)
    parser.add_argument("--variants", nargs="+", choices=sorted(VARIANTS), default=sorted(VARIANTS))
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--duckdb-binary", type=Path, default=BENCH_DUCKDB)
    args = parser.parse_args()
    BENCH_DUCKDB = args.duckdb_binary.resolve()

    results_dir = DATA_ROOT / "results" / sf_name(args.scale_factor)
    results_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for pair_count in args.pairs:
        pair_shape = "custom" if args.pair_table else args.pair_shape
        pair_table = args.pair_table or pair_table_name(pair_count, args.pair_shape)
        if args.pair_table is None:
            ensure_pair_table(args.scale_factor, pair_count, args.pair_shape)
        pair_profile = read_pair_profile(db_path(args.scale_factor), pair_table)
        for variant in args.variants:
            sql = setup_sql(args.scale_factor, args.threads) + VARIANTS[variant](pair_table, args.max_depth)
            started = time.perf_counter()
            row, query_s = run_duckdb_timed(sql, args.timeout)
            elapsed_s = time.perf_counter() - started
            row.update({
                "scale_factor": args.scale_factor,
                "threads": args.threads,
                "pair_table": pair_table,
                "duckdb_binary": str(BENCH_DUCKDB),
                "requested_pairs": pair_count,
                "pair_shape": pair_shape,
                "max_depth": args.max_depth,
                "query_s": f"{query_s:.6f}",
                "elapsed_s": f"{elapsed_s:.6f}",
            })
            row.update(pair_profile)
            rows.append(row)
            print(json.dumps(row, sort_keys=True))

    output_path = results_dir / f"recursive_sql_experiments_{int(time.time())}.csv"
    with output_path.open("w", newline="") as handle:
        fieldnames = [
            "scale_factor",
            "threads",
            "requested_pairs",
            "pair_table",
            "pair_shape",
            "pair_table_rows",
            "distinct_src_count",
            "distinct_dst_count",
            "unique_pair_count",
            "duplicate_pair_count",
            "self_pair_count",
            "duckdb_binary",
            "max_depth",
            "variant",
            "pair_count",
            "reachable_count",
            "total_len",
            "min_len",
            "max_len",
            "query_s",
            "elapsed_s",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
