#!/usr/bin/env python3
import argparse
import csv
import json
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "data" / "ldbc-pathfinding"
GENERATOR_ROOT = Path.home() / "git" / "ldbc-data-gen"
GENERATOR_DUCKDB = GENERATOR_ROOT / "build" / "release" / "duckdb"
GENERATOR_EXTENSION = GENERATOR_ROOT / "build" / "release" / "extension" / "ldbc_data_gen" / "ldbc_data_gen.duckdb_extension"
BENCH_DUCKDB = REPO_ROOT / "build" / "release" / "duckdb"
DUCKPGQ_EXTENSION = REPO_ROOT / "build" / "release" / "extension" / "duckpgq" / "duckpgq.duckdb_extension"


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_duckdb(binary, database, sql, quiet=False):
    cmd = [str(binary), "-unsigned"]
    if quiet:
        cmd.append("-csv")
    if database is not None:
        cmd.append(str(database))
    cmd.extend(["-c", sql])
    start = time.perf_counter()
    result = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)
    elapsed = time.perf_counter() - start
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return result.stdout.strip(), elapsed


def run_duckdb_timed_script(sql, timeout_s):
    script = ".timer on\n" + sql.strip() + "\n"
    result = subprocess.run(
        [str(BENCH_DUCKDB), "-unsigned", "-csv"],
        cwd=REPO_ROOT,
        text=True,
        input=script,
        capture_output=True,
        timeout=timeout_s,
    )
    output = (result.stdout + result.stderr).strip()
    if result.returncode != 0:
        sys.stderr.write(output)
        raise SystemExit(result.returncode)

    if "Error:" in output:
        raise RuntimeError(output)

    timers = []
    output_blocks = []
    current_block = []
    for line in output.splitlines():
        match = re.match(r"Run Time \(s\): real ([0-9.]+)", line)
        if match:
            timers.append(float(match.group(1)))
            block = "\n".join(current_block).strip()
            if block:
                output_blocks.append(block)
            current_block = []
        elif line.strip():
            current_block.append(line)
    if not timers:
        raise RuntimeError("DuckDB did not emit any .timer output:\n" + output)
    if not output_blocks:
        raise RuntimeError("DuckDB did not emit a benchmark result row:\n" + output)
    csv_output = output_blocks[-1]
    return csv_output, timers


def sf_name(scale_factor):
    return f"sf{str(scale_factor).replace('.', '_')}"


def source_dir(scale_factor):
    return DATA_ROOT / "sources" / f"{sf_name(scale_factor)}-parquet"


def db_path(scale_factor):
    return DATA_ROOT / "db" / f"ldbc_{sf_name(scale_factor)}.duckdb"


def find_relation_path(base_dir, relation_name):
    exact = sorted(base_dir.rglob(f"{relation_name}.parquet"))
    if exact:
        return exact[0]

    relation_dirs = [path for path in base_dir.rglob(relation_name) if path.is_dir()]
    for relation_dir in sorted(relation_dirs):
        if list(relation_dir.glob("*.parquet")):
            return relation_dir / "*.parquet"

    raise FileNotFoundError(f"Could not find {relation_name}.parquet under {base_dir}")


def generate_parquet(scale_factor, threads, force):
    out_dir = source_dir(scale_factor)
    try:
        find_relation_path(out_dir, "Person")
        find_relation_path(out_dir, "Person_knows_Person")
        if not force:
            print(f"{sf_name(scale_factor)} source parquet already exists: {out_dir}")
            return
    except FileNotFoundError:
        pass

    out_dir.parent.mkdir(parents=True, exist_ok=True)
    sql = f"""
LOAD {sql_string(GENERATOR_EXTENSION)};
CALL ldbcgen(
    sf := {scale_factor},
    target := 'files',
    output_dir := {sql_string(out_dir)},
    format := 'parquet',
    overwrite := true,
    threads := {threads}
);
"""
    print(f"Generating LDBC Parquet for {sf_name(scale_factor)} in {out_dir}")
    _, elapsed = run_duckdb(GENERATOR_DUCKDB, None, sql)
    print(f"Generated {sf_name(scale_factor)} parquet in {elapsed:.2f}s")


def materialize_database(scale_factor, pair_count, force):
    out_db = db_path(scale_factor)
    if out_db.exists() and not force:
        print(f"{sf_name(scale_factor)} DuckDB database already exists: {out_db}")
        ensure_pair_table(scale_factor, pair_count)
        return
    if out_db.exists():
        out_db.unlink()

    src_dir = source_dir(scale_factor)
    person_path = find_relation_path(src_dir, "Person")
    knows_path = find_relation_path(src_dir, "Person_knows_Person")
    out_db.parent.mkdir(parents=True, exist_ok=True)

    sql = f"""
CREATE TABLE person AS
SELECT id::BIGINT AS id
FROM read_parquet({sql_string(person_path)});

CREATE TABLE person_knows_person AS
SELECT Person1Id::BIGINT AS person1id, Person2Id::BIGINT AS person2id
FROM read_parquet({sql_string(knows_path)});

{pair_table_sql(pair_count)}

CREATE TABLE benchmark_metadata AS
SELECT *
FROM (
    VALUES
        ('scale_factor', {sql_string(scale_factor)}),
        ('person_rows', (SELECT count(*)::VARCHAR FROM person)),
        ('person_knows_person_rows', (SELECT count(*)::VARCHAR FROM person_knows_person)),
        ('pair_table', {sql_string(f"benchmark_pairs_{pair_count}")}),
        ('pair_rows', (SELECT count(*)::VARCHAR FROM benchmark_pairs_{pair_count}))
) AS metadata(key, value);

ANALYZE;
"""
    print(f"Materializing {sf_name(scale_factor)} benchmark database at {out_db}")
    _, elapsed = run_duckdb(BENCH_DUCKDB, out_db, sql)
    print(f"Materialized {sf_name(scale_factor)} database in {elapsed:.2f}s")


def pair_table_sql(pair_count):
    return f"""
CREATE TABLE IF NOT EXISTS benchmark_pairs_{pair_count} AS
WITH srcs AS (
    SELECT rowid::BIGINT AS src, row_number() OVER (ORDER BY id)::BIGINT - 1 AS rn
    FROM person
    ORDER BY id
    LIMIT {pair_count}
),
n AS (
    SELECT count(*)::BIGINT AS vertex_count
    FROM person
)
SELECT src, ((rn * 104729 + 15485863) % vertex_count)::BIGINT AS dst
FROM srcs, n;
"""


def ensure_pair_table(scale_factor, pair_count):
    out_db = db_path(scale_factor)
    if not out_db.exists():
        return

    sql = f"""
{pair_table_sql(pair_count)}
ANALYZE benchmark_pairs_{pair_count};
"""
    print(f"Ensuring {sf_name(scale_factor)} benchmark_pairs_{pair_count} exists")
    _, elapsed = run_duckdb(BENCH_DUCKDB, out_db, sql)
    print(f"Prepared benchmark_pairs_{pair_count} for {sf_name(scale_factor)} in {elapsed:.2f}s")


def prepare(args):
    for scale_factor in args.scale_factors:
        generate_parquet(scale_factor, args.threads, args.force)
        materialize_database(scale_factor, args.pairs, args.force)


def csr_cte(schema_prefix):
    person = f"{schema_prefix}.person"
    knows = f"{schema_prefix}.person_knows_person"
    return f"""
WITH csr_cte AS (
    SELECT cast(min(create_csr_edge(
        0,
        (SELECT count(a.id) FROM {person} a),
        CAST((
            SELECT sum(create_csr_vertex(
                0,
                (SELECT count(a.id) FROM {person} a),
                sub.dense_id,
                sub.cnt))
            FROM (
                SELECT a.rowid AS dense_id, count(k.person1id) AS cnt
                FROM {person} a
                LEFT JOIN {knows} k ON k.person1id = a.id
                GROUP BY a.rowid
            ) sub
        ) AS BIGINT),
        (SELECT count() FROM {knows} k JOIN {person} a ON a.id = k.person1id JOIN {person} c ON c.id = k.person2id),
        a.rowid,
        c.rowid,
        k.rowid
    )) AS BIGINT) AS csr_id
    FROM {knows} k
    JOIN {person} a ON a.id = k.person1id
    JOIN {person} c ON c.id = k.person2id
)
"""


def operator_sql(attached_db, pair_count, threads, benchmark_prefix):
    pairs = f"ldbc.benchmark_pairs_{pair_count}"
    return f"""
SET experimental_path_finding_operator_benchmark=true;
SET experimental_path_finding_operator_benchmark_prefix={sql_string(benchmark_prefix)};
{csr_cte("ldbc")}
SELECT 'operator' AS mode, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM (
    SELECT src, dst, iterativelengthoperator(src, dst, csr_id) AS len
    FROM {pairs}, csr_cte
);
"""


def scalar_sql(attached_db, pair_count, threads):
    pairs = f"ldbc.benchmark_pairs_{pair_count}"
    person = "ldbc.person"
    return f"""
{csr_cte("ldbc")},
csr_ready AS (
    SELECT multiply(0, count(csr_cte.csr_id)) AS temp
    FROM csr_cte
)
SELECT 'scalar' AS mode, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM (
    SELECT (__x.temp * 0 + iterativelength(0, (SELECT count(*) FROM {person}), p.src, p.dst)) AS len
    FROM {pairs} p
    CROSS JOIN csr_ready __x
);
"""


def recursive_sql(attached_db, pair_count, threads, max_depth):
    pairs = f"ldbc.benchmark_pairs_{pair_count}"
    return f"""
WITH RECURSIVE
pairs AS (
    SELECT rowid::BIGINT AS pair_id, src, dst
    FROM {pairs}
),
targets AS (
    SELECT DISTINCT dst
    FROM pairs
),
edges AS (
    SELECT a.rowid::BIGINT AS src, c.rowid::BIGINT AS dst
    FROM ldbc.person_knows_person k
    JOIN ldbc.person a ON a.id = k.person1id
    JOIN ldbc.person c ON c.id = k.person2id
),
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
SELECT 'recursive' AS mode, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM lengths;
"""


def setup_sql(attached_db, threads):
    return f"""
LOAD {sql_string(DUCKPGQ_EXTENSION)};
SET threads={threads};
SET experimental_path_finding_operator=true;
ATTACH {sql_string(attached_db)} AS ldbc;
-- Initializes DuckPGQState and its internal catalog table outside the timed query.
-- The CSR id is deliberately absent, so this does not build or retain graph state.
SELECT delete_csr(2147483647) AS duckpgq_state_init;
"""


def parse_csv_row(output):
    rows = list(csv.DictReader(output.splitlines()))
    if len(rows) != 1:
        raise ValueError(f"Expected one CSV result row, got {len(rows)} rows: {output}")
    return rows[0]


def benchmark_modes(mode):
    if mode == "both":
        return ["operator", "scalar"]
    if mode == "all":
        return ["operator", "scalar", "recursive"]
    return [mode]


def mode_sql(mode, attached_db, pair_count, threads, benchmark_prefix, recursive_max_depth):
    if mode == "operator":
        return operator_sql(attached_db, pair_count, threads, benchmark_prefix)
    if mode == "scalar":
        return scalar_sql(attached_db, pair_count, threads)
    if mode == "recursive":
        return recursive_sql(attached_db, pair_count, threads, recursive_max_depth)
    raise ValueError(f"Unsupported benchmark mode: {mode}")


def verify_result_rows(results):
    result_keys = ["pair_count", "reachable_count", "total_len", "min_len", "max_len"]
    for repeat in sorted({row["repeat"] for row in results}):
        rows = [row for row in results if row["repeat"] == repeat]
        if len(rows) < 2:
            continue
        expected = {key: rows[0][key] for key in result_keys}
        expected_mode = rows[0]["mode"]
        for row in rows[1:]:
            actual = {key: row[key] for key in result_keys}
            if actual != expected:
                raise RuntimeError(
                    f"Benchmark result mismatch for repeat {repeat}: {expected_mode}={expected}, "
                    f"{row['mode']}={actual}"
                )


def stdev(values):
    return statistics.stdev(values) if len(values) > 1 else 0.0


def summarize_results(results):
    stats = []
    modes = sorted({row["mode"] for row in results})
    for mode in modes:
        rows = [row for row in results if row["mode"] == mode]
        setup_times = [float(row["setup_s"]) for row in rows]
        query_times = [float(row["query_s"]) for row in rows]
        total_times = [float(row["total_s"]) for row in rows]
        stats.append(
            {
                "scale_factor": rows[0]["scale_factor"],
                "mode": mode,
                "threads": rows[0]["threads"],
                "repeats": len(rows),
                "recursive_max_depth": rows[0]["recursive_max_depth"],
                "pair_count": rows[0]["pair_count"],
                "reachable_count": rows[0]["reachable_count"],
                "total_len": rows[0]["total_len"],
                "min_len": rows[0]["min_len"],
                "max_len": rows[0]["max_len"],
                "setup_mean_s": f"{statistics.mean(setup_times):.6f}",
                "setup_stdev_s": f"{stdev(setup_times):.6f}",
                "query_mean_s": f"{statistics.mean(query_times):.6f}",
                "query_stdev_s": f"{stdev(query_times):.6f}",
                "query_min_s": f"{min(query_times):.6f}",
                "query_max_s": f"{max(query_times):.6f}",
                "total_mean_s": f"{statistics.mean(total_times):.6f}",
                "total_stdev_s": f"{stdev(total_times):.6f}",
                "database": rows[0]["database"],
            }
        )
    return stats


def run_benchmark(args):
    results_dir = DATA_ROOT / "results" / sf_name(args.scale_factor)
    results_dir.mkdir(parents=True, exist_ok=True)
    attached_db = db_path(args.scale_factor)
    if not attached_db.exists():
        raise SystemExit(f"Missing benchmark database: {attached_db}. Run prepare first.")
    ensure_pair_table(args.scale_factor, args.pairs)

    results = []
    modes = benchmark_modes(args.mode)
    for repeat in range(1, args.repeats + 1):
        for mode in modes:
            prefix = results_dir / f"{mode}_pairs{args.pairs}_threads{args.threads}_repeat{repeat}"
            query_sql = mode_sql(mode, attached_db, args.pairs, args.threads, prefix, args.recursive_max_depth)
            output, timers = run_duckdb_timed_script(setup_sql(attached_db, args.threads) + query_sql, args.timeout)
            row = parse_csv_row(output)
            row["scale_factor"] = args.scale_factor
            row["threads"] = args.threads
            row["repeat"] = repeat
            row["recursive_max_depth"] = args.recursive_max_depth if mode == "recursive" else ""
            row["setup_s"] = f"{sum(timers[:-1]):.6f}"
            row["query_s"] = f"{timers[-1]:.6f}"
            row["total_s"] = f"{sum(timers):.6f}"
            row["database"] = str(attached_db)
            results.append(row)
            print(json.dumps(row, sort_keys=True))

    if args.verify:
        verify_result_rows(results)

    timestamp = int(time.time())
    result_path = results_dir / f"summary_pairs{args.pairs}_threads{args.threads}_{timestamp}.csv"
    with result_path.open("w", newline="") as handle:
        fieldnames = [
            "scale_factor",
            "mode",
            "threads",
            "repeat",
            "recursive_max_depth",
            "pair_count",
            "reachable_count",
            "total_len",
            "min_len",
            "max_len",
            "setup_s",
            "query_s",
            "total_s",
            "database",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"Wrote summary: {result_path}")

    stats = summarize_results(results)
    stats_path = results_dir / f"stats_pairs{args.pairs}_threads{args.threads}_{timestamp}.csv"
    with stats_path.open("w", newline="") as handle:
        fieldnames = [
            "scale_factor",
            "mode",
            "threads",
            "repeats",
            "recursive_max_depth",
            "pair_count",
            "reachable_count",
            "total_len",
            "min_len",
            "max_len",
            "setup_mean_s",
            "setup_stdev_s",
            "query_mean_s",
            "query_stdev_s",
            "query_min_s",
            "query_max_s",
            "total_mean_s",
            "total_stdev_s",
            "database",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(stats)
    for row in stats:
        print(json.dumps(row, sort_keys=True))
    print(f"Wrote stats: {stats_path}")


def main():
    parser = argparse.ArgumentParser(description="Prepare and smoke-run pathfinding benchmarks on LDBC Person knows Person data.")
    subcommands = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subcommands.add_parser("prepare")
    prepare_parser.add_argument("--scale-factors", nargs="+", default=["1", "3", "10"])
    prepare_parser.add_argument("--threads", type=int, default=8)
    prepare_parser.add_argument("--pairs", type=int, default=1024)
    prepare_parser.add_argument("--force", action="store_true")
    prepare_parser.set_defaults(func=prepare)

    run_parser = subcommands.add_parser("run")
    run_parser.add_argument("--scale-factor", required=True)
    run_parser.add_argument("--threads", type=int, default=4)
    run_parser.add_argument("--pairs", type=int, default=1024)
    run_parser.add_argument("--repeats", type=int, default=1)
    run_parser.add_argument("--mode", choices=["operator", "scalar", "recursive", "both", "all"], default="both")
    run_parser.add_argument("--recursive-max-depth", type=int, default=8)
    run_parser.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True)
    run_parser.add_argument("--timeout", type=int, default=300)
    run_parser.set_defaults(func=run_benchmark)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
