#!/usr/bin/env python3
import argparse
import csv
import json
import re
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "data" / "ldbc-pathfinding"
GENERATOR_ROOT = Path.home() / "git" / "ldbc-data-gen"
GENERATOR_DUCKDB = GENERATOR_ROOT / "build" / "release" / "duckdb"
GENERATOR_EXTENSION = GENERATOR_ROOT / "build" / "release" / "extension" / "ldbc_data_gen" / "ldbc_data_gen.duckdb_extension"
BENCH_DUCKDB = REPO_ROOT / "build" / "release" / "duckdb"
DUCKPGQ_EXTENSION = REPO_ROOT / "build" / "release" / "extension" / "duckpgq" / "duckpgq.duckdb_extension"
PAIR_SHAPES = ("random", "same_dst", "same_src")


@dataclass(frozen=True)
class BenchmarkOptions:
    attached_db: Path
    pair_count: int
    pair_table: str
    threads: int
    benchmark_prefix: Path
    recursive_max_depth: int
    build_reverse_csr: bool
    metrics_enabled: bool
    push_pull_frontier_gate: int
    deduplicate_pairs: bool
    grouped_batches: bool
    threads_per_batch: int
    max_concurrent_batches: int
    reverse_orientation_ratio: int


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
    return pair_table_sql_for_shape(pair_count, "random")


def pair_table_name(pair_count, pair_shape):
    if pair_shape == "random":
        return f"benchmark_pairs_{pair_count}"
    if pair_shape == "same_dst":
        return f"benchmark_pairs_same_dst_{pair_count}"
    if pair_shape == "same_src":
        return f"benchmark_pairs_same_src_{pair_count}"
    raise ValueError(f"Unsupported pair shape: {pair_shape}")


def pair_table_sql_for_shape(pair_count, pair_shape):
    if pair_shape == "random":
        return f"""
CREATE TABLE IF NOT EXISTS {pair_table_name(pair_count, pair_shape)} AS
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
    if pair_shape == "same_dst":
        return f"""
CREATE TABLE IF NOT EXISTS {pair_table_name(pair_count, pair_shape)} AS
WITH srcs AS (
    SELECT rowid::BIGINT AS src
    FROM person
    ORDER BY id
    LIMIT {pair_count}
),
n AS (
    SELECT count(*)::BIGINT AS vertex_count
    FROM person
)
SELECT src, (15485863 % vertex_count)::BIGINT AS dst
FROM srcs, n;
"""
    if pair_shape == "same_src":
        return f"""
CREATE TABLE IF NOT EXISTS {pair_table_name(pair_count, pair_shape)} AS
WITH dsts AS (
    SELECT row_number() OVER (ORDER BY id)::BIGINT - 1 AS rn
    FROM person
    ORDER BY id
    LIMIT {pair_count}
),
n AS (
    SELECT count(*)::BIGINT AS vertex_count
    FROM person
)
SELECT 0::BIGINT AS src, ((rn * 104729 + 15485863) % vertex_count)::BIGINT AS dst
FROM dsts, n;
"""
    raise ValueError(f"Unsupported pair shape: {pair_shape}")


def pair_profile_sql(pair_table):
    return f"""
WITH pairs AS (
    SELECT src, dst
    FROM {pair_table}
),
unique_pairs AS (
    SELECT src, dst
    FROM pairs
    GROUP BY src, dst
)
SELECT count(*)::BIGINT AS pair_table_rows,
       count(DISTINCT src)::BIGINT AS distinct_src_count,
       count(DISTINCT dst)::BIGINT AS distinct_dst_count,
       (SELECT count(*)::BIGINT FROM unique_pairs) AS unique_pair_count,
       (count(*) - (SELECT count(*) FROM unique_pairs))::BIGINT AS duplicate_pair_count,
       coalesce(sum(CASE WHEN src = dst THEN 1 ELSE 0 END), 0)::BIGINT AS self_pair_count
FROM pairs;
"""


def ensure_pair_table(scale_factor, pair_count, pair_shape="random"):
    out_db = db_path(scale_factor)
    if not out_db.exists():
        return

    sql = f"""
{pair_table_sql_for_shape(pair_count, pair_shape)}
ANALYZE {pair_table_name(pair_count, pair_shape)};
"""
    print(f"Ensuring {sf_name(scale_factor)} {pair_table_name(pair_count, pair_shape)} exists")
    _, elapsed = run_duckdb(BENCH_DUCKDB, out_db, sql)
    print(f"Prepared {pair_table_name(pair_count, pair_shape)} for {sf_name(scale_factor)} in {elapsed:.2f}s")


def read_pair_profile(attached_db, pair_table):
    output, _ = run_duckdb(BENCH_DUCKDB, attached_db, pair_profile_sql(pair_table), quiet=True)
    rows = list(csv.DictReader(output.splitlines()))
    if len(rows) != 1:
        raise RuntimeError(f"Expected one pair profile row for {pair_table}, got: {output}")
    return rows[0]


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


def operator_sql(options):
    pairs = f"ldbc.{options.pair_table}"
    reverse_value = "true" if options.build_reverse_csr else "false"
    metrics_value = "true" if options.metrics_enabled else "false"
    dedupe_value = "true" if options.deduplicate_pairs else "false"
    grouped_value = "true" if options.grouped_batches else "false"
    return f"""
SET experimental_path_finding_operator_benchmark={metrics_value};
SET experimental_path_finding_operator_benchmark_prefix={sql_string(options.benchmark_prefix)};
SET experimental_path_finding_operator_build_reverse_csr={reverse_value};
SET experimental_path_finding_operator_deduplicate_pairs={dedupe_value};
SET experimental_path_finding_operator_grouped_batches={grouped_value};
SET experimental_path_finding_operator_threads_per_batch={options.threads_per_batch};
SET experimental_path_finding_operator_max_concurrent_batches={options.max_concurrent_batches};
SET experimental_path_finding_operator_reverse_orientation_ratio={options.reverse_orientation_ratio};
{csr_cte("ldbc")}
SELECT 'operator' AS mode, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM (
    SELECT src, dst, iterativelengthoperator(src, dst, csr_id) AS len
    FROM {pairs}, csr_cte
);
"""


def bidirectional_operator_sql(options):
    pairs = f"ldbc.{options.pair_table}"
    reverse_value = "true" if options.build_reverse_csr else "false"
    metrics_value = "true" if options.metrics_enabled else "false"
    dedupe_value = "true" if options.deduplicate_pairs else "false"
    return f"""
SET experimental_path_finding_operator_benchmark={metrics_value};
SET experimental_path_finding_operator_benchmark_prefix={sql_string(options.benchmark_prefix)};
SET experimental_path_finding_operator_build_reverse_csr={reverse_value};
SET experimental_path_finding_operator_deduplicate_pairs={dedupe_value};
{csr_cte("ldbc")}
SELECT 'bidirectional_operator' AS mode, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM (
    SELECT src, dst, bidirectionaliterativelengthoperator(src, dst, csr_id) AS len
    FROM {pairs}, csr_cte
);
"""


def pushpull_operator_sql(options):
    pairs = f"ldbc.{options.pair_table}"
    reverse_value = "true" if options.build_reverse_csr else "false"
    metrics_value = "true" if options.metrics_enabled else "false"
    dedupe_value = "true" if options.deduplicate_pairs else "false"
    return f"""
SET experimental_path_finding_operator_benchmark={metrics_value};
SET experimental_path_finding_operator_benchmark_prefix={sql_string(options.benchmark_prefix)};
SET experimental_path_finding_operator_build_reverse_csr={reverse_value};
SET experimental_path_finding_operator_push_pull_frontier_gate={options.push_pull_frontier_gate};
SET experimental_path_finding_operator_deduplicate_pairs={dedupe_value};
{csr_cte("ldbc")}
SELECT 'pushpull_operator' AS mode, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM (
    SELECT src, dst, pushpulliterativelengthoperator(src, dst, csr_id) AS len
    FROM {pairs}, csr_cte
);
"""


def csr_sql(options):
    return f"""
{csr_cte("ldbc")}
SELECT 'csr' AS mode, 0::BIGINT AS pair_count, NULL::BIGINT AS reachable_count,
       NULL::BIGINT AS total_len, NULL::BIGINT AS min_len, NULL::BIGINT AS max_len
FROM csr_cte;
"""


def scalar_sql(options):
    pairs = f"ldbc.{options.pair_table}"
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


def recursive_sql(options):
    pairs = f"ldbc.{options.pair_table}"
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
          AND dvr.len < {options.recursive_max_depth}
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


def setup_sql(options):
    return f"""
LOAD {sql_string(DUCKPGQ_EXTENSION)};
SET threads={options.threads};
SET experimental_path_finding_operator=true;
ATTACH {sql_string(options.attached_db)} AS ldbc;
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
    if mode == "operators":
        return ["operator", "pushpull_operator", "bidirectional_operator"]
    if mode == "all":
        return ["operator", "pushpull_operator", "bidirectional_operator", "scalar", "recursive"]
    return [mode]


def mode_sql(mode, options):
    if mode == "operator":
        return operator_sql(options)
    if mode == "bidirectional_operator":
        return bidirectional_operator_sql(options)
    if mode == "pushpull_operator":
        return pushpull_operator_sql(options)
    if mode == "csr":
        return csr_sql(options)
    if mode == "scalar":
        return scalar_sql(options)
    if mode == "recursive":
        return recursive_sql(options)
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


def phase_timing_path(benchmark_prefix):
    return Path(str(benchmark_prefix) + "_phase_timing.csv")


def bidirectional_phase_detail_path(benchmark_prefix):
    return Path(str(benchmark_prefix) + "_bidirectional_phase_detail.csv")


def pushpull_iteration_stats_path(benchmark_prefix):
    return Path(str(benchmark_prefix) + "_pushpull_iteration_stats.csv")


def pushpull_phase_detail_path(benchmark_prefix):
    return Path(str(benchmark_prefix) + "_pushpull_phase_detail.csv")


def read_phase_timing(benchmark_prefix):
    path = phase_timing_path(benchmark_prefix)
    result = {
        "local_csr_forward_s": "",
        "local_csr_reverse_s": "",
        "local_csr_pull_s": "",
        "bfs_s": "",
        "bfs_batches": "",
        "dedupe_build_s": "",
        "dedupe_scatter_s": "",
        "dedupe_batches": "",
        "dedupe_scatter_batches": "",
        "dedupe_input_pairs": "",
        "dedupe_unique_pairs": "",
        "dedupe_duplicate_pairs": "",
        "dedupe_remap_memory_bytes": "",
        "local_csr_forward_memory_bytes": "",
        "local_csr_reverse_memory_bytes": "",
        "local_csr_pull_memory_bytes": "",
    }
    if not path.exists():
        return result

    local_csr_forward_ms = 0.0
    local_csr_reverse_ms = 0.0
    local_csr_pull_ms = 0.0
    bfs_ms = 0.0
    bfs_batches = 0
    dedupe_build_ms = 0.0
    dedupe_scatter_ms = 0.0
    dedupe_batches = 0
    dedupe_scatter_batches = 0
    dedupe_input_pairs = 0
    dedupe_unique_pairs = 0
    dedupe_duplicate_pairs = 0
    dedupe_remap_memory = 0
    local_csr_forward_memory = ""
    local_csr_reverse_memory = ""
    local_csr_pull_memory = ""
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            phase = row["Phase"]
            time_ms = float(row["Time_ms"])
            if phase == "local_csr_forward":
                local_csr_forward_ms += time_ms
                local_csr_forward_memory = row["MemoryBytes"]
            elif phase == "local_csr_reverse":
                local_csr_reverse_ms += time_ms
                local_csr_reverse_memory = row["MemoryBytes"]
            elif phase == "local_csr_pull":
                local_csr_pull_ms += time_ms
                local_csr_pull_memory = row["MemoryBytes"]
            elif phase == "bfs_batch" or phase == "bfs_batch_grouped":
                bfs_ms += time_ms
                bfs_batches += 1
            elif phase == "dedupe_build":
                dedupe_build_ms += time_ms
                dedupe_batches += 1
                dedupe_input_pairs += int(row["PairCount"])
                dedupe_unique_pairs += int(row["EdgeCount"])
                dedupe_duplicate_pairs += int(row["PartitionCount"])
                dedupe_remap_memory += int(row["MemoryBytes"])
            elif phase == "dedupe_scatter":
                dedupe_scatter_ms += time_ms
                dedupe_scatter_batches += 1

    if local_csr_forward_ms:
        result["local_csr_forward_s"] = f"{local_csr_forward_ms / 1000.0:.6f}"
        result["local_csr_forward_memory_bytes"] = local_csr_forward_memory
    if local_csr_reverse_ms:
        result["local_csr_reverse_s"] = f"{local_csr_reverse_ms / 1000.0:.6f}"
        result["local_csr_reverse_memory_bytes"] = local_csr_reverse_memory
    if local_csr_pull_ms:
        result["local_csr_pull_s"] = f"{local_csr_pull_ms / 1000.0:.6f}"
        result["local_csr_pull_memory_bytes"] = local_csr_pull_memory
    if bfs_batches:
        result["bfs_s"] = f"{bfs_ms / 1000.0:.6f}"
        result["bfs_batches"] = bfs_batches
    if dedupe_batches:
        result["dedupe_build_s"] = f"{dedupe_build_ms / 1000.0:.6f}"
        result["dedupe_batches"] = dedupe_batches
        result["dedupe_input_pairs"] = dedupe_input_pairs
        result["dedupe_unique_pairs"] = dedupe_unique_pairs
        result["dedupe_duplicate_pairs"] = dedupe_duplicate_pairs
        result["dedupe_remap_memory_bytes"] = dedupe_remap_memory
    if dedupe_scatter_batches:
        result["dedupe_scatter_s"] = f"{dedupe_scatter_ms / 1000.0:.6f}"
        result["dedupe_scatter_batches"] = dedupe_scatter_batches
    return result


def mean_optional(rows, field):
    values = [float(row[field]) for row in rows if row[field]]
    return f"{statistics.mean(values):.6f}" if values else ""


def stdev_optional(rows, field):
    values = [float(row[field]) for row in rows if row[field]]
    return f"{stdev(values):.6f}" if values else ""


def mean_int_optional(rows, field):
    values = [int(row[field]) for row in rows if row[field] != ""]
    return f"{statistics.mean(values):.1f}" if values else ""


def summarize_results(results):
    stats = []
    modes = sorted({row["mode"] for row in results})
    for mode in modes:
        rows = [row for row in results if row["mode"] == mode]
        setup_times = [float(row["setup_s"]) for row in rows]
        csr_build_times = [float(row["csr_build_s"]) for row in rows if row["csr_build_s"]]
        query_times = [float(row["query_s"]) for row in rows]
        total_times = [float(row["total_s"]) for row in rows]
        stats.append(
            {
                "scale_factor": rows[0]["scale_factor"],
                "mode": mode,
                "threads": rows[0]["threads"],
                "repeats": len(rows),
                "metrics_enabled": rows[0]["metrics_enabled"],
                "deduplicate_pairs": rows[0]["deduplicate_pairs"],
                "grouped_batches": rows[0]["grouped_batches"],
                "threads_per_batch": rows[0]["threads_per_batch"],
                "max_concurrent_batches": rows[0]["max_concurrent_batches"],
                "reverse_orientation_ratio": rows[0]["reverse_orientation_ratio"],
                "recursive_max_depth": rows[0]["recursive_max_depth"],
                "pair_count": rows[0]["pair_count"],
                "pair_table": rows[0]["pair_table"],
                "pair_shape": rows[0]["pair_shape"],
                "pair_table_rows": rows[0]["pair_table_rows"],
                "distinct_src_count": rows[0]["distinct_src_count"],
                "distinct_dst_count": rows[0]["distinct_dst_count"],
                "unique_pair_count": rows[0]["unique_pair_count"],
                "duplicate_pair_count": rows[0]["duplicate_pair_count"],
                "self_pair_count": rows[0]["self_pair_count"],
                "reachable_count": rows[0]["reachable_count"],
                "total_len": rows[0]["total_len"],
                "min_len": rows[0]["min_len"],
                "max_len": rows[0]["max_len"],
                "setup_mean_s": f"{statistics.mean(setup_times):.6f}",
                "setup_stdev_s": f"{stdev(setup_times):.6f}",
                "csr_build_mean_s": f"{statistics.mean(csr_build_times):.6f}" if csr_build_times else "",
                "csr_build_stdev_s": f"{stdev(csr_build_times):.6f}" if csr_build_times else "",
                "local_csr_forward_mean_s": mean_optional(rows, "local_csr_forward_s"),
                "local_csr_forward_stdev_s": stdev_optional(rows, "local_csr_forward_s"),
                "local_csr_reverse_mean_s": mean_optional(rows, "local_csr_reverse_s"),
                "local_csr_reverse_stdev_s": stdev_optional(rows, "local_csr_reverse_s"),
                "local_csr_pull_mean_s": mean_optional(rows, "local_csr_pull_s"),
                "local_csr_pull_stdev_s": stdev_optional(rows, "local_csr_pull_s"),
                "bfs_mean_s": mean_optional(rows, "bfs_s"),
                "bfs_stdev_s": stdev_optional(rows, "bfs_s"),
                "dedupe_build_mean_s": mean_optional(rows, "dedupe_build_s"),
                "dedupe_build_stdev_s": stdev_optional(rows, "dedupe_build_s"),
                "dedupe_scatter_mean_s": mean_optional(rows, "dedupe_scatter_s"),
                "dedupe_scatter_stdev_s": stdev_optional(rows, "dedupe_scatter_s"),
                "dedupe_input_pairs_mean": mean_int_optional(rows, "dedupe_input_pairs"),
                "dedupe_unique_pairs_mean": mean_int_optional(rows, "dedupe_unique_pairs"),
                "dedupe_duplicate_pairs_mean": mean_int_optional(rows, "dedupe_duplicate_pairs"),
                "dedupe_remap_memory_bytes_mean": mean_int_optional(rows, "dedupe_remap_memory_bytes"),
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
    pair_shape = "custom" if args.pair_table else args.pair_shape
    pair_table = args.pair_table or pair_table_name(args.pairs, args.pair_shape)
    if args.pair_table is None:
        ensure_pair_table(args.scale_factor, args.pairs, args.pair_shape)
    pair_profile = read_pair_profile(attached_db, pair_table)

    results = []
    modes = benchmark_modes(args.mode)
    for repeat in range(1, args.repeats + 1):
        for mode in modes:
            prefix = results_dir / f"{mode}_pairs{args.pairs}_threads{args.threads}_repeat{repeat}"
            phase_path = phase_timing_path(prefix)
            if phase_path.exists():
                phase_path.unlink()
            bidirectional_phase_path = bidirectional_phase_detail_path(prefix)
            if bidirectional_phase_path.exists():
                bidirectional_phase_path.unlink()
            pushpull_iteration_path = pushpull_iteration_stats_path(prefix)
            if pushpull_iteration_path.exists():
                pushpull_iteration_path.unlink()
            pushpull_phase_path = pushpull_phase_detail_path(prefix)
            if pushpull_phase_path.exists():
                pushpull_phase_path.unlink()
            options = BenchmarkOptions(
                attached_db=attached_db,
                pair_count=args.pairs,
                pair_table=pair_table,
                threads=args.threads,
                benchmark_prefix=prefix,
                recursive_max_depth=args.recursive_max_depth,
                build_reverse_csr=args.build_reverse_csr,
                metrics_enabled=args.metrics,
                push_pull_frontier_gate=args.push_pull_frontier_gate,
                deduplicate_pairs=args.deduplicate_pairs,
                grouped_batches=args.grouped_batches,
                threads_per_batch=args.threads_per_batch,
                max_concurrent_batches=args.max_concurrent_batches,
                reverse_orientation_ratio=args.reverse_orientation_ratio,
            )
            query_sql = mode_sql(mode, options)
            output, timers = run_duckdb_timed_script(setup_sql(options) + query_sql, args.timeout)
            row = parse_csv_row(output)
            row["scale_factor"] = args.scale_factor
            row["threads"] = args.threads
            row["repeat"] = repeat
            row["metrics_enabled"] = int(args.metrics)
            row["deduplicate_pairs"] = int(args.deduplicate_pairs)
            row["grouped_batches"] = int(args.grouped_batches and mode == "operator")
            row["threads_per_batch"] = args.threads_per_batch if mode == "operator" else ""
            row["max_concurrent_batches"] = args.max_concurrent_batches if mode == "operator" else ""
            row["reverse_orientation_ratio"] = args.reverse_orientation_ratio if mode == "operator" else ""
            row["pair_table"] = pair_table
            row["pair_shape"] = pair_shape
            row.update(pair_profile)
            row["recursive_max_depth"] = args.recursive_max_depth if mode == "recursive" else ""
            if mode == "csr":
                row["setup_s"] = f"{sum(timers[:-1]):.6f}"
                row["csr_build_s"] = f"{timers[-1]:.6f}"
                row["query_s"] = f"{timers[-1]:.6f}"
            else:
                row["setup_s"] = f"{sum(timers[:-1]):.6f}"
                row["csr_build_s"] = ""
                row["query_s"] = f"{timers[-1]:.6f}"
            row["total_s"] = f"{sum(timers):.6f}"
            row.update(read_phase_timing(prefix))
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
            "metrics_enabled",
            "deduplicate_pairs",
            "grouped_batches",
            "threads_per_batch",
            "max_concurrent_batches",
            "reverse_orientation_ratio",
            "recursive_max_depth",
            "pair_count",
            "pair_table",
            "pair_shape",
            "pair_table_rows",
            "distinct_src_count",
            "distinct_dst_count",
            "unique_pair_count",
            "duplicate_pair_count",
            "self_pair_count",
            "reachable_count",
            "total_len",
            "min_len",
            "max_len",
            "setup_s",
            "csr_build_s",
            "local_csr_forward_s",
            "local_csr_reverse_s",
            "local_csr_pull_s",
            "bfs_s",
            "bfs_batches",
            "dedupe_build_s",
            "dedupe_scatter_s",
            "dedupe_batches",
            "dedupe_scatter_batches",
            "dedupe_input_pairs",
            "dedupe_unique_pairs",
            "dedupe_duplicate_pairs",
            "dedupe_remap_memory_bytes",
            "local_csr_forward_memory_bytes",
            "local_csr_reverse_memory_bytes",
            "local_csr_pull_memory_bytes",
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
            "metrics_enabled",
            "deduplicate_pairs",
            "grouped_batches",
            "threads_per_batch",
            "max_concurrent_batches",
            "reverse_orientation_ratio",
            "recursive_max_depth",
            "pair_count",
            "pair_table",
            "pair_shape",
            "pair_table_rows",
            "distinct_src_count",
            "distinct_dst_count",
            "unique_pair_count",
            "duplicate_pair_count",
            "self_pair_count",
            "reachable_count",
            "total_len",
            "min_len",
            "max_len",
            "setup_mean_s",
            "setup_stdev_s",
            "csr_build_mean_s",
            "csr_build_stdev_s",
            "local_csr_forward_mean_s",
            "local_csr_forward_stdev_s",
            "local_csr_reverse_mean_s",
            "local_csr_reverse_stdev_s",
            "local_csr_pull_mean_s",
            "local_csr_pull_stdev_s",
            "bfs_mean_s",
            "bfs_stdev_s",
            "dedupe_build_mean_s",
            "dedupe_build_stdev_s",
            "dedupe_scatter_mean_s",
            "dedupe_scatter_stdev_s",
            "dedupe_input_pairs_mean",
            "dedupe_unique_pairs_mean",
            "dedupe_duplicate_pairs_mean",
            "dedupe_remap_memory_bytes_mean",
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
    run_parser.add_argument(
        "--pair-table",
        default=None,
        help="Override the benchmark pair table name. Defaults to benchmark_pairs_<pairs>.",
    )
    run_parser.add_argument(
        "--pair-shape",
        choices=PAIR_SHAPES,
        default="random",
        help="Generated pair table shape when --pair-table is not set.",
    )
    run_parser.add_argument("--repeats", type=int, default=1)
    run_parser.add_argument(
        "--mode",
        choices=[
            "operator",
            "pushpull_operator",
            "bidirectional_operator",
            "csr",
            "scalar",
            "recursive",
            "both",
            "operators",
            "all",
        ],
        default="both",
    )
    run_parser.add_argument("--build-reverse-csr", action="store_true")
    run_parser.add_argument(
        "--push-pull-frontier-gate",
        type=int,
        default=2,
        help="Use pull when frontier_vertices * gate is at least the vertex count.",
    )
    run_parser.add_argument(
        "--metrics",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable internal operator benchmark CSV metrics. Disabled by default for clean wall-clock timing.",
    )
    run_parser.add_argument(
        "--deduplicate-pairs",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable exact source/destination pair deduplication inside path-finding operator batches.",
    )
    run_parser.add_argument(
        "--grouped-batches",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable grouped regular MS-BFS scheduling with bounded worker groups.",
    )
    run_parser.add_argument(
        "--threads-per-batch",
        type=int,
        default=0,
        help="Maximum workers per grouped regular MS-BFS batch; <= 0 uses all query threads.",
    )
    run_parser.add_argument(
        "--max-concurrent-batches",
        type=int,
        default=0,
        help="Maximum grouped regular MS-BFS batches admitted concurrently; <= 0 derives from thread budget.",
    )
    run_parser.add_argument(
        "--reverse-orientation-ratio",
        type=int,
        default=4,
        help="Use reverse MS-BFS for operator mode when estimated distinct_src >= ratio * estimated distinct_dst; <= 0 disables it.",
    )
    run_parser.add_argument("--recursive-max-depth", type=int, default=8)
    run_parser.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True)
    run_parser.add_argument("--timeout", type=int, default=300)
    run_parser.set_defaults(func=run_benchmark)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
