#!/usr/bin/env python3
"""Benchmark eager CSR construction against lazy construction and warm reuse.

The runner uses a fresh DuckDB process for every mode/trial so connection-local
CSR state cannot leak between measurements. Results and internal phase metrics
are written below data/ldbc-pathfinding/results/eager_csr by default.
"""

import argparse
import csv
import json
import re
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = REPO_ROOT / "build" / "release" / "duckdb"
DEFAULT_EXTENSION = REPO_ROOT / "build" / "release" / "extension" / "duckpgq" / "duckpgq.duckdb_extension"
DATA_ROOT = REPO_ROOT / "data" / "ldbc-pathfinding"
DEFAULT_OUTPUT = DATA_ROOT / "results" / "eager_csr"
TIMER_RE = re.compile(r"Run Time \(s\): real ([0-9.]+)")


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def run_command(command, *, input_text=None, timeout=1800):
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        input=input_text,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    if result.returncode != 0 or "Error:" in result.stdout:
        raise RuntimeError(result.stdout.strip())
    return result.stdout


def run_query(binary, sql, database=None):
    command = [str(binary), "-csv"]
    if database is not None:
        command.append(str(database))
    command.extend(["-c", sql])
    return run_command(command)


def timed_block(label, sql):
    return f"""
.print __EAGER_BENCH_BEGIN_{label}__
.timer on
{sql.strip()}
.timer off
.print __EAGER_BENCH_END_{label}__
"""


def parse_single_csv_row(output):
    rows = list(csv.DictReader(output.splitlines()))
    if len(rows) != 1:
        raise RuntimeError(f"Expected one result row, got {len(rows)}:\n{output}")
    return rows[0]


def phase_path(prefix):
    return Path(str(prefix) + "_phase_timing.csv")


def clear_phase_file(prefix):
    path = phase_path(prefix)
    if path.exists():
        path.unlink()


def summarize_phases(prefix):
    path = phase_path(prefix)
    summary = defaultdict(lambda: {"count": 0, "time_ms": 0.0, "memory_sum": 0, "memory_max": 0})
    if not path.exists():
        return summary
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            phase = row["Phase"]
            entry = summary[phase]
            memory = int(row["MemoryBytes"])
            entry["count"] += 1
            entry["time_ms"] += float(row["Time_ms"])
            entry["memory_sum"] += memory
            entry["memory_max"] = max(entry["memory_max"], memory)
    return summary


def add_phase_columns(row, prefix, column_prefix):
    phases = summarize_phases(prefix)
    selected = (
        "partitioned_csr_cache_hit",
        "partitioned_csr_cache_miss",
        "partitioned_csr_cache_publish",
        "partitioned_csr_metadata_lookup",
        "partitioned_csr_deserialize_load",
        "partitioned_csr_serialize_write",
        "partitioned_csr_invalidation",
        "partitioned_csr_rebuild",
        "endpoint_radix_partition",
        "endpoint_partition_build",
        "local_csr_forward",
        "source_group_bfs",
        "bfs_batch",
        "csr_build_buffer_manager_peak_delta",
        "csr_build_buffer_manager_swap_peak_delta",
    )
    for phase in selected:
        values = phases[phase]
        row[f"{column_prefix}_{phase}_count"] = values["count"]
        row[f"{column_prefix}_{phase}_s"] = values["time_ms"] / 1000.0
        row[f"{column_prefix}_{phase}_memory_sum_bytes"] = values["memory_sum"]
        row[f"{column_prefix}_{phase}_memory_max_bytes"] = values["memory_max"]


def dataset_path(name):
    if name.startswith("sf"):
        normalized = name[2:].replace(".", "_")
        return DATA_ROOT / "db" / f"ldbc_sf{normalized}.duckdb"
    return DATA_ROOT / "graphalytics" / "db" / f"{name}.duckdb"


def dataset_profile(binary, name, path):
    output = run_query(
        binary,
        """
SELECT (SELECT count(*) FROM person) AS vertex_count,
       (SELECT count(*) FROM person_knows_person) AS edge_count,
       (SELECT min(id) FROM person) AS source_id;
""",
        path,
    )
    row = parse_single_csv_row(output)
    profile = {key: int(value) for key, value in row.items()}
    if not name.startswith("sf"):
        properties_path = DATA_ROOT / "graphalytics" / "references" / name / f"{name}.properties"
        properties = {}
        for line in properties_path.read_text().splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key, value = stripped.split("=", 1)
                properties[key.strip()] = value.strip()
        profile["source_id"] = int(properties[f"graph.{name}.bfs.source-vertex"])
    return profile


def graphalytics_reference_profile(binary, name):
    reference_path = DATA_ROOT / "graphalytics" / "references" / name / f"{name}-BFS"
    output = run_query(
        binary,
        f"""
WITH reference AS (
    SELECT distance
    FROM read_csv(
        {sql_string(reference_path)},
        delim = ' ',
        header = false,
        columns = {{'vertex_id': 'BIGINT', 'distance': 'BIGINT'}}
    )
),
reachable AS (
    SELECT distance FROM reference WHERE distance <> 9223372036854775807
)
SELECT count(*)::BIGINT AS reachable_count,
       sum(distance)::BIGINT AS total_len,
       min(distance)::BIGINT AS min_len,
       max(distance)::BIGINT AS max_len
FROM reachable;
""",
    )
    return parse_single_csv_row(output)


def graph_sql(graph_name, edge_tables):
    edges = []
    for table_name, label in edge_tables:
        edges.append(
            f"""bench.{table_name}
        SOURCE KEY (src) REFERENCES bench.vertex (id)
        DESTINATION KEY (dst) REFERENCES bench.vertex (id)
        LABEL {label}"""
        )
    return f"""
CREATE PROPERTY GRAPH {graph_name}
VERTEX TABLES (bench.vertex PROPERTIES (id) LABEL Vertex)
EDGE TABLES (
    {', '.join(edges)}
);
"""


def real_graph_sql(graph_name):
    return f"""
CREATE PROPERTY GRAPH {graph_name}
VERTEX TABLES (bench.person PROPERTIES (id) LABEL Person)
EDGE TABLES (
    bench.person_knows_person
        SOURCE KEY (person1id) REFERENCES bench.person (id)
        DESTINATION KEY (person2id) REFERENCES bench.person (id)
        LABEL Knows
);
"""


def bfs_sql(graph_name, source_id):
    return f"""
SELECT count(*)::BIGINT AS pair_count,
       count(len)::BIGINT AS reachable_count,
       sum(len)::BIGINT AS total_len,
       min(len)::BIGINT AS min_len,
       max(len)::BIGINT AS max_len
FROM GRAPH_TABLE({graph_name}
    MATCH p = ANY SHORTEST
        (a:Person WHERE a.id = {source_id})-[k:Knows]->*(b:Person)
    COLUMNS (path_length(p) AS len)
);
"""


def real_trial(args, dataset, threads, trial, mode, output_dir):
    path = dataset_path(dataset)
    if not path.exists():
        raise RuntimeError(f"Missing dataset database: {path}")
    profile = dataset_profile(args.binary, dataset, path)
    graph_name = f"{mode}_pg"
    stem = output_dir / "metrics" / f"{dataset}_threads{threads}_trial{trial}_{mode}"
    create_prefix = Path(str(stem) + "_create")
    first_prefix = Path(str(stem) + "_first")
    warm_prefix = Path(str(stem) + "_warm")
    for prefix in (create_prefix, first_prefix, warm_prefix):
        prefix.parent.mkdir(parents=True, exist_ok=True)
        clear_phase_file(prefix)

    eager = mode == "eager"
    setup = f"""
LOAD {sql_string(args.extension)};
SET threads={threads};
SET experimental_path_finding_operator=true;
SET experimental_build_csr_on_create={'true' if eager else 'false'};
SET experimental_path_finding_operator_benchmark=true;
ATTACH {sql_string(path)} AS bench (READ_ONLY);
SELECT delete_csr(2147483647);
SET experimental_path_finding_operator_benchmark_prefix={sql_string(create_prefix)};
"""
    # Prefixes must change between timed queries, so construct the script directly in execution order.
    ordered_setup = setup
    ordered_setup += timed_block("create", real_graph_sql(graph_name))
    ordered_setup += f"SET experimental_path_finding_operator_benchmark_prefix={sql_string(first_prefix)};\n"
    ordered_setup += timed_block("first_bfs", bfs_sql(graph_name, profile["source_id"]))
    ordered_setup += f"SET experimental_path_finding_operator_benchmark_prefix={sql_string(warm_prefix)};\n"
    ordered_setup += timed_block("warm_bfs", bfs_sql(graph_name, profile["source_id"]))
    output = run_command(
        [str(args.binary), "-unsigned", "-batch", "-csv"],
        input_text=".timer off\n" + ordered_setup,
        timeout=args.timeout,
    )
    results = {}
    for label in ("create", "first_bfs", "warm_bfs"):
        start = f"__EAGER_BENCH_BEGIN_{label}__"
        end = f"__EAGER_BENCH_END_{label}__"
        segment = output.split(start, 1)[1].split(end, 1)[0]
        timers = TIMER_RE.findall(segment)
        if len(timers) != 1:
            raise RuntimeError(f"Expected one timer for {label}:\n{segment}")
        payload = "\n".join(
            line for line in segment.splitlines() if line.strip() and not TIMER_RE.search(line)
        )
        results[label] = {"seconds": float(timers[0]), "output": payload}

    first_result = parse_single_csv_row(results["first_bfs"]["output"])
    warm_result = parse_single_csv_row(results["warm_bfs"]["output"])
    if first_result != warm_result:
        raise RuntimeError(f"First/warm BFS mismatch for {dataset}: {first_result} != {warm_result}")
    reference_match = ""
    if not dataset.startswith("sf"):
        reference = graphalytics_reference_profile(args.binary, dataset)
        actual = {key: first_result[key] for key in reference}
        if actual != reference:
            raise RuntimeError(f"Graphalytics reference mismatch for {dataset}: {actual} != {reference}")
        reference_match = 1
    row = {
        "suite": "real",
        "dataset": dataset,
        "threads": threads,
        "trial": trial,
        "mode": mode,
        "vertex_count": profile["vertex_count"],
        "edge_count": profile["edge_count"],
        "edge_table_count": 1,
        "create_s": results["create"]["seconds"],
        "first_bfs_s": results["first_bfs"]["seconds"],
        "warm_bfs_s": results["warm_bfs"]["seconds"],
        "time_to_first_result_s": results["create"]["seconds"] + results["first_bfs"]["seconds"],
        "reference_match": reference_match,
        **first_result,
    }
    add_phase_columns(row, create_prefix, "create")
    add_phase_columns(row, first_prefix, "first")
    add_phase_columns(row, warm_prefix, "warm")
    return row


def prepare_synthetic_database(args, output_dir, vertices, edges_per_table, max_edge_tables):
    db_dir = output_dir / "synthetic"
    db_dir.mkdir(parents=True, exist_ok=True)
    path = db_dir / f"v{vertices}_e{edges_per_table}_tables{max_edge_tables}.duckdb"
    if path.exists() and not args.force_prepare:
        return path
    statements = [
        f"CREATE OR REPLACE TABLE vertex AS SELECT i::BIGINT AS id FROM range({vertices}) t(i);"
    ]
    for edge_index in range(1, max_edge_tables + 1):
        multiplier = 2 * edge_index + 1
        statements.append(
            f"CREATE OR REPLACE TABLE edge_{edge_index} AS "
            f"SELECT (i % {vertices})::BIGINT AS src, "
            f"((i * {multiplier} + {edge_index}) % {vertices})::BIGINT AS dst "
            f"FROM range({edges_per_table}) t(i);"
        )
    run_query(args.binary, "\n".join(statements), path)
    return path


def multi_edge_trial(args, database, vertices, edges_per_table, edge_table_count, threads, trial, output_dir):
    graph_name = "multi_eager_pg"
    edge_tables = [(f"edge_{index}", f"Edge{index}") for index in range(1, edge_table_count + 1)]
    stem = output_dir / "metrics" / (
        f"synthetic_v{vertices}_e{edges_per_table}_tables{edge_table_count}_threads{threads}_trial{trial}"
    )
    create_prefix = Path(str(stem) + "_create")
    verify_prefix = Path(str(stem) + "_verify")
    for prefix in (create_prefix, verify_prefix):
        prefix.parent.mkdir(parents=True, exist_ok=True)
        clear_phase_file(prefix)
    setup = f"""
LOAD {sql_string(args.extension)};
SET threads={threads};
SET experimental_path_finding_operator=true;
SET experimental_build_csr_on_create=true;
SET experimental_path_finding_operator_benchmark=true;
SET experimental_path_finding_operator_benchmark_prefix={sql_string(create_prefix)};
ATTACH {sql_string(database)} AS bench (READ_ONLY);
SELECT delete_csr(2147483647);
"""
    ordered_script = ".timer off\n" + setup
    ordered_script += timed_block("create", graph_sql(graph_name, edge_tables))
    ordered_script += f"SET experimental_path_finding_operator_benchmark_prefix={sql_string(verify_prefix)};\n"
    verify_labels = []
    for edge_index in range(1, edge_table_count + 1):
        label = f"verify_{edge_index}"
        verify_labels.append(label)
        verify_sql = f"""
SELECT count(*)::BIGINT AS pair_count, count(len)::BIGINT AS reachable_count
FROM GRAPH_TABLE({graph_name}
    MATCH p = ANY SHORTEST
        (a:Vertex WHERE a.id = 0)-[e:Edge{edge_index}]->*(b:Vertex WHERE b.id = 0)
    COLUMNS (path_length(p) AS len)
);
"""
        ordered_script += timed_block(label, verify_sql)
    output = run_command(
        [str(args.binary), "-unsigned", "-batch", "-csv"],
        input_text=ordered_script,
        timeout=args.timeout,
    )
    measurements = {}
    for label in ("create", *verify_labels):
        segment = output.split(f"__EAGER_BENCH_BEGIN_{label}__", 1)[1].split(
            f"__EAGER_BENCH_END_{label}__", 1
        )[0]
        timers = TIMER_RE.findall(segment)
        if len(timers) != 1:
            raise RuntimeError(f"Expected one timer for {label}:\n{segment}")
        payload = "\n".join(
            line for line in segment.splitlines() if line.strip() and not TIMER_RE.search(line)
        )
        measurements[label] = {"seconds": float(timers[0]), "output": payload}
    verify_results = [parse_single_csv_row(measurements[label]["output"]) for label in verify_labels]
    if any(result != {"pair_count": "1", "reachable_count": "1"} for result in verify_results):
        raise RuntimeError(f"Unexpected multi-edge self-path results: {verify_results}")
    verify_s = sum(measurements[label]["seconds"] for label in verify_labels)
    reachable_checksum = sum(int(result["reachable_count"]) for result in verify_results)
    row = {
        "suite": "multi_edge",
        "dataset": "synthetic",
        "threads": threads,
        "trial": trial,
        "mode": "eager",
        "vertex_count": vertices,
        "edge_count": edges_per_table * edge_table_count,
        "edges_per_table": edges_per_table,
        "edge_table_count": edge_table_count,
        "create_s": measurements["create"]["seconds"],
        "first_bfs_s": verify_s,
        "warm_bfs_s": "",
        "time_to_first_result_s": measurements["create"]["seconds"] + verify_s,
        "checksum": reachable_checksum,
    }
    add_phase_columns(row, create_prefix, "create")
    add_phase_columns(row, verify_prefix, "first")
    if row["create_partitioned_csr_cache_publish_count"] != edge_table_count:
        raise RuntimeError(
            f"Expected {edge_table_count} eager CSR publishes, got "
            f"{row['create_partitioned_csr_cache_publish_count']}"
        )
    if row["first_partitioned_csr_cache_hit_count"] != edge_table_count:
        raise RuntimeError(
            f"Expected {edge_table_count} first-query cache hits, got "
            f"{row['first_partitioned_csr_cache_hit_count']}"
        )
    return row


def write_rows(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_summary(rows):
    groups = defaultdict(list)
    for row in rows:
        key = (row["suite"], row["dataset"], row["threads"], row["mode"], row["edge_table_count"])
        groups[key].append(row)
    print("\nSummary (median seconds)")
    print("suite,dataset,threads,mode,edge_tables,create_s,first_bfs_s,warm_bfs_s,time_to_first_result_s")
    for key, values in sorted(groups.items()):
        def median(field):
            numbers = [float(value[field]) for value in values if value.get(field) not in ("", None)]
            return statistics.median(numbers) if numbers else float("nan")

        print(
            ",".join(
                [
                    *(str(value) for value in key),
                    f"{median('create_s'):.6f}",
                    f"{median('first_bfs_s'):.6f}",
                    f"{median('warm_bfs_s'):.6f}",
                    f"{median('time_to_first_result_s'):.6f}",
                ]
            )
        )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=("real", "multi-edge", "all"), default="all")
    parser.add_argument("--datasets", nargs="+", default=["sf1", "wiki-Talk", "kgs"])
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 4, 8])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--vertices", type=int, default=100_000)
    parser.add_argument("--edges-per-table", type=int, default=1_000_000)
    parser.add_argument("--edge-tables", nargs="+", type=int, default=[1, 2, 4])
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--extension", type=Path, default=DEFAULT_EXTENSION)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--force-prepare", action="store_true")
    parser.add_argument("--verbose", action="store_true", help="Print complete JSON for every measurement")
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.binary.exists():
        raise SystemExit(f"Missing release DuckDB binary: {args.binary}")
    if not args.extension.exists():
        raise SystemExit(f"Missing release DuckPGQ extension: {args.extension}")
    if args.repeats < 1:
        raise SystemExit("--repeats must be at least 1")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    started = time.strftime("%Y%m%dT%H%M%S", time.localtime())
    if args.suite in ("real", "all"):
        for dataset in args.datasets:
            for threads in args.threads:
                for trial in range(1, args.repeats + 1):
                    # Alternate order to reduce systematic filesystem-cache bias.
                    modes = ("eager", "lazy") if trial % 2 else ("lazy", "eager")
                    trial_rows = []
                    for mode in modes:
                        row = real_trial(args, dataset, threads, trial, mode, args.output_dir)
                        rows.append(row)
                        trial_rows.append(row)
                        if args.verbose:
                            print(json.dumps(row, sort_keys=True))
                        else:
                            print(
                                f"real {dataset} threads={threads} trial={trial} {mode}: "
                                f"create={row['create_s']:.3f}s first={row['first_bfs_s']:.3f}s "
                                f"warm={row['warm_bfs_s']:.3f}s"
                            )
                    result_keys = ("pair_count", "reachable_count", "total_len", "min_len", "max_len")
                    if any(trial_rows[0][key] != trial_rows[1][key] for key in result_keys):
                        raise RuntimeError(f"Eager/lazy BFS mismatch: {trial_rows}")
    if args.suite in ("multi-edge", "all"):
        database = prepare_synthetic_database(
            args, args.output_dir, args.vertices, args.edges_per_table, max(args.edge_tables)
        )
        for edge_table_count in args.edge_tables:
            for threads in args.threads:
                for trial in range(1, args.repeats + 1):
                    row = multi_edge_trial(
                        args,
                        database,
                        args.vertices,
                        args.edges_per_table,
                        edge_table_count,
                        threads,
                        trial,
                        args.output_dir,
                    )
                    rows.append(row)
                    if args.verbose:
                        print(json.dumps(row, sort_keys=True))
                    else:
                        print(
                            f"multi-edge tables={edge_table_count} threads={threads} trial={trial}: "
                            f"create={row['create_s']:.3f}s verify={row['first_bfs_s']:.3f}s"
                        )
    output_path = args.output_dir / f"eager_csr_{started}.csv"
    write_rows(output_path, rows)
    print_summary(rows)
    print(f"\nWrote {len(rows)} measurements to {output_path}")


if __name__ == "__main__":
    main()
