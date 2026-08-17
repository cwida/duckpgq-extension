#!/usr/bin/env python3
"""Run the local CSR persistence Load/Cold/Warm validation sweep.

Each measurement uses a private clone of a prepared DuckDB database. One
process creates the property graph and eagerly builds its CSR; a second process
reopens the database and runs the first and warm BFS. Persisted canonical CSRs
are paired with persistence-disabled, thread-tuned transient CSRs.
"""

import argparse
import csv
import hashlib
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import threading
import time
from collections import defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "data" / "ldbc-pathfinding"
DEFAULT_BINARY = REPO_ROOT / "build" / "release" / "duckdb"
DEFAULT_EXTENSION = REPO_ROOT / "build" / "release" / "extension" / "duckpgq" / "duckpgq.duckdb_extension"
DEFAULT_OUTPUT = DATA_ROOT / "results" / "csr_persistence"
TIMER_RE = re.compile(r"Run Time \(s\): real ([0-9.]+)")
PHASES = (
    "partitioned_csr_metadata_lookup",
    "partitioned_csr_deserialize_load",
    "partitioned_csr_rebuild",
    "partitioned_csr_serialize_write",
    "partitioned_csr_cache_hit",
    "partitioned_csr_cache_miss",
    "partitioned_csr_cache_publish",
    "source_group_bfs",
    "csr_build_buffer_manager_peak_delta",
    "csr_build_buffer_manager_swap_peak_delta",
)


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def dataset_path(name):
    if name.startswith("sf"):
        normalized = name[2:].replace(".", "_")
        return DATA_ROOT / "db" / f"ldbc_sf{normalized}.duckdb"
    return DATA_ROOT / "graphalytics" / "db" / f"{name}.duckdb"


def phase_path(prefix):
    return Path(str(prefix) + "_phase_timing.csv")


def timed_block(label, sql):
    return f"""
.print __CSR_PERSISTENCE_BEGIN_{label}__
.timer on
{sql.strip()}
.timer off
.print __CSR_PERSISTENCE_END_{label}__
"""


def monitor_peak_rss(process, stop_event, peak):
    while not stop_event.is_set():
        try:
            sample = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(process.pid)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=False,
            ).stdout.strip()
        except PermissionError:
            return
        if sample:
            try:
                peak[0] = max(peak[0], int(sample.split()[0]) * 1024)
            except ValueError:
                pass
        if process.poll() is not None:
            break
        stop_event.wait(0.01)


def run_command(command, *, input_text=None, timeout=1800, measure_rss=False):
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        text=True,
        stdin=subprocess.PIPE if input_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    stop_event = threading.Event()
    peak = [0]
    monitor = None
    if measure_rss:
        monitor = threading.Thread(target=monitor_peak_rss, args=(process, stop_event, peak), daemon=True)
        monitor.start()
    try:
        output, _ = process.communicate(input=input_text, timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        output, _ = process.communicate()
        raise RuntimeError(f"Command timed out after {timeout}s:\n{output.strip()}")
    finally:
        stop_event.set()
        if monitor is not None:
            monitor.join()
    if process.returncode != 0 or "Error:" in output:
        raise RuntimeError(output.strip())
    return output, peak[0]


def run_query(binary, sql, database=None):
    command = [str(binary), "-csv"]
    if database is not None:
        command.append(str(database))
    command.extend(["-c", sql])
    output, _ = run_command(command)
    return output


def parse_single_csv_row(output):
    rows = list(csv.DictReader(output.splitlines()))
    if len(rows) != 1:
        raise RuntimeError(f"Expected one result row, got {len(rows)}:\n{output}")
    return rows[0]


def parse_timed_output(output, labels):
    result = {}
    for label in labels:
        start = f"__CSR_PERSISTENCE_BEGIN_{label}__"
        end = f"__CSR_PERSISTENCE_END_{label}__"
        if start not in output or end not in output:
            raise RuntimeError(f"Missing timer markers for {label}:\n{output}")
        segment = output.split(start, 1)[1].split(end, 1)[0]
        timers = TIMER_RE.findall(segment)
        if len(timers) != 1:
            raise RuntimeError(f"Expected one timer for {label}:\n{segment}")
        payload = "\n".join(
            line for line in segment.splitlines() if line.strip() and not TIMER_RE.search(line)
        )
        result[label] = {"seconds": float(timers[0]), "output": payload}
    return result


def summarize_phases(prefix):
    summary = defaultdict(lambda: {"count": 0, "time_s": 0.0, "memory_max_bytes": 0, "partitions_max": 0})
    path = phase_path(prefix)
    if not path.exists():
        return summary
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            entry = summary[row["Phase"]]
            entry["count"] += 1
            entry["time_s"] += float(row["Time_ms"]) / 1000.0
            entry["memory_max_bytes"] = max(entry["memory_max_bytes"], int(row["MemoryBytes"]))
            entry["partitions_max"] = max(entry["partitions_max"], int(row["PartitionCount"]))
    return summary


def add_phase_columns(row, prefix, column_prefix):
    phases = summarize_phases(prefix)
    for phase in PHASES:
        entry = phases[phase]
        row[f"{column_prefix}_{phase}_count"] = entry["count"]
        row[f"{column_prefix}_{phase}_s"] = entry["time_s"]
        row[f"{column_prefix}_{phase}_memory_max_bytes"] = entry["memory_max_bytes"]
        row[f"{column_prefix}_{phase}_partitions"] = entry["partitions_max"]


def clear_phase_file(prefix):
    path = phase_path(prefix)
    if path.exists():
        path.unlink()


def dataset_profile(binary, name, path):
    row = parse_single_csv_row(
        run_query(
            binary,
            """
SELECT (SELECT count(*) FROM person) AS vertex_count,
       (SELECT count(*) FROM person_knows_person) AS edge_count,
       (SELECT min(id) FROM person) AS source_id;
""",
            path,
        )
    )
    profile = {key: int(value) for key, value in row.items()}
    if not name.startswith("sf"):
        properties_path = DATA_ROOT / "graphalytics" / "references" / name / f"{name}.properties"
        properties = {}
        for line in properties_path.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                properties[key.strip()] = value.strip()
        profile["source_id"] = int(properties[f"graph.{name}.bfs.source-vertex"])
    return profile


def graphalytics_reference_profile(binary, name):
    reference_path = DATA_ROOT / "graphalytics" / "references" / name / f"{name}-BFS"
    return parse_single_csv_row(
        run_query(
            binary,
            f"""
WITH reference AS (
    SELECT distance
    FROM read_csv({sql_string(reference_path)}, delim=' ', header=false,
                  columns={{'vertex_id': 'BIGINT', 'distance': 'BIGINT'}})
), reachable AS (
    SELECT distance FROM reference WHERE distance <> 9223372036854775807
)
SELECT count(*)::BIGINT AS reachable_count, sum(distance)::BIGINT AS total_len,
       min(distance)::BIGINT AS min_len, max(distance)::BIGINT AS max_len
FROM reachable;
""",
        )
    )


def graph_sql(graph_name):
    return f"""
CREATE PROPERTY GRAPH {graph_name}
VERTEX TABLES (person PROPERTIES (id) LABEL Person)
EDGE TABLES (
    person_knows_person
        SOURCE KEY (person1id) REFERENCES person (id)
        DESTINATION KEY (person2id) REFERENCES person (id)
        LABEL Knows
);
"""


def bfs_sql(graph_name, source_id):
    return f"""
SELECT count(*)::BIGINT AS pair_count, count(len)::BIGINT AS reachable_count,
       sum(len)::BIGINT AS total_len, min(len)::BIGINT AS min_len, max(len)::BIGINT AS max_len
FROM GRAPH_TABLE({graph_name}
    MATCH p = ANY SHORTEST
        (a:Person WHERE a.id = {source_id})-[k:Knows]->*(b:Person)
    COLUMNS (path_length(p) AS len)
);
"""


def clone_database(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    for path in (target, Path(str(target) + ".wal")):
        if path.exists():
            path.unlink()
    clone = subprocess.run(["cp", "-c", str(source), str(target)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if clone.returncode != 0:
        shutil.copy2(source, target)


def prepare_benchmark_base(args, dataset, run_dir):
    source = dataset_path(dataset)
    target = run_dir / "bases" / f"{dataset}.duckdb"
    target.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    sql = f"""
ATTACH {sql_string(target)} AS target (STORAGE_VERSION 'v2.0.0');
ATTACH {sql_string(source)} AS source (READ_ONLY);
CREATE TABLE target.person AS SELECT * FROM source.person;
CREATE TABLE target.person_knows_person AS SELECT * FROM source.person_knows_person;
CHECKPOINT;
"""
    run_query(args.binary, sql)
    elapsed = time.monotonic() - started
    storage = database_storage(args.binary, target)
    return {
        "path": target,
        "source_path": source,
        "preparation_s": elapsed,
        **storage,
    }


def database_storage(binary, database):
    row = parse_single_csv_row(
        run_query(
            binary,
            "SELECT block_size, total_blocks, used_blocks, free_blocks FROM pragma_database_size();",
            database,
        )
    )
    return {
        "file_bytes": database.stat().st_size,
        "block_size": int(row["block_size"]),
        "total_blocks": int(row["total_blocks"]),
        "used_blocks": int(row["used_blocks"]),
        "free_blocks": int(row["free_blocks"]),
        "used_bytes": int(row["block_size"]) * int(row["used_blocks"]),
    }


def benchmark_trial(args, dataset, database, profile, threads, trial, mode, run_dir):
    persisted = mode == "persisted_canonical"
    work_db = run_dir / "work" / f"{dataset}_t{threads}_r{trial}_{mode}.duckdb"
    clone_database(database, work_db)
    graph_name = "csr_persistence_bench"
    stem = run_dir / "metrics" / f"{dataset}_t{threads}_r{trial}_{mode}"
    setup_prefix = Path(str(stem) + "_setup")
    cold_prefix = Path(str(stem) + "_cold")
    warm_prefix = Path(str(stem) + "_warm")
    for prefix in (setup_prefix, cold_prefix, warm_prefix):
        prefix.parent.mkdir(parents=True, exist_ok=True)
        clear_phase_file(prefix)

    common = f"""
LOAD {sql_string(args.extension)};
SET threads={threads};
SET experimental_path_finding_operator=true;
SET experimental_persist_csr={'true' if persisted else 'false'};
SET experimental_path_finding_operator_benchmark=true;
"""
    setup_sql = ".timer off\n" + common
    setup_sql += "SET experimental_build_csr_on_create=true;\n"
    setup_sql += f"SET experimental_path_finding_operator_benchmark_prefix={sql_string(setup_prefix)};\n"
    setup_sql += timed_block("setup", graph_sql(graph_name))
    setup_sql += "CHECKPOINT;\n"
    setup_output, setup_peak_rss = run_command(
        [str(args.binary), "-unsigned", "-batch", "-csv", str(work_db)],
        input_text=setup_sql,
        timeout=args.timeout,
        measure_rss=True,
    )
    setup = parse_timed_output(setup_output, ("setup",))["setup"]
    storage = database_storage(args.binary, work_db)

    query_sql = ".timer off\n" + common
    query_sql += "SET experimental_build_csr_on_create=false;\n"
    query_sql += f"SET experimental_path_finding_operator_benchmark_prefix={sql_string(cold_prefix)};\n"
    query_sql += timed_block("cold", bfs_sql(graph_name, profile["source_id"]))
    query_sql += f"SET experimental_path_finding_operator_benchmark_prefix={sql_string(warm_prefix)};\n"
    query_sql += timed_block("warm", bfs_sql(graph_name, profile["source_id"]))
    query_output, query_peak_rss = run_command(
        [str(args.binary), "-unsigned", "-batch", "-csv", str(work_db)],
        input_text=query_sql,
        timeout=args.timeout,
        measure_rss=True,
    )
    query = parse_timed_output(query_output, ("cold", "warm"))
    cold_result = parse_single_csv_row(query["cold"]["output"])
    warm_result = parse_single_csv_row(query["warm"]["output"])
    if cold_result != warm_result:
        raise RuntimeError(f"Cold/warm mismatch for {dataset}: {cold_result} != {warm_result}")
    if not dataset.startswith("sf"):
        reference = graphalytics_reference_profile(args.binary, dataset)
        actual = {key: cold_result[key] for key in reference}
        if actual != reference:
            raise RuntimeError(f"Reference mismatch for {dataset}: {actual} != {reference}")

    row = {
        "dataset": dataset,
        "threads": threads,
        "trial": trial,
        "mode": mode,
        "vertex_count": profile["vertex_count"],
        "edge_count": profile["edge_count"],
        "source_id": profile["source_id"],
        "setup_total_s": setup["seconds"],
        "cold_total_s": query["cold"]["seconds"],
        "warm_total_s": query["warm"]["seconds"],
        "setup_peak_rss_bytes": setup_peak_rss,
        "query_peak_rss_bytes": query_peak_rss,
        **{f"database_{key}": value for key, value in storage.items()},
        **cold_result,
    }
    add_phase_columns(row, setup_prefix, "setup")
    add_phase_columns(row, cold_prefix, "cold")
    add_phase_columns(row, warm_prefix, "warm")
    persisted_load = row["cold_partitioned_csr_metadata_lookup_s"] + row[
        "cold_partitioned_csr_deserialize_load_s"
    ]
    transient_build = row["cold_partitioned_csr_rebuild_s"]
    row["paper_load_s"] = persisted_load if persisted else transient_build
    row["paper_cold_s"] = max(0.0, row["cold_total_s"] - row["paper_load_s"])
    row["paper_warm_s"] = row["warm_total_s"]
    row["cold_start_edges_per_s"] = profile["edge_count"] / row["cold_total_s"]
    row["warm_edges_per_s"] = profile["edge_count"] / row["warm_total_s"]

    if persisted:
        if row["setup_partitioned_csr_rebuild_count"] != 1 or row[
            "setup_partitioned_csr_serialize_write_count"
        ] != 1:
            raise RuntimeError(f"Persisted eager setup did not rebuild and serialize exactly once: {row}")
        if row["cold_partitioned_csr_deserialize_load_count"] != 1 or row[
            "cold_partitioned_csr_rebuild_count"
        ]:
            raise RuntimeError(f"Persisted cold query did not load exactly one generation: {row}")
    elif row["cold_partitioned_csr_rebuild_count"] != 1:
        raise RuntimeError(f"Transient cold query did not rebuild exactly once: {row}")
    if row["warm_partitioned_csr_cache_hit_count"] != 1:
        raise RuntimeError(f"Warm query did not hit the in-memory CSR exactly once: {row}")

    if not args.keep_work_databases:
        for path in (work_db, Path(str(work_db) + ".wal")):
            if path.exists():
                path.unlink()
    return row


def add_paired_metrics(rows):
    pairs = defaultdict(dict)
    for row in rows:
        pairs[(row["dataset"], row["threads"], row["trial"])][row["mode"]] = row
    for values in pairs.values():
        if set(values) != {"persisted_canonical", "transient_thread_tuned"}:
            continue
        persisted = values["persisted_canonical"]
        transient = values["transient_thread_tuned"]
        csr_disk_bytes = max(0, persisted["database_used_bytes"] - transient["database_used_bytes"])
        for row in values.values():
            row["csr_disk_bytes"] = csr_disk_bytes
            row["csr_disk_bytes_per_edge"] = csr_disk_bytes / row["edge_count"]
            row["disk_amplification_vs_two_int64_endpoints"] = csr_disk_bytes / (row["edge_count"] * 16)


def write_csv(path, rows):
    fieldnames = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["dataset"], row["threads"], row["mode"])].append(row)
    fields = (
        "setup_total_s",
        "paper_load_s",
        "paper_cold_s",
        "cold_total_s",
        "paper_warm_s",
        "setup_peak_rss_bytes",
        "query_peak_rss_bytes",
        "csr_disk_bytes",
        "disk_amplification_vs_two_int64_endpoints",
    )
    result = []
    for key, values in sorted(groups.items()):
        row = {"dataset": key[0], "threads": key[1], "mode": key[2], "repetitions": len(values)}
        for field in fields:
            numbers = [float(value[field]) for value in values]
            mean = statistics.mean(numbers)
            row[f"{field}_median"] = statistics.median(numbers)
            row[f"{field}_mean"] = mean
            row[f"{field}_stdev"] = statistics.stdev(numbers) if len(numbers) > 1 else 0.0
            row[f"{field}_cv"] = row[f"{field}_stdev"] / mean if mean else 0.0
        result.append(row)
    return result


def paper_rows(summary):
    return [
        {
            "dataset": row["dataset"],
            "threads": row["threads"],
            "mode": row["mode"],
            "repetitions": row["repetitions"],
            "load_s": row["paper_load_s_median"],
            "cold_s": row["paper_cold_s_median"],
            "warm_s": row["paper_warm_s_median"],
            "cold_start_s": row["cold_total_s_median"],
            "query_peak_rss_mib": row["query_peak_rss_bytes_median"] / 2**20,
            "csr_disk_mib": row["csr_disk_bytes_median"] / 2**20,
            "disk_amplification": row["disk_amplification_vs_two_int64_endpoints_median"],
            "load_cv": row["paper_load_s_cv"],
            "cold_cv": row["paper_cold_s_cv"],
            "warm_cv": row["paper_warm_s_cv"],
        }
        for row in summary
    ]


def evaluate_guardrails(summary, args):
    groups = defaultdict(dict)
    for row in summary:
        groups[(row["dataset"], row["threads"])][row["mode"]] = row
    checks = []
    for key, modes in sorted(groups.items()):
        if set(modes) != {"persisted_canonical", "transient_thread_tuned"}:
            continue
        persisted = modes["persisted_canonical"]
        transient = modes["transient_thread_tuned"]
        metrics = {
            "load_ratio_vs_rebuild": persisted["paper_load_s_median"] / transient["paper_load_s_median"],
            "warm_regression": persisted["paper_warm_s_median"] / transient["paper_warm_s_median"] - 1,
            "disk_amplification": persisted["disk_amplification_vs_two_int64_endpoints_median"],
        }
        limits = {
            "load_ratio_vs_rebuild": args.max_load_ratio,
            "warm_regression": args.max_warm_regression,
            "disk_amplification": args.max_disk_amplification,
        }
        for metric, value in metrics.items():
            if value is None:
                continue
            checks.append(
                {
                    "dataset": key[0],
                    "threads": key[1],
                    "metric": metric,
                    "value": value,
                    "limit": limits[metric],
                    "passed": value <= limits[metric],
                }
            )
    return checks


def command_output(command):
    return subprocess.run(command, cwd=REPO_ROOT, text=True, stdout=subprocess.PIPE, check=True).stdout.strip()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def environment(args):
    memory_bytes = None
    try:
        memory_bytes = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError):
        pass
    status = subprocess.run(
        ["git", "status", "--short"], cwd=REPO_ROOT, text=True, stdout=subprocess.PIPE, check=True
    ).stdout.splitlines()
    diff = subprocess.run(
        ["git", "diff", "--binary", "HEAD"], cwd=REPO_ROOT, stdout=subprocess.PIPE, check=True
    ).stdout
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpu_count": os.cpu_count(),
        "memory_bytes": memory_bytes,
        "git_commit": command_output(["git", "rev-parse", "HEAD"]),
        "git_status": status,
        "tracked_diff_sha256": hashlib.sha256(diff).hexdigest(),
        "benchmark_script_sha256": sha256(Path(__file__)),
        "binary_sha256": sha256(args.binary),
        "extension_sha256": sha256(args.extension),
        "binary": str(args.binary),
        "extension": str(args.extension),
        "datasets": args.datasets,
        "threads": args.threads,
        "repeats": args.repeats,
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datasets", nargs="+", default=["sf1", "wiki-Talk", "kgs"])
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 4, 8, 16])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--extension", type=Path, default=DEFAULT_EXTENSION)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=1800)
    parser.add_argument("--keep-work-databases", action="store_true")
    parser.add_argument("--max-load-ratio", type=float, default=1.0)
    parser.add_argument("--max-warm-regression", type=float, default=0.15)
    parser.add_argument("--max-disk-amplification", type=float, default=1.0)
    parser.add_argument("--strict-guardrails", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.repeats < 1:
        raise SystemExit("--repeats must be at least 1")
    if not args.binary.exists() or not args.extension.exists():
        raise SystemExit("Release DuckDB binary or DuckPGQ extension is missing")
    if any(thread < 1 for thread in args.threads):
        raise SystemExit("Thread counts must be positive")
    for dataset in args.datasets:
        if not dataset_path(dataset).exists():
            raise SystemExit(f"Missing prepared database for {dataset}: {dataset_path(dataset)}")

    started = time.strftime("%Y%m%dT%H%M%S", time.localtime())
    run_dir = args.output_dir / started
    run_dir.mkdir(parents=True, exist_ok=False)
    profiles = {dataset: dataset_profile(args.binary, dataset, dataset_path(dataset)) for dataset in args.datasets}
    bases = {dataset: prepare_benchmark_base(args, dataset, run_dir) for dataset in args.datasets}
    environment_data = environment(args)
    environment_data["benchmark_bases"] = {
        dataset: {key: str(value) if isinstance(value, Path) else value for key, value in base.items()}
        for dataset, base in bases.items()
    }
    (run_dir / "environment.json").write_text(json.dumps(environment_data, indent=2) + "\n")
    rows = []
    for dataset in args.datasets:
        for threads in args.threads:
            for trial in range(1, args.repeats + 1):
                modes = (
                    ("persisted_canonical", "transient_thread_tuned")
                    if trial % 2
                    else ("transient_thread_tuned", "persisted_canonical")
                )
                for mode in modes:
                    row = benchmark_trial(
                        args, dataset, bases[dataset]["path"], profiles[dataset], threads, trial, mode, run_dir
                    )
                    rows.append(row)
                    print(
                        f"{dataset} threads={threads} trial={trial} {mode}: "
                        f"load={row['paper_load_s']:.4f}s cold={row['paper_cold_s']:.4f}s "
                        f"warm={row['paper_warm_s']:.4f}s rss={row['query_peak_rss_bytes'] / 2**20:.1f}MiB",
                        flush=True,
                    )

    add_paired_metrics(rows)
    raw_path = run_dir / "raw.csv"
    write_csv(raw_path, rows)
    summary = summarize(rows)
    write_csv(run_dir / "summary.csv", summary)
    write_csv(run_dir / "paper.csv", paper_rows(summary))
    checks = evaluate_guardrails(summary, args)
    (run_dir / "guardrails.json").write_text(json.dumps(checks, indent=2) + "\n")
    failures = [check for check in checks if not check["passed"]]
    print(f"Wrote {len(rows)} measurements to {raw_path}")
    print(f"Guardrails: {len(checks) - len(failures)}/{len(checks)} passed")
    for failure in failures:
        print(
            f"WARN {failure['dataset']} threads={failure['threads']} {failure['metric']}="
            f"{failure['value']:.3f} > {failure['limit']:.3f}"
        )
    if failures and args.strict_guardrails:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
