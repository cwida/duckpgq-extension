#!/usr/bin/env python3
"""Compare DuckPGQ eager preparation with Kuzu native graph preparation.

The benchmark reports two deliberately separate boundaries:

* DuckPGQ table ingestion and eager CSR construction are timed independently.
* Kuzu schema, vertex import, and relationship import are timed independently,
  but relationship import also constructs Kuzu's native adjacency structures.

Both systems start from the same Parquet vertex/edge inputs.  Fresh database
files are used for every dataset/thread/trial cell, and the execution order is
alternated between trials to reduce systematic filesystem-cache bias.
"""

import argparse
import csv
import hashlib
import json
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "data" / "ldbc-pathfinding"
DEFAULT_BINARY = REPO_ROOT / "build" / "release" / "duckdb"
DEFAULT_EXTENSION = (
    REPO_ROOT / "build" / "release" / "extension" / "duckpgq" / "duckpgq.duckdb_extension"
)
DEFAULT_OUTPUT = DATA_ROOT / "results" / "eager_csr" / "kuzu_comparison"
TIMER_RE = re.compile(r"Run Time \(s\): real ([0-9.]+)")


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def path_size(path):
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(child.stat().st_size for child in path.rglob("*") if child.is_file())


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_output(*args):
    return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True).strip()


def remove_database(path, work_dir):
    resolved = path.resolve()
    if work_dir.resolve() not in resolved.parents:
        raise RuntimeError(f"Refusing to remove database outside benchmark work directory: {resolved}")
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()
    wal_path = Path(str(path) + ".wal")
    if wal_path.exists():
        wal_path.unlink()


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


def timed_block(label, sql):
    return f"""
.print __PREP_BEGIN_{label}__
.timer on
{sql.strip()}
.timer off
.print __PREP_END_{label}__
"""


def parse_timer(output, label):
    start = f"__PREP_BEGIN_{label}__"
    end = f"__PREP_END_{label}__"
    segment = output.split(start, 1)[1].split(end, 1)[0]
    timers = TIMER_RE.findall(segment)
    if not timers:
        raise RuntimeError(f"Expected at least one timer for {label}:\n{segment}")
    return sum(float(value) for value in timers)


def parse_result(output, label):
    start = f"__PREP_BEGIN_{label}__"
    end = f"__PREP_END_{label}__"
    segment = output.split(start, 1)[1].split(end, 1)[0]
    payload = "\n".join(
        line for line in segment.splitlines() if line.strip() and not TIMER_RE.search(line)
    )
    rows = list(csv.DictReader(payload.splitlines()))
    if len(rows) != 1:
        raise RuntimeError(f"Expected one result row for {label}:\n{segment}")
    return rows[0]


def dataset_spec(name):
    if name == "sf1":
        base = (
            DATA_ROOT
            / "sources"
            / "sf1-parquet"
            / "graphs"
            / "parquet"
            / "bi"
            / "composite-merged-fk"
            / "initial_snapshot"
            / "dynamic"
        )
        return {
            "vertex_path": base / "Person" / "*.parquet",
            "edge_path": base / "Person_knows_Person" / "*.parquet",
            "vertex_column": "id",
            "source_column": "Person1Id",
            "target_column": "Person2Id",
            "directed": True,
            "source_id": 14,
            "expected": (7536, 23014, 0, 7),
        }
    if name in ("wiki-Talk", "kgs"):
        base = DATA_ROOT / "graphalytics" / "sources" / name
        source_id = 2 if name == "wiki-Talk" else 239044
        expected = (2354316, 7931928, 0, 6) if name == "wiki-Talk" else (819249, 2331222, 0, 9)
        return {
            "vertex_path": base / f"{name}-v.parquet",
            "edge_path": base / f"{name}-e.parquet",
            "vertex_column": "id",
            "source_column": "source",
            "target_column": "target",
            "directed": name == "wiki-Talk",
            "source_id": source_id,
            "expected": expected,
        }
    raise ValueError(f"Unsupported dataset: {name}")


def validate_inputs(datasets):
    for dataset in datasets:
        spec = dataset_spec(dataset)
        for key in ("vertex_path", "edge_path"):
            path = spec[key]
            if "*" in str(path):
                if not list(path.parent.glob(path.name)):
                    raise SystemExit(f"Missing input files for {dataset}: {path}")
            elif not path.exists():
                raise SystemExit(f"Missing input file for {dataset}: {path}")


def duckpgq_import_sql(spec):
    vertex_path = sql_string(spec["vertex_path"])
    edge_path = sql_string(spec["edge_path"])
    vertex = spec["vertex_column"]
    source = spec["source_column"]
    target = spec["target_column"]
    edge_select = f"""
SELECT {source}::BIGINT AS person1id, {target}::BIGINT AS person2id
FROM read_parquet({edge_path})
"""
    if not spec["directed"]:
        edge_select += f"""
UNION ALL
SELECT {target}::BIGINT AS person1id, {source}::BIGINT AS person2id
FROM read_parquet({edge_path})
WHERE {source} <> {target}
"""
    return f"""
CREATE TABLE person AS
SELECT {vertex}::BIGINT AS id
FROM read_parquet({vertex_path});

CREATE TABLE person_knows_person AS
{edge_select};

ANALYZE person;
ANALYZE person_knows_person;
"""


def duckpgq_trial(args, dataset, threads, trial, work_dir):
    spec = dataset_spec(dataset)
    database = work_dir / f"duckpgq_{dataset}_{threads}_{trial}.duckdb"
    remove_database(database, work_dir)
    graph_name = "prep_graph"
    script = f"""
.timer off
LOAD {sql_string(args.extension)};
SET threads={threads};
SET experimental_path_finding_operator=true;
SET experimental_build_csr_on_create=true;
SET experimental_persist_csr=false;
"""
    script += timed_block("table_import", duckpgq_import_sql(spec))
    script += timed_block(
        "eager_csr",
        f"""
CREATE PROPERTY GRAPH {graph_name}
VERTEX TABLES (person PROPERTIES (id) LABEL Person)
EDGE TABLES (
    person_knows_person
        SOURCE KEY (person1id) REFERENCES person (id)
        DESTINATION KEY (person2id) REFERENCES person (id)
        LABEL Knows
);
        """,
    )
    bfs_sql = f"""
SELECT count(len)::BIGINT AS reachable_count,
       sum(len)::BIGINT AS total_len,
       min(len)::BIGINT AS min_len,
       max(len)::BIGINT AS max_len
FROM GRAPH_TABLE({graph_name}
    MATCH p = ANY SHORTEST
        (a:Person WHERE a.id = {spec['source_id']})-[k:Knows]->*(b:Person)
    COLUMNS (path_length(p) AS len)
);
"""
    script += timed_block("first_bfs", bfs_sql)
    script += timed_block("warm_bfs", bfs_sql)
    total_start = time.perf_counter()
    output = run_command(
        [str(args.binary), "-unsigned", "-batch", "-csv", str(database)],
        input_text=script,
        timeout=args.timeout,
    )
    table_import_s = parse_timer(output, "table_import")
    eager_csr_s = parse_timer(output, "eager_csr")
    first_bfs_s = parse_timer(output, "first_bfs")
    warm_bfs_s = parse_timer(output, "warm_bfs")
    first_result = parse_result(output, "first_bfs")
    warm_result = parse_result(output, "warm_bfs")
    if first_result != warm_result:
        raise RuntimeError(f"DuckPGQ first/warm BFS mismatch for {dataset}: {first_result} != {warm_result}")
    actual = tuple(int(first_result[key]) for key in ("reachable_count", "total_len", "min_len", "max_len"))
    if actual != spec["expected"]:
        raise RuntimeError(f"DuckPGQ reference mismatch for {dataset}: {actual} != {spec['expected']}")

    validation_start = time.perf_counter()
    validation = run_command(
        [
            str(args.binary),
            "-csv",
            str(database),
            "-c",
            "SELECT (SELECT count(*) FROM person), (SELECT count(*) FROM person_knows_person);",
        ],
        timeout=args.timeout,
    )
    validation_s = time.perf_counter() - validation_start
    counts = next(csv.reader(validation.splitlines()[1:]))
    database_bytes = path_size(database)
    run_wall_s = time.perf_counter() - total_start
    row = {
        "dataset": dataset,
        "threads": threads,
        "trial": trial,
        "system": "duckpgq",
        "schema_s": "",
        "vertex_import_s": "",
        "edge_import_s": "",
        "table_import_s": table_import_s,
        "eager_csr_s": eager_csr_s,
        "system_import_s": table_import_s + eager_csr_s,
        "first_bfs_s": first_bfs_s,
        "warm_bfs_s": warm_bfs_s,
        "time_to_first_result_s": table_import_s + eager_csr_s + first_bfs_s,
        "validation_s": validation_s,
        "total_prepare_s": table_import_s + eager_csr_s + validation_s,
        "run_wall_s": run_wall_s,
        "vertex_count": int(counts[0]),
        "edge_count": int(counts[1]),
        "database_bytes": database_bytes,
        "reachable_count": actual[0],
        "total_len": actual[1],
        "min_len": actual[2],
        "max_len": actual[3],
    }
    remove_database(database, work_dir)
    return row


def kuzu_scalar(conn, query):
    result = conn.execute(query)
    if hasattr(result, "get_next"):
        if not result.has_next():
            raise RuntimeError(f"Kuzu query returned no rows: {query}")
        row = result.get_next()
    else:
        rows = list(result)
        if not rows:
            raise RuntimeError(f"Kuzu query returned no rows: {query}")
        row = rows[0]
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


def kuzu_row(conn, query):
    result = conn.execute(query)
    if hasattr(result, "get_next"):
        if not result.has_next():
            raise RuntimeError(f"Kuzu query returned no rows: {query}")
        return result.get_next()
    rows = list(result)
    if not rows:
        raise RuntimeError(f"Kuzu query returned no rows: {query}")
    return rows[0]


def kuzu_load_path(path):
    # Kuzu accepts a glob string for partitioned Parquet inputs.
    return sql_string(path)


def kuzu_trial(args, kuzu, dataset, threads, trial, work_dir):
    spec = dataset_spec(dataset)
    database = work_dir / f"kuzu_{dataset}_{threads}_{trial}.kuzu"
    remove_database(database, work_dir)
    total_start = time.perf_counter()
    db = kuzu.Database(str(database))
    conn = kuzu.Connection(db, num_threads=threads)
    if hasattr(conn, "set_max_threads_for_exec"):
        conn.set_max_threads_for_exec(threads)

    phase_start = time.perf_counter()
    conn.execute("CREATE NODE TABLE Person(id INT64 PRIMARY KEY)")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)")
    schema_s = time.perf_counter() - phase_start

    phase_start = time.perf_counter()
    conn.execute(
        f"COPY Person FROM (LOAD FROM {kuzu_load_path(spec['vertex_path'])} "
        f"RETURN {spec['vertex_column']} AS id)"
    )
    vertex_import_s = time.perf_counter() - phase_start

    phase_start = time.perf_counter()
    conn.execute(
        f"COPY Knows FROM (LOAD FROM {kuzu_load_path(spec['edge_path'])} "
        f"RETURN {spec['source_column']}, {spec['target_column']})"
    )
    if not spec["directed"]:
        conn.execute(
            f"COPY Knows FROM (LOAD FROM {kuzu_load_path(spec['edge_path'])} "
            f"WHERE {spec['source_column']} <> {spec['target_column']} "
            f"RETURN {spec['target_column']}, {spec['source_column']})"
        )
    edge_import_s = time.perf_counter() - phase_start

    bfs_query = f"""
MATCH (s:Person {{id: {spec['source_id']}}})-[e:Knows* SHORTEST 1..30]->(d:Person)
WHERE d.id <> {spec['source_id']}
RETURN count(d.id), sum(length(e)), min(length(e)), max(length(e))
"""
    bfs_results = []
    bfs_times = []
    for _ in range(2):
        bfs_start = time.perf_counter()
        bfs_results.append(kuzu_row(conn, bfs_query))
        bfs_times.append(time.perf_counter() - bfs_start)
    if bfs_results[0] != bfs_results[1]:
        raise RuntimeError(f"Kuzu first/warm BFS mismatch for {dataset}: {bfs_results}")
    values = list(bfs_results[0].values()) if isinstance(bfs_results[0], dict) else bfs_results[0]
    actual = (int(values[0]) + 1, int(values[1] or 0), 0, int(values[3] or 0))
    if actual != spec["expected"]:
        raise RuntimeError(f"Kuzu reference mismatch for {dataset}: {actual} != {spec['expected']}")

    validation_start = time.perf_counter()
    vertex_count = int(kuzu_scalar(conn, "MATCH (p:Person) RETURN count(p.id)"))
    edge_count = int(kuzu_scalar(conn, "MATCH (:Person)-[e:Knows]->(:Person) RETURN count(e)"))
    validation_s = time.perf_counter() - validation_start
    conn.close()
    db.close()
    database_bytes = path_size(database)
    run_wall_s = time.perf_counter() - total_start
    row = {
        "dataset": dataset,
        "threads": threads,
        "trial": trial,
        "system": "kuzu",
        "schema_s": schema_s,
        "vertex_import_s": vertex_import_s,
        "edge_import_s": edge_import_s,
        "table_import_s": "",
        "eager_csr_s": "",
        "system_import_s": schema_s + vertex_import_s + edge_import_s,
        "first_bfs_s": bfs_times[0],
        "warm_bfs_s": bfs_times[1],
        "time_to_first_result_s": schema_s + vertex_import_s + edge_import_s + bfs_times[0],
        "validation_s": validation_s,
        "total_prepare_s": schema_s + vertex_import_s + edge_import_s + validation_s,
        "run_wall_s": run_wall_s,
        "vertex_count": vertex_count,
        "edge_count": edge_count,
        "database_bytes": database_bytes,
        "reachable_count": actual[0],
        "total_len": actual[1],
        "min_len": actual[2],
        "max_len": actual[3],
    }
    remove_database(database, work_dir)
    return row


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path, rows):
    groups = {}
    for row in rows:
        groups.setdefault((row["dataset"], row["threads"], row["system"]), []).append(row)
    summary = []
    metrics = (
        "schema_s",
        "vertex_import_s",
        "edge_import_s",
        "table_import_s",
        "eager_csr_s",
        "system_import_s",
        "first_bfs_s",
        "warm_bfs_s",
        "time_to_first_result_s",
        "validation_s",
        "total_prepare_s",
        "run_wall_s",
        "database_bytes",
    )
    for (dataset, threads, system), values in sorted(groups.items()):
        row = {"dataset": dataset, "threads": threads, "system": system, "repetitions": len(values)}
        row["vertex_count"] = values[0]["vertex_count"]
        row["edge_count"] = values[0]["edge_count"]
        for metric in metrics:
            numbers = [float(value[metric]) for value in values if value[metric] != ""]
            row[f"{metric}_median"] = statistics.median(numbers) if numbers else ""
            row[f"{metric}_mean"] = statistics.mean(numbers) if numbers else ""
            row[f"{metric}_stdev"] = statistics.stdev(numbers) if len(numbers) > 1 else 0 if numbers else ""
        summary.append(row)
    write_csv(path, summary)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--datasets", nargs="+", default=["sf1", "wiki-Talk", "kgs"])
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 4, 8, 16])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--extension", type=Path, default=DEFAULT_EXTENSION)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--timeout", type=int, default=1800)
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.binary.exists() or not args.extension.exists():
        raise SystemExit("Missing release DuckDB binary or DuckPGQ extension")
    if args.repeats < 1:
        raise SystemExit("--repeats must be at least one")
    validate_inputs(args.datasets)
    try:
        import kuzu
    except ImportError as exc:
        raise SystemExit("The Kuzu Python package is required; install kuzu in the runner environment") from exc

    started = time.strftime("%Y%m%dT%H%M%S", time.localtime())
    run_dir = args.output_dir / started
    work_dir = run_dir / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for dataset in args.datasets:
        for threads in args.threads:
            for trial in range(1, args.repeats + 1):
                systems = ("duckpgq", "kuzu") if trial % 2 else ("kuzu", "duckpgq")
                trial_rows = []
                for system in systems:
                    if system == "duckpgq":
                        row = duckpgq_trial(args, dataset, threads, trial, work_dir)
                    else:
                        row = kuzu_trial(args, kuzu, dataset, threads, trial, work_dir)
                    rows.append(row)
                    trial_rows.append(row)
                    print(
                        f"{dataset} threads={threads} trial={trial} {system}: "
                        f"import={row['system_import_s']:.3f}s total={row['total_prepare_s']:.3f}s"
                    )
                if trial_rows[0]["vertex_count"] != trial_rows[1]["vertex_count"]:
                    raise RuntimeError(f"Vertex-count mismatch: {trial_rows}")
                if trial_rows[0]["edge_count"] != trial_rows[1]["edge_count"]:
                    raise RuntimeError(f"Edge-count mismatch: {trial_rows}")

    raw_path = run_dir / "raw.csv"
    summary_path = run_dir / "summary.csv"
    write_csv(raw_path, rows)
    write_summary(summary_path, rows)
    environment = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "kuzu": getattr(kuzu, "__version__", ""),
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_tracked_status": git_output("status", "--porcelain", "--untracked-files=no").splitlines(),
        "datasets": args.datasets,
        "threads": args.threads,
        "repeats": args.repeats,
        "duckdb_binary": str(args.binary),
        "duckpgq_extension": str(args.extension),
        "duckdb_binary_sha256": file_sha256(args.binary),
        "duckpgq_extension_sha256": file_sha256(args.extension),
        "benchmark_script_sha256": file_sha256(Path(__file__)),
        "timing_boundary": "same Parquet inputs; DuckPGQ table import plus eager CSR versus Kuzu integrated native import",
        "filesystem_cache": "not purged; system order alternated by trial",
    }
    (run_dir / "environment.json").write_text(json.dumps(environment, indent=2) + "\n")
    work_dir.rmdir()
    print(f"Wrote {len(rows)} measurements to {raw_path}")
    print(f"Wrote summary to {summary_path}")


if __name__ == "__main__":
    main()
