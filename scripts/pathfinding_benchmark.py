#!/usr/bin/env python3
import argparse
import csv
import json
import platform
import re
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request
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
QUERY_PATTERNS = ("point_to_point", "sssp", "all_pairs", "graphalytics_bfs")
GRAPHALYTICS_BFS_UNREACHABLE = 9223372036854775807
GRAPHALYTICS_CORE_DATASETS = ("datagen-8_4-fb", "dota-league", "kgs", "graph500-22", "wiki-Talk", "cit-Patents")
GRAPHALYTICS_SMALL_DATASETS = (
    "wiki-Talk",
    "cit-Patents",
    "kgs",
    "dota-league",
    "datagen-7_5-fb",
    "datagen-7_6-fb",
    "datagen-7_7-zf",
    "datagen-7_8-zf",
    "datagen-7_9-fb",
    "graph500-22",
)
GRAPHALYTICS_MEDIUM_DATASETS = (
    "datagen-8_0-fb",
    "datagen-8_1-fb",
    "datagen-8_2-zf",
    "datagen-8_3-zf",
    "datagen-8_4-fb",
    "graph500-23",
    "graph500-24",
)
GRAPHALYTICS_LARGE_DATASETS = (
    "datagen-8_5-fb",
    "datagen-8_6-fb",
    "datagen-8_7-zf",
    "datagen-8_8-zf",
    "datagen-8_9-fb",
    "graph500-25",
)
GRAPHALYTICS_SMALL_EXTRA_DATASETS = tuple(dataset for dataset in GRAPHALYTICS_SMALL_DATASETS if dataset not in GRAPHALYTICS_CORE_DATASETS)
GRAPHALYTICS_MEDIUM_EXTRA_DATASETS = tuple(dataset for dataset in GRAPHALYTICS_MEDIUM_DATASETS if dataset not in GRAPHALYTICS_CORE_DATASETS)
GRAPHALYTICS_LARGE_EXTRA_DATASETS = tuple(dataset for dataset in GRAPHALYTICS_LARGE_DATASETS if dataset not in GRAPHALYTICS_CORE_DATASETS)
GRAPHALYTICS_DEFAULT_DATASETS = GRAPHALYTICS_CORE_DATASETS
GRAPHALYTICS_DATASET_ALIASES = {
    "cit-patents": "cit-Patents",
    "cti-patents": "cit-Patents",
    "wiki-talk": "wiki-Talk",
    "twitter-mpi": "twitter_mpi",
}
GRAPHALYTICS_DATASETS = {
    "cit-Patents": {"nodes": "3M", "edges": "16M", "scale": "XS", "size": "119.1 MB"},
    "com-friendster": {"nodes": "65M", "edges": "1B", "scale": "XL", "size": "6.7 GB"},
    "datagen-7_5-fb": {"nodes": "633k", "edges": "34M", "scale": "S", "size": "162.3 MB"},
    "datagen-7_6-fb": {"nodes": "754k", "edges": "42M", "scale": "S", "size": "200.0 MB"},
    "datagen-7_7-zf": {"nodes": "13M", "edges": "32M", "scale": "S", "size": "434.5 MB"},
    "datagen-7_8-zf": {"nodes": "16M", "edges": "41M", "scale": "S", "size": "544.3 MB"},
    "datagen-7_9-fb": {"nodes": "1M", "edges": "85M", "scale": "S", "size": "401.2 MB"},
    "datagen-8_0-fb": {"nodes": "1M", "edges": "107M", "scale": "M", "size": "502.5 MB"},
    "datagen-8_1-fb": {"nodes": "2M", "edges": "134M", "scale": "M", "size": "625.4 MB"},
    "datagen-8_2-zf": {"nodes": "43M", "edges": "106M", "scale": "M", "size": "1.4 GB"},
    "datagen-8_3-zf": {"nodes": "53M", "edges": "130M", "scale": "M", "size": "1.7 GB"},
    "datagen-8_4-fb": {"nodes": "3M", "edges": "269M", "scale": "M", "size": "1.2 GB"},
    "datagen-8_5-fb": {"nodes": "4M", "edges": "332M", "scale": "L", "size": "1.5 GB"},
    "datagen-8_6-fb": {"nodes": "5M", "edges": "421M", "scale": "L", "size": "1.9 GB"},
    "datagen-8_7-zf": {"nodes": "145M", "edges": "340M", "scale": "L", "size": "4.6 GB"},
    "datagen-8_8-zf": {"nodes": "168M", "edges": "413M", "scale": "L", "size": "5.3 GB"},
    "datagen-8_9-fb": {"nodes": "10M", "edges": "848M", "scale": "L", "size": "3.7 GB"},
    "datagen-9_0-fb": {"nodes": "12M", "edges": "1B", "scale": "XL", "size": "4.6 GB"},
    "datagen-9_1-fb": {"nodes": "16M", "edges": "1B", "scale": "XL", "size": "5.8 GB"},
    "datagen-9_2-zf": {"nodes": "434M", "edges": "1B", "scale": "XL", "size": "13.7 GB"},
    "datagen-9_3-zf": {"nodes": "555M", "edges": "1B", "scale": "XL", "size": "17.4 GB"},
    "datagen-9_4-fb": {"nodes": "29M", "edges": "2B", "scale": "XL", "size": "14.0 GB"},
    "datagen-sf3k-fb": {"nodes": "33M", "edges": "2B", "scale": "XL", "size": "12.7 GB"},
    "datagen-sf10k-fb": {"nodes": "100M", "edges": "9B", "scale": "2XL", "size": "40.5 GB"},
    "dota-league": {"nodes": "61k", "edges": "50M", "scale": "S", "size": "114.3 MB"},
    "example-directed": {"nodes": "10", "edges": "17", "scale": "-", "size": "1.0 KB"},
    "example-undirected": {"nodes": "9", "edges": "12", "scale": "-", "size": "1.0 KB"},
    "graph500-22": {"nodes": "2M", "edges": "64M", "scale": "S", "size": "202.4 MB"},
    "graph500-23": {"nodes": "4M", "edges": "129M", "scale": "M", "size": "410.6 MB"},
    "graph500-24": {"nodes": "8M", "edges": "260M", "scale": "M", "size": "847.7 MB"},
    "graph500-25": {"nodes": "17M", "edges": "523M", "scale": "L", "size": "1.7 GB"},
    "graph500-26": {"nodes": "32M", "edges": "1B", "scale": "XL", "size": "3.4 GB"},
    "graph500-27": {"nodes": "63M", "edges": "2B", "scale": "XL", "size": "7.1 GB"},
    "graph500-28": {"nodes": "121M", "edges": "4B", "scale": "2XL", "size": "14.4 GB"},
    "graph500-29": {"nodes": "232M", "edges": "8B", "scale": "2XL", "size": "29.6 GB"},
    "graph500-30": {"nodes": "447M", "edges": "17B", "scale": "3XL", "size": "60.8 GB"},
    "kgs": {"nodes": "832k", "edges": "17M", "scale": "XS", "size": "65.7 MB"},
    "test-bfs-directed": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-bfs-undirected": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-cdlp-directed": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-cdlp-undirected": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-lcc-directed": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-lcc-undirected": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-pr-directed": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-pr-undirected": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-sssp-directed": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-sssp-undirected": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-wcc-directed": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "test-wcc-undirected": {"nodes": "<100", "edges": "<100", "scale": "-", "size": "<2.0 KB"},
    "twitter_mpi": {"nodes": "52M", "edges": "1B", "scale": "XL", "size": "5.7 GB"},
    "wiki-Talk": {"nodes": "2M", "edges": "5M", "scale": "2XS", "size": "34.9 MB"},
}
GRAPHALYTICS_DATASET_GROUPS = {
    "core": GRAPHALYTICS_CORE_DATASETS,
    "all": GRAPHALYTICS_CORE_DATASETS,
    "small": GRAPHALYTICS_SMALL_DATASETS,
    "small-extra": GRAPHALYTICS_SMALL_EXTRA_DATASETS,
    "medium": GRAPHALYTICS_MEDIUM_DATASETS,
    "medium-extra": GRAPHALYTICS_MEDIUM_EXTRA_DATASETS,
    "large": GRAPHALYTICS_LARGE_DATASETS,
    "large-extra": GRAPHALYTICS_LARGE_EXTRA_DATASETS,
    "small-medium": GRAPHALYTICS_SMALL_DATASETS + GRAPHALYTICS_MEDIUM_DATASETS,
    "medium-large": GRAPHALYTICS_MEDIUM_DATASETS + GRAPHALYTICS_LARGE_DATASETS,
    "small-medium-large": GRAPHALYTICS_SMALL_DATASETS + GRAPHALYTICS_MEDIUM_DATASETS + GRAPHALYTICS_LARGE_DATASETS,
    "all-known": tuple(GRAPHALYTICS_DATASETS.keys()),
}
GRAPHALYTICS_PARQUET_BASE_URL = "https://datasets.ldbcouncil.org/graphalytics-parquet"
DEFAULT_SYSTEM_NAME = "duckpgq"
SYSTEMS = ("duckpgq", "kuzu")
RUN_METADATA_FIELDS = [
    "benchmark_run_id",
    "benchmark_started_at",
    "benchmark_system",
    "benchmark_profile",
    "benchmark_run_label",
    "benchmark_notes",
    "repo_commit",
    "repo_branch",
    "repo_dirty",
    "host_platform",
    "python_version",
    "duckdb_binary",
    "duckpgq_extension",
]
DATASET_METADATA_FIELDS = [
    "dataset_metadata_dataset_kind",
    "dataset_metadata_scale_factor",
    "dataset_metadata_dataset",
    "dataset_metadata_graphalytics_nodes",
    "dataset_metadata_graphalytics_edges",
    "dataset_metadata_graphalytics_scale",
    "dataset_metadata_graphalytics_package_size",
    "dataset_metadata_graphalytics_directed",
    "dataset_metadata_person_rows",
    "dataset_metadata_person_knows_person_rows",
    "dataset_metadata_pair_table",
    "dataset_metadata_pair_rows",
]


@dataclass(frozen=True)
class BenchmarkOptions:
    attached_db: Path
    query_pattern: str
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
    source_group_ratio: int


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


def require_kuzu():
    try:
        import kuzu
    except ImportError as exc:
        raise SystemExit(
            "The Kuzu Python package is required for --system kuzu. "
            "Install it with `.venv/bin/python -m pip install kuzu` or `python3 -m pip install kuzu`."
        ) from exc
    return kuzu


def command_output(cmd):
    result = subprocess.run(cmd, cwd=REPO_ROOT, text=True, capture_output=True)
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def git_metadata():
    status = command_output(["git", "status", "--porcelain"])
    return {
        "repo_commit": command_output(["git", "rev-parse", "HEAD"]),
        "repo_branch": command_output(["git", "branch", "--show-current"]),
        "repo_dirty": int(bool(status)),
    }


def benchmark_run_metadata(args, started_at, run_id):
    metadata = {
        "benchmark_run_id": run_id,
        "benchmark_started_at": started_at,
        "benchmark_system": args.system_name,
        "benchmark_profile": args.benchmark_profile,
        "benchmark_run_label": args.run_label,
        "benchmark_notes": args.notes,
        "host_platform": platform.platform(),
        "python_version": platform.python_version(),
        "duckdb_binary": str(BENCH_DUCKDB) if getattr(args, "system", "duckpgq") == "duckpgq" else "",
        "duckpgq_extension": str(DUCKPGQ_EXTENSION) if getattr(args, "system", "duckpgq") == "duckpgq" else "",
    }
    metadata.update(git_metadata())
    return metadata


def read_benchmark_metadata(attached_db):
    sql = """
SELECT key, value
FROM benchmark_metadata
ORDER BY key;
"""
    output, _ = run_duckdb(BENCH_DUCKDB, attached_db, sql, quiet=True)
    metadata = {}
    for row in csv.DictReader(output.splitlines()):
        metadata[f"dataset_metadata_{row['key']}"] = row["value"]
    return metadata


def read_kuzu_benchmark_metadata(dataset):
    metadata_path = graphalytics_kuzu_metadata_path(dataset)
    if not metadata_path.exists():
        raise SystemExit(
            f"Missing Kuzu benchmark metadata for {dataset}: {metadata_path}. "
            f"Run prepare --system kuzu --graphalytics-datasets {dataset} first."
        )
    raw_metadata = json.loads(metadata_path.read_text())
    return {f"dataset_metadata_{key}": value for key, value in raw_metadata.items()}


def sf_name(scale_factor):
    return f"sf{str(scale_factor).replace('.', '_')}"


def graphalytics_name(dataset):
    return GRAPHALYTICS_DATASET_ALIASES.get(dataset, dataset)


def expand_graphalytics_datasets(datasets):
    expanded = []
    seen = set()
    for dataset in datasets:
        key = dataset.lower()
        values = GRAPHALYTICS_DATASET_GROUPS.get(key)
        if values is None:
            values = (graphalytics_name(dataset),)
        for value in values:
            canonical = graphalytics_name(value)
            if canonical not in GRAPHALYTICS_DATASETS:
                groups = ", ".join(sorted(GRAPHALYTICS_DATASET_GROUPS))
                known = ", ".join(GRAPHALYTICS_DATASETS)
                raise SystemExit(
                    f"Unsupported Graphalytics dataset or group: {value}. "
                    f"Known groups: {groups}. Known datasets: {known}"
                )
            if canonical not in seen:
                expanded.append(canonical)
                seen.add(canonical)
    return expanded


def graphalytics_label(dataset):
    return f"graphalytics_{graphalytics_name(dataset).replace('-', '_').replace('.', '_')}"


def source_dir(scale_factor):
    return DATA_ROOT / "sources" / f"{sf_name(scale_factor)}-parquet"


def db_path(scale_factor):
    return DATA_ROOT / "db" / f"ldbc_{sf_name(scale_factor)}.duckdb"


def graphalytics_source_dir(dataset):
    return DATA_ROOT / "graphalytics" / "sources" / graphalytics_name(dataset)


def graphalytics_reference_dir(dataset):
    return DATA_ROOT / "graphalytics" / "references" / graphalytics_name(dataset)


def graphalytics_db_path(dataset):
    return DATA_ROOT / "graphalytics" / "db" / f"{graphalytics_name(dataset)}.duckdb"


def graphalytics_kuzu_db_path(dataset):
    return DATA_ROOT / "systems" / "kuzu" / "graphalytics" / f"{graphalytics_name(dataset)}.kuzu"


def graphalytics_kuzu_metadata_path(dataset):
    return graphalytics_kuzu_db_path(dataset).with_suffix(".metadata.json")


def graphalytics_parquet_url(dataset, kind):
    canonical = graphalytics_name(dataset)
    return f"{GRAPHALYTICS_PARQUET_BASE_URL}/{canonical}-{kind}.parquet"


def graphalytics_package_url(dataset):
    return f"https://datasets.ldbcouncil.org/graphalytics/{graphalytics_name(dataset)}.tar.zst"


def graphalytics_properties_path(dataset):
    canonical = graphalytics_name(dataset)
    return graphalytics_reference_dir(canonical) / f"{canonical}.properties"


def graphalytics_bfs_reference_path(dataset):
    canonical = graphalytics_name(dataset)
    return graphalytics_reference_dir(canonical) / f"{canonical}-BFS"


def graphalytics_bfs_pair_table_name():
    return "benchmark_pairs_graphalytics_bfs"


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


def download_graphalytics_file(dataset, kind, force):
    canonical = graphalytics_name(dataset)
    if canonical not in GRAPHALYTICS_DATASETS:
        known = ", ".join(GRAPHALYTICS_DEFAULT_DATASETS)
        raise SystemExit(f"Unsupported Graphalytics dataset: {dataset}. Initial supported set: {known}")

    out_dir = graphalytics_source_dir(canonical)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{canonical}-{kind}.parquet"
    if out_path.exists() and not force:
        print(f"Graphalytics {canonical} {kind}.parquet already exists: {out_path}")
        return out_path

    url = graphalytics_parquet_url(canonical, kind)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    print(f"Downloading {url}")
    start = time.perf_counter()
    request = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
    with urllib.request.urlopen(request) as response, tmp_path.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
    tmp_path.replace(out_path)
    elapsed = time.perf_counter() - start
    print(f"Downloaded {out_path} in {elapsed:.2f}s")
    return out_path


def download_graphalytics_dataset(dataset, force):
    vertex_path = download_graphalytics_file(dataset, "v", force)
    edge_path = download_graphalytics_file(dataset, "e", force)
    return vertex_path, edge_path


def download_url_to_file(url, out_path):
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    request = urllib.request.Request(url, headers={"User-Agent": "curl/8.0"})
    with urllib.request.urlopen(request) as response, tmp_path.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
    tmp_path.replace(out_path)


def extract_graphalytics_archive_member(archive_path, member_name, out_path):
    result = subprocess.run(
        ["tar", "--zstd", "-xOf", str(archive_path), member_name],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    out_path.write_text(result.stdout)


def download_graphalytics_reference_files(dataset, force):
    canonical = graphalytics_name(dataset)
    if canonical not in GRAPHALYTICS_DATASETS:
        known = ", ".join(GRAPHALYTICS_DEFAULT_DATASETS)
        raise SystemExit(f"Unsupported Graphalytics dataset: {dataset}. Initial supported set: {known}")

    ref_dir = graphalytics_reference_dir(canonical)
    ref_dir.mkdir(parents=True, exist_ok=True)
    properties_path = graphalytics_properties_path(canonical)
    bfs_path = graphalytics_bfs_reference_path(canonical)
    if properties_path.exists() and bfs_path.exists() and not force:
        print(f"Graphalytics {canonical} properties/BFS reference already exist: {ref_dir}")
        return properties_path, bfs_path

    package_path = ref_dir / f"{canonical}.tar.zst"
    url = graphalytics_package_url(canonical)
    print(f"Downloading Graphalytics reference package {url}")
    start = time.perf_counter()
    download_url_to_file(url, package_path)
    elapsed = time.perf_counter() - start
    print(f"Downloaded reference package for {canonical} in {elapsed:.2f}s")

    extract_graphalytics_archive_member(package_path, f"{canonical}.properties", properties_path)
    extract_graphalytics_archive_member(package_path, f"{canonical}-BFS", bfs_path)
    package_path.unlink()
    return properties_path, bfs_path


def read_graphalytics_properties(dataset):
    canonical = graphalytics_name(dataset)
    properties_path = graphalytics_properties_path(canonical)
    if not properties_path.exists():
        raise SystemExit(
            f"Missing Graphalytics properties for {canonical}: {properties_path}. "
            f"Run prepare --graphalytics-datasets {canonical} first."
        )

    properties = {}
    for line in properties_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        properties[key.strip()] = value.strip()
    return properties


def graphalytics_bfs_source_vertex(dataset):
    canonical = graphalytics_name(dataset)
    properties = read_graphalytics_properties(canonical)
    key = f"graph.{canonical}.bfs.source-vertex"
    if key not in properties:
        raise SystemExit(f"Graphalytics BFS source vertex not found in {graphalytics_properties_path(canonical)}")
    return int(properties[key])


def graphalytics_is_directed(dataset):
    canonical = graphalytics_name(dataset)
    properties = read_graphalytics_properties(canonical)
    key = f"graph.{canonical}.directed"
    if key not in properties:
        raise SystemExit(f"Graphalytics directed flag not found in {graphalytics_properties_path(canonical)}")
    return properties[key].lower() == "true"


def graphalytics_has_weight_property(dataset):
    canonical = graphalytics_name(dataset)
    properties = read_graphalytics_properties(canonical)
    return "weight" in properties.get(f"graph.{canonical}.edge-properties.names", "").split(",")


def materialize_graphalytics_database(dataset, pair_count, force):
    canonical = graphalytics_name(dataset)
    stats = GRAPHALYTICS_DATASETS.get(canonical)
    if stats is None:
        known = ", ".join(GRAPHALYTICS_DEFAULT_DATASETS)
        raise SystemExit(f"Unsupported Graphalytics dataset: {dataset}. Initial supported set: {known}")

    out_db = graphalytics_db_path(canonical)
    if out_db.exists() and not force:
        print(f"Graphalytics {canonical} DuckDB database already exists: {out_db}")
        ensure_pair_table_for_db(out_db, graphalytics_label(canonical), pair_count)
        return
    if out_db.exists():
        out_db.unlink()

    vertex_path, edge_path = download_graphalytics_dataset(canonical, force=False)
    out_db.parent.mkdir(parents=True, exist_ok=True)
    pair_table = pair_table_name(pair_count, "random")
    directed = graphalytics_is_directed(canonical)
    weight_expr = "weight::DOUBLE" if graphalytics_has_weight_property(canonical) else "1.0::DOUBLE"
    edge_select_sql = f"""
SELECT source::BIGINT AS person1id, target::BIGINT AS person2id, {weight_expr} AS weight
FROM read_parquet({sql_string(edge_path)})
"""
    if not directed:
        edge_select_sql = f"""
SELECT source::BIGINT AS person1id, target::BIGINT AS person2id, {weight_expr} AS weight
FROM read_parquet({sql_string(edge_path)})
UNION ALL
SELECT target::BIGINT AS person1id, source::BIGINT AS person2id, {weight_expr} AS weight
FROM read_parquet({sql_string(edge_path)})
WHERE source <> target
"""

    sql = f"""
CREATE TABLE person AS
SELECT id::BIGINT AS id
FROM read_parquet({sql_string(vertex_path)});

CREATE TABLE person_knows_person AS
{edge_select_sql};

{pair_table_sql(pair_count)}

CREATE TABLE benchmark_metadata AS
SELECT *
FROM (
    VALUES
        ('dataset_kind', 'graphalytics'),
        ('dataset', {sql_string(canonical)}),
        ('graphalytics_nodes', {sql_string(stats["nodes"])}),
        ('graphalytics_edges', {sql_string(stats["edges"])}),
        ('graphalytics_scale', {sql_string(stats["scale"])}),
        ('graphalytics_package_size', {sql_string(stats["size"])}),
        ('graphalytics_directed', {sql_string(str(directed).lower())}),
        ('person_rows', (SELECT count(*)::VARCHAR FROM person)),
        ('person_knows_person_rows', (SELECT count(*)::VARCHAR FROM person_knows_person)),
        ('pair_table', {sql_string(pair_table)}),
        ('pair_rows', (SELECT count(*)::VARCHAR FROM {pair_table}))
) AS metadata(key, value);

ANALYZE;
"""
    print(f"Materializing Graphalytics {canonical} benchmark database at {out_db}")
    _, elapsed = run_duckdb(BENCH_DUCKDB, out_db, sql)
    print(f"Materialized Graphalytics {canonical} database in {elapsed:.2f}s")


def kuzu_query_single_row(conn, query):
    result = conn.execute(query)
    if hasattr(result, "get_next"):
        if not result.has_next():
            raise RuntimeError(f"Kuzu query returned no rows: {query}")
        return result.get_next()
    rows = list(result)
    if not rows:
        raise RuntimeError(f"Kuzu query returned no rows: {query}")
    return rows[0]


def kuzu_row_get(row, index, default=None):
    if isinstance(row, dict):
        return list(row.values())[index] if index < len(row) else default
    return row[index] if index < len(row) else default


def materialize_graphalytics_kuzu_database(dataset, force):
    kuzu = require_kuzu()
    canonical = graphalytics_name(dataset)
    stats = GRAPHALYTICS_DATASETS.get(canonical)
    if stats is None:
        known = ", ".join(GRAPHALYTICS_DEFAULT_DATASETS)
        raise SystemExit(f"Unsupported Graphalytics dataset: {dataset}. Initial supported set: {known}")

    out_db = graphalytics_kuzu_db_path(canonical)
    metadata_path = graphalytics_kuzu_metadata_path(canonical)
    if out_db.exists() and not force:
        print(f"Graphalytics {canonical} Kuzu database already exists: {out_db}")
        return
    if out_db.exists():
        if out_db.is_dir():
            shutil.rmtree(out_db)
        else:
            out_db.unlink()

    vertex_path, edge_path = download_graphalytics_dataset(canonical, force=False)
    out_db.parent.mkdir(parents=True, exist_ok=True)
    directed = graphalytics_is_directed(canonical)

    print(f"Materializing Graphalytics {canonical} Kuzu database at {out_db}")
    start = time.perf_counter()
    db = kuzu.Database(str(out_db))
    conn = kuzu.Connection(db)
    conn.execute("CREATE NODE TABLE Person(id INT64 PRIMARY KEY)")
    conn.execute("CREATE REL TABLE Knows(FROM Person TO Person)")
    conn.execute(f"COPY Person FROM (LOAD FROM {sql_string(vertex_path)} RETURN id)")
    conn.execute(f"COPY Knows FROM (LOAD FROM {sql_string(edge_path)} RETURN source, target)")
    if not directed:
        conn.execute(
            f"""
            COPY Knows FROM (
                LOAD FROM {sql_string(edge_path)}
                WHERE source <> target
                RETURN target, source
            )
            """
        )

    person_rows = int(kuzu_row_get(kuzu_query_single_row(conn, "MATCH (p:Person) RETURN count(p.id)"), 0))
    edge_rows = int(kuzu_row_get(kuzu_query_single_row(conn, "MATCH (:Person)-[e:Knows]->(:Person) RETURN count(e)"), 0))
    metadata = {
        "dataset_kind": "graphalytics",
        "dataset": canonical,
        "graphalytics_nodes": stats["nodes"],
        "graphalytics_edges": stats["edges"],
        "graphalytics_scale": stats["scale"],
        "graphalytics_package_size": stats["size"],
        "graphalytics_directed": str(directed).lower(),
        "person_rows": str(person_rows),
        "person_knows_person_rows": str(edge_rows),
        "pair_table": graphalytics_bfs_pair_table_name(),
        "pair_rows": str(person_rows),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    elapsed = time.perf_counter() - start
    print(f"Materialized Graphalytics {canonical} Kuzu database in {elapsed:.2f}s")


def pair_table_sql(pair_count):
    return pair_table_sql_for_shape(pair_count, "random")


def pair_table_name(pair_count, pair_shape, source_count=None, target_count=None):
    if pair_shape == "random":
        return f"benchmark_pairs_{pair_count}"
    if pair_shape == "same_dst":
        return f"benchmark_pairs_same_dst_{pair_count}"
    if pair_shape == "same_src":
        return f"benchmark_pairs_same_src_{pair_count}"
    if pair_shape == "all_pairs":
        if source_count is None or target_count is None:
            raise ValueError("all_pairs requires source_count and target_count")
        return f"benchmark_pairs_all_pairs_s{source_count}_t{target_count}"
    raise ValueError(f"Unsupported pair shape: {pair_shape}")


def generated_pair_shape(query_pattern, pair_shape):
    if query_pattern == "point_to_point":
        return pair_shape
    if query_pattern == "sssp":
        return "same_src"
    if query_pattern == "all_pairs":
        return "all_pairs"
    if query_pattern == "graphalytics_bfs":
        return "graphalytics_bfs"
    raise ValueError(f"Unsupported query pattern: {query_pattern}")


def pair_table_sql_for_shape(pair_count, pair_shape, source_count=None, target_count=None):
    if pair_shape == "random":
        return f"""
CREATE OR REPLACE TABLE {pair_table_name(pair_count, pair_shape, source_count, target_count)} AS
WITH rows AS (
    SELECT range::BIGINT AS rn
    FROM range({pair_count})
),
n AS (
    SELECT count(*)::BIGINT AS vertex_count
    FROM person
)
SELECT (rn % vertex_count)::BIGINT AS src,
       ((rn * 104729 + 15485863) % vertex_count)::BIGINT AS dst
FROM rows, n;
"""
    if pair_shape == "same_dst":
        return f"""
CREATE OR REPLACE TABLE {pair_table_name(pair_count, pair_shape, source_count, target_count)} AS
WITH rows AS (
    SELECT range::BIGINT AS rn
    FROM range({pair_count})
),
n AS (
    SELECT count(*)::BIGINT AS vertex_count
    FROM person
)
SELECT (rn % vertex_count)::BIGINT AS src,
       (15485863 % vertex_count)::BIGINT AS dst
FROM rows, n;
"""
    if pair_shape == "same_src":
        return f"""
CREATE OR REPLACE TABLE {pair_table_name(pair_count, pair_shape, source_count, target_count)} AS
WITH rows AS (
    SELECT range::BIGINT AS rn
    FROM range({pair_count})
),
n AS (
    SELECT count(*)::BIGINT AS vertex_count
    FROM person
)
SELECT 0::BIGINT AS src, ((rn * 104729 + 15485863) % vertex_count)::BIGINT AS dst
FROM rows, n;
"""
    if pair_shape == "all_pairs":
        if source_count is None:
            source_count = pair_count
        if target_count is None:
            target_count = pair_count
        return f"""
CREATE OR REPLACE TABLE {pair_table_name(pair_count, pair_shape, source_count, target_count)} AS
WITH srcs AS (
    SELECT range::BIGINT AS src_rn
    FROM range({source_count})
),
dsts AS (
    SELECT range::BIGINT AS dst_rn
    FROM range({target_count})
),
n AS (
    SELECT count(*)::BIGINT AS vertex_count
    FROM person
)
SELECT (src_rn % vertex_count)::BIGINT AS src,
       (dst_rn % vertex_count)::BIGINT AS dst
FROM srcs, dsts, n;
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


def ensure_pair_table_for_db(out_db, label, pair_count, pair_shape="random", source_count=None, target_count=None):
    if not out_db.exists():
        return

    table_name = pair_table_name(pair_count, pair_shape, source_count, target_count)
    sql = f"""
{pair_table_sql_for_shape(pair_count, pair_shape, source_count, target_count)}
ANALYZE {table_name};
"""
    print(f"Ensuring {label} {table_name} exists")
    _, elapsed = run_duckdb(BENCH_DUCKDB, out_db, sql)
    print(f"Prepared {table_name} for {label} in {elapsed:.2f}s")


def ensure_pair_table(scale_factor, pair_count, pair_shape="random", source_count=None, target_count=None):
    ensure_pair_table_for_db(db_path(scale_factor), sf_name(scale_factor), pair_count, pair_shape, source_count, target_count)


def ensure_graphalytics_bfs_pair_table(attached_db, dataset):
    canonical = graphalytics_name(dataset)
    table_name = graphalytics_bfs_pair_table_name()
    source_vertex = graphalytics_bfs_source_vertex(canonical)
    sql = f"""
CREATE OR REPLACE TABLE {table_name} AS
WITH source_vertex AS (
    SELECT rowid::BIGINT AS src
    FROM person
    WHERE id = {source_vertex}
)
SELECT source_vertex.src, person.rowid::BIGINT AS dst
FROM source_vertex, person;
ANALYZE {table_name};
"""
    print(f"Ensuring {graphalytics_label(canonical)} {table_name} exists for BFS source vertex {source_vertex}")
    _, elapsed = run_duckdb(BENCH_DUCKDB, attached_db, sql)
    print(f"Prepared {table_name} for {graphalytics_label(canonical)} in {elapsed:.2f}s")
    return table_name, source_vertex


def graphalytics_bfs_reference_profile(dataset):
    canonical = graphalytics_name(dataset)
    reference_path = graphalytics_bfs_reference_path(canonical)
    if not reference_path.exists():
        raise SystemExit(
            f"Missing Graphalytics BFS reference for {canonical}: {reference_path}. "
            f"Run prepare --graphalytics-datasets {canonical} first."
        )
    sql = f"""
WITH reference AS (
    SELECT vertex_id, distance
    FROM read_csv(
        {sql_string(reference_path)},
        delim = ' ',
        header = false,
        columns = {{'vertex_id': 'BIGINT', 'distance': 'BIGINT'}}
    )
),
reachable AS (
    SELECT distance
    FROM reference
    WHERE distance <> {GRAPHALYTICS_BFS_UNREACHABLE}
)
SELECT (SELECT count(*)::BIGINT FROM reference) AS pair_count,
       (SELECT count(*)::BIGINT FROM reachable) AS reachable_count,
       (SELECT sum(distance)::BIGINT FROM reachable) AS total_len,
       (SELECT min(distance)::BIGINT FROM reachable) AS min_len,
       (SELECT max(distance)::BIGINT FROM reachable) AS max_len;
"""
    output, _ = run_duckdb(BENCH_DUCKDB, None, sql, quiet=True)
    rows = list(csv.DictReader(output.splitlines()))
    if len(rows) != 1:
        raise RuntimeError(f"Expected one Graphalytics BFS reference profile row for {canonical}, got: {output}")
    return rows[0]


def read_pair_profile(attached_db, pair_table):
    output, _ = run_duckdb(BENCH_DUCKDB, attached_db, pair_profile_sql(pair_table), quiet=True)
    rows = list(csv.DictReader(output.splitlines()))
    if len(rows) != 1:
        raise RuntimeError(f"Expected one pair profile row for {pair_table}, got: {output}")
    return rows[0]


def prepare(args):
    if args.system != "duckpgq" and not args.graphalytics_datasets:
        raise SystemExit(f"prepare --system {args.system} currently supports only --graphalytics-datasets")

    if args.graphalytics_datasets:
        datasets = expand_graphalytics_datasets(args.graphalytics_datasets)
        for dataset in datasets:
            canonical = graphalytics_name(dataset)
            download_graphalytics_dataset(canonical, args.force)
            download_graphalytics_reference_files(canonical, args.force)
            if args.system == "duckpgq":
                materialize_graphalytics_database(canonical, args.pairs, args.force or args.force_materialize)
            elif args.system == "kuzu":
                materialize_graphalytics_kuzu_database(canonical, args.force or args.force_materialize)
            else:
                raise SystemExit(f"Unsupported benchmark system: {args.system}")
        return

    for scale_factor in args.scale_factors:
        generate_parquet(scale_factor, args.threads, args.force)
        materialize_database(scale_factor, args.pairs, args.force)


def benchmark_target(args):
    dataset = getattr(args, "dataset", None)
    if dataset:
        canonical = graphalytics_name(dataset)
        if canonical not in GRAPHALYTICS_DATASETS:
            known = ", ".join(GRAPHALYTICS_DEFAULT_DATASETS)
            raise SystemExit(f"Unsupported Graphalytics dataset: {dataset}. Initial supported set: {known}")
        return canonical, graphalytics_label(canonical), graphalytics_db_path(canonical)

    if not args.scale_factor:
        raise SystemExit("Missing --scale-factor for LDBC run, or pass --dataset for a Graphalytics run.")
    return args.scale_factor, sf_name(args.scale_factor), db_path(args.scale_factor)


def csr_cte(schema_prefix, compact=False, validated_edges=False):
    person = f"{schema_prefix}.person"
    knows = f"{schema_prefix}.person_knows_person"
    edge_function = "create_compact_csr_edge" if compact else "create_csr_edge"
    edge_id_argument = "" if compact else ",\n        k.rowid"
    edge_count = (
        f"(SELECT count() FROM {knows})"
        if validated_edges
        else f"(SELECT count() FROM {knows} k JOIN {person} a ON a.id = k.person1id "
        f"JOIN {person} c ON c.id = k.person2id)"
    )
    return f"""
WITH csr_cte AS (
    SELECT cast(min({edge_function}(
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
        {edge_count},
        a.rowid,
        c.rowid{edge_id_argument}
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
SET experimental_path_finding_operator_source_group_ratio={options.source_group_ratio};
{csr_cte("ldbc", compact=True, validated_edges=True)}
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
{csr_cte("ldbc", compact=True, validated_edges=True)}
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
{csr_cte("ldbc", compact=True, validated_edges=True)}
SELECT 'pushpull_operator' AS mode, count(*) AS pair_count, count(len) AS reachable_count,
       sum(len) AS total_len, min(len) AS min_len, max(len) AS max_len
FROM (
    SELECT src, dst, pushpulliterativelengthoperator(src, dst, csr_id) AS len
    FROM {pairs}, csr_cte
);
"""


def csr_sql(options):
    return f"""
{csr_cte("ldbc", compact=True, validated_edges=True)}
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


def recursive_sssp_sql(options):
    pairs = f"ldbc.{options.pair_table}"
    return f"""
WITH RECURSIVE
pairs AS (
    SELECT rowid::BIGINT AS pair_id, src, dst
    FROM {pairs}
),
sources AS (
    SELECT DISTINCT src
    FROM pairs
),
edges AS (
    SELECT a.rowid::BIGINT AS src, c.rowid::BIGINT AS dst
    FROM ldbc.person_knows_person k
    JOIN ldbc.person a ON a.id = k.person1id
    JOIN ldbc.person c ON c.id = k.person2id
),
reach(source, here, len) USING KEY (source, here) AS (
    SELECT sources.src AS source, sources.src AS here, 0::BIGINT AS len
    FROM sources
    UNION ALL (
        SELECT reach.source, edges.dst, reach.len + 1 AS len
        FROM reach
        JOIN edges ON edges.src = reach.here
        LEFT JOIN recurring.reach rec ON rec.source = reach.source AND rec.here = edges.dst
        WHERE reach.len < {options.recursive_max_depth}
          AND reach.len + 1 < coalesce(rec.len, 9223372036854775807)
        ORDER BY len DESC
    )
),
lengths AS (
    SELECT pairs.pair_id, reach.len
    FROM pairs
    LEFT JOIN reach ON reach.source = pairs.src AND reach.here = pairs.dst
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
        if options.query_pattern == "sssp" or options.query_pattern == "graphalytics_bfs":
            return recursive_sssp_sql(options)
        return recursive_sql(options)
    raise ValueError(f"Unsupported benchmark mode: {mode}")


def recursive_depth_hint(row, expected, actual):
    if row["mode"] != "recursive" or row.get("recursive_max_depth", "") == "":
        return ""

    depth = int(row["recursive_max_depth"])
    actual_max = int(actual["max_len"]) if actual["max_len"] else -1
    expected_reachable = int(expected["reachable_count"]) if expected["reachable_count"] else 0
    actual_reachable = int(actual["reachable_count"]) if actual["reachable_count"] else 0
    if actual_max >= depth or actual_reachable < expected_reachable:
        return f" Recursive SQL is capped by --recursive-max-depth={depth}; increase it for a fair comparison."
    return ""


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
                hint = recursive_depth_hint(row, expected, actual)
                raise RuntimeError(
                    f"Benchmark result mismatch for repeat {repeat}: {expected_mode}={expected}, "
                    f"{row['mode']}={actual}.{hint}"
                )


def verify_graphalytics_bfs_result(row, reference_profile):
    result_keys = ["pair_count", "reachable_count", "total_len", "min_len", "max_len"]
    mismatches = [key for key in result_keys if str(row[key]) != str(reference_profile[key])]
    if mismatches:
        details = ", ".join(
            f"{key}: observed={row[key]} reference={reference_profile[key]}" for key in mismatches
        )
        raise RuntimeError(f"Graphalytics BFS reference aggregate mismatch for {row['scale_factor']}: {details}")


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
        "source_group_build_s": "",
        "source_group_bfs_s": "",
        "source_group_count": "",
        "source_group_output_chunks": "",
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
    source_group_build_ms = 0.0
    source_group_bfs_ms = 0.0
    source_group_count = 0
    source_group_output_chunks = 0
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
            elif phase == "source_group_build":
                source_group_build_ms += time_ms
                source_group_count += int(row["EdgeCount"])
                source_group_output_chunks += int(row["PartitionCount"])
            elif phase == "source_group_bfs":
                source_group_bfs_ms += time_ms

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
    if source_group_count:
        result["source_group_build_s"] = f"{source_group_build_ms / 1000.0:.6f}"
        result["source_group_count"] = source_group_count
        result["source_group_output_chunks"] = source_group_output_chunks
    if source_group_bfs_ms:
        result["source_group_bfs_s"] = f"{source_group_bfs_ms / 1000.0:.6f}"
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
        row_stats = {field: rows[0].get(field, "") for field in RUN_METADATA_FIELDS + DATASET_METADATA_FIELDS}
        row_stats.update(
            {
                "scale_factor": rows[0]["scale_factor"],
                "mode": mode,
                "threads": rows[0]["threads"],
                "repeats": len(rows),
                "query_pattern": rows[0]["query_pattern"],
                "metrics_enabled": rows[0]["metrics_enabled"],
                "deduplicate_pairs": rows[0]["deduplicate_pairs"],
                "grouped_batches": rows[0]["grouped_batches"],
                "threads_per_batch": rows[0]["threads_per_batch"],
                "max_concurrent_batches": rows[0]["max_concurrent_batches"],
                "reverse_orientation_ratio": rows[0]["reverse_orientation_ratio"],
                "source_group_ratio": rows[0]["source_group_ratio"],
                "recursive_max_depth": rows[0]["recursive_max_depth"],
                "graphalytics_algorithm": rows[0].get("graphalytics_algorithm", ""),
                "graphalytics_source_vertex": rows[0].get("graphalytics_source_vertex", ""),
                "graphalytics_reference_match": rows[0].get("graphalytics_reference_match", ""),
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
                "source_group_build_mean_s": mean_optional(rows, "source_group_build_s"),
                "source_group_bfs_mean_s": mean_optional(rows, "source_group_bfs_s"),
                "source_group_count_mean": mean_int_optional(rows, "source_group_count"),
                "source_group_output_chunks_mean": mean_int_optional(rows, "source_group_output_chunks"),
                "query_mean_s": f"{statistics.mean(query_times):.6f}",
                "query_stdev_s": f"{stdev(query_times):.6f}",
                "query_min_s": f"{min(query_times):.6f}",
                "query_max_s": f"{max(query_times):.6f}",
                "total_mean_s": f"{statistics.mean(total_times):.6f}",
                "total_stdev_s": f"{stdev(total_times):.6f}",
                "database": rows[0]["database"],
            }
        )
        stats.append(row_stats)
    return stats


def summary_fieldnames():
    return RUN_METADATA_FIELDS + DATASET_METADATA_FIELDS + [
        "scale_factor",
        "mode",
        "threads",
        "repeat",
        "query_pattern",
        "metrics_enabled",
        "deduplicate_pairs",
        "grouped_batches",
        "threads_per_batch",
        "max_concurrent_batches",
        "reverse_orientation_ratio",
        "source_group_ratio",
        "recursive_max_depth",
        "graphalytics_algorithm",
        "graphalytics_source_vertex",
        "graphalytics_reference_match",
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
        "source_group_build_s",
        "source_group_bfs_s",
        "source_group_count",
        "source_group_output_chunks",
        "local_csr_forward_memory_bytes",
        "local_csr_reverse_memory_bytes",
        "local_csr_pull_memory_bytes",
        "query_s",
        "total_s",
        "database",
    ]


def stats_fieldnames():
    return RUN_METADATA_FIELDS + DATASET_METADATA_FIELDS + [
        "scale_factor",
        "mode",
        "threads",
        "repeats",
        "query_pattern",
        "metrics_enabled",
        "deduplicate_pairs",
        "grouped_batches",
        "threads_per_batch",
        "max_concurrent_batches",
        "reverse_orientation_ratio",
        "source_group_ratio",
        "recursive_max_depth",
        "graphalytics_algorithm",
        "graphalytics_source_vertex",
        "graphalytics_reference_match",
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
        "source_group_build_mean_s",
        "source_group_bfs_mean_s",
        "source_group_count_mean",
        "source_group_output_chunks_mean",
        "query_mean_s",
        "query_stdev_s",
        "query_min_s",
        "query_max_s",
        "total_mean_s",
        "total_stdev_s",
        "database",
    ]


def write_benchmark_outputs(results_dir, query_pattern, pair_shape, pair_label, mode_label, results):
    timestamp = int(time.time())
    result_path = results_dir / f"summary_{query_pattern}_{pair_shape}_pairs{pair_label}_threads{results[0]['threads']}_mode{mode_label}_{timestamp}.csv"
    with result_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=summary_fieldnames())
        writer.writeheader()
        writer.writerows(results)
    print(f"Wrote summary: {result_path}")

    stats = summarize_results(results)
    stats_path = results_dir / f"stats_{query_pattern}_{pair_shape}_pairs{pair_label}_threads{results[0]['threads']}_mode{mode_label}_{timestamp}.csv"
    with stats_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=stats_fieldnames())
        writer.writeheader()
        writer.writerows(stats)
    for row in stats:
        print(json.dumps(row, sort_keys=True))
    print(f"Wrote stats: {stats_path}")
    return result_path, stats_path


def run_duckpgq_benchmark(args):
    target_value, target_label, attached_db = benchmark_target(args)
    results_dir = DATA_ROOT / "results" / target_label
    results_dir.mkdir(parents=True, exist_ok=True)
    if not attached_db.exists():
        raise SystemExit(f"Missing benchmark database: {attached_db}. Run prepare first.")
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    generated_run_id = (
        f"{int(time.time())}_{args.system_name}_{target_label}_{args.query_pattern}_"
        f"{args.mode}_threads{args.threads}"
    )
    run_metadata = benchmark_run_metadata(args, started_at, args.run_id or generated_run_id)
    dataset_metadata = read_benchmark_metadata(attached_db)
    generated_shape = generated_pair_shape(args.query_pattern, args.pair_shape)
    graphalytics_algorithm = ""
    graphalytics_source_vertex = ""
    graphalytics_reference_profile = None
    if generated_shape == "graphalytics_bfs":
        if not args.dataset:
            raise SystemExit("--query-pattern graphalytics_bfs requires --dataset")
        if args.pair_table is not None:
            raise SystemExit("--query-pattern graphalytics_bfs generates its official pair table; do not pass --pair-table")
        pair_table, graphalytics_source_vertex = ensure_graphalytics_bfs_pair_table(attached_db, target_value)
        pair_shape = "graphalytics_bfs"
        pair_label = "official_bfs"
        graphalytics_algorithm = "bfs"
        graphalytics_reference_profile = graphalytics_bfs_reference_profile(target_value)
    else:
        source_count = args.source_count
        target_count = args.target_count
        pair_label = str(args.pairs)
        if generated_shape == "all_pairs":
            source_count = args.pairs if source_count is None else source_count
            target_count = args.pairs if target_count is None else target_count
            pair_label = f"{source_count}x{target_count}"
        pair_shape = "custom" if args.pair_table else generated_shape
        pair_table = args.pair_table or pair_table_name(args.pairs, generated_shape, source_count, target_count)
        if args.pair_table is None:
            ensure_pair_table_for_db(attached_db, target_label, args.pairs, generated_shape, source_count, target_count)
    pair_profile = read_pair_profile(attached_db, pair_table)
    actual_pair_count = int(pair_profile["pair_table_rows"])

    results = []
    modes = benchmark_modes(args.mode)
    for repeat in range(1, args.repeats + 1):
        for mode in modes:
            prefix = (
                results_dir
                / f"{mode}_{args.query_pattern}_{pair_shape}_pairs{pair_label}_threads{args.threads}_repeat{repeat}"
            )
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
                query_pattern=args.query_pattern,
                pair_count=actual_pair_count,
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
                source_group_ratio=args.source_group_ratio,
            )
            query_sql = mode_sql(mode, options)
            output, timers = run_duckdb_timed_script(setup_sql(options) + query_sql, args.timeout)
            row = parse_csv_row(output)
            row["scale_factor"] = target_value
            row["threads"] = args.threads
            row["repeat"] = repeat
            row["query_pattern"] = args.query_pattern
            row["metrics_enabled"] = int(args.metrics)
            row["deduplicate_pairs"] = int(args.deduplicate_pairs)
            row["grouped_batches"] = int(args.grouped_batches and mode == "operator")
            row["threads_per_batch"] = args.threads_per_batch if mode == "operator" else ""
            row["max_concurrent_batches"] = args.max_concurrent_batches if mode == "operator" else ""
            row["reverse_orientation_ratio"] = args.reverse_orientation_ratio if mode == "operator" else ""
            row["source_group_ratio"] = args.source_group_ratio if mode == "operator" else ""
            row["pair_table"] = pair_table
            row["pair_shape"] = pair_shape
            row["graphalytics_algorithm"] = graphalytics_algorithm
            row["graphalytics_source_vertex"] = graphalytics_source_vertex
            row["graphalytics_reference_match"] = ""
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
            row.update(run_metadata)
            row.update(dataset_metadata)
            if graphalytics_reference_profile is not None:
                verify_graphalytics_bfs_result(row, graphalytics_reference_profile)
                row["graphalytics_reference_match"] = 1
            results.append(row)
            print(json.dumps(row, sort_keys=True))

    if args.verify:
        verify_result_rows(results)

    write_benchmark_outputs(results_dir, args.query_pattern, pair_shape, pair_label, args.mode, results)


def run_kuzu_graphalytics_bfs(args):
    if not args.dataset:
        raise SystemExit("--system kuzu currently requires --dataset")
    if args.query_pattern != "graphalytics_bfs":
        raise SystemExit("--system kuzu currently supports --query-pattern graphalytics_bfs")
    if args.pair_table is not None:
        raise SystemExit("--system kuzu graphalytics_bfs uses the official source/all-targets shape; do not pass --pair-table")

    kuzu = require_kuzu()
    target_value = graphalytics_name(args.dataset)
    target_label = graphalytics_label(target_value)
    db_path = graphalytics_kuzu_db_path(target_value)
    if not db_path.exists():
        raise SystemExit(f"Missing Kuzu benchmark database: {db_path}. Run prepare --system kuzu first.")

    results_dir = DATA_ROOT / "results" / target_label
    results_dir.mkdir(parents=True, exist_ok=True)
    source_vertex = graphalytics_bfs_source_vertex(target_value)
    reference_profile = graphalytics_bfs_reference_profile(target_value)
    max_depth = min(args.recursive_max_depth, 30)
    if args.verify and int(reference_profile["max_len"]) > max_depth:
        raise SystemExit(
            f"Kuzu shortest-path upper bound is capped at {max_depth}, but {target_value} "
            f"reference max_len is {reference_profile['max_len']}."
        )
    dataset_metadata = read_kuzu_benchmark_metadata(target_value)
    vertex_count = int(dataset_metadata["dataset_metadata_person_rows"])
    pair_profile = {
        "pair_table_rows": str(vertex_count),
        "distinct_src_count": "1",
        "distinct_dst_count": str(vertex_count),
        "unique_pair_count": str(vertex_count),
        "duplicate_pair_count": "0",
        "self_pair_count": "1",
    }

    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    generated_run_id = f"{int(time.time())}_{args.system_name}_{target_label}_{args.query_pattern}_native_threads{args.threads}"
    run_metadata = benchmark_run_metadata(args, started_at, args.run_id or generated_run_id)
    query = f"""
MATCH (s:Person {{id: {source_vertex}}})-[e:Knows* SHORTEST 1..{max_depth}]->(d:Person)
WHERE d.id <> {source_vertex}
RETURN count(d.id), sum(length(e)), min(length(e)), max(length(e))
"""

    db = kuzu.Database(str(db_path), read_only=True)
    conn = kuzu.Connection(db, num_threads=args.threads)
    if hasattr(conn, "set_max_threads_for_exec"):
        conn.set_max_threads_for_exec(args.threads)

    results = []
    for repeat in range(1, args.repeats + 1):
        start = time.perf_counter()
        output = kuzu_query_single_row(conn, query)
        query_s = time.perf_counter() - start
        reachable_without_source = int(kuzu_row_get(output, 0, 0) or 0)
        total_without_source = int(kuzu_row_get(output, 1, 0) or 0)
        min_without_source = kuzu_row_get(output, 2, None)
        max_without_source = kuzu_row_get(output, 3, None)
        reachable_count = reachable_without_source + 1
        total_len = total_without_source
        min_len = 0
        max_len = int(max_without_source or 0)
        if min_without_source is not None:
            min_len = min(0, int(min_without_source))

        row = {
            "scale_factor": target_value,
            "mode": "native",
            "threads": args.threads,
            "repeat": repeat,
            "query_pattern": args.query_pattern,
            "metrics_enabled": 0,
            "deduplicate_pairs": 0,
            "grouped_batches": 0,
            "threads_per_batch": "",
            "max_concurrent_batches": "",
            "reverse_orientation_ratio": "",
            "source_group_ratio": "",
            "recursive_max_depth": max_depth,
            "graphalytics_algorithm": "bfs",
            "graphalytics_source_vertex": source_vertex,
            "graphalytics_reference_match": "",
            "pair_count": str(vertex_count),
            "pair_table": graphalytics_bfs_pair_table_name(),
            "pair_shape": "graphalytics_bfs",
            "reachable_count": str(reachable_count),
            "total_len": str(total_len),
            "min_len": str(min_len),
            "max_len": str(max_len),
            "setup_s": "0.000000",
            "csr_build_s": "",
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
            "source_group_build_s": "",
            "source_group_bfs_s": "",
            "source_group_count": "",
            "source_group_output_chunks": "",
            "local_csr_forward_memory_bytes": "",
            "local_csr_reverse_memory_bytes": "",
            "local_csr_pull_memory_bytes": "",
            "query_s": f"{query_s:.6f}",
            "total_s": f"{query_s:.6f}",
            "database": str(db_path),
        }
        row.update(pair_profile)
        row.update(run_metadata)
        row.update(dataset_metadata)
        if args.verify:
            verify_graphalytics_bfs_result(row, reference_profile)
            row["graphalytics_reference_match"] = 1
        results.append(row)
        print(json.dumps(row, sort_keys=True))

    write_benchmark_outputs(results_dir, args.query_pattern, "graphalytics_bfs", "official_bfs", "native", results)


def run_benchmark(args):
    args.system_name = args.system_name or args.system
    if args.system == "duckpgq":
        run_duckpgq_benchmark(args)
    elif args.system == "kuzu":
        run_kuzu_graphalytics_bfs(args)
    else:
        raise SystemExit(f"Unsupported benchmark system: {args.system}")


def sweep_graphalytics_bfs(args):
    args.system_name = args.system_name or args.system
    datasets = expand_graphalytics_datasets(args.datasets)
    completed = 0
    skipped = []
    for dataset in datasets:
        db_file = graphalytics_kuzu_db_path(dataset) if args.system == "kuzu" else graphalytics_db_path(dataset)
        if args.skip_missing and not db_file.exists():
            print(f"Skipping {dataset}: missing {db_file}. Run prepare --system {args.system} --graphalytics-datasets {dataset} first.")
            skipped.append(dataset)
            continue
        for threads in args.threads:
            run_args = argparse.Namespace(**vars(args))
            run_args.scale_factor = None
            run_args.dataset = dataset
            run_args.threads = threads
            run_args.pairs = 1024
            run_args.query_pattern = "graphalytics_bfs"
            run_args.source_count = None
            run_args.target_count = None
            run_args.pair_table = None
            run_args.pair_shape = "random"
            run_args.mode = "operator"
            run_args.build_reverse_csr = False
            run_args.run_id = f"{args.run_id}_{graphalytics_label(dataset)}_threads{threads}" if args.run_id else None
            print(f"Running Graphalytics BFS sweep: dataset={dataset} threads={threads} repeats={args.repeats}")
            run_benchmark(run_args)
            completed += 1
    print(f"Completed {completed} Graphalytics BFS sweep runs.")
    if skipped:
        print("Skipped missing datasets: " + ", ".join(skipped))


def main():
    parser = argparse.ArgumentParser(description="Prepare and smoke-run pathfinding benchmarks on LDBC Person knows Person data.")
    subcommands = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subcommands.add_parser("prepare")
    prepare_parser.add_argument("--system", choices=SYSTEMS, default=DEFAULT_SYSTEM_NAME)
    prepare_parser.add_argument("--scale-factors", nargs="+", default=["1", "3", "10"])
    prepare_parser.add_argument(
        "--graphalytics-datasets",
        nargs="+",
        default=None,
        help=(
            "Download and materialize Graphalytics datasets instead of LDBC scale factors. "
            "Groups: core/all, small, small-extra, medium, medium-extra, large, large-extra, "
            "small-medium, medium-large, small-medium-large, all-known. "
            f"'all' remains scoped to the core set: {', '.join(GRAPHALYTICS_DEFAULT_DATASETS)}."
        ),
    )
    prepare_parser.add_argument("--threads", type=int, default=8)
    prepare_parser.add_argument("--pairs", type=int, default=1024)
    prepare_parser.add_argument("--force", action="store_true")
    prepare_parser.add_argument(
        "--force-materialize",
        action="store_true",
        help="For Graphalytics, rebuild the local DuckDB DB from existing downloads without forcing downloads.",
    )
    prepare_parser.set_defaults(func=prepare)

    run_parser = subcommands.add_parser("run")
    run_parser.add_argument("--system", choices=SYSTEMS, default=DEFAULT_SYSTEM_NAME)
    run_parser.add_argument("--scale-factor", default=None)
    run_parser.add_argument(
        "--dataset",
        default=None,
        help="Run against a prepared Graphalytics dataset, e.g. wiki-Talk, kgs, graph500-22, cit-Patents.",
    )
    run_parser.add_argument("--threads", type=int, default=4)
    run_parser.add_argument("--pairs", type=int, default=1024)
    run_parser.add_argument(
        "--query-pattern",
        choices=QUERY_PATTERNS,
        default="point_to_point",
        help=(
            "Benchmark pattern. point_to_point uses --pair-shape; sssp generates one source "
            "with --pairs target vertices; all_pairs generates a source_count x target_count "
            "block, defaulting both counts to --pairs; graphalytics_bfs uses the official "
            "Graphalytics BFS source vertex and all target vertices."
        ),
    )
    run_parser.add_argument(
        "--source-count",
        type=int,
        default=None,
        help="For all_pairs, number of generated source vertices. Defaults to --pairs.",
    )
    run_parser.add_argument(
        "--target-count",
        type=int,
        default=None,
        help="For all_pairs, number of generated target vertices. Defaults to --pairs.",
    )
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
        "--system-name",
        default=None,
        help="Logical system under test recorded in result metadata. Defaults to --system.",
    )
    run_parser.add_argument(
        "--benchmark-profile",
        default="exploratory",
        help="Free-form profile recorded in result metadata, e.g. exploratory, paper, smoke.",
    )
    run_parser.add_argument("--run-label", default="", help="Optional human-readable run label recorded in metadata.")
    run_parser.add_argument("--run-id", default=None, help="Optional stable run id. Defaults to a generated id.")
    run_parser.add_argument("--notes", default="", help="Optional notes recorded in result metadata.")
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
    run_parser.add_argument(
        "--source-group-ratio",
        type=int,
        default=4,
        help="Use source-grouped regular MS-BFS when pair_count >= ratio * oriented distinct source count; <= 0 disables it.",
    )
    run_parser.add_argument("--recursive-max-depth", type=int, default=8)
    run_parser.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True)
    run_parser.add_argument("--timeout", type=int, default=300)
    run_parser.set_defaults(func=run_benchmark)

    sweep_parser = subcommands.add_parser("sweep-graphalytics-bfs")
    sweep_parser.add_argument("--system", choices=SYSTEMS, default=DEFAULT_SYSTEM_NAME)
    sweep_parser.add_argument(
        "--datasets",
        nargs="+",
        default=["core"],
        help=(
            "Graphalytics datasets or groups to sweep. Groups: core/all, small, small-extra, medium, "
            "medium-extra, large, large-extra, small-medium, medium-large, small-medium-large, all-known. "
            "Missing prepared DBs are skipped by default."
        ),
    )
    sweep_parser.add_argument("--threads", type=int, nargs="+", default=[8, 16, 24, 32])
    sweep_parser.add_argument("--repeats", type=int, default=3)
    sweep_parser.add_argument(
        "--skip-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip datasets whose local DuckDB database has not been prepared.",
    )
    sweep_parser.add_argument(
        "--metrics",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable internal operator benchmark CSV metrics. Disabled by default for clean wall-clock timing.",
    )
    sweep_parser.add_argument(
        "--deduplicate-pairs",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable exact source/destination pair deduplication inside path-finding operator batches.",
    )
    sweep_parser.add_argument(
        "--grouped-batches",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable grouped regular MS-BFS scheduling with bounded worker groups.",
    )
    sweep_parser.add_argument("--threads-per-batch", type=int, default=0)
    sweep_parser.add_argument("--max-concurrent-batches", type=int, default=0)
    sweep_parser.add_argument("--reverse-orientation-ratio", type=int, default=4)
    sweep_parser.add_argument("--source-group-ratio", type=int, default=4)
    sweep_parser.add_argument("--push-pull-frontier-gate", type=int, default=2)
    sweep_parser.add_argument("--recursive-max-depth", type=int, default=64)
    sweep_parser.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True)
    sweep_parser.add_argument("--timeout", type=int, default=1200)
    sweep_parser.add_argument("--system-name", default=None)
    sweep_parser.add_argument("--benchmark-profile", default="exploratory")
    sweep_parser.add_argument("--run-label", default="graphalytics-bfs-sweep")
    sweep_parser.add_argument("--run-id", default=None)
    sweep_parser.add_argument("--notes", default="")
    sweep_parser.set_defaults(func=sweep_graphalytics_bfs)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
