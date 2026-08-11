#!/usr/bin/env python3
import argparse
from collections import Counter
import csv
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import pathfinding_benchmark as benchmark


DEFAULT_SAMPLE = Path("/usr/bin/sample")
MARKER_COLD = "__DUCKPGQ_COLD_DONE__"
MARKER_WARM = "__DUCKPGQ_WARM_DONE__"


def require_tool(name, explicit_path=None):
    if explicit_path is not None and explicit_path.exists():
        return explicit_path
    resolved = shutil.which(name)
    if resolved is None:
        raise SystemExit(f"Missing required tool: {name}")
    return Path(resolved)


def wait_for_marker(process, marker, timeout_s):
    result = {"output": [], "found": False}

    def read_output():
        for line in process.stdout:
            result["output"].append(line)
            if marker in line:
                result["found"] = True
                return

    reader = threading.Thread(target=read_output, daemon=True)
    reader.start()
    reader.join(timeout_s)
    if reader.is_alive() or not result["found"]:
        process.terminate()
        raise RuntimeError(f"DuckDB did not emit {marker} within {timeout_s} seconds")
    return "".join(result["output"])


def parse_last_csv_result(output):
    lines = [line for line in output.splitlines() if line and not line.startswith("__DUCKPGQ_")]
    for index in range(len(lines) - 2, -1, -1):
        if lines[index].startswith('"mode",') or lines[index].startswith("mode,"):
            rows = list(csv.DictReader(lines[index:]))
            if rows:
                return rows[-1]
    raise RuntimeError("Could not find the benchmark result in DuckDB output")


def graphalytics_options(args, output_dir):
    dataset = benchmark.graphalytics_name(args.dataset)
    database = benchmark.graphalytics_db_path(dataset)
    if not database.exists():
        raise SystemExit(f"Missing benchmark database: {database}")

    pair_table, _ = benchmark.ensure_graphalytics_bfs_pair_table(database, dataset)
    metadata = benchmark.read_benchmark_metadata(database)
    pair_profile = benchmark.read_pair_profile(database, pair_table)
    options = benchmark.BenchmarkOptions(
        attached_db=database,
        query_pattern="graphalytics_bfs",
        pair_count=int(pair_profile["pair_table_rows"]),
        pair_table=pair_table,
        vertex_count=int(metadata["dataset_metadata_person_rows"]),
        edge_count=int(metadata["dataset_metadata_person_knows_person_rows"]),
        threads=args.threads,
        benchmark_prefix=output_dir / "unused_internal_metrics",
        recursive_max_depth=64,
        build_reverse_csr=False,
        metrics_enabled=False,
        push_pull_frontier_gate=2,
        deduplicate_pairs=False,
        grouped_batches=False,
        threads_per_batch=0,
        max_concurrent_batches=0,
        reverse_orientation_ratio=4,
        source_group_ratio=4,
    )
    return dataset, options


def start_duckdb(stderr_path):
    stderr_handle = stderr_path.open("w")
    process = subprocess.Popen(
        [str(benchmark.BENCH_DUCKDB), "-unsigned", "-csv"],
        cwd=benchmark.REPO_ROOT,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=stderr_handle,
        text=True,
        bufsize=1,
    )
    return process, stderr_handle


def start_sample(sample_tool, process, args, sample_path, log_path):
    log_handle = log_path.open("w")
    sampler = subprocess.Popen(
        [
            str(sample_tool),
            str(process.pid),
            str(args.sample_seconds),
            str(args.interval_ms),
            "-mayDie",
            "-fullPaths",
            "-file",
            str(sample_path),
        ],
        cwd=benchmark.REPO_ROOT,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
    )
    time.sleep(0.25)
    return sampler, log_handle


def write_sql(process, sql, marker):
    process.stdin.write(sql.strip() + f"\n.print {marker}\n")
    process.stdin.flush()


def stop_duckdb(process, stderr_handle):
    if process.poll() is None:
        process.stdin.write(".quit\n")
        process.stdin.flush()
    process.wait(timeout=30)
    stderr_handle.close()


def capture_cold(args, sample_tool, options, output_dir):
    sample_path = output_dir / "cold.sample.txt"
    process, stderr_handle = start_duckdb(output_dir / "cold.duckdb.log")
    sampler, sample_log = start_sample(
        sample_tool, process, args, sample_path, output_dir / "cold.sample.log"
    )
    sql = benchmark.setup_sql(options) + benchmark.operator_sql(options)
    write_sql(process, sql, MARKER_COLD)
    output = wait_for_marker(process, MARKER_COLD, args.timeout)
    row = parse_last_csv_result(output)
    stop_duckdb(process, stderr_handle)
    sampler.wait(timeout=args.sample_seconds + 30)
    sample_log.close()
    return sample_path, row


def capture_warm(args, sample_tool, options, output_dir):
    sample_path = output_dir / "warm.sample.txt"
    process, stderr_handle = start_duckdb(output_dir / "warm.duckdb.log")

    cold_sql = benchmark.setup_sql(options) + benchmark.operator_sql(options)
    write_sql(process, cold_sql, MARKER_COLD)
    cold_output = wait_for_marker(process, MARKER_COLD, args.timeout)
    cold_row = parse_last_csv_result(cold_output)

    sampler, sample_log = start_sample(
        sample_tool, process, args, sample_path, output_dir / "warm.sample.log"
    )
    warm_sql = "".join(benchmark.cached_operator_sql(options) for _ in range(args.warm_repeats))
    write_sql(process, warm_sql, MARKER_WARM)
    warm_output = wait_for_marker(process, MARKER_WARM, args.timeout)
    warm_row = parse_last_csv_result(warm_output)
    sampler.wait(timeout=args.sample_seconds + 30)
    sample_log.close()
    stop_duckdb(process, stderr_handle)
    return sample_path, cold_row, warm_row


def render_flamegraph(sample_path, output_dir, phase, dataset, threads, stackcollapse, flamegraph):
    folded_path = output_dir / f"{phase}.folded"
    svg_path = output_dir / f"{phase}.svg"
    with sample_path.open() as source, folded_path.open("w") as folded:
        subprocess.run([str(stackcollapse)], stdin=source, stdout=folded, check=True, text=True)
    with folded_path.open() as folded, svg_path.open("w") as svg:
        subprocess.run(
            [
                str(flamegraph),
                "--hash",
                "--colors",
                "hot",
                "--title",
                f"DuckPGQ {dataset} Graphalytics BFS: {phase}",
                "--subtitle",
                f"{threads}-thread CPU stack samples; internal metrics disabled",
            ],
            stdin=folded,
            stdout=svg,
            check=True,
            text=True,
        )
    return folded_path, svg_path


def summarize_folded(folded_path, output_dir, phase, limit=50):
    leaf_samples = Counter()
    inclusive_samples = Counter()
    total_samples = 0
    with folded_path.open(errors="replace") as source:
        for line in source:
            try:
                stack, sample_text = line.rsplit(" ", 1)
                samples = int(sample_text)
            except ValueError:
                continue
            if samples <= 0:
                continue
            frames = stack.split(";")
            total_samples += samples
            leaf_samples[frames[-1]] += samples
            for frame in set(frames):
                inclusive_samples[frame] += samples

    hotspot_path = output_dir / f"{phase}.hotspots.csv"
    with hotspot_path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["kind", "rank", "samples", "percent", "frame"])
        for kind, counts in (("leaf", leaf_samples), ("inclusive", inclusive_samples)):
            for rank, (frame, samples) in enumerate(counts.most_common(limit), 1):
                writer.writerow(
                    [kind, rank, samples, f"{100.0 * samples / total_samples:.4f}", frame]
                )

    print(f"{phase.capitalize()} CPU samples: {total_samples}")
    for frame, samples in leaf_samples.most_common(8):
        print(f"  {100.0 * samples / total_samples:6.2f}%  {frame}")
    print(f"{phase.capitalize()} hotspots: {hotspot_path}")
    return hotspot_path


def validate_result(dataset, row):
    reference = benchmark.graphalytics_bfs_reference_profile(dataset)
    benchmark.verify_graphalytics_bfs_result(row, reference)


def main():
    parser = argparse.ArgumentParser(
        description="Capture cold and warm DuckPGQ Graphalytics BFS flame graphs on macOS."
    )
    parser.add_argument("--dataset", default="datagen-8_4-fb")
    parser.add_argument("--phase", choices=("cold", "warm", "both"), default="both")
    parser.add_argument("--threads", type=int, default=16)
    parser.add_argument("--sample-seconds", type=int, default=10)
    parser.add_argument("--interval-ms", type=int, default=1)
    parser.add_argument("--warm-repeats", type=int, default=64)
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()

    sample_tool = require_tool("sample", DEFAULT_SAMPLE)
    stackcollapse = require_tool("stackcollapse-sample.awk")
    flamegraph = require_tool("flamegraph.pl")
    output_dir = (
        benchmark.DATA_ROOT
        / "profiles"
        / benchmark.graphalytics_name(args.dataset).replace("-", "_")
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset, options = graphalytics_options(args, output_dir)

    if args.phase in ("cold", "both"):
        sample_path, cold_row = capture_cold(args, sample_tool, options, output_dir)
        validate_result(dataset, cold_row)
        folded_path, svg_path = render_flamegraph(
            sample_path, output_dir, "cold", dataset, args.threads, stackcollapse, flamegraph
        )
        print(f"Cold profile: {svg_path}")
        print(f"Cold folded stacks: {folded_path}")
        summarize_folded(folded_path, output_dir, "cold")

    if args.phase in ("warm", "both"):
        sample_path, cold_row, warm_row = capture_warm(
            args, sample_tool, options, output_dir
        )
        validate_result(dataset, cold_row)
        validate_result(dataset, warm_row)
        folded_path, svg_path = render_flamegraph(
            sample_path, output_dir, "warm", dataset, args.threads, stackcollapse, flamegraph
        )
        print(f"Warm profile: {svg_path}")
        print(f"Warm folded stacks: {folded_path}")
        summarize_folded(folded_path, output_dir, "warm")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.SubprocessError) as error:
        print(f"profile failed: {error}", file=sys.stderr)
        raise SystemExit(1)
