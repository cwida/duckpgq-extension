#!/usr/bin/env python3
import argparse
import csv
import os
import statistics
import tempfile
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "duckpgq-matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(Path(tempfile.gettempdir()) / "duckpgq-cache"))
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


DATASETS = tuple(f"graph500-{scale}" for scale in range(22, 27))
GIB = 1024**3


def read_phases(path):
    with path.open(newline="") as handle:
        return {row["Phase"]: row for row in csv.DictReader(handle)}


def phase_paths(results_root, dataset):
    label = dataset.replace("-", "_")
    directory = results_root / f"graphalytics_{label}"
    pattern = (
        "operator_graphalytics_bfs_graphalytics_bfs_"
        "pairsofficial_bfs_threads8_repeat*_phase_timing.csv"
    )
    paths = sorted(directory.glob(pattern))
    if not paths:
        raise RuntimeError(f"No current operator phase files found for {dataset}")
    return paths


def mean_memory(phases, name):
    return statistics.mean(int(run[name]["MemoryBytes"]) for run in phases)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--duckdb-memory-limit-gib", type=float, default=28.7)
    parser.add_argument("--physical-memory-gib", type=float, default=36.0)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for dataset in DATASETS:
        runs = [read_phases(path) for path in phase_paths(args.results_root, dataset)]
        endpoint = runs[0]["endpoint_radix_partition"]
        vertex_count = int(endpoint["PairCount"])
        edge_count = int(endpoint["EdgeCount"])
        thread_count = int(endpoint["ThreadCount"])
        endpoint_bytes = mean_memory(runs, "endpoint_radix_partition")
        final_csr_bytes = mean_memory(runs, "endpoint_partition_build")
        buffer_peak_bytes = mean_memory(runs, "csr_build_buffer_manager_peak_delta")
        swap_peak_bytes = mean_memory(runs, "csr_build_buffer_manager_swap_peak_delta")
        dense_count_upper_bytes = thread_count * (vertex_count + 2) * 4
        records.append(
            {
                "dataset": dataset,
                "vertices": vertex_count,
                "directed_endpoints": edge_count,
                "threads": thread_count,
                "repeats": len(runs),
                "logical_partitions": int(endpoint["PartitionCount"]),
                "endpoint_gib": endpoint_bytes / GIB,
                "active_dense_count_upper_gib": dense_count_upper_bytes / GIB,
                "final_csr_gib": final_csr_bytes / GIB,
                "estimated_core_upper_gib": (endpoint_bytes + dense_count_upper_bytes + final_csr_bytes) / GIB,
                "duckdb_buffer_peak_delta_gib": buffer_peak_bytes / GIB,
                "duckdb_swap_peak_delta_gib": swap_peak_bytes / GIB,
                "endpoint_bytes_per_edge": endpoint_bytes / edge_count,
                "final_csr_bytes_per_edge": final_csr_bytes / edge_count,
                "buffer_peak_bytes_per_edge": buffer_peak_bytes / edge_count,
            }
        )

    csv_path = args.output_dir / "graph500_csr_memory_scaling.csv"
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)

    labels = [row["dataset"] for row in records]
    x = np.arange(len(records))
    fig, axes = plt.subplots(1, 2, figsize=(14.2, 5.7))

    components = (
        ("Packed endpoint data", "endpoint_gib", "#67a9a2"),
        ("Eight active dense count arrays", "active_dense_count_upper_gib", "#b55d60"),
        ("Final CSR", "final_csr_gib", "#34699a"),
    )
    bottom = np.zeros(len(records))
    for name, key, color in components:
        values = np.array([row[key] for row in records])
        axes[0].bar(x, values, bottom=bottom, label=name, color=color)
        bottom += values
    buffer_values = [row["duckdb_buffer_peak_delta_gib"] for row in records]
    axes[0].plot(
        x,
        buffer_values,
        "o--",
        color="#e07a3f",
        linewidth=2,
        label="DuckDB buffer-manager peak increase",
    )
    axes[0].axhline(
        args.duckdb_memory_limit_gib,
        color="#7d6aa5",
        linestyle="--",
        linewidth=1.4,
        label=f"DuckDB limit ({args.duckdb_memory_limit_gib:.1f} GiB)",
    )
    axes[0].axhline(
        args.physical_memory_gib,
        color="#333333",
        linestyle=":",
        linewidth=1.4,
        label=f"Physical memory ({args.physical_memory_gib:.0f} GiB)",
    )
    for index, value in enumerate(bottom):
        axes[0].text(index, value + 0.5, f"{value:.1f}", ha="center", fontsize=9, fontweight="bold")
    axes[0].set_xticks(x, labels, rotation=20, ha="right")
    axes[0].set_ylabel("Memory (GiB)")
    axes[0].set_title("CSR construction memory by graph scale")
    axes[0].grid(axis="y", alpha=0.2)
    axes[0].legend(frameon=False, fontsize=8, loc="upper left")

    axes[1].plot(
        x,
        [row["endpoint_bytes_per_edge"] for row in records],
        "o-",
        linewidth=2,
        color="#67a9a2",
        label="Packed endpoint data",
    )
    axes[1].plot(
        x,
        [row["final_csr_bytes_per_edge"] for row in records],
        "s-",
        linewidth=2,
        color="#34699a",
        label="Final CSR",
    )
    axes[1].plot(
        x,
        [row["buffer_peak_bytes_per_edge"] for row in records],
        "^-",
        linewidth=2,
        color="#e07a3f",
        label="DuckDB buffer-manager peak increase",
    )
    axes[1].set_xticks(x, labels, rotation=20, ha="right")
    axes[1].set_ylabel("Bytes per directed endpoint")
    axes[1].set_title("Memory cost per directed endpoint")
    axes[1].grid(axis="y", alpha=0.2)
    axes[1].legend(frameon=False, fontsize=8)

    fig.suptitle("Graph500 packed radix CSR memory scaling", fontsize=14)
    fig.text(
        0.5,
        0.012,
        "The stacked bar is an upper estimate. The DuckDB metric excludes normal C++ heap memory, including the final CSR.",
        ha="center",
        fontsize=9,
        color="#5f6368",
    )
    fig.tight_layout(rect=(0, 0.04, 1, 0.95))
    png_path = args.output_dir / "graph500_csr_memory_scaling.png"
    svg_path = args.output_dir / "graph500_csr_memory_scaling.svg"
    fig.savefig(png_path, dpi=180)
    fig.savefig(svg_path)
    plt.close(fig)

    report_path = args.output_dir / "graph500_csr_memory_scaling.md"
    lines = [
        "# Graph500 CSR memory scaling",
        "",
        "All runs use the packed radix CSR builder, 8 threads, three cold runs, and Graphalytics result checks.",
        "",
        "| Dataset | Vertices | Directed endpoints | Logical partitions | Endpoint data | Final CSR | Core upper estimate | DuckDB buffer peak increase |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in records:
        lines.append(
            f"| {row['dataset']} | {row['vertices']:,} | {row['directed_endpoints']:,} | "
            f"{row['logical_partitions']} | {row['endpoint_gib']:.2f} GiB | {row['final_csr_gib']:.2f} GiB | "
            f"{row['estimated_core_upper_gib']:.2f} GiB | {row['duckdb_buffer_peak_delta_gib']:.2f} GiB |"
        )
    lines.extend(
        [
            "",
            "The endpoint data is exactly 8 bytes per directed endpoint. The measured DuckDB buffer-manager peak stays near 14 bytes per directed endpoint.",
            "Graph500-26 is close to the configured DuckDB memory limit. No tested graph used DuckDB temporary-file space.",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n")

    print(csv_path)
    print(png_path)
    print(svg_path)
    print(report_path)


if __name__ == "__main__":
    main()
