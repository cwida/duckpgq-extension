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


DATASETS = ("graph500-23", "graph500-24", "graph500-25")
GIB = 1024**3


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def current_phase_paths(results_root, dataset):
    label = dataset.replace("-", "_")
    directory = results_root / f"graphalytics_{label}"
    pattern = (
        "operator_graphalytics_bfs_graphalytics_bfs_"
        "pairsofficial_bfs_threads8_repeat*_phase_timing.csv"
    )
    return sorted(directory.glob(pattern))


def mean_phase(paths, phase):
    rows = []
    for path in paths:
        phases = {row["Phase"]: row for row in read_csv(path)}
        rows.append(phases[phase])
    if not rows:
        raise RuntimeError(f"No rows found for phase {phase}")
    return {
        "time_s": statistics.mean(float(row["Time_ms"]) / 1000 for row in rows),
        "memory_bytes": statistics.mean(int(row["MemoryBytes"]) for row in rows),
        "edge_count": int(rows[0]["EdgeCount"]),
        "partition_count": int(rows[0]["PartitionCount"]),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--baseline-csv", type=Path, required=True)
    parser.add_argument("--previous-radix-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    baseline = {row["dataset"]: row for row in read_csv(args.baseline_csv)}
    previous_radix = {row["dataset"]: row for row in read_csv(args.previous_radix_csv)}
    records = []
    for dataset in DATASETS:
        paths = current_phase_paths(args.results_root, dataset)
        partition = mean_phase(paths, "endpoint_radix_partition")
        build = mean_phase(paths, "endpoint_partition_build")
        total = mean_phase(paths, "local_csr_forward")
        buffer_peak = mean_phase(paths, "csr_build_buffer_manager_peak_delta")
        swap_peak = mean_phase(paths, "csr_build_buffer_manager_swap_peak_delta")
        edge_count = partition["edge_count"]
        vertex_count = int(baseline[dataset]["vertices"])
        endpoint_gib = partition["memory_bytes"] / GIB
        final_csr_gib = total["memory_bytes"] / GIB
        active_dense_gib = 8 * (vertex_count + 2) * 4 / GIB
        records.append(
            {
                "dataset": dataset,
                "vertices": vertex_count,
                "edges": edge_count,
                "repeats": len(paths),
                "logical_partitions": partition["partition_count"],
                "buffered_csr_s": float(baseline[dataset]["csr_total_s"]),
                "radix_16b_csr_s": float(previous_radix[dataset]["radix_csr_s"]),
                "packed_8b_partition_s": partition["time_s"],
                "packed_8b_build_s": build["time_s"],
                "packed_8b_csr_s": total["time_s"],
                "speedup_vs_buffered": float(baseline[dataset]["csr_total_s"]) / total["time_s"],
                "speedup_vs_radix_16b": float(previous_radix[dataset]["radix_csr_s"]) / total["time_s"],
                "packed_endpoint_gib": endpoint_gib,
                "active_dense_count_gib": active_dense_gib,
                "final_csr_gib": final_csr_gib,
                "packed_core_estimate_gib": endpoint_gib + active_dense_gib + final_csr_gib,
                "duckdb_buffer_peak_delta_gib": buffer_peak["memory_bytes"] / GIB,
                "duckdb_swap_peak_delta_gib": swap_peak["memory_bytes"] / GIB,
            }
        )

    fallback_path = (
        args.results_root
        / "graphalytics_graph500_25"
        / "precounted_operator_graphalytics_bfs_graphalytics_bfs_"
        "pairsofficial_bfs_threads8_repeat1_phase_timing.csv"
    )
    fallback_phases = {row["Phase"]: row for row in read_csv(fallback_path)}
    two_pass_s = float(fallback_phases["local_csr_forward"]["Time_ms"]) / 1000

    csv_path = args.output_dir / "graph500_packed_radix_csr_comparison.csv"
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)

    labels = [row["dataset"] for row in records]
    x = np.arange(len(records))
    width = 0.25
    fig, axes = plt.subplots(1, 2, figsize=(13.4, 5.4))
    variants = (
        ("Previous buffered", "buffered_csr_s", "#b55d60"),
        ("Radix, 16-byte endpoint", "radix_16b_csr_s", "#7d6aa5"),
        ("Radix, 8-byte endpoint", "packed_8b_csr_s", "#34699a"),
    )
    for index, (name, key, color) in enumerate(variants):
        axes[0].bar(x + (index - 1) * width, [row[key] for row in records], width, label=name, color=color)
    for index, row in enumerate(records):
        axes[0].text(
            index + width,
            row["packed_8b_csr_s"] + 0.8,
            f'{row["speedup_vs_radix_16b"]:.2f}x',
            ha="center",
            fontsize=9,
            fontweight="bold",
            color="#254f75",
        )
    axes[0].set_xticks(x, labels)
    axes[0].set_ylabel("CSR construction time (seconds)")
    axes[0].set_title("CSR construction by builder version")
    axes[0].grid(axis="y", alpha=0.2)
    axes[0].legend(frameon=False, fontsize=8)

    largest = records[-1]
    old = baseline["graph500-25"]
    previous = previous_radix["graph500-25"]
    phase_labels = ["Previous\nbuffered", "Two-pass", "Radix\n16-byte", "Radix\n8-byte"]
    phase_values = [
        [float(old["count_scan_s"]), float(old["allocate_s"]), float(old["fill_s"]), float(old["sparse_finalize_s"]), 0],
        [
            float(fallback_phases["precount_scan"]["Time_ms"]) / 1000,
            float(fallback_phases["precount_allocate"]["Time_ms"]) / 1000,
            float(fallback_phases["precount_fill"]["Time_ms"]) / 1000,
            float(fallback_phases["precount_sparse_finalize"]["Time_ms"]) / 1000,
            0,
        ],
        [float(previous["radix_partition_s"]), 0, 0, 0, float(previous["partition_csr_build_s"])],
        [largest["packed_8b_partition_s"], 0, 0, 0, largest["packed_8b_build_s"]],
    ]
    phase_names = ["Count scan or radix partition", "Allocate", "Endpoint fill", "Sparse finalize", "Partition CSR build"]
    colors = ["#68a9a2", "#b55d60", "#34699a", "#e07a3f", "#7d6aa5"]
    bottom = np.zeros(len(phase_labels))
    for phase_index, (phase_name, color) in enumerate(zip(phase_names, colors)):
        values = [row[phase_index] for row in phase_values]
        axes[1].bar(phase_labels, values, bottom=bottom, label=phase_name, color=color)
        bottom += values
    axes[1].set_ylabel("Time (seconds)")
    axes[1].set_title("graph500-25 construction phases")
    axes[1].grid(axis="y", alpha=0.2)
    axes[1].legend(frameon=False, fontsize=8)

    fig.suptitle("Packed endpoints reduce radix CSR construction time", fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    time_plot = args.output_dir / "graph500_packed_radix_csr_comparison.png"
    fig.savefig(time_plot, dpi=180)
    fig.savefig(args.output_dir / "graph500_packed_radix_csr_comparison.svg")
    plt.close(fig)

    previous_memory = [float(previous_radix[label]["core_memory_upper_bound_gib"]) for label in labels]
    packed_memory = [row["packed_core_estimate_gib"] for row in records]
    measured_buffer = [row["duckdb_buffer_peak_delta_gib"] for row in records]
    fig, axes = plt.subplots(1, 2, figsize=(13.4, 5.4))
    axes[0].bar(x - width / 2, previous_memory, width, label="Radix, 16-byte endpoint", color="#7d6aa5")
    axes[0].bar(x + width / 2, packed_memory, width, label="Radix, 8-byte endpoint", color="#34699a")
    axes[0].plot(x, measured_buffer, "o--", color="#e07a3f", label="DuckDB buffer-manager peak increase")
    axes[0].set_xticks(x, labels)
    axes[0].set_ylabel("Memory (GiB)")
    axes[0].set_title("Construction memory by graph scale")
    axes[0].grid(axis="y", alpha=0.2)
    axes[0].legend(frameon=False, fontsize=8)

    breakdown_labels = ["Radix\n16-byte", "Radix\n8-byte"]
    breakdown = {
        "Endpoint collection": [float(previous["endpoint_collection_gib"]), largest["packed_endpoint_gib"]],
        "Active dense counts": [float(previous["active_dense_count_gib"]), largest["active_dense_count_gib"]],
        "Final CSR": [float(previous["final_csr_gib"]), largest["final_csr_gib"]],
    }
    bottom = np.zeros(2)
    for (name, values), color in zip(breakdown.items(), ["#68a9a2", "#b55d60", "#34699a"]):
        axes[1].bar(breakdown_labels, values, bottom=bottom, label=name, color=color)
        bottom += values
    axes[1].set_ylabel("Estimated core construction state (GiB)")
    axes[1].set_title("graph500-25 memory breakdown")
    axes[1].grid(axis="y", alpha=0.2)
    axes[1].legend(frameon=False, fontsize=8)
    for index, value in enumerate(bottom):
        axes[1].text(index, value + 0.5, f"{value:.2f} GiB", ha="center", fontsize=9, fontweight="bold")

    fig.suptitle("Packed endpoints cut retained endpoint memory in half", fontsize=14)
    fig.text(
        0.5,
        0.012,
        "The core estimate and DuckDB buffer-manager metric have different scopes. The metric excludes C++ heap memory.",
        ha="center",
        fontsize=9,
        color="#5f6368",
    )
    fig.tight_layout(rect=(0, 0.04, 1, 0.95))
    memory_plot = args.output_dir / "graph500_packed_radix_csr_memory.png"
    fig.savefig(memory_plot, dpi=180)
    fig.savefig(args.output_dir / "graph500_packed_radix_csr_memory.svg")
    plt.close(fig)

    report_path = args.output_dir / "graph500_packed_radix_csr_comparison.md"
    lines = [
        "# Packed radix CSR construction",
        "",
        "All runs use 8 threads, three repeats, and the official Graphalytics BFS result checks.",
        "",
        "| Dataset | Logical partitions | Buffered CSR | Radix 16-byte | Radix 8-byte | Gain vs. 16-byte | Endpoint data | Final CSR | DuckDB buffer peak increase |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in records:
        lines.append(
            f"| {row['dataset']} | {row['logical_partitions']} | {row['buffered_csr_s']:.2f} s | "
            f"{row['radix_16b_csr_s']:.2f} s | {row['packed_8b_csr_s']:.2f} s | "
            f"{row['speedup_vs_radix_16b']:.2f}x | {row['packed_endpoint_gib']:.2f} GiB | "
            f"{row['final_csr_gib']:.2f} GiB | {row['duckdb_buffer_peak_delta_gib']:.2f} GiB |"
        )
    lines.extend(
        [
            "",
            "Each retained endpoint is one 64-bit value. It contains the source row ID, destination partition, and local destination ID.",
            "The logical CSR partition count is separate from the power-of-two radix bucket count. This avoids empty CSR partitions.",
            "Workers build the largest logical partitions first. This lets large endpoint partitions be released earlier.",
            "The DuckDB buffer-manager peak metric does not include the final CSR vectors or other normal C++ heap allocations.",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n")
    print(csv_path)
    print(time_plot)
    print(memory_plot)
    print(report_path)


if __name__ == "__main__":
    main()
