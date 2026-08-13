#!/usr/bin/env python3
import argparse
import csv
import os
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


def phase_path(results_root, dataset):
    label = dataset.replace("-", "_")
    return (
        results_root
        / f"graphalytics_{label}"
        / "sql_match_cold_graphalytics_bfs_graphalytics_bfs_"
        "pairsofficial_bfs_threads8_repeat0_phase_timing.csv"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--baseline-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    baseline = {row["dataset"]: row for row in read_csv(args.baseline_csv)}
    records = []
    for dataset in DATASETS:
        phases = {row["Phase"]: row for row in read_csv(phase_path(args.results_root, dataset))}
        partition_phase = phases["endpoint_radix_partition"]
        build_phase = phases["endpoint_partition_build"]
        total_phase = phases["local_csr_forward"]
        edge_count = int(partition_phase["EdgeCount"])
        vertex_count = int(baseline[dataset]["vertices"])
        endpoint_collection_gib = edge_count * 16 / GIB
        final_csr_gib = int(total_phase["MemoryBytes"]) / GIB
        active_dense_count_gib = 8 * (vertex_count + 2) * 4 / GIB
        records.append(
            {
                "dataset": dataset,
                "vertices": vertex_count,
                "edges": edge_count,
                "legacy_csr_s": float(baseline[dataset]["csr_total_s"]),
                "radix_partition_s": float(partition_phase["Time_ms"]) / 1000,
                "partition_csr_build_s": float(build_phase["Time_ms"]) / 1000,
                "radix_csr_s": float(total_phase["Time_ms"]) / 1000,
                "speedup": float(baseline[dataset]["csr_total_s"])
                / (float(total_phase["Time_ms"]) / 1000),
                "endpoint_collection_gib": endpoint_collection_gib,
                "active_dense_count_gib": active_dense_count_gib,
                "final_csr_gib": final_csr_gib,
                "core_memory_upper_bound_gib": (
                    endpoint_collection_gib + active_dense_count_gib + final_csr_gib
                ),
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

    csv_path = args.output_dir / "graph500_radix_csr_comparison.csv"
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)

    labels = [row["dataset"] for row in records]
    x = np.arange(len(records))
    width = 0.34
    fig, axes = plt.subplots(1, 2, figsize=(13.2, 5.4))

    axes[0].bar(
        x - width / 2,
        [row["legacy_csr_s"] for row in records],
        width,
        label="Previous buffered builder",
        color="#b55d60",
    )
    axes[0].bar(
        x + width / 2,
        [row["radix_csr_s"] for row in records],
        width,
        label="Radix-partitioned construction",
        color="#34699a",
    )
    axes[0].scatter(
        [2],
        [two_pass_s],
        marker="D",
        s=55,
        color="#e07a3f",
        label="Two-pass fallback",
        zorder=3,
    )
    for index, row in enumerate(records):
        axes[0].text(
            index + width / 2,
            row["radix_csr_s"] + 0.8,
            f'{row["speedup"]:.2f}x',
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
            color="#254f75",
        )
    axes[0].set_xticks(x, labels)
    axes[0].set_ylabel("CSR construction time (seconds)")
    axes[0].set_title("End-to-end CSR construction")
    axes[0].grid(axis="y", alpha=0.2)
    axes[0].legend(frameon=False, fontsize=8)

    old = baseline["graph500-25"]
    phase_labels = ["Previous buffered", "Two-pass", "Radix-partitioned"]
    phase_values = [
        [
            float(old["count_scan_s"]),
            float(old["allocate_s"]),
            float(old["fill_s"]),
            float(old["sparse_finalize_s"]),
            0,
        ],
        [
            float(fallback_phases["precount_scan"]["Time_ms"]) / 1000,
            float(fallback_phases["precount_allocate"]["Time_ms"]) / 1000,
            float(fallback_phases["precount_fill"]["Time_ms"]) / 1000,
            float(fallback_phases["precount_sparse_finalize"]["Time_ms"]) / 1000,
            0,
        ],
        [records[-1]["radix_partition_s"], 0, 0, 0, records[-1]["partition_csr_build_s"]],
    ]
    phase_names = [
        "Count scan or radix partition",
        "Allocate",
        "Endpoint fill",
        "Sparse finalize",
        "Partition CSR build",
    ]
    colors = ["#68a9a2", "#b55d60", "#34699a", "#e07a3f", "#7d6aa5"]
    bottom = np.zeros(3)
    for phase_idx, (phase_name, color) in enumerate(zip(phase_names, colors)):
        values = [row[phase_idx] for row in phase_values]
        axes[1].bar(phase_labels, values, bottom=bottom, label=phase_name, color=color)
        bottom += values
    axes[1].set_ylabel("Time (seconds)")
    axes[1].set_title("graph500-25 construction phases")
    axes[1].grid(axis="y", alpha=0.2)
    axes[1].legend(frameon=False, fontsize=8)

    fig.suptitle("Radix partitioning removes the graph500-25 CSR construction collapse", fontsize=14)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    plot_path = args.output_dir / "graph500_radix_csr_comparison.png"
    fig.savefig(plot_path, dpi=180)
    fig.savefig(args.output_dir / "graph500_radix_csr_comparison.svg")
    plt.close(fig)

    largest = records[-1]
    previous_memory = [float(baseline[label]["minimum_core_transient_gib"]) for label in labels]
    radix_memory = [row["core_memory_upper_bound_gib"] for row in records]
    fig, axes = plt.subplots(1, 2, figsize=(13.2, 5.4))
    axes[0].bar(
        x - width / 2,
        previous_memory,
        width,
        label="Previous buffered builder",
        color="#b55d60",
    )
    axes[0].bar(
        x + width / 2,
        radix_memory,
        width,
        label="Radix-partitioned builder",
        color="#34699a",
    )
    axes[0].axhline(28.7, color="#e07a3f", linestyle="--", linewidth=1.5, label="DuckDB limit (28.7 GiB)")
    axes[0].axhline(36.0, color="#333333", linestyle=":", linewidth=1.5, label="Physical memory (36 GiB)")
    axes[0].set_xticks(x, labels)
    axes[0].set_ylabel("Estimated core construction state (GiB)")
    axes[0].set_title("Memory bound by graph scale")
    axes[0].grid(axis="y", alpha=0.2)
    axes[0].legend(frameon=False, fontsize=8)
    for index, value in enumerate(radix_memory):
        reduction = previous_memory[index] / value
        axes[0].text(
            index + width / 2,
            value + 0.6,
            f"{reduction:.2f}x lower",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
            color="#254f75",
        )

    largest_old = baseline["graph500-25"]
    breakdown_labels = ["Previous buffered", "Radix-partitioned"]
    breakdown = {
        "Endpoint collection": [float(largest_old["endpoint_spool_gib"]), largest["endpoint_collection_gib"]],
        "Dense count arrays": [float(largest_old["dense_count_gib"]), largest["active_dense_count_gib"]],
        "Allocated CSR state": [float(largest_old["edge_payload_gib"]), largest["final_csr_gib"]],
    }
    breakdown_colors = ["#68a9a2", "#b55d60", "#34699a"]
    bottom = np.zeros(2)
    for (name, values), color in zip(breakdown.items(), breakdown_colors):
        axes[1].bar(breakdown_labels, values, bottom=bottom, label=name, color=color)
        bottom += values
    axes[1].axhline(28.7, color="#e07a3f", linestyle="--", linewidth=1.5)
    axes[1].set_ylabel("Estimated core construction state (GiB)")
    axes[1].set_title("graph500-25 memory breakdown")
    axes[1].grid(axis="y", alpha=0.2)
    axes[1].legend(frameon=False, fontsize=8)
    for index, value in enumerate(bottom):
        axes[1].text(index, value + 0.6, f"{value:.2f} GiB", ha="center", va="bottom", fontsize=9, fontweight="bold")

    fig.suptitle("Radix partitioning bounds dense CSR count memory by active workers", fontsize=14)
    fig.text(
        0.5,
        0.015,
        "Core estimate only. DuckDB blocks, joins, query state, and operating-system memory are additional.",
        ha="center",
        fontsize=9,
        color="#5f6368",
    )
    fig.tight_layout(rect=(0, 0.04, 1, 0.95))
    memory_plot_path = args.output_dir / "graph500_radix_csr_memory.png"
    fig.savefig(memory_plot_path, dpi=180)
    fig.savefig(args.output_dir / "graph500_radix_csr_memory.svg")
    plt.close(fig)

    report_path = args.output_dir / "graph500_radix_csr_comparison.md"
    lines = [
        "# Radix-partitioned CSR construction",
        "",
        "All runs use 8 threads and pass the official Graphalytics BFS distance checks.",
        "",
        "| Dataset | Vertices | Directed endpoint rows | Legacy CSR | Radix CSR | Speedup | Endpoint collection | Active dense counts | Final CSR | Core upper bound |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in records:
        lines.append(
            f"| {row['dataset']} | {row['vertices']:,} | {row['edges']:,} | "
            f"{row['legacy_csr_s']:.2f} s | {row['radix_csr_s']:.2f} s | {row['speedup']:.2f}x | "
            f"{row['endpoint_collection_gib']:.2f} GiB | {row['active_dense_count_gib']:.2f} GiB | "
            f"{row['final_csr_gib']:.2f} GiB | {row['core_memory_upper_bound_gib']:.2f} GiB |"
        )
    lines.extend(
        [
            "",
            "The new builder keeps endpoint rows in DuckDB radix partitions. A worker scans one destination "
            "partition, creates one dense source-count array, fills a compact CSR partition, changes it to sparse "
            "row metadata, and releases the endpoint partition.",
            "",
            f"For graph500-25, CSR construction decreased from {largest['legacy_csr_s']:.2f} seconds to "
            f"{largest['radix_csr_s']:.2f} seconds. This is a {largest['speedup']:.2f}x speedup. The two-pass "
            f"fallback took {two_pass_s:.2f} seconds, so radix construction is {two_pass_s / largest['radix_csr_s']:.2f}x faster.",
            "",
            "The result removes the dense `partition_count x vertex_count` count matrix. Dense count memory is "
            "now bounded by the number of active CSR workers.",
            "The core memory upper bound is the logical endpoint payload plus the final CSR plus one dense count "
            "array per active worker. It does not include DuckDB block, query, or operating-system overhead.",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n")
    print(csv_path)
    print(plot_path)
    print(memory_plot_path)
    print(report_path)


if __name__ == "__main__":
    main()
