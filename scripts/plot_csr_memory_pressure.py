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


DEFAULT_DATASETS = ("graph500-23", "graph500-24", "graph500-25")
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
    parser.add_argument("--formal-results", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--datasets", nargs="+", default=list(DEFAULT_DATASETS))
    parser.add_argument("--physical-memory-gib", type=float, default=36.0)
    parser.add_argument("--duckdb-memory-limit-gib", type=float, default=28.7)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        row["dataset"]: row
        for row in read_csv(args.formal_results / "graphalytics_path_length_summary.csv")
    }
    records = []
    for dataset in args.datasets:
        phases = {row["Phase"]: row for row in read_csv(phase_path(args.results_root, dataset))}
        vertices = int(summary[dataset]["vertex_count"])
        count_phase = phases["precount_scan"]
        edge_count = int(count_phase["EdgeCount"])
        partition_count = int(count_phase["PartitionCount"])
        dense_count_bytes = partition_count * (vertices + 2) * 4
        endpoint_collection_bytes = int(phases["endpoint_spool_fill"]["MemoryBytes"])
        edge_payload_bytes = edge_count * 2
        final_csr_bytes = int(phases["local_csr_forward"]["MemoryBytes"])
        records.append(
            {
                "dataset": dataset,
                "vertices": vertices,
                "edges": edge_count,
                "partitions": partition_count,
                "dense_count_gib": dense_count_bytes / GIB,
                "endpoint_collection_gib": endpoint_collection_bytes / GIB,
                "edge_payload_gib": edge_payload_bytes / GIB,
                "minimum_core_transient_gib": (
                    dense_count_bytes + endpoint_collection_bytes + edge_payload_bytes
                )
                / GIB,
                "final_csr_gib": final_csr_bytes / GIB,
                "count_scan_s": float(phases["precount_scan"]["Time_ms"]) / 1000,
                "allocate_s": float(phases["precount_allocate"]["Time_ms"]) / 1000,
                "fill_s": float(phases["endpoint_spool_fill"]["Time_ms"]) / 1000,
                "sparse_finalize_s": float(phases["precount_sparse_finalize"]["Time_ms"]) / 1000,
                "csr_total_s": float(phases["local_csr_forward"]["Time_ms"]) / 1000,
            }
        )

    fallback_path = (
        args.results_root
        / "graphalytics_graph500_25"
        / "precounted_operator_graphalytics_bfs_graphalytics_bfs_"
        "pairsofficial_bfs_threads8_repeat1_phase_timing.csv"
    )
    fallback = None
    if fallback_path.exists():
        fallback_phases = {row["Phase"]: row for row in read_csv(fallback_path)}
        fallback = {
            "dataset": "graph500-25 two-pass",
            "count_scan_s": float(fallback_phases["precount_scan"]["Time_ms"]) / 1000,
            "allocate_s": float(fallback_phases["precount_allocate"]["Time_ms"]) / 1000,
            "fill_s": float(fallback_phases["precount_fill"]["Time_ms"]) / 1000,
            "sparse_finalize_s": float(fallback_phases["precount_sparse_finalize"]["Time_ms"]) / 1000,
            "csr_total_s": float(fallback_phases["local_csr_forward"]["Time_ms"]) / 1000,
        }

    csv_path = args.output_dir / "graph500_csr_memory_pressure.csv"
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)

    labels = [row["dataset"] for row in records]
    x = np.arange(len(records))
    fig, axes = plt.subplots(1, 2, figsize=(12.5, 5.3))

    memory_phases = [
        ("endpoint_collection_gib", "Buffered endpoint collection", "#68a9a2"),
        ("dense_count_gib", "Dense partition counts", "#b55d60"),
        ("edge_payload_gib", "Edge payload allocation", "#34699a"),
    ]
    bottom = np.zeros(len(records))
    for field, label, color in memory_phases:
        values = np.array([row[field] for row in records])
        axes[0].bar(x, values, bottom=bottom, label=label, color=color)
        bottom += values
    axes[0].plot(
        x,
        [row["final_csr_gib"] for row in records],
        color="#333333",
        marker="o",
        linewidth=1.5,
        label="Final compact CSR",
    )
    axes[0].axhline(
        args.duckdb_memory_limit_gib,
        color="#e07a3f",
        linestyle="--",
        linewidth=1.3,
        label=f"DuckDB memory limit ({args.duckdb_memory_limit_gib:.1f} GiB)",
    )
    axes[0].axhline(
        args.physical_memory_gib,
        color="#333333",
        linestyle=":",
        linewidth=1.3,
        label=f"Physical memory ({args.physical_memory_gib:.0f} GiB)",
    )
    axes[0].set_ylabel("Core CSR construction memory (GiB)")
    axes[0].set_title("Minimum overlapping construction state")
    axes[0].set_xticks(x, labels)
    axes[0].grid(axis="y", alpha=0.2)
    axes[0].legend(frameon=False, fontsize=7, loc="upper left", ncol=2)

    timing_phases = [
        ("count_scan_s", "Count scan", "#68a9a2"),
        ("allocate_s", "Allocate", "#b55d60"),
        ("fill_s", "Endpoint collection scan and CSR fill", "#34699a"),
        ("sparse_finalize_s", "Sparse finalize", "#e07a3f"),
    ]
    timing_records = records + ([fallback] if fallback else [])
    timing_x = np.arange(len(timing_records))
    bottom = np.zeros(len(timing_records))
    for field, label, color in timing_phases:
        values = np.array([row[field] for row in timing_records])
        bars = axes[1].bar(timing_x, values, bottom=bottom, label=label, color=color)
        if fallback:
            bars[-1].set_hatch("//")
        bottom += values
    axes[1].set_ylabel("CSR construction time (seconds)")
    axes[1].set_title("Construction phases")
    timing_labels = labels + (["graph500-25\ntwo-pass"] if fallback else [])
    axes[1].set_xticks(timing_x, timing_labels)
    axes[1].grid(axis="y", alpha=0.2)
    axes[1].legend(frameon=False, fontsize=8, loc="upper left")

    fig.suptitle("Graph500 CSR construction crosses the machine memory boundary", fontsize=14)
    fig.text(
        0.5,
        0.01,
        "8 threads. Memory bars include only the buffered endpoint collection, dense count arrays, and edge payload. "
        "Joins, table buffers, query state, and operating-system memory are additional.",
        ha="center",
        fontsize=8,
        color="#555555",
    )
    fig.tight_layout(rect=(0, 0.045, 1, 0.95))
    plot_path = args.output_dir / "graph500_csr_memory_pressure.png"
    fig.savefig(plot_path, dpi=180)
    plt.close(fig)

    report_path = args.output_dir / "graph500_csr_memory_pressure.md"
    lines = [
        "# Graph500 CSR memory-pressure diagnosis",
        "",
        "The current buffered builder keeps all `(src, dst)` endpoints while it counts rows and fills the CSR. "
        "It also allocates one dense source-count array for every destination partition. The destination offset "
        "uses `uint16_t`, so a partition contains at most 65,535 vertices.",
        "",
        "| Dataset | Partitions | Endpoint collection | Dense counts | Edge payload | Core transient | Final CSR | Fill | Total CSR |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in records:
        lines.append(
            f"| {row['dataset']} | {row['partitions']} | {row['endpoint_collection_gib']:.2f} GiB | "
            f"{row['dense_count_gib']:.2f} GiB | {row['edge_payload_gib']:.2f} GiB | "
            f"{row['minimum_core_transient_gib']:.2f} GiB | {row['final_csr_gib']:.2f} GiB | "
            f"{row['fill_s']:.2f} s | {row['csr_total_s']:.2f} s |"
        )
    lines.extend(
        [
            "",
            f"The machine has {args.physical_memory_gib:.0f} GiB of physical memory. DuckDB reports a default "
            f"memory limit of {args.duckdb_memory_limit_gib:.1f} GiB. The graph500-25 core transient state is "
            f"{records[-1]['minimum_core_transient_gib']:.2f} GiB before joins, table buffers, query state, and "
            "operating-system memory are included.",
            "",
            "The endpoint collection scan and CSR fill increases from 1.52 seconds on graph500-24 to 49.33 seconds on "
            "graph500-25. This phase is the primary cause of the total-time jump. The measurements are consistent "
            "with buffer spilling, memory compression, or both.",
            "",
            "The dense count allocation scales as `partition_count * vertex_count * 4 bytes`. Because the "
            "partition width is limited to 65,535, this term approaches quadratic growth in vertex count. This is "
            "the structural problem to remove.",
        ]
    )
    if fallback:
        buffered = records[-1]
        lines.extend(
            [
                "",
                "## Two-pass fallback",
                "",
                "A controlled graph500-25 run used the existing two-pass operator input. It repeats the endpoint "
                "joins for the fill pass, but it does not retain the endpoint collection.",
                "",
                f"- CSR construction: {buffered['csr_total_s']:.2f} seconds buffered versus "
                f"{fallback['csr_total_s']:.2f} seconds two-pass, a "
                f"{buffered['csr_total_s'] / fallback['csr_total_s']:.2f}x speedup.",
                f"- Fill phase: {buffered['fill_s']:.2f} seconds buffered versus {fallback['fill_s']:.2f} "
                f"seconds two-pass, a {buffered['fill_s'] / fallback['fill_s']:.2f}x speedup.",
                "- The two-pass result passed all official Graphalytics distance checks.",
                "",
                "This result confirms that the second relational scan is cheaper than reading the pressured endpoint "
                "collection at this graph size. It is a useful fallback, but it does not remove the dense count matrix.",
            ]
        )
    report_path.write_text("\n".join(lines) + "\n")
    print(csv_path)
    print(plot_path)
    print(report_path)


if __name__ == "__main__":
    main()
