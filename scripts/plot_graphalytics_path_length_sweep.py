#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    summary = read_csv(args.input_dir / "graphalytics_path_length_summary.csv")
    comparison = read_csv(args.input_dir / "graphalytics_path_length_vs_count_only.csv")
    diagnostics = read_csv(args.input_dir / "graphalytics_path_length_diagnostics.csv")

    datasets = [row["dataset"] for row in summary]
    x = np.arange(len(datasets))
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(x - 0.2, [float(row["cold_mean_s"]) for row in summary], 0.4, label="CSR cold", color="#34699a")
    ax.bar(x + 0.2, [float(row["warm_mean_s"]) for row in summary], 0.4, label="CSR warm", color="#e07a3f")
    ax.set_yscale("log")
    ax.set_ylabel("Query time (seconds, log scale)")
    ax.set_xticks(x, datasets, rotation=55, ha="right")
    ax.set_title("Graphalytics SQL MATCH: cold and warm path-length queries")
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(args.output_dir / "graphalytics_path_length_cold_warm.png", dpi=180)
    plt.close(fig)

    diagnostic_by_dataset = {row["dataset"]: row for row in diagnostics}
    diagnostic_rows = [diagnostic_by_dataset[dataset] for dataset in datasets]
    phase_names = [
        ("pair_analysis_s", "Pair analysis", "#68a9a2"),
        ("csr_build_s", "CSR build", "#34699a"),
        ("bfs_s", "BFS", "#e07a3f"),
        ("scatter_s", "Scatter", "#b55d60"),
        ("other_wall_s", "Other wall time", "#a5a5a5"),
    ]
    fig, ax = plt.subplots(figsize=(14, 6))
    bottom = np.zeros(len(datasets))
    for field, label, color in phase_names:
        values = np.array([float(row[field]) for row in diagnostic_rows])
        ax.bar(x, values, bottom=bottom, label=label, color=color)
        bottom += values
    ax.set_yscale("log")
    ax.set_ylabel("Diagnostic cold-query time (seconds, log scale)")
    ax.set_xticks(x, datasets, rotation=55, ha="right")
    ax.set_title("Cold-query phase attribution")
    ax.legend(frameon=False, ncol=5, loc="upper left")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(args.output_dir / "graphalytics_path_length_cold_phases.png", dpi=180)
    plt.close(fig)

    labels = [row["dataset"] for row in comparison]
    x = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(10, 5))
    width = 0.25
    fields = [
        ("ratio_cold_mean_s", "Cold", "#34699a"),
        ("ratio_warm_mean_s", "Warm", "#e07a3f"),
        ("ratio_q10_amortized_mean_s", "10-query average", "#68a9a2"),
    ]
    for index, (field, label, color) in enumerate(fields):
        ax.bar(x + (index - 1) * width, [float(row[field]) for row in comparison], width, label=label, color=color)
    ax.axhline(1.0, color="#333333", linewidth=1)
    ax.set_ylabel("Path-length time / count-only time")
    ax.set_xticks(x, labels, rotation=30, ha="right")
    ax.set_title("Cost of path-length projection and aggregation")
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(args.output_dir / "graphalytics_path_length_overhead.png", dpi=180)
    plt.close(fig)


if __name__ == "__main__":
    main()
