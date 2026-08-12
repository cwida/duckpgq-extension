#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def dataset_family(dataset):
    if dataset.startswith("datagen-") and dataset.endswith("-zf"):
        return "Datagen zf"
    if dataset.startswith("datagen-") and dataset.endswith("-fb"):
        return "Datagen fb"
    if dataset.startswith("graph500-"):
        return "Graph500"
    return "Real network"


def r_squared(observed, predicted):
    residual = np.sum((observed - predicted) ** 2)
    total = np.sum((observed - np.mean(observed)) ** 2)
    return 1.0 - residual / total


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

    summary_by_dataset = {row["dataset"]: row for row in summary}
    csr_datasets = [row["dataset"] for row in diagnostics]
    vertices = np.array(
        [float(summary_by_dataset[dataset]["vertex_count"]) for dataset in csr_datasets]
    )
    edges = np.array(
        [float(summary_by_dataset[dataset]["directed_edge_count"]) for dataset in csr_datasets]
    )
    csr_time = np.array([float(row["csr_build_s"]) for row in diagnostics])
    log_vertices = np.log10(vertices)
    log_edges = np.log10(edges)
    log_csr_time = np.log10(csr_time)

    vertex_fit = np.polyfit(log_vertices, log_csr_time, 1)
    edge_fit = np.polyfit(log_edges, log_csr_time, 1)
    model_input = np.column_stack(
        [np.ones(len(csr_time)), log_vertices, log_edges]
    )
    combined_fit = np.linalg.lstsq(model_input, log_csr_time, rcond=None)[0]
    predicted_log_time = model_input @ combined_fit
    predicted_time = 10 ** predicted_log_time

    family_styles = {
        "Datagen zf": ("#b55d60", "o"),
        "Datagen fb": ("#34699a", "s"),
        "Graph500": ("#e07a3f", "^"),
        "Real network": ("#408a72", "D"),
    }
    highlight_labels = {
        "datagen-7_8-zf",
        "datagen-8_2-zf",
        "datagen-8_3-zf",
        "datagen-8_9-fb",
        "dota-league",
        "graph500-25",
        "wiki-Talk",
    }

    fig, axes = plt.subplots(1, 3, figsize=(17, 5.6))
    for family, (color, marker) in family_styles.items():
        family_indexes = [
            index
            for index, dataset in enumerate(csr_datasets)
            if dataset_family(dataset) == family
        ]
        axes[0].scatter(
            vertices[family_indexes],
            csr_time[family_indexes],
            color=color,
            marker=marker,
            s=48,
            label=family,
            alpha=0.9,
        )
        axes[1].scatter(
            edges[family_indexes],
            csr_time[family_indexes],
            color=color,
            marker=marker,
            s=48,
            alpha=0.9,
        )
        axes[2].scatter(
            predicted_time[family_indexes],
            csr_time[family_indexes],
            color=color,
            marker=marker,
            s=48,
            alpha=0.9,
        )

    vertex_range = np.geomspace(vertices.min(), vertices.max(), 200)
    edge_range = np.geomspace(edges.min(), edges.max(), 200)
    axes[0].plot(
        vertex_range,
        10 ** np.polyval(vertex_fit, np.log10(vertex_range)),
        color="#333333",
        linewidth=1.2,
        linestyle="--",
    )
    axes[1].plot(
        edge_range,
        10 ** np.polyval(edge_fit, np.log10(edge_range)),
        color="#333333",
        linewidth=1.2,
        linestyle="--",
    )
    identity_range = np.geomspace(
        min(predicted_time.min(), csr_time.min()),
        max(predicted_time.max(), csr_time.max()),
        200,
    )
    axes[2].plot(
        identity_range,
        identity_range,
        color="#333333",
        linewidth=1.2,
        linestyle="--",
    )

    for index, dataset in enumerate(csr_datasets):
        if dataset not in highlight_labels:
            continue
        short_label = dataset.removeprefix("datagen-")
        axes[0].annotate(
            short_label,
            (vertices[index], csr_time[index]),
            xytext=(5, 4),
            textcoords="offset points",
            fontsize=7,
        )
        axes[1].annotate(
            short_label,
            (edges[index], csr_time[index]),
            xytext=(5, 4),
            textcoords="offset points",
            fontsize=7,
        )

    vertex_r2 = r_squared(
        log_csr_time, np.polyval(vertex_fit, log_vertices)
    )
    edge_r2 = r_squared(log_csr_time, np.polyval(edge_fit, log_edges))
    combined_r2 = r_squared(log_csr_time, predicted_log_time)
    axes[0].set_title(f"CSR time versus vertices ($R^2$ = {vertex_r2:.2f})")
    axes[1].set_title(f"CSR time versus edge rows ($R^2$ = {edge_r2:.2f})")
    axes[2].set_title(f"Vertex + edge model ($R^2$ = {combined_r2:.2f})")
    axes[0].set_xlabel("Vertex count")
    axes[1].set_xlabel("CSR input rows")
    axes[2].set_xlabel("Predicted CSR build time (seconds)")
    axes[0].set_ylabel("CSR build time (seconds)")
    axes[1].set_ylabel("CSR build time (seconds)")
    axes[2].set_ylabel("Measured CSR build time (seconds)")
    for ax in axes:
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.grid(alpha=0.2)
    axes[0].legend(frameon=False, fontsize=8, loc="upper left")
    axes[2].text(
        0.04,
        0.96,
        "$log_{10}(T) = "
        f"{combined_fit[0]:.2f} + {combined_fit[1]:.2f}log_{{10}}(V) "
        f"+ {combined_fit[2]:.2f}log_{{10}}(E)$",
        transform=axes[2].transAxes,
        va="top",
        fontsize=8,
    )
    fig.suptitle("Graph scale and cold CSR construction time", fontsize=14)
    fig.text(
        0.5,
        0.01,
        "One metrics-enabled cold run per dataset, 8 threads. Fits use log10 values.",
        ha="center",
        fontsize=8,
        color="#555555",
    )
    fig.tight_layout(rect=(0, 0.035, 1, 0.95))
    fig.savefig(args.output_dir / "graphalytics_csr_build_scaling.png", dpi=180)
    plt.close(fig)


if __name__ == "__main__":
    main()
