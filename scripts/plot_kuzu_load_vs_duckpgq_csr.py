#!/usr/bin/env python3
import argparse
import csv
import json
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


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def format_seconds(value):
    return f"{value:.2f} s"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--formal-results", type=Path, required=True)
    parser.add_argument("--kuzu-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--datasets", nargs="+", default=list(DEFAULT_DATASETS))
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        row["dataset"]: row
        for row in read_csv(args.formal_results / "graphalytics_path_length_summary.csv")
    }
    diagnostics = {
        row["dataset"]: row
        for row in read_csv(args.formal_results / "graphalytics_path_length_diagnostics.csv")
    }

    rows = []
    for dataset in args.datasets:
        metadata_path = args.kuzu_dir / f"{dataset}.metadata.json"
        metadata = json.loads(metadata_path.read_text())
        directed_rows = int(summary[dataset]["directed_edge_count"])
        duckpgq_csr_s = float(diagnostics[dataset]["csr_build_s"])
        kuzu_import_s = float(metadata["system_import_s"])
        kuzu_total_s = float(metadata["system_total_prepare_s"])
        rows.append(
            {
                "dataset": dataset,
                "vertices": int(summary[dataset]["vertex_count"]),
                "directed_rows": directed_rows,
                "duckpgq_csr_s": duckpgq_csr_s,
                "kuzu_schema_s": float(metadata["system_schema_s"]),
                "kuzu_vertex_import_s": float(metadata["system_vertex_import_s"]),
                "kuzu_edge_import_s": float(metadata["system_edge_import_forward_s"])
                + float(metadata["system_edge_import_reverse_s"]),
                "kuzu_import_s": kuzu_import_s,
                "kuzu_validation_s": float(metadata["system_validation_s"]),
                "kuzu_total_prepare_s": kuzu_total_s,
                "kuzu_over_duckpgq": kuzu_import_s / duckpgq_csr_s,
                "duckpgq_mrows_s": directed_rows / duckpgq_csr_s / 1_000_000,
                "kuzu_mrows_s": directed_rows / kuzu_import_s / 1_000_000,
                "kuzu_database_gib": int(metadata["system_database_bytes"]) / (1024**3),
                "kuzu_version": metadata["system_version"],
            }
        )

    csv_path = args.output_dir / "kuzu_load_vs_duckpgq_csr.csv"
    with csv_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    labels = [row["dataset"] for row in rows]
    x = np.arange(len(rows))
    width = 0.34
    fig, axes = plt.subplots(1, 2, figsize=(12, 5.2))

    duckpgq_values = [row["duckpgq_csr_s"] for row in rows]
    kuzu_values = [row["kuzu_import_s"] for row in rows]
    axes[0].bar(x - width / 2, duckpgq_values, width, label="DuckPGQ runtime CSR", color="#34699a")
    axes[0].bar(x + width / 2, kuzu_values, width, label="Kuzu fresh persistent load", color="#408a72")
    axes[0].set_yscale("log")
    axes[0].set_ylabel("Construction time (seconds, log scale)")
    axes[0].set_xticks(x, labels)
    axes[0].set_title("Construction time")
    axes[0].grid(axis="y", alpha=0.22)
    axes[0].legend(frameon=False)
    for index, row in enumerate(rows):
        axes[0].text(
            x[index] + width / 2,
            row["kuzu_import_s"] * 1.08,
            f"{row['kuzu_over_duckpgq']:.1f}x",
            ha="center",
            va="bottom",
            fontsize=8,
            color="#285e4d",
        )

    axes[1].bar(
        x - width / 2,
        [row["duckpgq_mrows_s"] for row in rows],
        width,
        label="DuckPGQ runtime CSR",
        color="#34699a",
    )
    axes[1].bar(
        x + width / 2,
        [row["kuzu_mrows_s"] for row in rows],
        width,
        label="Kuzu fresh persistent load",
        color="#408a72",
    )
    axes[1].set_ylabel("Directed input rows per second (millions)")
    axes[1].set_xticks(x, labels)
    axes[1].set_title("Effective construction throughput")
    axes[1].grid(axis="y", alpha=0.22)
    axes[1].legend(frameon=False)

    fig.suptitle("Kuzu persistent graph load versus DuckPGQ runtime CSR construction", fontsize=14)
    fig.text(
        0.5,
        0.01,
        "Same Graphalytics graph and directed adjacency rows, 8 threads. "
        "Kuzu load is persistent; DuckPGQ CSR is in memory and starts from loaded DuckDB tables.",
        ha="center",
        fontsize=8,
        color="#555555",
    )
    fig.tight_layout(rect=(0, 0.045, 1, 0.95))
    plot_path = args.output_dir / "kuzu_load_vs_duckpgq_csr.png"
    fig.savefig(plot_path, dpi=180)
    plt.close(fig)

    report_path = args.output_dir / "kuzu_load_vs_duckpgq_csr.md"
    lines = [
        "# Kuzu load versus DuckPGQ CSR construction",
        "",
        "Both systems used 8 threads and the same Graphalytics Parquet inputs. Kuzu creates persistent native graph storage. "
        "DuckPGQ creates an in-memory CSR after the relational tables are already loaded. The values therefore show two "
        "different graph-preparation choices, not interchangeable operations.",
        "",
        "| Dataset | Vertices | Directed rows | DuckPGQ CSR | Kuzu load | Kuzu / DuckPGQ | Kuzu total | Kuzu DB |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['dataset']} | {row['vertices']:,} | {row['directed_rows']:,} | "
            f"{format_seconds(row['duckpgq_csr_s'])} | {format_seconds(row['kuzu_import_s'])} | "
            f"{row['kuzu_over_duckpgq']:.2f}x | {format_seconds(row['kuzu_total_prepare_s'])} | "
            f"{row['kuzu_database_gib']:.2f} GiB |"
        )
    lines.extend(
        [
            "",
            "Kuzu load includes schema creation, vertex import, and both directed edge imports. Kuzu total also includes "
            "row-count validation and close time. Download time is excluded.",
            "",
            "DuckPGQ is faster at construction on all three datasets. Its advantage falls sharply on graph500-25. "
            "This result is consistent with memory pressure in the runtime CSR build. Kuzu pays more before queries, "
            "but its graph representation is persistent and does not need a query-time CSR build.",
        ]
    )
    report_path.write_text("\n".join(lines) + "\n")
    print(csv_path)
    print(plot_path)
    print(report_path)


if __name__ == "__main__":
    main()
