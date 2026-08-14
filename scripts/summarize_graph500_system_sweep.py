#!/usr/bin/env python3

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


DATASETS = ("graph500-22", "graph500-23", "graph500-24", "graph500-25", "graph500-26")
SYSTEMS = ("duckpgq", "kuzu", "neo4j")
COLORS = {"duckpgq": "#2f6f9f", "kuzu": "#408a72", "neo4j": "#b75c45"}
DUCKPGQ_BUILD_LABELS = {
    "graph500-22": "packed-radix-graph500-22",
    "graph500-23": "packed-radix-final",
    "graph500-24": "packed-radix-final",
    "graph500-25": "packed-radix-final",
    "graph500-26": "packed-radix-graph500-26",
}
CAPACITY_FAILURES = {
    ("graph500-24", "neo4j"): "Java heap OOM during delta-stepping query at 3 GB and 4 GB heap",
    ("graph500-25", "neo4j"): "Java heap OOM during GDS graph projection at 4 GB heap",
    ("graph500-26", "kuzu"): "relationship import failed at 8 threads; native process crashed at 4 threads",
    ("graph500-26", "neo4j"): "not attempted; expected to exceed the fixed 8 GB Neo4j capacity limit",
}


def mean(values):
    return statistics.mean(values) if values else None


def stdev(values):
    return statistics.stdev(values) if len(values) > 1 else 0.0 if values else None


def database_size(path):
    if path.is_file():
        return path.stat().st_size
    if path.is_dir():
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
    return None


def read_structure_build_times(results_root):
    times = {}
    for dataset, run_label in DUCKPGQ_BUILD_LABELS.items():
        result_dir = results_root / f"graphalytics_{dataset.replace('-', '_')}"
        values = []
        for path in result_dir.glob("summary_graphalytics_bfs_*_threads8_modeoperator_*.csv"):
            for row in csv.DictReader(path.open()):
                if row.get("benchmark_run_label") != run_label:
                    continue
                partition_s = row.get("endpoint_radix_partition_s")
                build_s = row.get("endpoint_partition_build_s")
                if partition_s and build_s:
                    values.append(float(partition_s) + float(build_s))
        if values:
            times[(dataset, "duckpgq")] = {
                "mean_s": mean(values),
                "stdev_s": stdev(values),
                "trials": len(values),
                "threads": 8,
                "method": "query-local packed radix CSR build",
            }

        metadata_path = (
            results_root.parent / "systems" / "kuzu" / "graphalytics" / f"{dataset}.metadata.json"
        )
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text())
            forward_s = metadata.get("system_edge_import_forward_s")
            reverse_s = metadata.get("system_edge_import_reverse_s")
            if forward_s is not None and reverse_s is not None:
                times[(dataset, "kuzu")] = {
                    "mean_s": float(forward_s) + float(reverse_s),
                    "stdev_s": None,
                    "trials": 1,
                    "threads": 8,
                    "method": "persistent forward and reverse relationship import",
                }
    return times


def read_trials(results_root, run_label):
    trials = {}
    for dataset in DATASETS:
        result_dir = results_root / f"graphalytics_{dataset.replace('-', '_')}"
        for path in result_dir.glob("summary_graphalytics_bfs_*_threads4_mode*.csv"):
            rows = list(csv.DictReader(path.open()))
            if not rows or rows[0].get("benchmark_run_label") != run_label:
                continue
            system = rows[0]["benchmark_system"]
            run_id = rows[0]["benchmark_run_id"]
            trials[(dataset, system, run_id)] = rows
    return trials


def aggregate_trials(trials, structure_build_times):
    grouped = defaultdict(list)
    for (dataset, system, _), rows in trials.items():
        grouped[(dataset, system)].append(rows)

    output = []
    for dataset in DATASETS:
        for system in SYSTEMS:
            runs = grouped.get((dataset, system), [])
            if not runs:
                failure = CAPACITY_FAILURES.get((dataset, system), "not measured")
                output.append({
                    "dataset": dataset,
                    "system": system,
                    "status": failure,
                    "trials": 0,
                })
                continue

            first_times = []
            first_total_times = []
            warm_trial_means = []
            graph_setup_times = []
            first_row = runs[0][0]
            for rows in runs:
                if system == "duckpgq":
                    first = next(row for row in rows if row["mode"] == "sql_match_cold")
                    warm = [row for row in rows if row["mode"] == "sql_match_warm"]
                else:
                    first = min(rows, key=lambda row: int(row["repeat"]))
                    warm = [row for row in rows if int(row["repeat"]) > 1]
                first_times.append(float(first["query_s"]))
                first_total_times.append(float(first["total_s"]))
                warm_trial_means.append(statistics.mean(float(row["query_s"]) for row in warm))
                if first.get("graph_projection_s"):
                    graph_setup_times.append(float(first["graph_projection_s"]))

            build = structure_build_times.get((dataset, system), {})
            output.append({
                "dataset": dataset,
                "system": system,
                "status": "ok",
                "trials": len(runs),
                "vertices": int(first_row["dataset_metadata_person_rows"]),
                "stored_directed_edges": int(first_row["dataset_metadata_person_knows_person_rows"]),
                "first_query_mean_s": mean(first_times),
                "first_query_stdev_s": stdev(first_times),
                "first_total_mean_s": mean(first_total_times),
                "warm_query_mean_s": mean(warm_trial_means),
                "warm_query_stdev_s": stdev(warm_trial_means),
                "graph_setup_mean_s": mean(graph_setup_times),
                "graph_setup_stdev_s": stdev(graph_setup_times),
                "structure_build_mean_s": build.get("mean_s"),
                "structure_build_stdev_s": build.get("stdev_s"),
                "structure_build_trials": build.get("trials"),
                "structure_build_threads": build.get("threads"),
                "structure_build_method": build.get("method"),
                "import_s": float(first_row["dataset_metadata_system_import_s"])
                if first_row.get("dataset_metadata_system_import_s") else None,
                "database_bytes": int(first_row["dataset_metadata_system_database_bytes"])
                if first_row.get("dataset_metadata_system_database_bytes")
                else database_size(Path(first_row["database"])),
                "reachable_count": int(first_row["reachable_count"]),
                "total_distance": int(first_row["total_len"]),
                "max_distance": int(first_row["max_len"]),
            })
    return output


def format_value(value):
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def write_csv(rows, path):
    fields = [
        "dataset", "system", "status", "trials", "vertices", "stored_directed_edges",
        "first_query_mean_s", "first_query_stdev_s", "first_total_mean_s",
        "warm_query_mean_s", "warm_query_stdev_s", "graph_setup_mean_s",
        "graph_setup_stdev_s", "structure_build_mean_s", "structure_build_stdev_s",
        "structure_build_trials", "structure_build_threads", "structure_build_method",
        "import_s", "database_bytes", "reachable_count",
        "total_distance", "max_distance",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: format_value(row.get(field)) for field in fields})


def svg_text(x, y, value, size=13, anchor="middle", weight=400, color="#202124"):
    escaped = str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return (
        f'<text x="{x}" y="{y}" text-anchor="{anchor}" font-family="Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{color}">{escaped}</text>'
    )


def nice_axis_max(value, ticks=4):
    if value <= 0:
        return 1.0
    raw_step = value / ticks
    magnitude = 10 ** math.floor(math.log10(raw_step))
    normalized = raw_step / magnitude
    if normalized <= 1:
        step = magnitude
    elif normalized <= 1.5:
        step = 1.5 * magnitude
    elif normalized <= 2:
        step = 2 * magnitude
    elif normalized <= 2.5:
        step = 2.5 * magnitude
    elif normalized <= 4:
        step = 4 * magnitude
    elif normalized <= 5:
        step = 5 * magnitude
    else:
        step = 10 * magnitude
    return step * ticks


def draw_bar_panel(parts, rows, x0, top, panel_width, panel_height, key, title, subtitle,
                   systems, y_max):
    by_key = {(row["dataset"], row["system"]): row for row in rows}
    y0 = top + panel_height
    parts.append(svg_text(x0 + panel_width / 2, top - 27, title, 16, weight=700))
    parts.append(svg_text(x0 + panel_width / 2, top - 8, subtitle, 10, color="#5f6368"))
    for tick in range(5):
        value = y_max * tick / 4
        y = y0 - panel_height * tick / 4
        parts.append(
            f'<line x1="{x0}" y1="{y:.1f}" x2="{x0 + panel_width}" y2="{y:.1f}" stroke="#e5e7eb"/>'
        )
        parts.append(svg_text(x0 - 8, y + 4, f"{value:g}", 10, "end", color="#5f6368"))

    group_width = panel_width / len(DATASETS)
    bar_width = min(25, group_width * 0.72 / len(systems))
    for dataset_idx, dataset in enumerate(DATASETS):
        center = x0 + group_width * (dataset_idx + 0.5)
        parts.append(svg_text(center, y0 + 23, dataset.replace("graph500-", "G500-"), 10))
        for system_idx, system in enumerate(systems):
            row = by_key[(dataset, system)]
            offset = (system_idx - (len(systems) - 1) / 2) * bar_width
            x = center + offset - (bar_width - 2) / 2
            value = row.get(key)
            if value is None:
                failure = row.get("status", "")
                if "OOM" in failure:
                    marker = "OOM"
                elif "failed" in failure or "crashed" in failure:
                    marker = "FAIL"
                else:
                    marker = "n/a"
                parts.append(
                    f'<rect x="{x:.1f}" y="{y0 - 6}" width="{bar_width - 2:.1f}" height="6" fill="#d1d5db"/>'
                )
                parts.append(svg_text(x + (bar_width - 2) / 2, y0 - 10, marker, 8, color="#6b7280"))
                continue
            bar_height = panel_height * value / y_max
            y = y0 - bar_height
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width - 2:.1f}" height="{bar_height:.1f}" '
                f'fill="{COLORS[system]}" rx="1"/>'
            )
            label_y = max(top + 9, y - 5)
            parts.append(svg_text(x + (bar_width - 2) / 2, label_y, f"{value:.2f}", 8))


def write_svg(rows, path):
    width, height = 1600, 650
    left, right, top = 72, 28, 132
    gap = 52
    panel_width = (width - left - right - 2 * gap) / 3
    panel_height = 390
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        svg_text(width / 2, 29, "Graph500 single-source BFS and graph-structure construction", 21, weight=700),
        svg_text(width / 2, 51, "Query times: 4 threads, three processes, four warm queries per process", 12, color="#5f6368"),
    ]

    query_values = [
        row.get(key)
        for row in rows
        for key in ("first_query_mean_s", "warm_query_mean_s")
        if row.get(key) is not None
    ]
    query_y_max = nice_axis_max(max(query_values) * 1.08)
    build_values = [row.get("structure_build_mean_s") for row in rows if row.get("structure_build_mean_s")]
    build_y_max = nice_axis_max(max(build_values) * 1.08)
    draw_bar_panel(
        parts, rows, left, top, panel_width, panel_height,
        "first_query_mean_s", "First query (s)", "Query only; setup shown at right", SYSTEMS, query_y_max,
    )
    draw_bar_panel(
        parts, rows, left + panel_width + gap, top, panel_width, panel_height,
        "warm_query_mean_s", "Warm query (s)", "Same y-axis as first query", SYSTEMS, query_y_max,
    )
    draw_bar_panel(
        parts, rows, left + 2 * (panel_width + gap), top, panel_width, panel_height,
        "structure_build_mean_s", "Graph structure build (s)", "Direct phase time, 8 threads", ("duckpgq", "kuzu"), build_y_max,
    )

    legend_y = 83
    for idx, system in enumerate(SYSTEMS):
        x = width / 2 - 180 + idx * 180
        parts.append(f'<rect x="{x}" y="{legend_y - 12}" width="14" height="14" fill="{COLORS[system]}" rx="2"/>')
        parts.append(svg_text(x + 22, legend_y, system, 12, "start"))
    parts.append(svg_text(
        left, 580,
        "DuckPGQ build: query-local packed radix CSR. Kuzu build: persistent forward and reverse relationship import.",
        11, "start", color="#3c4043",
    ))
    parts.append(svg_text(
        left, 600,
        "Neo4j query bars exclude GDS projection. G500-24 and G500-25 exceeded the fixed 8 GB container limit; G500-26 was not attempted.",
        11, "start", color="#5f6368",
    ))
    parts.append(svg_text(
        left, 620,
        "Kuzu G500-26 relationship import failed at 8 threads and crashed at 4 threads. All successful runs match the references.",
        11, "start", color="#5f6368",
    ))
    parts.append("</svg>")
    path.write_text("\n".join(parts))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-root", type=Path, default=Path("data/ldbc-pathfinding/results"))
    parser.add_argument("--run-label", default="graph500-three-system-4t")
    parser.add_argument("--output-dir", type=Path, default=Path("data/ldbc-pathfinding/results/sweeps"))
    args = parser.parse_args()

    structure_build_times = read_structure_build_times(args.results_root)
    rows = aggregate_trials(read_trials(args.results_root, args.run_label), structure_build_times)
    csv_path = args.output_dir / "graph500_three_system_4t_20260813.csv"
    svg_path = args.output_dir / "graph500_three_system_4t_20260813.svg"
    write_csv(rows, csv_path)
    write_svg(rows, svg_path)
    print(csv_path)
    print(svg_path)


if __name__ == "__main__":
    main()
