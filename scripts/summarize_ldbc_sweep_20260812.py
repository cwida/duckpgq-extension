#!/usr/bin/env python3
"""Aggregate and plot the 12 August 2026 LDBC SNB pathfinding sweep."""

import csv
import glob
import html
import math
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "data" / "ldbc-pathfinding" / "results"
OUT = RESULTS / "sweeps"

CURRENT_LABELS = {
    18: {
        "ldbc-sf1-sf100-current-cached-sweep",
        "ldbc-sf1-sf100-current-cached-sweep-dedupe-fix",
    },
    8: {"ldbc-current-matched-8t"},
}

RUN_PRIORITY = {
    "ldbc-sf1-sf100-current-cached-sweep": 1,
    "ldbc-sf1-sf100-current-cached-sweep-dedupe-fix": 2,
    "ldbc-current-matched-8t": 1,
}

DATASETS = {
    "1": (10_295, 173_014),
    "3": (25_066, 528_896),
    "10": (68_673, 1_839_354),
    "30": (170_654, 5_524_302),
    "100": (473_001, 18_655_515),
}

# Best clean, validated 8-thread result recorded before the current CSR cache
# and exact one-source zero-copy changes.
OLD_8T = {
    ("1", "8-source to all"): 0.022000,
    ("3", "8-source to all"): 0.033000,
    ("10", "random 65k"): 1.393000,
    ("10", "source to all"): 0.038000,
    ("10", "8-source to all"): 0.066000,
    ("10", "duplicate 65k"): 0.066667,
    ("30", "random 65k"): 6.083000,
    ("30", "source to all"): 0.087000,
    ("30", "8-source to all"): 0.156000,
    ("30", "duplicate 65k"): 0.163000,
    ("100", "random 65k"): 10.238000,
    ("100", "source to all"): 0.271000,
    ("100", "8-source to all"): 0.419000,
    ("100", "duplicate 65k"): 0.427000,
}


def workload(row):
    if row["query_pattern"] == "sssp":
        return "source to all"
    if row["query_pattern"] == "all_pairs":
        return "8-source to all"
    if row["pair_shape"] == "custom":
        return "duplicate 65k"
    if row["pair_shape"] == "random":
        return "random 65k"
    return None


def load_current():
    selected = {}
    pattern = str(RESULTS / "sf*" / "stats_*_modecached_operator_*.csv")
    for name in glob.glob(pattern):
        with open(name, newline="") as handle:
            rows = list(csv.DictReader(handle))
        if not rows:
            continue
        threads = int(rows[0]["threads"])
        run_label = rows[0].get("benchmark_run_label")
        if run_label not in CURRENT_LABELS.get(threads, set()):
            continue
        for row in rows:
            label = workload(row)
            if label is None:
                continue
            key = (threads, row["scale_factor"], label, row["mode"])
            priority = RUN_PRIORITY[run_label]
            if key not in selected or priority > selected[key][0]:
                selected[key] = (priority, row)
    return {key: value[1] for key, value in selected.items()}


def write_current(rows):
    path = OUT / "ldbc_current_sweep_20260812.csv"
    fields = [
        "threads", "scale_factor", "vertices", "edges", "workload", "pair_count",
        "first_query_s", "warm_mean_s", "warm_stdev_s", "first_to_warm_speedup",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for threads in (8, 18):
            for sf in ("1", "3", "10", "30", "100"):
                for name in ("random 65k", "source to all", "8-source to all", "duplicate 65k"):
                    warm = rows.get((threads, sf, name, "cached_operator"))
                    first = rows.get((threads, sf, name, "operator_build"))
                    if not warm or not first:
                        continue
                    first_s = float(first["query_mean_s"])
                    warm_s = float(warm["query_mean_s"])
                    writer.writerow({
                        "threads": threads,
                        "scale_factor": sf,
                        "vertices": DATASETS[sf][0],
                        "edges": DATASETS[sf][1],
                        "workload": name,
                        "pair_count": warm["pair_count"],
                        "first_query_s": f"{first_s:.6f}",
                        "warm_mean_s": f"{warm_s:.6f}",
                        "warm_stdev_s": warm["query_stdev_s"],
                        "first_to_warm_speedup": f"{first_s / warm_s:.3f}",
                    })
    return path


def write_comparison(rows):
    path = OUT / "ldbc_matched_8t_progress_20260812.csv"
    fields = [
        "scale_factor", "workload", "old_query_s", "current_first_query_s", "current_warm_query_s",
        "first_query_speedup", "warm_query_speedup",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for (sf, name), old_s in OLD_8T.items():
            first = float(rows[(8, sf, name, "operator_build")]["query_mean_s"])
            warm = float(rows[(8, sf, name, "cached_operator")]["query_mean_s"])
            writer.writerow({
                "scale_factor": sf,
                "workload": name,
                "old_query_s": f"{old_s:.6f}",
                "current_first_query_s": f"{first:.6f}",
                "current_warm_query_s": f"{warm:.6f}",
                "first_query_speedup": f"{old_s / first:.3f}",
                "warm_query_speedup": f"{old_s / warm:.3f}",
            })
    return path


def svg_text(x, y, value, size=12, anchor="start", color="#202124", weight=400):
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" text-anchor="{anchor}" '
        f'fill="{color}" font-weight="{weight}">{html.escape(str(value))}</text>'
    )


def write_plot(rows):
    values = []
    for (sf, name), old_s in OLD_8T.items():
        first = float(rows[(8, sf, name, "operator_build")]["query_mean_s"])
        warm = float(rows[(8, sf, name, "cached_operator")]["query_mean_s"])
        values.append((sf, name, old_s, first, warm))

    width, height = 1280, 850
    left, right, top, row_h = 245, 90, 110, 48
    x0, x1 = left, width - right
    min_s, max_s = 0.003, 20.0
    sx = lambda value: x0 + (math.log10(value) - math.log10(min_s)) / (math.log10(max_s) - math.log10(min_s)) * (x1 - x0)
    colors = {"old": "#7a8491", "first": "#d29a38", "warm": "#167c68"}
    body = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;letter-spacing:0}</style>',
        svg_text(width / 2, 34, "LDBC SNB Pathfinding Progress", 23, "middle", weight=700),
        svg_text(width / 2, 60, "Matched 8 threads; current first query includes CSR build, warm query reuses cached CSR", 12, "middle", "#5f6368"),
    ]
    for tick in (0.003, 0.01, 0.03, 0.1, 0.3, 1, 3, 10):
        x = sx(tick)
        body.append(f'<line x1="{x:.1f}" y1="82" x2="{x:.1f}" y2="{height - 62}" stroke="#e4e7eb"/>')
        body.append(svg_text(x, height - 40, f"{tick:g}s", 10, "middle", "#5f6368"))
    for index, (sf, name, old_s, first, warm) in enumerate(values):
        y = top + index * row_h
        body.append(svg_text(left - 16, y + 4, f"SF{sf}  {name}", 11, "end", weight=600))
        body.append(f'<line x1="{sx(warm):.1f}" y1="{y:.1f}" x2="{sx(old_s):.1f}" y2="{y:.1f}" stroke="#c2c7ce" stroke-width="2"/>')
        for value, key, radius in ((old_s, "old", 6), (first, "first", 6), (warm, "warm", 7)):
            body.append(f'<circle cx="{sx(value):.1f}" cy="{y:.1f}" r="{radius}" fill="{colors[key]}" stroke="#ffffff" stroke-width="1"/>')
        body.append(svg_text(sx(old_s) + 10, y - 8, f"{old_s / warm:.1f}x warm", 10, "start", colors["warm"], 700))
    legend_y = height - 12
    lx = left
    for label, key in (("older query", "old"), ("current first query", "first"), ("current warm query", "warm")):
        body.append(f'<circle cx="{lx}" cy="{legend_y - 4}" r="6" fill="{colors[key]}"/>')
        body.append(svg_text(lx + 12, legend_y, label, 10, "start", "#5f6368"))
        lx += 180
    body.append("</svg>")
    path = OUT / "ldbc_matched_8t_progress_20260812.svg"
    path.write_text("\n".join(body))
    return path


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    rows = load_current()
    outputs = [write_current(rows), write_comparison(rows), write_plot(rows)]
    for path in outputs:
        print(path)


if __name__ == "__main__":
    main()
