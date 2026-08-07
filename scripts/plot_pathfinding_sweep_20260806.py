#!/usr/bin/env python3
import csv
import html
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / "data" / "ldbc-pathfinding" / "results" / "plots"
SUMMARY_PATH = OUT_DIR / "pathfinding_sweep_2026_08_06_summary.csv"


STAT_FILES = [
    "data/ldbc-pathfinding/results/sf30/stats_pairs65536_threads8_1786026691.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs65536_threads8_1786026798.csv",
    "data/ldbc-pathfinding/results/sf30/stats_pairs131072_threads8_1786026914.csv",
    "data/ldbc-pathfinding/results/sf30/stats_pairs262144_threads8_1786026982.csv",
    "data/ldbc-pathfinding/results/sf30/stats_pairs131072_threads4_1786027050.csv",
    "data/ldbc-pathfinding/results/sf30/stats_pairs131072_threads16_1786027113.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs131072_threads8_1786027193.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs131072_threads16_1786027278.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs131072_threads16_1786027256.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs262144_threads8_1786027432.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs262144_threads16_1786027475.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs262144_threads16_1786027537.csv",
    "data/ldbc-pathfinding/results/sf1/stats_pairs1024_threads8_1786027911.csv",
    "data/ldbc-pathfinding/results/sf30/stats_pairs65536_threads8_1786027949.csv",
    "data/ldbc-pathfinding/results/sf30/stats_pairs65536_threads8_1786027959.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs65536_threads8_1786028002.csv",
    "data/ldbc-pathfinding/results/sf100/stats_pairs65536_threads8_1786028009.csv",
]

COLORS = {
    "operator": "#2f6fbb",
    "pushpull_operator": "#1b8a5a",
    "bidirectional_operator": "#c75146",
    "scalar": "#7a5ccf",
    "recursive": "#7a7a7a",
}

MODE_LABELS = {
    "operator": "Regular",
    "pushpull_operator": "Push/pull",
    "bidirectional_operator": "Bidirectional",
    "scalar": "Scalar",
    "recursive": "Recursive SQL",
}


def load_rows():
    rows = []
    for file_name in STAT_FILES:
        path = REPO_ROOT / file_name
        if not path.exists():
            continue
        with path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                row = dict(row)
                row["workload"] = "duplicate-heavy" if row["pair_table"] == "benchmark_pairs_dup_65536" else "random"
                row["query_s"] = float(row["query_mean_s"])
                row["sf"] = row["scale_factor"]
                row["pairs"] = int(row["pair_count"])
                row["threads_i"] = int(row["threads"])
                row["dedupe_i"] = int(row["deduplicate_pairs"])
                rows.append(row)
    return rows


def write_summary(rows):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fields = ["sf", "workload", "pairs", "threads", "deduplicate_pairs", "mode", "query_mean_s", "repeats",
              "reachable_count", "max_len", "pair_table"]
    with SUMMARY_PATH.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "sf": row["sf"],
                "workload": row["workload"],
                "pairs": row["pairs"],
                "threads": row["threads"],
                "deduplicate_pairs": row["deduplicate_pairs"],
                "mode": row["mode"],
                "query_mean_s": row["query_mean_s"],
                "repeats": row["repeats"],
                "reachable_count": row["reachable_count"],
                "max_len": row["max_len"],
                "pair_table": row["pair_table"],
            })


def svg_bar_chart(path, title, subtitle, groups, modes, y_label="Query time (s)", log_scale=False):
    width = 1120
    height = 660
    margin = {"left": 82, "right": 36, "top": 92, "bottom": 122}
    plot_w = width - margin["left"] - margin["right"]
    plot_h = height - margin["top"] - margin["bottom"]
    values = [value for group in groups for value in group["values"].values() if value is not None]
    max_v = max(values) if values else 1.0
    min_v = min(values) if values else 0.0

    def y_pos(value):
        if log_scale:
            import math
            floor = max(min_v * 0.7, 0.001)
            lo = math.log10(floor)
            hi = math.log10(max_v * 1.2)
            return margin["top"] + (hi - math.log10(max(value, floor))) / (hi - lo) * plot_h
        return margin["top"] + (max_v * 1.18 - value) / (max_v * 1.18) * plot_h

    group_gap = 34
    inner_gap = 8
    group_w = (plot_w - group_gap * (len(groups) - 1)) / len(groups)
    bar_w = min(54, (group_w - inner_gap * (len(modes) - 1)) / len(modes))
    chart_bottom = margin["top"] + plot_h

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{margin["left"]}" y="38" font-family="Arial, sans-serif" font-size="24" font-weight="700" fill="#202124">{html.escape(title)}</text>',
        f'<text x="{margin["left"]}" y="64" font-family="Arial, sans-serif" font-size="14" fill="#5f6368">{html.escape(subtitle)}</text>',
        f'<line x1="{margin["left"]}" y1="{chart_bottom}" x2="{width - margin["right"]}" y2="{chart_bottom}" stroke="#b8bec7"/>',
        f'<line x1="{margin["left"]}" y1="{margin["top"]}" x2="{margin["left"]}" y2="{chart_bottom}" stroke="#b8bec7"/>',
    ]

    if log_scale:
        import math
        floor = max(min_v * 0.7, 0.001)
        upper = max_v * 1.2
        start_power = math.ceil(math.log10(floor))
        end_power = math.floor(math.log10(upper))
        ticks = [10 ** power for power in range(start_power, end_power + 1)]
    else:
        ticks = [max_v * t / 5 for t in range(6)]
    for tick in ticks:
        if tick <= 0 or (not log_scale and tick > max_v * 1.18):
            continue
        y = y_pos(tick)
        parts.append(f'<line x1="{margin["left"]}" y1="{y:.1f}" x2="{width - margin["right"]}" y2="{y:.1f}" stroke="#edf0f2"/>')
        label = f"{tick:.2g}" if log_scale else f"{tick:.1f}"
        parts.append(f'<text x="{margin["left"] - 10}" y="{y + 4:.1f}" text-anchor="end" font-family="Arial, sans-serif" font-size="12" fill="#5f6368">{label}</text>')

    for gi, group in enumerate(groups):
        gx = margin["left"] + gi * (group_w + group_gap)
        for mi, mode in enumerate(modes):
            value = group["values"].get(mode)
            if value is None:
                continue
            x = gx + mi * (bar_w + inner_gap)
            y = y_pos(value)
            h = chart_bottom - y
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" fill="{COLORS[mode]}" rx="3"/>')
            parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{y - 6:.1f}" text-anchor="middle" font-family="Arial, sans-serif" font-size="11" fill="#202124">{value:.3g}</text>')
        label_x = gx + (len(modes) * bar_w + (len(modes) - 1) * inner_gap) / 2
        parts.append(f'<text x="{label_x:.1f}" y="{chart_bottom + 30}" text-anchor="middle" font-family="Arial, sans-serif" font-size="13" fill="#202124">{html.escape(group["label"])}</text>')

    legend_x = margin["left"]
    legend_y = height - 46
    for mode in modes:
        parts.append(f'<rect x="{legend_x}" y="{legend_y - 12}" width="12" height="12" fill="{COLORS[mode]}" rx="2"/>')
        parts.append(f'<text x="{legend_x + 18}" y="{legend_y - 2}" font-family="Arial, sans-serif" font-size="13" fill="#202124">{MODE_LABELS[mode]}</text>')
        legend_x += 150
    parts.append(f'<text transform="translate(22 {margin["top"] + plot_h / 2}) rotate(-90)" text-anchor="middle" font-family="Arial, sans-serif" font-size="13" fill="#5f6368">{html.escape(y_label)}</text>')
    parts.append('</svg>')
    path.write_text("\n".join(parts))


def group_values(rows, labels, modes):
    groups = []
    for label, predicate in labels:
        values = {}
        for mode in modes:
            matches = [row for row in rows if row["mode"] == mode and predicate(row)]
            values[mode] = matches[0]["query_s"] if matches else None
        groups.append({"label": label, "values": values})
    return groups


def main():
    rows = load_rows()
    write_summary(rows)
    operator_modes = ["operator", "pushpull_operator", "bidirectional_operator"]

    svg_bar_chart(
        OUT_DIR / "random_operator_sweep.svg",
        "Random pair sweep: physical operators",
        "Clean wall-clock query timing, metrics disabled. SF100 stress points are single-repeat.",
        group_values(rows, [
            ("SF30 65k 8t", lambda r: r["workload"] == "random" and r["sf"] == "30" and r["pairs"] == 65536 and r["threads_i"] == 8),
            ("SF30 131k 8t", lambda r: r["workload"] == "random" and r["sf"] == "30" and r["pairs"] == 131072 and r["threads_i"] == 8),
            ("SF30 171k 8t", lambda r: r["workload"] == "random" and r["sf"] == "30" and r["pairs"] == 170654 and r["threads_i"] == 8),
            ("SF100 65k 8t", lambda r: r["workload"] == "random" and r["sf"] == "100" and r["pairs"] == 65536 and r["threads_i"] == 8),
            ("SF100 131k 8t", lambda r: r["workload"] == "random" and r["sf"] == "100" and r["pairs"] == 131072 and r["threads_i"] == 8),
            ("SF100 262k 8t", lambda r: r["workload"] == "random" and r["sf"] == "100" and r["pairs"] == 262144 and r["threads_i"] == 8),
        ], operator_modes),
        operator_modes,
    )

    svg_bar_chart(
        OUT_DIR / "thread_scaling_sf30_131k.svg",
        "SF30 / 131k random pairs: thread scaling",
        "Regular MS-BFS is best at 4 threads in this sweep; 8 and 16 threads do not improve wall time.",
        group_values(rows, [
            ("4 threads", lambda r: r["workload"] == "random" and r["sf"] == "30" and r["pairs"] == 131072 and r["threads_i"] == 4),
            ("8 threads", lambda r: r["workload"] == "random" and r["sf"] == "30" and r["pairs"] == 131072 and r["threads_i"] == 8),
            ("16 threads", lambda r: r["workload"] == "random" and r["sf"] == "30" and r["pairs"] == 131072 and r["threads_i"] == 16),
        ], operator_modes),
        operator_modes,
    )

    svg_bar_chart(
        OUT_DIR / "duplicate_heavy_dedupe.svg",
        "Duplicate-heavy workload: global pair dedupe",
        "65,536 rows repeating 1,024 unique source/destination pairs. Dedupe collapses redundant BFS work.",
        group_values(rows, [
            ("SF30 off", lambda r: r["workload"] == "duplicate-heavy" and r["sf"] == "30" and r["dedupe_i"] == 0),
            ("SF30 on", lambda r: r["workload"] == "duplicate-heavy" and r["sf"] == "30" and r["dedupe_i"] == 1),
            ("SF100 off", lambda r: r["workload"] == "duplicate-heavy" and r["sf"] == "100" and r["dedupe_i"] == 0),
            ("SF100 on", lambda r: r["workload"] == "duplicate-heavy" and r["sf"] == "100" and r["dedupe_i"] == 1),
        ], operator_modes),
        operator_modes,
        log_scale=True,
    )

    svg_bar_chart(
        OUT_DIR / "recursive_baseline_sf1_1k.svg",
        "SF1 / 1k random pairs: recursive SQL baseline",
        "Recursive SQL is orders of magnitude slower even on a small baseline.",
        group_values(rows, [
            ("SF1 1k", lambda r: r["workload"] == "random" and r["sf"] == "1" and r["pairs"] == 1024 and r["threads_i"] == 8),
        ], ["scalar", "operator", "pushpull_operator", "bidirectional_operator", "recursive"]),
        ["scalar", "operator", "pushpull_operator", "bidirectional_operator", "recursive"],
        log_scale=True,
    )

    print(SUMMARY_PATH)
    for path in sorted(OUT_DIR.glob("*.svg")):
        print(path)


if __name__ == "__main__":
    main()
