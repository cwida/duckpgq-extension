#!/usr/bin/env python3
"""Render the 2026-08-04 through 2026-08-11 pathfinding benchmark summary."""

import csv
import html
import math
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "data" / "ldbc-pathfinding" / "results"
OUT = RESULTS / "weekly_2026_08_11"

INK = "#202124"
MUTED = "#5f6368"
GRID = "#e4e7eb"
GREEN = "#2f7d62"
BLUE = "#356fa8"
GOLD = "#c28b2c"
RED = "#b84a4a"
GRAY = "#aeb4bc"


OPTIMIZATIONS = [
    ("Sparse LocalCSR, 4t", "SF100 random 65k", 19.134, 15.110, "kept"),
    ("Sparse LocalCSR, 8t", "SF100 random 65k", 12.718, 10.461, "kept"),
    ("Sparse LocalCSR, 12t", "SF100 random 65k", 10.608, 8.514, "kept"),
    ("Scheduler cleanup", "SF100 random 65k, 8t", 10.461, 10.347, "kept"),
    ("Grouped batches", "SF10 random 65k, 38t", 2.458, 0.703, "kept"),
    ("Grouped batches", "SF100 random 65k, 38t", 11.564, 9.262, "kept"),
    ("Exact pair dedupe", "SF10 duplicate-heavy 65k", 2.127, 0.066667, "shape-specific"),
    ("Compact CSR allocation", "graph500-22 BFS, 8t", 2.622, 2.103, "kept"),
    ("Compact CSR allocation", "datagen-8_4-fb BFS, 8t", 13.101, 9.780, "kept"),
    ("Pre-counted direct CSR", "graph500-22 BFS, 8t", 1.741, 1.765, "tradeoff"),
]


GRAPHALYTICS = [
    ("dota-league", "real", 61_170, 101_740_626, 0.837, 16),
    ("datagen-7_5-fb", "fb", 633_432, 68_371_494, 0.829, 16),
    ("datagen-7_6-fb", "fb", 754_147, 84_325_976, 1.044, 16),
    ("kgs", "real", 832_247, 35_783_396, 0.428, 24),
    ("datagen-7_9-fb", "fb", 1_387_587, 171_341_046, 2.332, 16),
    ("wiki-Talk", "real", 2_394_385, 5_021_410, 0.266, 24),
    ("graph500-22", "graph500", 2_396_657, 128_311_470, 1.880, 16),
    ("cit-Patents", "real", 3_774_768, 16_518_947, 0.558, 32),
    ("datagen-8_0-fb", "fb", 1_706_561, 215_014_752, 2.892, 24),
    ("datagen-8_1-fb", "fb", 2_072_117, 268_535_644, 3.452, 24),
    ("datagen-8_4-fb", "fb", 3_809_084, 538_958_354, 11.252, 24),
    ("graph500-23", "graph500", 4_610_222, 258_667_354, 5.243, 16),
    ("graph500-24", "graph500", 8_870_942, 520_759_040, 13.858, 24),
    ("datagen-7_7-zf", "zf", 13_180_508, 65_582_534, 2.733, 32),
    ("datagen-7_8-zf", "zf", 16_521_886, 82_050_510, 4.558, 32),
    ("datagen-8_2-zf", "zf", 43_734_497, 212_880_376, 62.275, 16),
    ("datagen-8_3-zf", "zf", 53_525_014, 261_159_818, 98.015, 16),
    ("datagen-8_5-fb", "fb", 4_599_739, 664_053_804, 11.115, 24),
    ("datagen-8_6-fb", "fb", 5_667_674, 843_977_238, 20.642, 16),
    ("datagen-8_9-fb", "fb", 10_572_901, 1_697_363_816, 369.930, 24),
    ("graph500-25", "graph500", 17_062_472, 1_047_205_662, 44.315, 16),
]


CSR_VARIANTS = [
    ("Legacy global", 1.741, 454.85),
    ("One-scan segmented", 0.831, 1360.0),
    ("Pre-counted direct", 1.765, 448.52),
]


SCALAR_OPERATOR_PROGRESSION = [
    ("Initial baseline", "SF1, 128 pairs", "4 threads", 0.012333, 0.012333),
    ("Initial scale-up", "SF1, 1,024 pairs", "4 threads", 0.016000, 0.015000),
    ("Larger random batch", "SF100, 65,536 pairs", "16 threads", 60.204000, 10.060000),
    ("Before source grouping", "SF10, 68,673 targets", "8 threads", 0.652667, 0.773667),
    ("After source grouping", "SF10, 68,673 targets", "8 threads", 0.652667, 0.038000),
]


# These phase values come from metrics-enabled LDBC SNB runs. "Other" is the
# query time that the current phase timers do not cover.
LDBC_POINT_BREAKDOWN = [
    ("Random pairs", 1.393000, {"Local CSR": 0.005369, "BFS": 1.313438, "Pair work": 0.0}),
    ("Duplicate rows", 2.127000, {"Local CSR": 0.005158, "BFS": 2.042915, "Pair work": 0.0}),
    ("After dedupe", 0.066667, {"Local CSR": 0.005297, "BFS": 0.031763, "Pair work": 0.001020}),
]


LDBC_SSSP_BREAKDOWN = [
    ("Before source grouping", 0.773667, {"Local CSR": 0.005100, "BFS": 0.688420, "Group build": 0.0}),
    ("After source grouping", 0.038000, {"Local CSR": 0.005120, "BFS": 0.002550, "Group build": 0.001136}),
]


LDBC_APSP_BREAKDOWN = [
    ("SF1", 13.205000, {"Local CSR": 0.001197, "Group build": 1.872960, "BFS": 9.807100}),
    ("SF3", 55.043000, {"Local CSR": 0.003413, "Group build": 12.184600, "BFS": 30.116679}),
    ("SF10", 572.859000, {"Local CSR": 0.016528, "Group build": 129.545000, "BFS": 139.360632}),
]


PHASE_COLORS = {
    "Local CSR": BLUE,
    "BFS": GREEN,
    "Pair work": GOLD,
    "Group build": GOLD,
    "Other": GRAY,
}


def esc(value):
    return html.escape(str(value))


def svg_text(x, y, value, size=12, anchor="start", color=INK, weight=400, rotate=None):
    transform = "" if rotate is None else f' transform="rotate({rotate} {x} {y})"'
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" text-anchor="{anchor}" '
        f'fill="{color}" font-weight="{weight}"{transform}>{esc(value)}</text>'
    )


def svg_doc(width, height, body):
    return "\n".join([
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;letter-spacing:0}</style>',
        *body,
        "</svg>",
    ])


def write_optimization_plot():
    width, height = 1240, 650
    left, right, top, row_h = 360, 155, 100, 48
    axis_w = width - left - right
    x_min, x_max = -5.0, 100.0
    x = lambda value: left + (value - x_min) / (x_max - x_min) * axis_w
    body = [
        svg_text(width / 2, 34, "Measured Optimization Impact", 22, "middle", weight=700),
        svg_text(width / 2, 58, "Query-time reduction within each controlled workload; higher is better", 12, "middle", MUTED),
    ]
    for tick in [0, 20, 40, 60, 80, 100]:
        tx = x(tick)
        body.append(f'<line x1="{tx:.1f}" y1="{top - 20}" x2="{tx:.1f}" y2="{height - 58}" stroke="{GRID}"/>')
        body.append(svg_text(tx, height - 36, f"{tick}%", 11, "middle", MUTED))
    zero_x = x(0)
    body.append(f'<line x1="{zero_x:.1f}" y1="{top - 20}" x2="{zero_x:.1f}" y2="{height - 58}" stroke="{INK}"/>')
    for idx, (name, workload, before, after, status) in enumerate(OPTIMIZATIONS):
        y = top + idx * row_h
        reduction = (before - after) / before * 100
        speedup = before / after
        color = RED if reduction < 0 else GOLD if status == "shape-specific" else GREEN
        body.append(svg_text(left - 16, y + 2, name, 12, "end", INK, 600))
        body.append(svg_text(left - 16, y + 19, workload, 10, "end", MUTED))
        bx = min(x(0), x(reduction))
        bw = max(2, abs(x(reduction) - x(0)))
        body.append(f'<rect x="{bx:.1f}" y="{y - 13:.1f}" width="{bw:.1f}" height="24" fill="{color}" rx="2"/>')
        if reduction > 65:
            label_x, anchor, label_color = x(reduction) - 8, "end", "#ffffff"
        elif reduction < 0:
            label_x, anchor, label_color = x(0) + 8, "start", INK
        else:
            label_x, anchor, label_color = x(reduction) + 8, "start", INK
        suffix = f"{reduction:.1f}%  ({speedup:.2f}x; {before:.3g}s to {after:.3g}s)"
        body.append(svg_text(label_x, y + 4, suffix, 11, anchor, label_color))
    body.append(svg_text(left, height - 10, "Gold is input-shape specific. Red is an architectural tradeoff, not a retained speedup.", 11, "start", MUTED))
    path = OUT / "weekly_optimization_impact.svg"
    path.write_text(svg_doc(width, height, body))


def write_graphalytics_plot():
    width, height = 1140, 720
    left, right, top, bottom = 100, 240, 90, 95
    plot_w, plot_h = width - left - right, height - top - bottom
    min_x, max_x = 5_000_000, 2_000_000_000
    min_y, max_y = 0.2, 500.0
    sx = lambda value: left + (math.log10(value) - math.log10(min_x)) / (math.log10(max_x) - math.log10(min_x)) * plot_w
    sy = lambda value: top + plot_h - (math.log10(value) - math.log10(min_y)) / (math.log10(max_y) - math.log10(min_y)) * plot_h
    colors = {"real": BLUE, "fb": GREEN, "zf": GOLD, "graph500": RED}
    labels = {"real": "real-world", "fb": "datagen FB", "zf": "datagen ZF", "graph500": "Graph500"}
    body = [
        svg_text(width / 2, 34, "Graphalytics BFS Scaling", 22, "middle", weight=700),
        svg_text(width / 2, 58, "Best clean repeat-3 mean per dataset over the tested 8/16/24/32-thread sweep", 12, "middle", MUTED),
    ]
    for tick, label in [(5e6, "5M"), (1e7, "10M"), (5e7, "50M"), (1e8, "100M"), (5e8, "500M"), (1e9, "1B"), (2e9, "2B")]:
        tx = sx(tick)
        body.append(f'<line x1="{tx:.1f}" y1="{top}" x2="{tx:.1f}" y2="{top + plot_h}" stroke="{GRID}"/>')
        body.append(svg_text(tx, top + plot_h + 25, label, 11, "middle", MUTED))
    for tick in [0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500]:
        ty = sy(tick)
        body.append(f'<line x1="{left}" y1="{ty:.1f}" x2="{left + plot_w}" y2="{ty:.1f}" stroke="{GRID}"/>')
        body.append(svg_text(left - 12, ty + 4, f"{tick:g}s", 11, "end", MUTED))
    for name, family, vertices, edges, query_s, threads in GRAPHALYTICS:
        px, py = sx(edges), sy(query_s)
        body.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="6" fill="{colors[family]}" stroke="#ffffff" stroke-width="1.5"/>')
        short = name.replace("datagen-", "d-")
        body.append(svg_text(px + 8, py - 7, short, 9, "start", INK))
    body.append(svg_text(left + plot_w / 2, height - 35, "Materialized traversal edges (log scale)", 12, "middle", MUTED))
    body.append(svg_text(24, top + plot_h / 2, "End-to-end query time (log scale)", 12, "middle", MUTED, rotate=-90))
    ly = top + 15
    for family in ["real", "fb", "zf", "graph500"]:
        body.append(f'<circle cx="{width - right + 36}" cy="{ly:.1f}" r="6" fill="{colors[family]}"/>')
        body.append(svg_text(width - right + 50, ly + 4, labels[family], 12, "start", INK))
        ly += 28
    body.append(svg_text(width - right + 28, ly + 30, "ZF runtime rises faster because", 11, "start", MUTED))
    body.append(svg_text(width - right + 28, ly + 47, "vertex/output cardinality and depth", 11, "start", MUTED))
    body.append(svg_text(width - right + 28, ly + 64, "matter in addition to edge count.", 11, "start", MUTED))
    path = OUT / "weekly_graphalytics_scaling.svg"
    path.write_text(svg_doc(width, height, body))


def read_kuzu_rows():
    path = RESULTS / "graphalytics_bfs_core_kuzu_duckpgq_8t_comparison.csv"
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def write_kuzu_plot():
    rows = read_kuzu_rows()
    width, height = 1060, 570
    left, right, top, bottom = 85, 35, 90, 110
    plot_w, plot_h = width - left - right, height - top - bottom
    y_min, y_max = 0.05, 20.0
    sy = lambda value: top + plot_h - (math.log10(value) - math.log10(y_min)) / (math.log10(y_max) - math.log10(y_min)) * plot_h
    group_w = plot_w / len(rows)
    body = [
        svg_text(width / 2, 34, "Kuzu vs DuckPGQ: Official Graphalytics BFS", 22, "middle", weight=700),
        svg_text(width / 2, 58, "8 threads, three repeats, steady-state query time; log scale", 12, "middle", MUTED),
    ]
    for tick in [0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20]:
        ty = sy(tick)
        body.append(f'<line x1="{left}" y1="{ty:.1f}" x2="{left + plot_w}" y2="{ty:.1f}" stroke="{GRID}"/>')
        body.append(svg_text(left - 10, ty + 4, f"{tick:g}s", 11, "end", MUTED))
    for idx, row in enumerate(rows):
        cx = left + group_w * (idx + 0.5)
        for offset, key, color in [(-22, "duckpgq_s", BLUE), (22, "kuzu_s", GREEN)]:
            value = float(row[key])
            y = sy(value)
            base = sy(y_min)
            body.append(f'<rect x="{cx + offset - 18:.1f}" y="{y:.1f}" width="36" height="{base - y:.1f}" fill="{color}" rx="2"/>')
        body.append(svg_text(cx, top + plot_h + 24, row["dataset"], 10, "middle", INK, rotate=28))
        body.append(svg_text(cx, top + plot_h + 70, f'{float(row["kuzu_speedup_vs_duckpgq"]):.1f}x', 11, "middle", RED, 600))
    body.append(f'<rect x="{left}" y="{height - 34}" width="12" height="12" fill="{BLUE}"/>')
    body.append(svg_text(left + 18, height - 23, "DuckPGQ", 11, "start"))
    body.append(f'<rect x="{left + 110}" y="{height - 34}" width="12" height="12" fill="{GREEN}"/>')
    body.append(svg_text(left + 128, height - 23, "Kuzu", 11, "start"))
    body.append(svg_text(width - right, height - 23, "Red labels: Kuzu speedup", 11, "end", MUTED))
    path = OUT / "weekly_kuzu_comparison.svg"
    path.write_text(svg_doc(width, height, body))


def write_csr_plot():
    width, height = 1050, 540
    body = [
        svg_text(width / 2, 34, "CSR Construction Tradeoff on Graph500-22", 22, "middle", weight=700),
        svg_text(width / 2, 58, "8 threads, official source-to-all BFS; segmented memory is approximate", 12, "middle", MUTED),
    ]
    panels = [("End-to-end query", "seconds", 2.0, 90, BLUE, 1), ("Retained CSR memory", "MB", 1500.0, 570, GOLD, 0)]
    for title, unit, maximum, left, color, decimals in panels:
        top, plot_h, plot_w = 105, 310, 390
        body.append(svg_text(left + plot_w / 2, 88, title, 15, "middle", INK, 600))
        for tick in range(5):
            value = maximum * tick / 4
            y = top + plot_h - value / maximum * plot_h
            body.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" stroke="{GRID}"/>')
            body.append(svg_text(left - 8, y + 4, f"{value:.{decimals}f}", 10, "end", MUTED))
        for idx, (name, query_s, memory_mb) in enumerate(CSR_VARIANTS):
            value = query_s if unit == "seconds" else memory_mb
            x = left + 36 + idx * 122
            h = value / maximum * plot_h
            y = top + plot_h - h
            body.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="66" height="{h:.1f}" fill="{color}" rx="2"/>')
            value_label = f"{value:.3f}s" if unit == "seconds" else f"{value:.0f} MB"
            body.append(svg_text(x + 33, y - 8, value_label, 11, "middle", INK, 600))
            short_name = {
                "Legacy global": "Legacy",
                "One-scan segmented": "Segmented direct",
                "Pre-counted direct": "Pre-count direct",
            }[name]
            body.append(svg_text(x + 33, top + plot_h + 24, short_name, 10, "middle", INK))
        body.append(svg_text(left - 56, top + plot_h / 2, unit, 11, "middle", MUTED, rotate=-90))
    body.append(svg_text(width / 2, height - 22, "The pre-counted direct path recovers compact memory and BFS locality, but its second endpoint scan removes the one-shot speed gain.", 11, "middle", MUTED))
    path = OUT / "weekly_csr_tradeoff.svg"
    path.write_text(svg_doc(width, height, body))


def write_scalar_operator_progression_plot():
    width, height = 1240, 680
    left, right, top, bottom = 105, 35, 100, 180
    plot_w, plot_h = width - left - right, height - top - bottom
    y_min, y_max = 0.01, 100.0
    sy = lambda value: top + plot_h - (math.log10(value) - math.log10(y_min)) / (math.log10(y_max) - math.log10(y_min)) * plot_h
    group_w = plot_w / len(SCALAR_OPERATOR_PROGRESSION)
    body = [
        svg_text(width / 2, 34, "Scalar Baseline to Current Native Operator", 22, "middle", weight=700),
        svg_text(width / 2, 58, "End-to-end query time from separate controlled workloads; lower is better; log scale", 12, "middle", MUTED),
    ]
    for tick in [0.01, 0.1, 1, 10, 100]:
        ty = sy(tick)
        body.append(f'<line x1="{left}" y1="{ty:.1f}" x2="{left + plot_w}" y2="{ty:.1f}" stroke="{GRID}"/>')
        body.append(svg_text(left - 12, ty + 4, f"{tick:g}s", 11, "end", MUTED))
    baseline = sy(y_min)
    for idx, (stage, workload, threads, scalar_s, operator_s) in enumerate(SCALAR_OPERATOR_PROGRESSION):
        cx = left + group_w * (idx + 0.5)
        for offset, value, color in [(-28, scalar_s, GOLD), (28, operator_s, BLUE)]:
            y = sy(value)
            body.append(f'<rect x="{cx + offset - 22:.1f}" y="{y:.1f}" width="44" height="{baseline - y:.1f}" fill="{color}" rx="2"/>')
            body.append(svg_text(cx + offset, y - 8, f"{value:.3f}s", 10, "middle", INK, 600))
        ratio = scalar_s / operator_s
        if math.isclose(ratio, 1.0, rel_tol=0.01):
            result = "equal"
            result_color = MUTED
        elif ratio > 1.0:
            result = f"native {ratio:.2f}x faster"
            result_color = GREEN
        else:
            result = f"scalar {1.0 / ratio:.2f}x faster"
            result_color = RED
        label_y = top + plot_h + 24
        body.append(svg_text(cx, label_y, stage, 11, "middle", INK, 600))
        body.append(svg_text(cx, label_y + 19, workload, 10, "middle", MUTED))
        body.append(svg_text(cx, label_y + 36, threads, 10, "middle", MUTED))
        body.append(svg_text(cx, label_y + 62, result, 11, "middle", result_color, 600))
    legend_y = height - 35
    body.append(f'<rect x="{left}" y="{legend_y - 11}" width="12" height="12" fill="{GOLD}"/>')
    body.append(svg_text(left + 18, legend_y, "Scalar UDF", 11, "start", INK))
    body.append(f'<rect x="{left + 115}" y="{legend_y - 11}" width="12" height="12" fill="{BLUE}"/>')
    body.append(svg_text(left + 133, legend_y, "Native operator", 11, "start", INK))
    body.append(svg_text(width - right, legend_y, "The source-to-all scalar value is the same control measurement in the last two groups.", 11, "end", MUTED))
    (OUT / "weekly_scalar_operator_progression.svg").write_text(svg_doc(width, height, body))


def write_scalar_operator_progression_csv():
    path = OUT / "weekly_scalar_operator_progression.csv"
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["stage", "workload", "threads", "scalar_s", "native_operator_s", "native_speedup"])
        for stage, workload, threads, scalar_s, operator_s in SCALAR_OPERATOR_PROGRESSION:
            writer.writerow([stage, workload, threads, scalar_s, operator_s, scalar_s / operator_s])


def complete_breakdown_rows(rows):
    completed = []
    for label, total, phases in rows:
        values = dict(phases)
        values["Other"] = max(0.0, total - sum(values.values()))
        completed.append((label, total, values))
    return completed


def write_two_panel_breakdown(filename, title, subtitle, rows, phase_order, note):
    rows = complete_breakdown_rows(rows)
    width, height = 1160, 590
    top, plot_h = 120, 320
    left_abs, panel_w, gap = 95, 430, 115
    left_pct = left_abs + panel_w + gap
    max_total = max(total for _, total, _ in rows) * 1.08
    body = [
        svg_text(width / 2, 34, title, 22, "middle", weight=700),
        svg_text(width / 2, 58, subtitle, 12, "middle", MUTED),
        svg_text(left_abs + panel_w / 2, 91, "Measured time", 14, "middle", INK, 600),
        svg_text(left_pct + panel_w / 2, 91, "Share of query time", 14, "middle", INK, 600),
    ]
    bar_gap = panel_w / len(rows)
    bar_w = min(86, bar_gap * 0.56)
    for panel_left, normalized in [(left_abs, False), (left_pct, True)]:
        scale_max = 100.0 if normalized else max_total
        for tick in range(5):
            value = scale_max * tick / 4
            y = top + plot_h - value / scale_max * plot_h
            body.append(f'<line x1="{panel_left}" y1="{y:.1f}" x2="{panel_left + panel_w}" y2="{y:.1f}" stroke="{GRID}"/>')
            tick_label = f"{value:.0f}%" if normalized else f"{value:.2f}s"
            body.append(svg_text(panel_left - 8, y + 4, tick_label, 10, "end", MUTED))
        for idx, (label, total, phases) in enumerate(rows):
            x = panel_left + bar_gap * (idx + 0.5) - bar_w / 2
            y_cursor = top + plot_h
            for phase in phase_order:
                value = phases.get(phase, 0.0)
                draw_value = value / total * 100 if normalized and total else value
                bar_h = draw_value / scale_max * plot_h
                if bar_h <= 0:
                    continue
                y_cursor -= bar_h
                body.append(f'<rect x="{x:.1f}" y="{y_cursor:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" fill="{PHASE_COLORS[phase]}"/>')
                if normalized and bar_h >= 22:
                    body.append(svg_text(x + bar_w / 2, y_cursor + bar_h / 2 + 4, f"{value / total * 100:.0f}%", 10, "middle", "#ffffff", 600))
            body.append(svg_text(x + bar_w / 2, top + plot_h + 22, label, 10, "middle", INK))
            body.append(svg_text(x + bar_w / 2, top + plot_h + 40, f"total {total:.3f}s", 10, "middle", MUTED))
    legend_x = left_abs
    legend_y = height - 72
    for phase in phase_order:
        body.append(f'<rect x="{legend_x}" y="{legend_y - 11}" width="12" height="12" fill="{PHASE_COLORS[phase]}"/>')
        body.append(svg_text(legend_x + 18, legend_y, phase, 11, "start", INK))
        legend_x += 130
    body.append(svg_text(width / 2, height - 22, note, 11, "middle", MUTED))
    (OUT / filename).write_text(svg_doc(width, height, body))


def write_ldbc_point_breakdown_plot():
    write_two_panel_breakdown(
        "weekly_ldbc_snb_point_breakdown.svg",
        "LDBC SNB SF10 Point Queries: Time by Phase",
        "65,536 rows, 8 threads, metrics enabled; dedupe leaves 1,024 unique pairs",
        LDBC_POINT_BREAKDOWN,
        ["Local CSR", "BFS", "Pair work", "Other"],
        "Meeting question: after dedupe removes repeated BFS work, which parts of the remaining Other time can we measure and remove?",
    )


def write_ldbc_sssp_breakdown_plot():
    write_two_panel_breakdown(
        "weekly_ldbc_snb_sssp_breakdown.svg",
        "LDBC SNB SF10 Source-to-All: Time by Phase",
        "68,673 targets, one source, 8 threads, metrics enabled",
        LDBC_SSSP_BREAKDOWN,
        ["Local CSR", "Group build", "BFS", "Other"],
        "Source grouping cuts query time by about 20x. Other query work then uses about 77% of the query time.",
    )


def write_ldbc_apsp_breakdown_plot():
    rows = complete_breakdown_rows(LDBC_APSP_BREAKDOWN)
    width, height = 1160, 610
    top, plot_h = 125, 315
    left_pct, pct_w = 90, 580
    left_total, total_w = 790, 280
    body = [
        svg_text(width / 2, 34, "LDBC SNB All-Pairs: Time Moves to Pair Output", 22, "middle", weight=700),
        svg_text(width / 2, 58, "Full source and target sets, 8 threads, metrics enabled", 12, "middle", MUTED),
        svg_text(left_pct + pct_w / 2, 92, "Share of query time", 14, "middle", INK, 600),
        svg_text(left_total + total_w / 2, 92, "Total time", 14, "middle", INK, 600),
    ]
    row_h = 82
    phase_order = ["Local CSR", "Group build", "BFS", "Other"]
    for idx, (label, total, phases) in enumerate(rows):
        y = top + idx * row_h
        body.append(svg_text(left_pct - 12, y + 20, label, 12, "end", INK, 600))
        x_cursor = left_pct
        for phase in phase_order:
            value = phases.get(phase, 0.0)
            segment_w = value / total * pct_w
            if segment_w <= 0:
                continue
            body.append(f'<rect x="{x_cursor:.1f}" y="{y:.1f}" width="{segment_w:.1f}" height="34" fill="{PHASE_COLORS[phase]}"/>')
            if segment_w >= 48:
                body.append(svg_text(x_cursor + segment_w / 2, y + 22, f"{value / total * 100:.0f}%", 10, "middle", "#ffffff", 600))
            x_cursor += segment_w
        body.append(svg_text(left_pct, y + 55, f"{int(total):,} s total; {int(LDBC_APSP_BREAKDOWN[idx][2].get('BFS', 0) * 1000):,} ms in grouped BFS", 10, "start", MUTED))

    min_total, max_total = 10.0, 1000.0
    sy = lambda value: top + plot_h - (math.log10(value) - math.log10(min_total)) / (math.log10(max_total) - math.log10(min_total)) * plot_h
    for tick in [10, 30, 100, 300, 1000]:
        ty = sy(tick)
        body.append(f'<line x1="{left_total}" y1="{ty:.1f}" x2="{left_total + total_w}" y2="{ty:.1f}" stroke="{GRID}"/>')
        body.append(svg_text(left_total - 8, ty + 4, f"{tick}s", 10, "end", MUTED))
    bar_gap = total_w / len(rows)
    for idx, (label, total, _) in enumerate(rows):
        x = left_total + bar_gap * (idx + 0.5) - 27
        y = sy(total)
        base = sy(min_total)
        body.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="54" height="{base - y:.1f}" fill="{BLUE}" rx="2"/>')
        body.append(svg_text(x + 27, y - 8, f"{total:.1f}s", 10, "middle", INK, 600))
        body.append(svg_text(x + 27, top + plot_h + 22, label, 11, "middle", INK))
    legend_x = left_pct
    legend_y = height - 82
    for phase in phase_order:
        body.append(f'<rect x="{legend_x}" y="{legend_y - 11}" width="12" height="12" fill="{PHASE_COLORS[phase]}"/>')
        body.append(svg_text(legend_x + 18, legend_y, phase, 11, "start", INK))
        legend_x += 130
    body.append(svg_text(width / 2, height - 24, "At SF10, Other work is 303.9 s. This includes pair scan, result scatter, output chunks, and aggregation.", 11, "middle", MUTED))
    (OUT / "weekly_ldbc_snb_apsp_breakdown.svg").write_text(svg_doc(width, height, body))


def write_ldbc_phase_csv():
    path = OUT / "weekly_ldbc_snb_phase_breakdown.csv"
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["experiment", "label", "query_s", "local_csr_s", "group_build_s", "bfs_s", "pair_work_s", "other_s"])
        for experiment, rows in [
            ("point", LDBC_POINT_BREAKDOWN),
            ("sssp", LDBC_SSSP_BREAKDOWN),
            ("apsp", LDBC_APSP_BREAKDOWN),
        ]:
            for label, total, phases in complete_breakdown_rows(rows):
                writer.writerow([
                    experiment,
                    label,
                    total,
                    phases.get("Local CSR", 0.0),
                    phases.get("Group build", 0.0),
                    phases.get("BFS", 0.0),
                    phases.get("Pair work", 0.0),
                    phases.get("Other", 0.0),
                ])


def write_optimization_csv():
    path = OUT / "weekly_optimization_impact.csv"
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["optimization", "workload", "before_s", "after_s", "reduction_pct", "speedup", "classification"])
        for name, workload, before, after, status in OPTIMIZATIONS:
            writer.writerow([name, workload, before, after, (before - after) / before * 100, before / after, status])


def write_gallery():
    sections = []
    for filename, title in [
        ("weekly_scalar_operator_progression.svg", "Scalar baseline and native operator progression"),
        ("weekly_ldbc_snb_point_breakdown.svg", "LDBC SNB point-query phases"),
        ("weekly_ldbc_snb_sssp_breakdown.svg", "LDBC SNB source-to-all phases"),
        ("weekly_ldbc_snb_apsp_breakdown.svg", "LDBC SNB all-pairs phases"),
        ("weekly_optimization_impact.svg", "Optimization impact"),
        ("weekly_graphalytics_scaling.svg", "Graphalytics scaling"),
        ("weekly_kuzu_comparison.svg", "Kuzu comparison"),
        ("weekly_csr_tradeoff.svg", "CSR creation tradeoff"),
    ]:
        svg = (OUT / filename).read_text()
        sections.append(f"<section><h2>{html.escape(title)}</h2>{svg}</section>")
    page = """<!doctype html><html><head><meta charset=\"utf-8\"><style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:24px;color:#202124;background:#f5f6f7}
main{max-width:1180px;margin:auto}section{background:white;margin:0 0 24px;padding:18px;border:1px solid #dde1e6}
h1,h2{letter-spacing:0}svg{display:block;max-width:100%;height:auto;margin:auto}
</style></head><body><main><h1>DuckPGQ pathfinding benchmark week: 4-11 August 2026</h1>""" + "".join(sections) + "</main></body></html>"
    (OUT / "weekly_pathfinding_summary.html").write_text(page)


def render_pngs():
    converter = shutil.which("rsvg-convert")
    if converter is None:
        return
    for source in OUT.glob("*.svg"):
        subprocess.run([converter, str(source), "-o", str(source.with_suffix(".png"))], check=True)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    write_scalar_operator_progression_plot()
    write_optimization_plot()
    write_graphalytics_plot()
    write_kuzu_plot()
    write_csr_plot()
    write_ldbc_point_breakdown_plot()
    write_ldbc_sssp_breakdown_plot()
    write_ldbc_apsp_breakdown_plot()
    write_scalar_operator_progression_csv()
    write_optimization_csv()
    write_ldbc_phase_csv()
    write_gallery()
    render_pngs()
    for path in sorted(OUT.iterdir()):
        print(path)


if __name__ == "__main__":
    main()
