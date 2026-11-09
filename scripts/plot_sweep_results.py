#!/usr/bin/env python3
"""Figures for the W27/W17 sweeps: random pairs, sources x all, LDBC queries, client-side concurrency.

Reads every sweep_pairs_*.csv and ldbc_queries_*.csv under data/ldbc-pathfinding/results/sf{10,100,300}; when a
(system, shape, count) point appears in several files the newest file wins, so reruns replace earlier numbers.
Writes PDF and PNG to paper/figures/ (DUCKPGQ_FIGURES_DIR overrides it, as DUCKPGQ_TABLES_DIR does for the LaTeX
tables, so a test render never touches the paper). Colours follow the entity (one colour per system across every figure), six
categorical slots in the validated order of the data-viz palette; the three slots with low contrast on the light
surface get direct labels at the line ends (relief rule) and every number is also in the plan's tables.
"""
import csv
import os
import re
import statistics
import textwrap
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
RESULTS = Path(os.environ.get("DUCKPGQ_RESULTS_DIR", str(REPO / "data" / "ldbc-pathfinding" / "results")))
FIGURES = Path(os.environ.get("DUCKPGQ_FIGURES_DIR", str(REPO / "paper" / "figures")))
SCALE_FACTORS = ["10", "100", "300"]
TIMEOUT_S = 600.0

# Entity colours (validated categorical slots 1..6, light mode) and display names
SYSTEMS = [
    ("duckpgq_index", "DuckPGQ index", "#2a78d6"),
    ("duckpgq_build", "DuckPGQ build", "#eb6834"),
    ("neo4j", "Neo4j", "#1baf7a"),
    ("kuzu", "Kuzu", "#eda100"),
    ("falkordb", "FalkorDB", "#e87ba4"),
    ("memgraph", "Memgraph", "#008300"),
]
COLOR = {key: color for key, _, color in SYSTEMS}
SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"

plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 8,
    "axes.edgecolor": AXIS,
    "axes.labelcolor": INK2,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "axes.titlecolor": INK,
    "axes.titlesize": 9,
    "axes.facecolor": SURFACE,
    "figure.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "grid.color": GRID,
    "grid.linewidth": 0.5,
    "axes.grid": True,
    "axes.axisbelow": True,
    "legend.frameon": False,
    "legend.fontsize": 7.5,
})


def load(prefix, sf):
    """{(system, shape_or_query, count_or_param): {"mean": s, "status": ok|exceeded|not_run|error}}"""
    # The run's start time is the last field of the file name; modification times change with every copy or checkout
    # sf is an LDBC scale factor ("10") or a Graphalytics results directory ("graphalytics_cit_Patents")
    directory = RESULTS / (f"sf{sf}" if sf.replace(".", "").isdigit() else sf)
    files = sorted(directory.glob(f"{prefix}_*.csv"), key=lambda p: int(p.stem.rsplit("_", 1)[1]))
    points = {}
    for path in files:
        groups = defaultdict(list)
        for row in csv.DictReader(open(path)):
            if prefix == "sweep_pairs":
                count = row["pairs"] if row["shape"] == "random_pairs" else row["sources"]
                key = (row["system"], row["shape"], int(count))
            else:
                key = (row["system"], row["query"], int(row["param_index"]))
            groups[key].append(row)
        for key, rows in groups.items():
            ok = [float(r["query_s"]) for r in rows if r["status"] == "ok"]
            if ok:
                reached = [float(r["reachable_count"]) for r in rows if r["status"] == "ok" and r.get("reachable_count")]
                # "mean" is the summary every figure and table plots: the median of the repeats (decided 29 Oct, a
                # baseline's first repeat is often cold); the arithmetic mean stays available as "arith_mean"
                points[key] = {"mean": statistics.median(ok), "median": statistics.median(ok),
                               "arith_mean": statistics.mean(ok), "status": "ok",
                               "reachable": statistics.mean(reached) if reached else None}
            elif any(r["status"].startswith(("timed_out", "budget_exceeded")) for r in rows):
                points[key] = {"mean": None, "status": "exceeded"}
            elif any(r["status"].startswith("exceeds_pair_limit") for r in rows):
                points[key] = {"mean": None, "status": "not_run"}
            else:
                points[key] = {"mean": None, "status": "error"}
    return points


def best_path(points, system, shape, count):
    """The system's row with 32 client connections when it exists for this point, else the single-query row"""
    for label in (f"{system}@32", system):
        if (label, shape, count) in points:
            return points[(label, shape, count)]
    return None


def style_axes(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.tick_params(length=0)


def count_label(count):
    return f"{count // 1024}k" if count >= 1024 else str(count)


def direct_labels(ax, positions, x_right, factor=2.6):
    """Series names beside the right edge of the panel at the height of each line's last point, pushed apart on the
    log axis when they would collide and kept below the top of the axis"""
    positions.sort()
    placed = []
    for y, x, name, color in positions:
        placed.append(y if not placed or y / placed[-1] >= factor else placed[-1] * factor)
    top = ax.get_ylim()[1] / 1.6
    if placed and placed[-1] > top:
        shift = placed[-1] / top
        placed = [yy / shift for yy in placed]
    for (y, x, name, color), yy in zip(positions, placed):
        ax.annotate(name, (x_right, yy), xytext=(4, 0), textcoords="offset points", color=color, fontsize=6.5,
                    va="center", ha="left", annotation_clip=False)


def series_legend(fig, systems, ncol, y=-0.04, marker="o", linewidth=1.6, timed_out=True):
    handles = [Line2D([], [], color=c, linewidth=linewidth, marker=marker, markersize=3.6, markeredgecolor=SURFACE)
               for _, _, c in systems]
    labels = [n for _, n, _ in systems]
    if timed_out:
        handles.append(Line2D([], [], linestyle="none", marker="^", markersize=5, markerfacecolor=SURFACE, markeredgecolor=INK2))
        labels.append("timed out")
    fig.legend(handles, labels, loc="lower center", ncol=ncol, bbox_to_anchor=(0.5, y), handlelength=2.4, columnspacing=1.2)


OURS = ("duckpgq_index", "duckpgq_build")


def emphasis(system):
    """(linewidth, alpha, zorder): our two routes at full weight, the baselines muted behind them"""
    return (2.0, 1.0, 5) if system in OURS else (1.2, 0.6, 3)


def plot_series(ax, xs, ys, exceeded_x, color, linestyle="-", linewidth=1.6, alpha=1.0, hollow=True, zorder=3):
    if xs:
        ax.plot(xs, ys, color=color, linewidth=linewidth, linestyle=linestyle, marker="o", markersize=3.6,
                markeredgecolor=SURFACE, markeredgewidth=0.8, zorder=zorder, alpha=alpha)
    if exceeded_x:
        ax.plot(exceeded_x, [TIMEOUT_S] * len(exceeded_x), linestyle="none", marker="^", markersize=5,
                markerfacecolor=SURFACE if hollow else color, markeredgecolor=color, markeredgewidth=1.2,
                zorder=zorder + 1, alpha=alpha)


# The line charts show our two routes and the two strongest baselines of each shape; every system is in the tables
SPARSE = {
    "random_pairs": ("duckpgq_index", "duckpgq_build", "neo4j", "kuzu"),
    "multi_source": ("duckpgq_index", "duckpgq_build", "kuzu", "memgraph"),
}


def without_build(entries):
    """The throughput and scaling figures show one DuckPGQ line; the build route is in the tables."""
    return [entry for entry in entries if entry[0] != "duckpgq_build"]


def sparse_systems(*shapes):
    keys = [key for shape in shapes for key in SPARSE[shape]]
    return [entry for entry in SYSTEMS if entry[0] in keys]


def sweep_figure(shape, xlabel, filename, title):
    systems = sparse_systems(shape)
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.6), sharey=True)
    for ax, sf in zip(axes, SCALE_FACTORS):
        points = load("sweep_pairs", sf)
        counts = sorted({k[2] for k in points if k[1] == shape})
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.axhline(TIMEOUT_S, color=AXIS, linewidth=0.6)
        ax.text(counts[0], TIMEOUT_S * 1.25, "600 s timeout", color=MUTED, fontsize=6.5, va="bottom")
        label_positions = []
        for system, name, color in systems:
            xs, ys, exceeded_x = [], [], []
            for count in counts:
                point = best_path(points, system, shape, count)
                if point is None:
                    continue
                if point["status"] == "ok":
                    xs.append(count)
                    ys.append(point["mean"])
                elif point["status"] == "exceeded":
                    exceeded_x.append(count)
            linewidth, alpha, zorder = emphasis(system)
            plot_series(ax, xs, ys, exceeded_x, color, linewidth=linewidth, alpha=alpha, zorder=zorder)
            if xs:
                label_positions.append((ys[-1], xs[-1], name, color))
        ax.set_ylim(0.001, 3000)
        if sf == SCALE_FACTORS[-1]:
            direct_labels(ax, label_positions, max(counts) * 1.3)
        ax.set_title(f"LDBC SF{sf}", loc="left")
        ax.set_xticks(counts)
        ax.set_xticklabels([count_label(c) for c in counts], fontsize=6)
        style_axes(ax)
    axes[1].set_xlabel(xlabel)
    axes[0].set_ylabel("seconds per query (median of 3)")
    series_legend(fig, systems, ncol=5)
    fig.suptitle(title, x=0.01, ha="left", fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.06, 0.9, 0.95))
    save(fig, filename)


# W30: the Graphalytics graphs next to LDBC knows (results directory, display name, what differs from LDBC)
GRAPHALYTICS = [
    ("graphalytics_cit_Patents", "cit-Patents", "3.8M V, 16.5M E, directed, deep levels"),
    ("graphalytics_graph500_24", "graph500-24", "8.9M V, 521M E, hubs"),
    ("graphalytics_datagen_8_4_fb", "datagen-8_4-fb", "3.8M V, 539M E, dense"),
]


def graphalytics_figure(filename):
    """Both sweep shapes on the three Graphalytics graphs: one column per graph, random pairs above sources x all;
    every system that has a result on the graph (cit-Patents runs every baseline, the two large graphs Kuzu only)"""
    datasets = [entry for entry in GRAPHALYTICS if load("sweep_pairs", entry[0])]
    if not datasets:
        print("no Graphalytics sweep results yet, skipping", filename)
        return
    shapes = [("random_pairs", "pairs per query"), ("multi_source", "sources per query (each to every vertex)")]
    fig, axes = plt.subplots(2, len(datasets), figsize=(2.4 * len(datasets) + 0.6, 4.4), sharey="row", squeeze=False)
    used = []
    for col, (directory, name, note) in enumerate(datasets):
        points = load("sweep_pairs", directory)
        for row, (shape, xlabel) in enumerate(shapes):
            ax = axes[row][col]
            counts = sorted({k[2] for k in points if k[1] == shape})
            if not counts:
                ax.set_axis_off()
                continue
            ax.set_xscale("log", base=2)
            ax.set_yscale("log")
            ax.axhline(TIMEOUT_S, color=AXIS, linewidth=0.6)
            if col == 0:
                ax.text(counts[0], TIMEOUT_S * 1.25, "600 s timeout", color=MUTED, fontsize=6.5, va="bottom")
            label_positions = []
            for system, sysname, color in SYSTEMS:
                xs, ys, exceeded_x = [], [], []
                for count in counts:
                    point = best_path(points, system, shape, count)
                    if point is None:
                        continue
                    if point["status"] == "ok":
                        xs.append(count)
                        ys.append(point["mean"])
                    elif point["status"] == "exceeded":
                        exceeded_x.append(count)
                if not xs and not exceeded_x:
                    continue
                if (system, sysname, color) not in used:
                    used.append((system, sysname, color))
                linewidth, alpha, zorder = emphasis(system)
                plot_series(ax, xs, ys, exceeded_x, color, linewidth=linewidth, alpha=alpha, zorder=zorder)
                if xs:
                    label_positions.append((ys[-1], xs[-1], sysname, color))
            ax.set_ylim(0.001, 3000)
            if col == len(datasets) - 1:
                direct_labels(ax, label_positions, max(counts) * 1.3)
            if row == 0:
                ax.set_title(f"{name}\n{note}", loc="left", fontsize=8)
            ax.set_xticks(counts)
            ax.set_xticklabels([count_label(c) for c in counts], fontsize=6)
            ax.set_xlabel(xlabel, fontsize=7.5)
            style_axes(ax)
    axes[0][0].set_ylabel("seconds per query (median of 3)")
    axes[1][0].set_ylabel("seconds per query (median of 3)")
    series_legend(fig, [entry for entry in SYSTEMS if entry in used], ncol=4)
    fig.suptitle("Graph structure: the sweep shapes on three Graphalytics graphs", x=0.01, ha="left", fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.05, 0.9, 0.96))
    save(fig, filename)


def ldbc_figure(filename):
    queries = [("ic13", "IC13\n1 × 1"), ("bi19", "BI19\ncity × city"), ("bi20", "BI20\ncompany × 1")]
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.7), sharey=True)
    offsets = {system: (i - (len(SYSTEMS) - 1) / 2) * 0.12 for i, (system, _, _) in enumerate(SYSTEMS)}
    for ax, sf in zip(axes, SCALE_FACTORS):
        points = load("ldbc_queries", sf)
        ax.set_yscale("log")
        # One band per query so the groups read as separate columns
        for column in range(len(queries)):
            if column % 2 == 1:
                ax.axvspan(column - 0.5, column + 0.5, color="#f1f1ee", zorder=0, linewidth=0)
        ax.axhline(TIMEOUT_S, color=AXIS, linewidth=0.6, zorder=1)
        ax.text(-0.45, TIMEOUT_S * 1.3, "600 s timeout", color=MUTED, fontsize=6.5, va="bottom", ha="left")
        for column, (query, _) in enumerate(queries):
            for system, name, color in SYSTEMS:
                params = sorted({k[2] for k in points if k[1] == query and k[0] in (system, f"{system}@32")})
                values, exceeded = [], False
                for param in params:
                    point = best_path(points, system, query, param)
                    if point is None:
                        continue
                    if point["status"] == "ok":
                        values.append(point["median"])
                    elif point["status"] == "exceeded":
                        exceeded = True
                x = column + offsets[system]
                _, alpha, zorder = emphasis(system)
                size = 5.2 if system in OURS else 4.2
                if values and not exceeded:
                    ax.plot([x], [statistics.mean(values)], marker="o", markersize=size, color=color, linestyle="none",
                            markeredgecolor=SURFACE, markeredgewidth=0.8, zorder=zorder, alpha=alpha)
                elif exceeded:
                    ax.plot([x], [TIMEOUT_S], marker="^", markersize=5.5, linestyle="none", markerfacecolor=SURFACE,
                            markeredgecolor=color, markeredgewidth=1.2, zorder=zorder, alpha=alpha)
        ax.set_xticks(range(len(queries)))
        ax.set_xticklabels([label for _, label in queries], fontsize=7, color=INK2)
        ax.set_xlim(-0.5, len(queries) - 0.5)
        ax.set_ylim(0.002, 3000)
        ax.set_title(f"LDBC SF{sf}", loc="left")
        ax.grid(axis="x", visible=False)
        style_axes(ax)
    axes[0].set_ylabel("seconds per query")
    handles = [Line2D([], [], linestyle="none", marker="o", markersize=4.4, color=c, markeredgecolor=SURFACE) for _, _, c in SYSTEMS]
    handles.append(Line2D([], [], linestyle="none", marker="^", markersize=5.5, markerfacecolor=SURFACE, markeredgecolor=INK2))
    labels = [n for _, n, _ in SYSTEMS] + ["timed out"]
    fig.legend(handles, labels, loc="lower center", ncol=7, bbox_to_anchor=(0.5, -0.02), handlelength=1.2, columnspacing=1.2)
    fig.suptitle("LDBC queries as hop-count shortest paths on undirected knows, median of 3 runs per parameter set, mean over 5 sets; baselines on their best path",
                 x=0.01, ha="left", fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.07, 1, 0.94))
    save(fig, filename)


def client_threads_figure(filename):
    systems = [("kuzu", "Kuzu", COLOR["kuzu"]), ("falkordb", "FalkorDB", COLOR["falkordb"]), ("memgraph", "Memgraph", COLOR["memgraph"])]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.6), sharey=True)
    for ax, sf in zip(axes, ["10", "100"]):
        points = load("sweep_pairs", sf)
        counts = sorted({k[2] for k in points if k[1] == "random_pairs"})
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.axhline(TIMEOUT_S, color=AXIS, linewidth=0.6)
        ax.text(counts[0], TIMEOUT_S * 1.25, "600 s timeout", color=MUTED, fontsize=6.5, va="bottom")
        ref = [(c, points[("duckpgq_index", "random_pairs", c)]["mean"]) for c in counts
               if points.get(("duckpgq_index", "random_pairs", c), {}).get("status") == "ok"]
        plot_series(ax, [c for c, _ in ref], [v for _, v in ref], [], COLOR["duckpgq_index"])
        ax.annotate("DuckPGQ index", ref[-1], xytext=(5, 0), textcoords="offset points", color=COLOR["duckpgq_index"],
                    fontsize=6.5, va="center", annotation_clip=False)
        for system, name, color in systems:
            for label, dashed in ((system, True), (f"{system}@32", False)):
                xs, ys, exceeded_x = [], [], []
                for count in counts:
                    point = points.get((label, "random_pairs", count))
                    if not point:
                        continue
                    if point["status"] == "ok":
                        xs.append(count)
                        ys.append(point["mean"])
                    elif point["status"] == "exceeded":
                        exceeded_x.append(count)
                plot_series(ax, xs, ys, exceeded_x, color, linestyle=(0, (3, 2)) if dashed else "-",
                            linewidth=1.2 if dashed else 1.8, alpha=0.7 if dashed else 1.0, hollow=not dashed)
        ax.set_title(f"LDBC SF{sf}, random pairs", loc="left")
        ax.set_xlabel("pairs per query")
        ax.set_xticks(counts)
        ax.set_xticklabels([count_label(c) for c in counts], fontsize=6)
        ax.set_ylim(0.001, 3000)
        style_axes(ax)
    axes[0].set_ylabel("seconds per query (median of 3)")
    handles = [Line2D([], [], color=c, linewidth=1.8) for _, _, c in systems]
    handles += [Line2D([], [], color=COLOR["duckpgq_index"], linewidth=1.6),
                Line2D([], [], color=INK2, linewidth=1.8), Line2D([], [], color=INK2, linewidth=1.2, linestyle=(0, (3, 2)))]
    labels = [n for _, n, _ in systems] + ["DuckPGQ index", "32 client connections", "one query"]
    fig.legend(handles, labels, loc="lower center", ncol=6, bbox_to_anchor=(0.5, -0.04), handlelength=2.4, columnspacing=1.4)
    fig.suptitle("Client-side concurrency: the pair list split over 32 connections against one query", x=0.01, ha="left",
                 fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.06, 0.93, 0.95))
    save(fig, filename)


BASELINES = [entry for entry in SYSTEMS if entry[0] not in OURS]


def speedup_figure(filename):
    """Baseline time divided by the DuckPGQ index time, per point; a timed-out baseline is at least 600 s / ours"""
    shapes = [("random_pairs", "random pairs per query"), ("multi_source", "sources per query, each to every person")]
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 4.4), sharey="row")
    width = 0.17
    for row, (shape, xlabel) in enumerate(shapes):
        top = 1
        for col, sf in enumerate(SCALE_FACTORS):
            ax = axes[row][col]
            points = load("sweep_pairs", sf)
            counts = sorted({k[2] for k in points if k[1] == shape and k[0] == "duckpgq_index"})
            ax.set_yscale("log", base=2)
            ax.axhline(1, color=INK2, linewidth=0.8, zorder=2)
            for i, (system, name, color) in enumerate(BASELINES):
                xs, heights, open_ended = [], [], []
                for j, count in enumerate(counts):
                    ours = points.get(("duckpgq_index", shape, count))
                    theirs = best_path(points, system, shape, count)
                    if not ours or ours["status"] != "ok" or theirs is None:
                        continue
                    if theirs["status"] == "ok":
                        ratio, lower_bound = theirs["mean"] / ours["mean"], False
                    elif theirs["status"] == "exceeded":
                        ratio, lower_bound = TIMEOUT_S / ours["mean"], True
                    else:
                        continue
                    xs.append(j + (i - (len(BASELINES) - 1) / 2) * width)
                    heights.append(ratio)
                    open_ended.append(lower_bound)
                    top = max(top, ratio)
                for x, h, lower_bound in zip(xs, heights, open_ended):
                    ax.bar(x, h, width=width * 0.9, bottom=None, color=SURFACE if lower_bound else color,
                           edgecolor=color, linewidth=1.0 if lower_bound else 0, zorder=3,
                           hatch="////" if lower_bound else None)
                    if lower_bound:
                        ax.text(x, h * 1.15, "≥", color=color, fontsize=6, ha="center", va="bottom")
            ax.set_xticks(range(len(counts)))
            ax.set_xticklabels([count_label(c) for c in counts], fontsize=6.5)
            ax.set_xlim(-0.6, len(counts) - 0.4)
            ax.set_title(f"SF{sf}: {'sources × all' if shape == 'multi_source' else 'random pairs'}", loc="left")
            ax.grid(axis="x", visible=False)
            style_axes(ax)
        axes[row][1].set_xlabel(xlabel)
        ymax = 2 ** (int(top).bit_length() + 1)
        for ax in axes[row]:
            ax.set_ylim(0.25, ymax)
            ticks = [2 ** k for k in range(-2, ymax.bit_length(), 2)]
            ax.set_yticks(ticks)
            ax.set_yticklabels([f"{t}×" if t >= 1 else f"1/{int(1 / t)}×" for t in ticks], fontsize=6.5)
            ax.set_yticks([], minor=True)
        axes[row][0].set_ylabel("× slower than DuckPGQ index")
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for _, _, c in BASELINES]
    handles.append(plt.Rectangle((0, 0), 1, 1, facecolor=SURFACE, edgecolor=INK2, hatch="////"))
    labels = [n for _, n, _ in BASELINES] + ["baseline timed out: at least this"]
    fig.legend(handles, labels, loc="lower center", ncol=5, bbox_to_anchor=(0.5, -0.01), handlelength=1.4, columnspacing=1.2)
    fig.suptitle("Baseline time relative to the DuckPGQ index route (best path per baseline; below 1× the baseline is faster)",
                 x=0.01, ha="left", fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    save(fig, filename)


def scaling_figure(filename):
    """Time against scale factor at the largest point of each shape"""
    panels = [("random_pairs", 65536, "65 536 random pairs per query"), ("multi_source", 1024, "1024 sources, each to every person")]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.6), sharey=True)
    edges = {"10": 1.84, "100": 18.7, "300": 55.7}
    for ax, (shape, count, title) in zip(axes, panels):
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.axhline(TIMEOUT_S, color=AXIS, linewidth=0.6)
        ax.text(edges["10"], TIMEOUT_S * 1.3, "600 s timeout", color=MUTED, fontsize=6.5, va="bottom")
        label_positions = []
        for system, name, color in without_build(sparse_systems(shape)):
            xs, ys, exceeded_x = [], [], []
            for sf in SCALE_FACTORS:
                points = load("sweep_pairs", sf)
                point = best_path(points, system, shape, count)
                if point is None:
                    continue
                if point["status"] == "ok":
                    xs.append(edges[sf])
                    ys.append(point["mean"])
                elif point["status"] == "exceeded":
                    exceeded_x.append(edges[sf])
            linewidth, alpha, zorder = emphasis(system)
            plot_series(ax, xs, ys, exceeded_x, color, linewidth=linewidth, alpha=alpha, zorder=zorder)
            if xs:
                label_positions.append((ys[-1], xs[-1], name, color))
        ax.set_ylim(0.01, 3000)
        direct_labels(ax, label_positions, edges["300"] * 1.15, factor=1.9)
        ax.set_xticks([edges[sf] for sf in SCALE_FACTORS])
        ax.set_xticklabels([f"SF{sf}\n{edges[sf]}M" for sf in SCALE_FACTORS], fontsize=6.5)
        ax.set_xticks([], minor=True)  # no minor tick labels on the three-point log axis
        ax.set_xlim(1.2, 90)
        ax.set_xlabel("scale factor, edges")
        ax.set_title(title, loc="left")
        style_axes(ax)
    axes[0].set_ylabel("seconds per query (median of 3)")
    series_legend(fig, without_build(sparse_systems("random_pairs", "multi_source")), ncol=6)
    fig.suptitle("Growth with the graph: the heaviest query of each sweep from SF10 to SF300", x=0.01, ha="left",
                 fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.06, 0.9, 0.95))
    save(fig, filename)


def throughput_figure(filename):
    """Answers per second: pairs per second for random pairs, (source, vertex, distance) results per second for
    sources x all. A timed-out point is an upper bound: its work divided by 600 s, drawn as a hollow triangle."""
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 4.6), sharey="row")
    rows_spec = [("random_pairs", "pairs per query", "pairs answered per second", lambda point, count: count),
                 ("multi_source", "sources per query, each to every person", "distances produced per second",
                  lambda point, count: point["reachable"])]
    for row, (shape, xlabel, ylabel, work) in enumerate(rows_spec):
        for col, sf in enumerate(SCALE_FACTORS):
            ax = axes[row][col]
            points = load("sweep_pairs", sf)
            counts = sorted({k[2] for k in points if k[1] == shape})
            ax.set_xscale("log", base=2)
            ax.set_yscale("log")
            label_positions = []
            for system, name, color in without_build(sparse_systems(shape)):
                xs, ys, bound_x, bound_y = [], [], [], []
                for count in counts:
                    point = best_path(points, system, shape, count)
                    if point is None:
                        continue
                    if point["status"] == "ok":
                        amount = work(point, count)
                        if amount:
                            xs.append(count)
                            ys.append(amount / point["mean"])
                    elif point["status"] == "exceeded" and shape == "random_pairs":
                        bound_x.append(count)
                        bound_y.append(count / TIMEOUT_S)
                linewidth, alpha, zorder = emphasis(system)
                if xs:
                    ax.plot(xs, ys, color=color, linewidth=linewidth, marker="o", markersize=3.6, markeredgecolor=SURFACE,
                            markeredgewidth=0.8, zorder=zorder, alpha=alpha)
                    label_positions.append((ys[-1], xs[-1], name, color))
                if bound_x:
                    ax.plot(bound_x, bound_y, linestyle="none", marker="v", markersize=5, markerfacecolor=SURFACE,
                            markeredgecolor=color, markeredgewidth=1.2, zorder=zorder + 1, alpha=alpha)
            ax.set_xticks(counts)
            ax.set_xticklabels([count_label(c) for c in counts], fontsize=6)
            ax.set_title(f"SF{sf}: {'sources × all' if shape == 'multi_source' else 'random pairs'}", loc="left")
            style_axes(ax)
            if col == 2:
                ax.set_ylim(*ax.get_ylim())
                direct_labels(ax, label_positions, max(counts) * 1.3, factor=2.2)
        axes[row][1].set_xlabel(xlabel)
        axes[row][0].set_ylabel(ylabel)
    shown = without_build(sparse_systems("random_pairs", "multi_source"))
    handles = [Line2D([], [], color=c, linewidth=1.6, marker="o", markersize=3.6, markeredgecolor=SURFACE) for _, _, c in shown]
    handles.append(Line2D([], [], linestyle="none", marker="v", markersize=5, markerfacecolor=SURFACE, markeredgecolor=INK2))
    labels = [n for _, n, _ in shown] + ["timed out (upper bound)"]
    fig.legend(handles, labels, loc="lower center", ncol=7, bbox_to_anchor=(0.5, -0.01), handlelength=1.8, columnspacing=1.0)
    fig.suptitle("Throughput: a set-oriented operator amortises the traversal, a per-pair system is a flat line",
                 x=0.01, ha="left", fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.05, 0.9, 0.95))
    save(fig, filename)


# Load or import times in seconds, read from the run logs under ~/sweep on the m7a.8xlarge (23 Oct 2026):
# "loaded in" lines of sweep_pairs_sf*.out for the graph databases, "Loaded Kuzu database" for Kuzu's import.
LOAD_S = {
    "memgraph": {"10": 5.8, "100": 71.0, "300": 278.9},
    "neo4j": {"10": 22.8, "100": 257.8, "300": 841.3},
    "falkordb": {"10": 85.3, "100": 872.1, "300": 2596.7},
    "kuzu": {"10": 0.9, "100": 3.0, "300": 7.9},
}


def index_build_s(sf):
    """CSR index build time at 32 threads from the index_comparison summaries (mode index_build)"""
    files = sorted((RESULTS / f"sf{sf}").glob("summary_sssp_same_src_pairs1024_threads32_modeindex_comparison_*.csv"))
    values = []
    for path in files:
        for r in csv.DictReader(open(path)):
            if r.get("mode") == "index_build" and r.get("csr_build_s"):
                values.append(float(r["csr_build_s"]))
    return statistics.mean(values) if values else None


def first_query_figure(filename):
    """Time from the data being on disk to the first answer: import or load plus the first single-pair query. DuckPGQ
    queries the tables as they are; the index route adds the index build (or a 90 ms load when it is persisted)."""
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.5), sharex=True, sharey=True)
    for ax, sf in zip(axes, SCALE_FACTORS):
        points = load("sweep_pairs", sf)
        entries = []
        for system, name, color in SYSTEMS:
            first = best_path(points, system, "random_pairs", 1)
            first_s = first["mean"] if first and first["status"] == "ok" else None
            if first_s is None:
                continue
            if system == "duckpgq_build":
                total = first_s
            elif system == "duckpgq_index":
                build = index_build_s(sf)
                if build is None:
                    continue
                total = build + first_s
                name = "DuckPGQ index (build + query)"
            else:
                total = LOAD_S[system][sf] + first_s
            entries.append((name, total, color))
        ys = range(len(entries))
        ax.barh(list(ys), [t for _, t, _ in entries], color=[c for _, _, c in entries], height=0.62, zorder=3)
        for y, (name, total, color) in zip(ys, entries):
            text = f"{total * 1000:.0f} ms" if total < 1 else (f"{total:.1f} s" if total < 100 else f"{total / 60:.0f} min")
            ax.text(total * 1.25, y, text, va="center", ha="left", fontsize=6.5, color=INK2)
        ax.set_yticks(list(ys))
        ax.set_yticklabels([n for n, _, _ in entries], fontsize=7, color=INK2)
        ax.invert_yaxis()
        ax.set_xscale("log")
        ax.set_xlim(0.003, 30000)
        ax.set_xticks([0.01, 1, 100, 10000])
        ax.set_xticklabels(["10 ms", "1 s", "100 s", "3 h"], fontsize=6.5)
        ax.set_xticks([], minor=True)
        ax.set_title(f"LDBC SF{sf}", loc="left")
        ax.grid(axis="y", visible=False)
        style_axes(ax)
    axes[1].set_xlabel("from data on disk to the first answer")
    fig.suptitle("Time to first query: import or load plus one single-pair shortest path", x=0.01, ha="left",
                 fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.02, 1, 0.94))
    save(fig, filename)


def update_rows(pattern, system):
    """Rows of the newest one-query-per-batch update run for a system at SF10"""
    files = sorted((RESULTS / "sf10").glob(pattern), key=lambda p: int(p.stem.rsplit("_", 1)[1]))
    for path in reversed(files):
        rows = list(csv.DictReader(open(path)))
        if rows and rows[0].get("queries_per_batch", "1") == "1":
            return rows
    return []


def updates_figure(filename):
    """The LDBC insert/delete stream at SF10: 190 commits of 2000 operations, one single-source query (1024
    destinations) after each commit. Left: query latency during the stream; right: cost of applying a commit."""
    series = [("duckpgq_index", "DuckPGQ index", COLOR["duckpgq_index"], ("update_sssp_pairs1024_threads32_batch2000x190_*.csv", "index_auto")),
              ("duckpgq_build", "DuckPGQ build", COLOR["duckpgq_build"], ("update_sssp_pairs1024_threads32_batch2000x190_*.csv", "build")),
              ("neo4j", "Neo4j", COLOR["neo4j"], ("update_neo4j_sssp_pairs1024_threads32_batch2000x190_*.csv", None)),
              ("kuzu", "Kuzu", COLOR["kuzu"], ("update_kuzu_sssp_pairs1024_threads32_batch2000x190_*.csv", None)),
              ("falkordb", "FalkorDB", COLOR["falkordb"], ("update_falkordb_sssp_pairs1024_threads32_batch2000x190_*.csv", None)),
              ("memgraph", "Memgraph", COLOR["memgraph"], ("update_memgraph_sssp_pairs1024_threads32_batch2000x190_*.csv", None))]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.5), sharey=True)
    names, queries, updates, colors = [], [], [], []
    for system, name, color, (pattern, config) in series:
        rows = update_rows(pattern, system)
        if config:
            rows = [r for r in rows if r["config"] == config]
        if not rows:
            continue
        names.append(name)
        colors.append(color)
        queries.append(statistics.mean(float(r["query_s"]) for r in rows))
        updates.append(statistics.mean(float(r["update_s"]) for r in rows if r["update_s"]))
    ys = list(range(len(names)))
    for ax, values, title in ((axes[0], queries, "query latency during the stream"),
                              (axes[1], updates, "applying one commit of 2000 operations")):
        ax.barh(ys, values, color=colors, height=0.62, zorder=3)
        for y, v in zip(ys, values):
            ax.text(v * 1.2, y, f"{v * 1000:.0f} ms" if v < 1 else f"{v:.1f} s", va="center", ha="left", fontsize=6.5, color=INK2)
        ax.set_xscale("log")
        ax.set_xlim(0.002, 60)
        ax.set_title(title, loc="left")
        ax.grid(axis="y", visible=False)
        ax.set_xlabel("seconds (log), mean over the stream")
        style_axes(ax)
    axes[0].set_yticks(ys)
    axes[0].set_yticklabels(names, fontsize=7, color=INK2)
    axes[0].invert_yaxis()
    fig.suptitle("Under updates, LDBC SF10: single source to 1024 destinations after each of 190 commits", x=0.01, ha="left",
                 fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.02, 1, 0.94))
    save(fig, filename)


def updates_timeline_figure(filename):
    """The update stream commit by commit: the cost of applying each commit on the build route and on the index with
    automatic folds, over the steps of the sequential update run, one panel per scale factor with data. Commits in
    which DuckDB ran an automatic checkpoint (the WAL shrank; checkpoint_threshold at its default) are marked, and so
    are the index's folds: the two kinds of spike the medians hide and the text explains. The defaults are kept; the
    figure shows where their costs fall."""
    panels = []
    for sf in SCALE_FACTORS:
        files = newest_files(RESULTS / f"sf{sf}", "update_sssp_pairs1024_threads32_batch*.csv")
        if not files:
            continue
        rows = [r for r in csv.DictReader(open(files[-1])) if r.get("step") and int(r["step"]) > 0 and r.get("update_s")]
        if not rows or "checkpoint" not in rows[0]:
            continue
        panels.append((sf, rows))
    if not panels:
        print("no update runs with checkpoint markers yet, skipping", filename)
        return
    fig, axes = plt.subplots(1, len(panels), figsize=(3.4 * len(panels) + 0.4, 2.6), sharey=False, squeeze=False)
    configs = [("build", "DuckPGQ build", COLOR["duckpgq_build"]), ("index_auto", "DuckPGQ index", COLOR["duckpgq_index"])]
    for ax, (sf, rows) in zip(axes[0], panels):
        ax.set_yscale("log")
        style_axes(ax)
        for config, name, color in configs:
            series = sorted((int(r["step"]), 1000 * float(r["update_s"]), r.get("checkpoint") == "1",
                             int(r["folds"] or 0) if r.get("folds") else 0) for r in rows if r["config"] == config)
            if not series:
                continue
            xs = [s_ for s_, *_ in series]
            ys = [y for _, y, *_ in series]
            ax.plot(xs, ys, color=color, linewidth=1.0, alpha=0.9, zorder=3)
            checkpoints = [(s_, y) for s_, y, ck, _ in series if ck]
            if checkpoints:
                ax.plot([c[0] for c in checkpoints], [c[1] for c in checkpoints], linestyle="none", marker="s",
                        markersize=5, markerfacecolor=SURFACE, markeredgecolor=color, markeredgewidth=1.2, zorder=5)
            folds = [(s_, y) for (s_, y, _, f), (_, _, _, f_prev) in zip(series[1:], series[:-1]) if f > f_prev]
            if folds:
                ax.plot([f[0] for f in folds], [f[1] for f in folds], linestyle="none", marker="D", markersize=4.5,
                        color=color, markeredgecolor=SURFACE, zorder=6)
        ax.set_title(f"LDBC SF{sf}", loc="left")
        ax.set_xlabel("commit")
        ax.set_ylabel("commit latency (ms)")
    handles = [Line2D([], [], color=c, linewidth=1.2) for _, _, c in configs]
    handles.append(Line2D([], [], linestyle="none", marker="s", markersize=5, markerfacecolor=SURFACE, markeredgecolor=INK2, markeredgewidth=1.2))
    handles.append(Line2D([], [], linestyle="none", marker="D", markersize=4.5, color=INK2))
    fig.legend(handles, [n for _, n, _ in configs] + ["DuckDB checkpoint in this commit", "index fold after this commit"],
               loc="lower center", ncol=4, bbox_to_anchor=(0.5, -0.02), handlelength=1.8, columnspacing=1.2, fontsize=7)
    top = wrapped_title(fig, "Applying the LDBC knows update stream commit by commit: checkpoints and folds are the spikes")
    fig.tight_layout(rect=(0, 0.1, 1, top))
    save(fig, filename)


def newest_files(directory, pattern):
    """Result files matching pattern in a results directory, oldest first by the run time in the name"""
    if not directory.is_dir():
        return []
    return sorted(directory.glob(pattern), key=lambda p: int(p.stem.rsplit("_", 1)[1]))


def p99(values):
    """The 99th percentile by rank over a non-empty list (the rule the harness's throughput rows use)"""
    ordered = sorted(values)
    return ordered[int(0.99 * (len(ordered) - 1))]


def ms_label(ms):
    """Milliseconds with one decimal under 100 and none above"""
    return f"{ms:.1f}" if ms < 100 else f"{ms:.0f}"


def stats_value(row, key):
    """A number from the driver's semicolon stats string (epochs=1;delta=0;folds=2;last_fold_ms=3051.0;...), or None"""
    match = re.search(rf"(?:^|;){key}=([-\d.]+)", row.get("stats") or "")
    return float(match.group(1)) if match else None


def latency_band(ax, xs, medians, p99s, color, linewidth=1.6, alpha=1.0, zorder=3):
    """Median as the line and a translucent band from the median up to the p99 in the same colour; a thin whisker at
    every point so a series with a single point still shows its tail"""
    plot_series(ax, xs, medians, [], color, linewidth=linewidth, alpha=alpha, zorder=zorder)
    if len(xs) > 1:
        ax.fill_between(xs, medians, p99s, color=color, alpha=0.18 * alpha, linewidth=0, zorder=zorder - 1)
    ax.vlines(xs, medians, p99s, color=color, alpha=0.55 * alpha, linewidth=0.9, zorder=zorder - 1)


ROUTES = [("duckpgq_index", "DuckPGQ index", COLOR["duckpgq_index"]), ("duckpgq_build", "DuckPGQ build", COLOR["duckpgq_build"])]


def wrapped_title(fig, text):
    """Workload title above a figure whose width follows its panel count, wrapped to that width so a one-panel
    figure does not clip it at the edge (about 14 characters per inch at 9.5 pt). Returns the top of the
    tight_layout rect."""
    width_in = fig.get_size_inches()[0]
    lines = textwrap.wrap(text, width=max(30, int(width_in * 14)))
    fig.suptitle("\n".join(lines), x=0.01, ha="left", fontsize=9.5, color=INK)
    # tight_layout reserves the suptitle's own height (matplotlib 3.3+); the rect only keeps a hairline above it
    return 0.99


def concurrent_figure(filename):
    """Readers against query latency under a writer committing every 200 ms (W16 concurrent pass): index route
    against build-per-query route, one panel per scale factor. The median is the line; the p99 of the same queries
    is a hollow marker above each point, joined to it by a hairline (no band: a band spanning an order of magnitude
    hides the line). The writer's commit latency under each route's readers is written into the panel at the
    largest reader count both routes ran; the full numbers are in the concurrent table. One route per file since
    2 Nov, the newest file per (route, readers) wins; the fold-latency step (fold ratio 0.0005) is excluded, its
    readers pay for the folds it forces (see fold_timeline_figure). At SF10 building the graph inside the query
    costs 22 ms and the saving disappears into CPU contention; at SF100 and SF300 (128 and 264 ms per build) the
    index's saving shows."""
    panels = []
    for sf in SCALE_FACTORS:
        # route -> readers -> (query median, query p99, commit median), all ms
        series = {"query_index": {}, "query_build": {}}
        for path in newest_files(RESULTS / f"sf{sf}",
                                 "concurrent_sssp_pairs1024_threads32_readers*_pause200_fold0.1_*.csv"):
            rows = list(csv.DictReader(open(path)))
            if not rows or not rows[0].get("readers"):
                continue
            readers = int(rows[0]["readers"])
            commits = [1000 * float(r["elapsed_s"]) for r in rows if r["kind"] == "commit"]
            for kind in series:
                values = [1000 * float(r["elapsed_s"]) for r in rows if r["kind"] == kind]
                if values:
                    series[kind][readers] = (statistics.median(values), p99(values),
                                             statistics.median(commits) if commits else None)
        if series["query_index"] or series["query_build"]:
            panels.append((sf, series))
    if not panels:
        print("no concurrent results yet, skipping", filename)
        return
    fig, axes = plt.subplots(1, len(panels), figsize=(2.4 * len(panels) + 0.9, 2.7), sharey=True, squeeze=False)
    for ax, (sf, series) in zip(axes[0], panels):
        readers = sorted(set(series["query_index"]) | set(series["query_build"]))
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        style_axes(ax)
        for (system, name, color), kind in zip(ROUTES, ("query_index", "query_build")):
            xs = [r for r in readers if r in series[kind]]
            if not xs:
                continue
            medians = [series[kind][r][0] for r in xs]
            tails = [series[kind][r][1] for r in xs]
            linewidth, alpha, zorder = emphasis(system)
            plot_series(ax, xs, medians, [], color, linewidth=linewidth, alpha=alpha, zorder=zorder)
            ax.vlines(xs, medians, tails, color=color, alpha=0.5 * alpha, linewidth=0.7, zorder=zorder - 1)
            ax.plot(xs, tails, linestyle="none", marker="o", markersize=3.6, markerfacecolor=SURFACE,
                    markeredgecolor=color, markeredgewidth=1.0, alpha=alpha, zorder=zorder)
            ax.annotate(f"p99 {ms_label(tails[-1])}", (xs[-1], tails[-1]), xytext=(4, 0), textcoords="offset points",
                        fontsize=6, color=color, va="center", annotation_clip=False)
        both = [r for r in readers if r in series["query_index"] and r in series["query_build"]
                and series["query_index"][r][2] is not None and series["query_build"][r][2] is not None]
        if both:
            r = both[-1]
            ax.text(0.03, 0.03, f"writer commits at {r} readers\nindex {ms_label(series['query_index'][r][2])} ms, "
                                f"build {ms_label(series['query_build'][r][2])} ms",
                    transform=ax.transAxes, fontsize=6, color=INK2, va="bottom", ha="left", linespacing=1.3)
        ax.set_title(f"LDBC SF{sf}", loc="left")
        ax.set_xticks(readers)
        ax.set_xticklabels([str(r) for r in readers], fontsize=6.5)
        ax.set_xlabel("concurrent readers")
    axes[0][0].set_ylabel("query latency (ms)")
    handles = [Line2D([], [], color=c, linewidth=1.6, marker="o", markersize=3.6, markeredgecolor=SURFACE)
               for _, _, c in ROUTES]
    handles.append(Line2D([], [], linestyle="none", marker="o", markersize=3.6, markerfacecolor=SURFACE,
                          markeredgecolor=INK2, markeredgewidth=1.0))
    fig.legend(handles, [n for _, n, _ in ROUTES] + ["p99 of the same queries"], loc="lower center", ncol=3,
               bbox_to_anchor=(0.5, -0.02), handlelength=2.4, columnspacing=1.2)
    top = wrapped_title(fig, "Readers under a writer committing 2000 operations every 200 ms, one source to 1024 "
                             "destinations: median latency per reader count, p99 above it")
    fig.tight_layout(rect=(0, 0.08, 0.92, top))
    save(fig, filename)


def client_throughput_figure(filename):
    """Single-pair queries from 1 to 32 clients, index route against build-per-query route, one column per scale
    factor (W35, 29 Oct; median and p99 since 2 Nov): the index's natural workload is many short queries from many
    clients. Top: queries per second. Bottom: the latency each client sees, the median as the line and a band up to
    the p99, so the throughput gain is read next to the wait it costs or saves."""
    panels = []
    workload = None
    for sf in SCALE_FACTORS:
        points = {}
        for path in newest_files(RESULTS / f"sf{sf}", "throughput_sf*_threads32_*.csv"):
            for r in csv.DictReader(open(path)):
                if r["queries_per_s"]:
                    points[(r["route"], int(r["clients"]))] = (float(r["queries_per_s"]), float(r["median_ms"]), float(r["p99_ms"]))
                    workload = (r["threads"], float(r["duration_s"]))
        if points:
            panels.append((sf, points))
    if not panels:
        print("no throughput results yet, skipping", filename)
        return
    fig, axes = plt.subplots(2, len(panels), figsize=(2.4 * len(panels) + 1.1, 4.4), sharex="col", sharey="row", squeeze=False)
    for col, (sf, points) in enumerate(panels):
        top, bottom = axes[0][col], axes[1][col]
        clients = sorted({c for _, c in points})
        for ax in (top, bottom):
            ax.set_xscale("log", base=2)
            ax.set_yscale("log")
            style_axes(ax)
        for (system, name, color), route in zip(ROUTES, ("index", "build")):
            xs = [c for c in clients if (route, c) in points]
            if not xs:
                continue
            rates = [points[(route, c)][0] for c in xs]
            medians = [points[(route, c)][1] for c in xs]
            tails = [points[(route, c)][2] for c in xs]
            linewidth, alpha, zorder = emphasis(system)
            plot_series(top, xs, rates, [], color, linewidth=linewidth, alpha=alpha, zorder=zorder)
            top.annotate(f"{rates[-1]:.0f}/s", (xs[-1], rates[-1]), xytext=(5, 0), textcoords="offset points",
                         fontsize=6.5, color=color, va="center", annotation_clip=False)
            latency_band(bottom, xs, medians, tails, color, linewidth=linewidth, alpha=alpha, zorder=zorder)
            bottom.annotate(f"{ms_label(medians[-1])} / {ms_label(tails[-1])} ms", (xs[-1], medians[-1]), xytext=(5, 0),
                            textcoords="offset points", fontsize=6.5, color=color, va="center", annotation_clip=False)
        top.set_title(f"LDBC SF{sf}", loc="left")
        bottom.set_xticks(clients)
        bottom.set_xticklabels([str(c) for c in clients], fontsize=6.5)
        bottom.set_xlabel("clients")
    axes[0][0].set_ylabel("queries per second")
    axes[1][0].set_ylabel("latency (ms)")
    series_legend(fig, ROUTES, ncol=2, y=-0.02, timed_out=False)
    threads, duration = workload
    top = wrapped_title(fig, f"Single random pairs, one pair per query, {threads} threads shared, {duration:g} s per "
                             f"point: median as the line, band to the p99")
    fig.tight_layout(rect=(0, 0.06, 0.88, top))
    save(fig, filename)


def fold_timeline_figure(filename):
    """The fold-latency step at SF300 (16 readers, a writer committing every 200 ms, fold ratio 0.0005 so the
    pending log crosses the trigger a few times in one run) on a wall-clock axis. Top: the median and the p99 of the
    index queries started in each half-second window; the query that ran a fold (the reader that crossed the
    trigger folds inside its own query) is taken out of the windows and drawn as one marker at its start, so the
    lines show what the other 15 readers see. Each fold is a shaded span: it ends at the first query that saw the
    fold counted in the stats it reads before running and reaches back the fold's last_fold_ms. Bottom strip: the
    writer's commits. The reading: the p99 line stays near its usual level through a fold, one point sits at the
    fold's duration, and the commits do not move (before 2 Nov the fold held the index entry's read handle and
    every reader and the writer waited for it)."""
    files = newest_files(RESULTS / "sf300", "concurrent_sssp_pairs1024_threads32_readers16_*_pause200_fold0.0005_*.csv")
    if not files:
        print("no fold-latency step results yet, skipping", filename)
        return
    rows = list(csv.DictReader(open(files[-1])))
    t0 = min(float(r["start_s"]) for r in rows)
    queries = sorted((float(r["start_s"]) - t0, 1000 * float(r["elapsed_s"]), stats_value(r, "folds") or 0,
                      stats_value(r, "last_fold_ms") or 0) for r in rows if r["kind"] == "query_index")
    commits = [(float(r["start_s"]) - t0, 1000 * float(r["elapsed_s"])) for r in rows if r["kind"] == "commit"]
    if not queries:
        print("no index queries in", files[-1].name, "skipping", filename)
        return
    # Fold spans and the queries that ran them (a query longer than a second here is a fold: the others take ~60 ms)
    fold_count = int(max(q[2] for q in queries))
    spans = []
    for k in range(1, fold_count + 1):
        first = next(q for q in queries if q[2] >= k)
        spans.append((max(0.0, first[0] - first[3] / 1000), first[0]))
    # The load ends with the writer's last commit: the readers then stop one by one and the windows after it would
    # show the thinning reader count, not the steady state, so the axis stops there (a fold still running at that
    # point shows as a clipped span)
    end_of_run = max(c[0] + c[1] / 1000 for c in commits) if commits else max(q[0] + q[1] / 1000 for q in queries)
    folding = [q for q in queries if q[1] > 1000 and q[0] <= end_of_run]
    regular = [q for q in queries if q[1] <= 1000 and q[0] <= end_of_run]
    window = 0.5
    windows = defaultdict(list)
    for start, latency, _, _ in regular:
        windows[int(start / window)].append(latency)
    centers = sorted(windows)
    xs = [(w + 0.5) * window for w in centers]
    medians = [statistics.median(windows[w]) for w in centers]
    tails = [p99(windows[w]) for w in centers]

    fig, (ax, strip) = plt.subplots(2, 1, figsize=(7.2, 3.2), sharex=True, gridspec_kw={"height_ratios": [4, 1]})
    index_color = COLOR["duckpgq_index"]
    for a, b in spans:
        if a >= end_of_run:
            continue
        for axis in (ax, strip):
            axis.axvspan(a, min(b, end_of_run), color=index_color, alpha=0.12, linewidth=0, zorder=1)
        label = f"{b - a:.1f} s fold" if b <= end_of_run else f"{b - a:.1f} s fold (runs past the writer's last commit)"
        ax.annotate(label, ((a + min(b, end_of_run)) / 2, 0.97), xycoords=("data", "axes fraction"), ha="center",
                    va="top", fontsize=6.5, color=INK2, annotation_clip=False)
    ax.set_yscale("log")
    ax.plot(xs, tails, color=index_color, linewidth=1.0, linestyle=(0, (2, 1.5)), zorder=3)
    ax.plot(xs, medians, color=index_color, linewidth=1.8, zorder=4)
    if folding:
        ax.plot([q[0] for q in folding], [q[1] for q in folding], linestyle="none", marker="D", markersize=5,
                markerfacecolor=index_color, markeredgecolor=SURFACE, markeredgewidth=0.8, zorder=5)
        for q in folding:
            ax.annotate(f"{q[1] / 1000:.1f} s", (q[0], q[1]), xytext=(5, -2), textcoords="offset points", fontsize=6,
                        color=index_color, va="top")
    ax.set_ylim(min(medians) / 2, max(q[1] for q in queries) * 2.5)
    ax.set_ylabel("query latency (ms)")
    strip.set_yscale("log")
    strip.plot([c[0] for c in commits], [c[1] for c in commits], linestyle="none", marker="D", markersize=3,
               color=INK2, zorder=3)
    commit_values = [c[1] for c in commits]
    strip.set_ylim(min(commit_values) / 2, max(commit_values) * 2.5)
    strip.set_ylabel("commit (ms)", fontsize=7)
    strip.set_xlabel("wall time (s)")
    strip.set_xlim(0, end_of_run)
    for axis in (ax, strip):
        style_axes(axis)
    handles = [Line2D([], [], color=index_color, linewidth=1.8),
               Line2D([], [], color=index_color, linewidth=1.0, linestyle=(0, (2, 1.5))),
               Line2D([], [], linestyle="none", marker="D", markersize=4.5, color=index_color),
               plt.Rectangle((0, 0), 1, 1, color=index_color, alpha=0.18, linewidth=0),
               Line2D([], [], linestyle="none", marker="D", markersize=3, color=INK2)]
    fig.legend(handles, ["median per 0.5 s window", "p99 per window", "the query that ran the fold", "fold",
                         "writer commits"], loc="lower center", ncol=5, bbox_to_anchor=(0.5, -0.02), handlelength=1.6,
               columnspacing=1.4, fontsize=7)
    top = wrapped_title(fig, "Folds under load at SF300: 16 readers, a writer every 200 ms, fold ratio 0.0005")
    fig.tight_layout(rect=(0, 0.08, 1, top))
    save(fig, filename)


CROSSOVER_KERNELS = [("single_source_kernel", "single-source traversal", "#2a78d6"),
                     ("multi_source_dedup", "MS-BFS, lane per distinct source", "#1baf7a"),
                     ("lane_per_pair", "MS-BFS, lane per pair", "#eb6834")]


def crossover_figure(filename):
    """Section 3: where the single-source traversal stops winning, and what the lanes pay for it. Rows: k random
    pairs (one destination per source) and k sources x 200 destinations, each as a time row and a memory row below
    it; columns: scale factors. For each k the three ways of running the searches are forced by settings
    (few_sources_limit, source_group_ratio) and timed on the index route; the line is the median of the repeats.
    Memory is the kernel's working set from the strategy string (the per-source search's bytes, or the MS-BFS
    batches' working sets), not the process peak, which the index load dominates. A dotted vertical line marks the
    smallest k at which the single-source traversal is no longer the fastest; the default kernel choice (few-sources
    limit) should sit at or below it. The reading: the lanes buy time with memory that grows with k, the per-source
    kernel holds a few MB whatever k is, and one lane per pair is never the best choice."""
    kernels = {c for c, _, _ in CROSSOVER_KERNELS}
    panels = {}
    for sf in SCALE_FACTORS:
        # newest rows per (config, shape, k) across the microbenchmark files of the scale factor
        latest = {}
        for path in newest_files(RESULTS / f"sf{sf}", "microbench_sf*_threads32_*.csv"):
            for r in csv.DictReader(open(path)):
                if r["config"] not in kernels or r["status"] != "ok":
                    continue
                k = int(r["pairs"]) if r["shape"] == "random_pairs" else int(r["sources"] or 0)
                latest.setdefault((r["config"], r["shape"], k, path.name), []).append(r)
        series = {}
        for (config, shape, k, _), rows in sorted(latest.items(), key=lambda item: item[0][3]):
            working = [m for m in (strategy_working_set_mb(r.get("strategy")) for r in rows) if m is not None]
            series[(config, shape, k)] = (statistics.median(float(r["query_s"]) for r in rows),
                                          max(working) if working else None)  # the newest file wins
        if series:
            panels[sf] = series
    if not panels:
        print("no crossover results yet, skipping", filename)
        return
    shapes = [("random_pairs", "k random pairs, one destination per source"),
              ("all_pairs", "k sources × 200 destinations each")]
    sfs = [sf for sf in SCALE_FACTORS if sf in panels]
    fig, axes = plt.subplots(2 * len(shapes), len(sfs), figsize=(2.4 * len(sfs) + 0.6, 6.4), sharex=True,
                             squeeze=False, gridspec_kw={"height_ratios": [3, 2] * len(shapes)})
    for shape_idx, (shape, shape_title) in enumerate(shapes):
        time_row, memory_row = 2 * shape_idx, 2 * shape_idx + 1
        for col, sf in enumerate(sfs):
            series = panels[sf]
            ks = sorted({k for (c, s_, k) in series if s_ == shape})
            # The same rule as the harness's crossover-table: the k after the largest one where the single-source
            # traversal is still within 10% of the best other way (the margin absorbs noise; for one source every
            # way runs the same per-source search)
            last_winning = None
            for k in ks:
                times = {c: series[(c, shape, k)][0] for c, _, _ in CROSSOVER_KERNELS if (c, shape, k) in series}
                if "single_source_kernel" in times and len(times) >= 2 and times["single_source_kernel"] <= 1.1 * min(
                        t for c, t in times.items() if c != "single_source_kernel"):
                    last_winning = k
            later = [k for k in ks if last_winning is None or k > last_winning]
            crossover = later[0] if later else None
            for row, value_idx in ((time_row, 0), (memory_row, 1)):
                ax = axes[row][col]
                ax.set_xscale("log", base=2)
                ax.set_yscale("log")
                style_axes(ax)
                for config, name, color in CROSSOVER_KERNELS:
                    xs = [k for k in ks if (config, shape, k) in series and series[(config, shape, k)][value_idx]]
                    ys = [series[(config, shape, k)][value_idx] for k in xs]
                    plot_series(ax, xs, ys, [], color, linewidth=1.6 if config == "single_source_kernel" else 1.3,
                                zorder=5 if config == "single_source_kernel" else 3)
                if crossover is not None:
                    ax.axvline(crossover, color=INK2, linestyle=(0, (1.5, 2)), linewidth=0.8, zorder=2)
                    if value_idx == 0:
                        ax.annotate(f"k = {crossover}", (crossover, ax.get_ylim()[1]), xytext=(3, -2),
                                    textcoords="offset points", fontsize=6.5, color=INK2, va="top", ha="left")
                if ks:
                    ax.set_xticks(ks)
                    ax.set_xticklabels([str(k) if k in (1, 4, 16, 64, 256, 1024, 4096) else "" for k in ks],
                                       fontsize=6.5)
            if time_row == 0:
                axes[0][col].set_title(f"LDBC SF{sf}", loc="left")
            if memory_row == 2 * len(shapes) - 1:
                axes[memory_row][col].set_xlabel("distinct sources k")
        axes[time_row][0].set_ylabel("query time (s)")
        axes[memory_row][0].set_ylabel("working set (MB)", fontsize=7)
        axes[time_row][0].annotate(shape_title, (0, 1.0), xycoords="axes fraction",
                                   xytext=(0, 16 if time_row == 0 else 4), textcoords="offset points", fontsize=7,
                                   color=INK2, va="bottom", ha="left")
    handles = [Line2D([], [], color=c, linewidth=1.6, marker="o", markersize=3.6, markeredgecolor=SURFACE)
               for _, _, c in CROSSOVER_KERNELS]
    fig.legend(handles, [n for _, n, _ in CROSSOVER_KERNELS], loc="lower center", ncol=3, bbox_to_anchor=(0.5, -0.01),
               handlelength=2.0, columnspacing=1.0, fontsize=7)
    top = wrapped_title(fig, "Where the single-source traversal stops winning, and what the lanes pay: the three ways "
                             "of running k distinct sources, forced, on the index route")
    fig.tight_layout(rect=(0, 0.05, 1, top))
    save(fig, filename)


def strategy_working_set_mb(strategy):
    """The kernel's working memory from the strategy string: the MS-BFS waves' or batches' working sets, or the
    per-source search's bytes (whichever the run recorded), in MB"""
    for key in ("wave_grouped_working_sets_mb", "grouped_working_sets_mb", "source_search_mb"):
        match = re.search(rf"(?:^|;){key}=(\d+)", strategy or "")
        if match:
            return int(match.group(1))
    return None


TABLES = Path(os.environ.get("DUCKPGQ_TABLES_DIR", str(REPO / "paper" / "tables")))
NAME_OF = {key: name for key, name, _ in SYSTEMS}
NAME_OF["neo4j_gds"] = "Neo4j GDS (4 thr.)"
NAME_OF["duckpgq_recursive"] = "DuckDB recursive CTE"
NAME_OF["duckpgq_scalar"] = "DuckPGQ 2023 UDFs"
ORDER = [key for key, _, _ in SYSTEMS] + ["duckpgq_scalar", "duckpgq_recursive", "neo4j_gds"]


def cell(point, bold=False):
    if point is None:
        return "--"
    if point["status"] == "ok":
        v = point["mean"]
        text = f"{v * 1000:.0f}\,ms" if v < 1 else (f"{v:.1f}\,s" if v < 100 else f"{v:.0f}\,s")
        return f"\\textbf{{{text}}}" if bold else text
    if point["status"] == "exceeded":
        return "timeout"
    if point["status"] == "not_run":
        return "n/r"
    return "err"


def column_minima(rows):
    """Per column, the smallest completed mean among the rows of one scale-factor block; None when no row completed."""
    minima = []
    for col in range(len(rows[0][1]) if rows else 0):
        oks = [r[1][col]["mean"] for r in rows if r[1][col] is not None and r[1][col]["status"] == "ok"]
        minima.append(min(oks) if oks else None)
    return minima


def format_rows(rows):
    """Rows of (label, points); the fastest completed cell of every column is bold."""
    minima = column_minima(rows)
    lines = []
    for lab, points_of_row in rows:
        cells = [cell(pt, bold=pt is not None and pt["status"] == "ok" and pt["mean"] == minima[i])
                 for i, pt in enumerate(points_of_row)]
        lines.append(f"\\quad {row_label(lab)} & " + " & ".join(cells) + r" \\")
    return lines


def row_label(label):
    base, _, suffix = label.partition("@")
    return NAME_OF.get(base, base) + (f" ({suffix} conn.)" if suffix else "")


def sorted_labels(labels):
    return sorted(labels, key=lambda l: (ORDER.index(l.split("@")[0]) if l.split("@")[0] in ORDER else 99, "@" in l))


def write_table(filename, lines):
    TABLES.mkdir(parents=True, exist_ok=True)
    (TABLES / f"{filename}.tex").write_text("\n".join(lines) + "\n")
    print(f"wrote {TABLES / filename}.tex")


def sweep_table(shape, filename, caption, label, head):
    lines = []
    for sf in SCALE_FACTORS:
        points = load("sweep_pairs", sf)
        counts = [c for c in sorted({k[2] for k in points if k[1] == shape}) if c != 2048]
        if not lines:
            lines += [r"\begin{table}[t]\centering\scriptsize\setlength{\tabcolsep}{2.6pt}",
                      f"\\caption{{{caption}}}\\label{{{label}}}",
                      r"\begin{tabular}{l" + "r" * len(counts) + r"}\toprule",
                      f"{head} & " + " & ".join(count_label(c) for c in counts) + r" \\ \midrule"]
        lines.append(f"\\multicolumn{{{len(counts) + 1}}}{{l}}{{\\textit{{LDBC SF{sf}}}}} \\\\")
        rows = [(lab, [points.get((lab, shape, c)) for c in counts])
                for lab in sorted_labels({k[0] for k in points if k[1] == shape})]
        lines += format_rows(rows)
        if sf != SCALE_FACTORS[-1]:
            lines.append(r"\addlinespace")
    lines.append(r"\bottomrule\end{tabular}\end{table}")
    write_table(filename, lines)


def ldbc_table(filename):
    queries = ["ic13", "bi19", "bi20"]
    lines = [r"\begin{table}[t]\centering\scriptsize\setlength{\tabcolsep}{3pt}",
             r"\caption{LDBC IC13, BI19 and BI20 as hop-count shortest paths on the undirected knows graph, seconds per query, the median of three runs per parameter set averaged over five parameter sets; rows marked conn.\ split the sources over 32 client connections.}\label{tab:ldbc}",
             r"\begin{tabular}{lrrr}\toprule", r"system & IC13 & BI19 & BI20 \\ \midrule"]
    for sf in SCALE_FACTORS:
        points = load("ldbc_queries", sf)
        lines.append(f"\\multicolumn{{4}}{{l}}{{\\textit{{LDBC SF{sf}}}}} \\\\")
        rows = []
        for lab in sorted_labels({k[0] for k in points}):
            aggregated = []
            for query in queries:
                values = [points[(lab, query, pr)] for pr in sorted({k[2] for k in points if k[0] == lab and k[1] == query})]
                if not values:
                    aggregated.append(None)
                elif all(v["status"] == "ok" for v in values):
                    aggregated.append({"status": "ok", "mean": statistics.mean(v["median"] for v in values)})
                elif any(v["status"] == "exceeded" for v in values):
                    aggregated.append({"status": "exceeded"})
                else:
                    aggregated.append({"status": "err"})
            rows.append((lab, aggregated))
        lines += format_rows(rows)
        if sf != SCALE_FACTORS[-1]:
            lines.append(r"\addlinespace")
    lines.append(r"\bottomrule\end{tabular}\end{table}")
    write_table(filename, lines)


def teaser_figure(filename, sf="100"):
    """Page-one figure: seconds against random pairs per query on linear axes, each system on its fastest path"""
    points = load("sweep_pairs", sf)
    shape = "random_pairs"
    # DuckDB's own recursive CTE (WITH RECURSIVE ... USING KEY) is the in-engine reference: drawn in ink, not in a
    # system colour, since it is the same engine without the operator
    order = ["duckpgq_recursive", "memgraph", "falkordb", "kuzu", "neo4j", "duckpgq_index"]
    names = {"duckpgq_index": "DuckPGQ", "neo4j": "Neo4j", "kuzu": "Kuzu", "falkordb": "FalkorDB",
             "memgraph": "Memgraph", "duckpgq_recursive": "DuckDB recursive CTE"}
    colors = dict(COLOR)
    colors["duckpgq_recursive"] = INK2
    fig, ax = plt.subplots(figsize=(3.33, 2.05))
    ax.axhline(TIMEOUT_S, color=MUTED, linewidth=0.8, linestyle=(0, (4, 3)), zorder=1)
    ax.text(40000, TIMEOUT_S + 8, "600 s limit", color=MUTED, fontsize=6.5, va="bottom", ha="left")
    ends = []
    for system in order:
        counts = sorted({k[2] for k in points if k[1] == shape and k[0] in (system, f"{system}@32")})
        xs, ys, exceeded_at = [], [], None
        for count in counts:
            candidates = [points.get((label, shape, count)) for label in (system, f"{system}@32")]
            ok = [c["mean"] for c in candidates if c and c["status"] == "ok"]
            if ok:
                xs.append(count)
                ys.append(min(ok))
            elif any(c and c["status"] == "exceeded" for c in candidates):
                exceeded_at = count
                break
        if not xs and exceeded_at is None:
            continue  # not measured at this scale factor (yet)
        color = colors[system]
        ours = system == "duckpgq_index"
        lw, z = (2.4, 5) if ours else (1.5, 3)
        if system == "duckpgq_recursive" and exceeded_at is not None and not xs:
            xs, ys = [exceeded_at], [TIMEOUT_S]
        ax.plot(xs, ys, color=color, linewidth=lw, marker="o", markersize=2.8 if ours else 2.4,
                markeredgecolor=SURFACE, markeredgewidth=0.5, zorder=z, clip_on=False)
        if exceeded_at is not None:
            ax.plot([xs[-1], exceeded_at], [ys[-1], TIMEOUT_S], color=color, linewidth=1.2, linestyle=(0, (1.5, 1.5)),
                    zorder=z)
            ax.plot([exceeded_at], [TIMEOUT_S], linestyle="none", marker="^", markersize=5, markerfacecolor=SURFACE,
                    markeredgecolor=color, markeredgewidth=1.1, zorder=z + 1, clip_on=False)
            if system == "duckpgq_recursive":
                # Dies within the first percent of the axis: label it at its triangle, not at the right edge
                k = f"{exceeded_at // 1024}k" if exceeded_at >= 1024 else str(exceeded_at)
                ax.annotate(f"{names[system]}: > 600 s at {k} pairs", (exceeded_at, TIMEOUT_S), xytext=(7, -9),
                            textcoords="offset points", color=color, fontsize=6.5, va="top", ha="left")
            else:
                ends.append((TIMEOUT_S, exceeded_at, f"{names[system]} > 600 s", color))
        else:
            value = ys[-1]
            text = f"{value:.1f} s" if value < 10 else f"{value:.0f} s"
            ends.append((value, xs[-1], f"{names[system]} {text}", color))
    ax.set_xlim(0, 65536)
    ax.set_ylim(0, 640)
    ax.set_xticks([0, 16384, 32768, 49152, 65536])
    ax.set_xticklabels(["0", "16k", "32k", "48k", "64k"])
    ax.set_yticks([0, 150, 300, 450, 600])
    ax.set_xlabel("shortest-path pairs in one query", labelpad=2)
    ax.set_ylabel("seconds", labelpad=2)
    ax.grid(axis="x", visible=False)
    style_axes(ax)
    # Line-end labels right of the panel, pushed apart on the linear axis
    ends.sort()
    placed = []
    for y, x, name, color in ends:
        placed.append(y if not placed or y - placed[-1] >= 48 else placed[-1] + 48)
    for (y, x, name, color), yy in zip(ends, placed):
        bold = name.startswith("DuckPGQ")
        ax.annotate(name, (65536, yy), xytext=(6, 0), textcoords="offset points", color=color, va="center",
                    ha="left", fontsize=7 if bold else 6.5, fontweight="bold" if bold else "normal",
                    annotation_clip=False)
    fig.subplots_adjust(left=0.13, right=0.70, bottom=0.19, top=0.95)
    save(fig, filename)


# BI19 over the index with lanes per distinct source, measured after rc1 (29 Oct); remove once the rerun has it
BI19_OVERRIDE_S = {}  # rc13 measured BI19 on the frozen build; no override (the pre-rc13 values were 0.015, 0.252, 1.6 s)


def ldbc_query_seconds(points, system, query):
    """Median of the runs per parameter set, averaged over the sets, on the system's faster configuration; None for
    no result, "exceeded" when any set timed out"""
    best = None
    for label in (system, f"{system}@32"):
        params = sorted({k[2] for k in points if k[0] == label and k[1] == query})
        if not params:
            continue
        values = [points[(label, query, pr)] for pr in params]
        if any(v["status"] == "exceeded" for v in values):
            value = "exceeded"
        elif all(v["status"] == "ok" for v in values):
            value = statistics.mean(v["median"] for v in values)
        else:
            continue
        if best is None or best == "exceeded" or (value != "exceeded" and value < best):
            best = value
    return best


def teaser_bi19_figure(filename, sf="300"):
    """Page-one figure: LDBC BI19 at one scale factor, seconds on a linear axis, one bar per system"""
    points = load("ldbc_queries", sf)
    systems = [("duckpgq_index", "DuckPGQ"), ("neo4j", "Neo4j"), ("kuzu", "Kuzu"), ("memgraph", "Memgraph"),
               ("falkordb", "FalkorDB"), ("neo4j_gds", "Neo4j GDS")]
    colors = dict(COLOR)
    colors["neo4j_gds"] = "#7fcfae"
    rows = []
    for system, name in systems:
        value = ldbc_query_seconds(points, system, "bi19")
        if system == "duckpgq_index" and sf in BI19_OVERRIDE_S:
            value = BI19_OVERRIDE_S[sf]
        if value is not None:
            rows.append((name, value, colors[system], system == "duckpgq_index"))
    fig, ax = plt.subplots(figsize=(3.33, 1.75))
    ys = list(range(len(rows)))[::-1]
    for y, (name, value, color, ours) in zip(ys, rows):
        if value == "exceeded":
            ax.barh(y, TIMEOUT_S, height=0.62, color=SURFACE, edgecolor=color, hatch="////", linewidth=0.6,
                    alpha=0.55, zorder=3)
            ax.text(TIMEOUT_S - 10, y, "timed out, > 600 s", va="center", ha="right", fontsize=6.3, color=INK2,
                    zorder=4, bbox=dict(boxstyle="square,pad=0.15", facecolor=SURFACE, edgecolor="none"))
        else:
            ax.barh(y, value, height=0.62, color=color, zorder=3)
            text = f"{value:.1f} s" if value < 10 else f"{value:.0f} s"
            ax.text(value + 8, y, text, va="center", ha="left", fontsize=7.5 if ours else 6.5,
                    fontweight="bold" if ours else "normal", color=color if ours else INK2, zorder=4)
    ax.set_yticks(ys)
    ax.set_yticklabels([r[0] for r in rows], fontsize=7)
    for label, row in zip(ax.get_yticklabels(), rows):
        if row[3]:
            label.set_fontweight("bold")
            label.set_color(colors["duckpgq_index"])
    ax.set_xlim(0, 640)
    ax.set_xticks([0, 150, 300, 450, 600])
    ax.set_xlabel(f"seconds per query, LDBC BI19 at SF{sf}", labelpad=2)
    ax.axvline(TIMEOUT_S, color=MUTED, linewidth=0.8, linestyle=(0, (4, 3)), zorder=2)
    ax.grid(axis="y", visible=False)
    style_axes(ax)
    fig.subplots_adjust(left=0.22, right=0.97, bottom=0.22, top=0.97)
    save(fig, filename)


def teaser_bi19_scales_figure(filename):
    """Page-one figure: LDBC BI19 at SF10, SF100 and SF300, one linear panel each, scaled to Neo4j's time so that the
    DuckPGQ bar is read against the strongest baseline; slower systems run off the panel with their value"""
    systems = [("duckpgq_index", "DuckPGQ"), ("neo4j", "Neo4j"), ("kuzu", "Kuzu"), ("memgraph", "Memgraph"),
               ("falkordb", "FalkorDB"), ("neo4j_gds", "Neo4j GDS")]
    colors = dict(COLOR)
    colors["neo4j_gds"] = "#7fcfae"
    fig, axes = plt.subplots(1, 3, figsize=(3.33, 1.85), sharey=True)
    for ax, sf in zip(axes, SCALE_FACTORS):
        points = load("ldbc_queries", sf)
        rows = []
        for system, name in systems:
            value = ldbc_query_seconds(points, system, "bi19")
            if system == "duckpgq_index" and sf in BI19_OVERRIDE_S:
                value = BI19_OVERRIDE_S[sf]
            rows.append((name, value, colors[system], system == "duckpgq_index"))
        ours = rows[0][1]
        edge = rows[1][1] * 2.1
        ys = list(range(len(rows)))[::-1]
        for y, (name, value, color, is_ours) in zip(ys, rows):
            timed_out = not isinstance(value, float)
            if timed_out or value > edge:
                ax.barh(y, edge, height=0.64, color=SURFACE if timed_out else color, edgecolor=color,
                        hatch="////" if timed_out else None, linewidth=0.5, alpha=0.5 if timed_out else 0.55, zorder=3)
                # Break marks: the bar continues beyond the panel
                for dx in (0.10, 0.13):
                    ax.plot([edge * (1 - dx) - edge * 0.012, edge * (1 - dx) + edge * 0.012], [y - 0.38, y + 0.38],
                            color=SURFACE, linewidth=1.4, zorder=3.5, solid_capstyle="butt")
                if timed_out:
                    text = "> 600 s"
                else:
                    text = f"{value * 1000:.0f} ms" if value < 1 else (f"{value:.1f} s" if value < 10 else f"{value:.0f} s")
                    text += f" {value / ours:.0f}×"
                ax.text(edge * 0.84, y, text, va="center", ha="right", fontsize=5.4, color=INK2, zorder=4,
                        bbox=dict(boxstyle="square,pad=0.1", facecolor=SURFACE, edgecolor="none"))
                continue
            ax.barh(y, value, height=0.64, color=color, zorder=3)
            text = f"{value * 1000:.0f} ms" if value < 1 else (f"{value:.1f} s" if value < 10 else f"{value:.0f} s")
            if not is_ours:
                text += f" {value / ours:.0f}×" if value / ours >= 10 else f" {value / ours:.1f}×"
            ax.text(value + edge * 0.03, y, text, va="center", ha="left", fontsize=6 if is_ours else 5.4,
                    fontweight="bold" if is_ours else "normal", color=color if is_ours else INK2, zorder=4)
        ax.set_xlim(0, edge)
        ax.set_xticks([])
        ax.set_title(f"SF{sf}", loc="left", fontsize=7, pad=2)
        ax.grid(False)
        style_axes(ax)
        ax.spines["bottom"].set_visible(False)
    axes[0].set_yticks(list(range(len(systems)))[::-1])
    axes[0].set_yticklabels([n for _, n in systems], fontsize=6.3)
    for label in axes[0].get_yticklabels():
        if label.get_text() == "DuckPGQ":
            label.set_fontweight("bold")
            label.set_color(colors["duckpgq_index"])
    fig.subplots_adjust(left=0.20, right=0.99, bottom=0.03, top=0.90, wspace=0.12)
    save(fig, filename)


# ---------------------------------------------------------------------------------------------------------------
# Paper figures and tables from the rc13 archive (9 Nov): one scaling grid, the Graphalytics table, fixed costs,
# the update stream against the baselines, the concurrency figure and the Section 3 crossover at SF100.

EXTRA_SYSTEMS = [
    ("neo4j_gds", "Neo4j GDS", "#1baf7a", (0, (3, 1.5))),
    ("duckpgq_scalar", "DuckPGQ 2023 UDFs", INK2, (0, (3, 1.5))),
    ("duckpgq_recursive", "DuckDB recursive CTE", MUTED, (0, (1, 1.5))),
]
LINESTYLE = {key: style for key, _, _, style in EXTRA_SYSTEMS}
ALL_SYSTEMS = SYSTEMS + [(key, name, color) for key, name, color, _ in EXTRA_SYSTEMS]


def fastest_path(points, system, shape, count):
    """The faster of the system's single-connection and 32-connection rows at this point; a completed row beats a
    timed-out one (the paper's "fastest formulation")"""
    candidates = [points[(label, shape, count)] for label in (system, f"{system}@32") if (label, shape, count) in points]
    if not candidates:
        return None
    ok = [c for c in candidates if c["status"] == "ok"]
    return min(ok, key=lambda c: c["mean"]) if ok else candidates[0]


def scaling_grid_figure(filename):
    """Figure 6.1: random pairs (top) and sources x all (bottom) at the three scale factors, every system in its
    fastest formulation, the two DuckDB-native routes dashed in ink."""
    shapes = [("random_pairs", "pairs per query", "random pairs"),
              ("multi_source", "sources per query, each to every reachable person", "sources × all")]
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 4.9), sharey="row", squeeze=False)
    used = []
    for row, (shape, xlabel, rowtitle) in enumerate(shapes):
        for col, sf in enumerate(SCALE_FACTORS):
            ax = axes[row][col]
            points = load("sweep_pairs", sf)
            counts = sorted({k[2] for k in points if k[1] == shape})
            ax.set_xscale("log", base=2)
            ax.set_yscale("log")
            ax.axhline(TIMEOUT_S, color=AXIS, linewidth=0.6)
            if col == 0:
                ax.text(counts[0], TIMEOUT_S * 1.25, "600 s timeout", color=MUTED, fontsize=6.5, va="bottom")
            label_positions = []
            for system, name, color in ALL_SYSTEMS:
                xs, ys, exceeded_x = [], [], []
                for count in counts:
                    point = fastest_path(points, system, shape, count)
                    if point is None:
                        continue
                    if point["status"] == "ok":
                        xs.append(count)
                        ys.append(point["mean"])
                    elif point["status"] == "exceeded":
                        exceeded_x.append(count)
                if not xs and not exceeded_x:
                    continue
                if (system, name, color) not in used:
                    used.append((system, name, color))
                linewidth, alpha, zorder = emphasis(system)
                plot_series(ax, xs, ys, exceeded_x, color, linestyle=LINESTYLE.get(system, "-"),
                            linewidth=linewidth, alpha=alpha, zorder=zorder)
                if xs:
                    label_positions.append((ys[-1], xs[-1], name, color))
            ax.set_ylim(0.001, 3000)
            if col == len(SCALE_FACTORS) - 1:
                direct_labels(ax, label_positions, max(counts) * 1.3, factor=2.0)
            if row == 0:
                ax.set_title(f"LDBC SF{sf}", loc="left")
            ax.set_xticks(counts)
            ax.set_xticklabels([count_label(c) if c != 2048 else "" for c in counts], fontsize=6)
            if col == 1:
                ax.set_xlabel(xlabel, fontsize=7.5)
            style_axes(ax)
        axes[row][0].set_ylabel(f"{rowtitle}: seconds per query")
    handles = [Line2D([], [], color=c, linewidth=1.6, linestyle=LINESTYLE.get(k, "-"), marker="o", markersize=3.6,
                      markeredgecolor=SURFACE) for k, _, c in used]
    labels = [n for _, n, _ in used]
    handles.append(Line2D([], [], linestyle="none", marker="^", markersize=5, markerfacecolor=SURFACE, markeredgecolor=INK2))
    labels.append("timed out")
    fig.legend(handles, labels, loc="lower center", ncol=5, bbox_to_anchor=(0.5, -0.01), handlelength=2.4, columnspacing=1.1)
    fig.tight_layout(rect=(0, 0.08, 0.9, 1.0))
    save(fig, filename)


def graphalytics_table(filename):
    """Table: sources x all on the three Graphalytics graphs (k = 1, 16, 128, 1024) and the 65,536-pair point where
    pairs are meaningful (not on the citation DAG, where every random pair is unreachable)."""
    sources = [1, 16, 128, 1024]
    lines = [r"\begin{table}[t]\centering\scriptsize\setlength{\tabcolsep}{3pt}",
             r"\caption{Three Graphalytics graphs: distances from $k$ sources to every reachable vertex, and 65\,536 random "
             r"pairs where the pairs are reachable (on the citation DAG no random pair is). Median of three runs, 32 threads, "
             r"600\,s timeout, each baseline in its fastest formulation.}\label{tab:graphalytics}",
             r"\begin{tabular}{lrrrrr}\toprule",
             r"system & 1 source & 16 & 128 & 1024 & 65\,536 pairs \\ \midrule"]
    blocks = 0
    for directory, name, note in GRAPHALYTICS:
        points = load("sweep_pairs", directory)
        if not points:
            continue
        if blocks:
            lines.append(r"\addlinespace")
        blocks += 1
        lines.append(f"\\multicolumn{{6}}{{l}}{{\\textit{{{name.replace(chr(95), chr(92) + chr(95))}: {note}}}}} \\\\")
        rows = []
        for sysname in sorted_labels({k[0].split("@")[0] for k in points}):
            pts = [fastest_path(points, sysname, "multi_source", k) for k in sources]
            pair_pt = None if "cit_Patents" in directory else fastest_path(points, sysname, "random_pairs", 65536)
            if all(p is None for p in pts) and pair_pt is None:
                continue
            rows.append((sysname, pts + [pair_pt]))
        lines += format_rows(rows)
    lines.append(r"\bottomrule\end{tabular}\end{table}")
    write_table(filename, lines)


FIXED_COST_GRAPHS = [("10", "SNB SF10"), ("100", "SNB SF100"), ("300", "SNB SF300"),
                     ("graphalytics_cit_Patents", "cit-Patents"), ("graphalytics_datagen_8_4_fb", r"datagen-8\_4-fb"),
                     ("graphalytics_graph500_24", "graph500-24")]


def seconds_label(s):
    if s is None:
        return "--"
    if s < 10:
        return f"{s:.2f}\\,s" if s < 1 else f"{s:.1f}\\,s"
    if s < 120:
        return f"{s:.0f}\\,s"
    return f"{s / 60:.0f}\\,min"


def fixed_costs_table(filename):
    """Table: edges, index creation and bytes per edge against the native systems' import time and store size."""
    lines = [r"\begin{table}[t]\centering\scriptsize\setlength{\tabcolsep}{2.5pt}",
             r"\caption{Fixed costs per graph, 32 threads: creating the CSR index over the loaded edge table and its size "
             r"per edge, against the native systems' import from the same CSV files (Kuzu from Parquet) and Kuzu's store "
             r"size per edge. Memgraph, FalkorDB and Neo4j were not loaded with the two large Graphalytics graphs.}"
             r"\label{tab:fixed-costs}",
             r"\begin{tabular}{lrrrrrrrr}\toprule",
             r"graph & edges & \multicolumn{2}{c}{CSR index} & \multicolumn{2}{c}{Kuzu} & Neo4j & Memgraph & FalkorDB \\",
             r" & & create & B/edge & import & B/edge & import & import & import \\ \midrule"]
    for sf, name in FIXED_COST_GRAPHS:
        directory = RESULTS / (f"sf{sf}" if sf.isdigit() else sf)
        files = newest_files(directory, "fixed_costs_*.csv")
        if not files:
            continue
        rows = list(csv.DictReader(open(files[-1])))
        edges = int(rows[0]["edges"])
        builds = [float(r["seconds"]) for r in rows if r["system"] == "duckpgq_index" and r["measure"].startswith("index_build")]
        bpe = next((float(r["bytes_per_edge"]) for r in rows if r["system"] == "duckpgq_index"), None)
        loads = {r["system"]: (float(r["seconds"]), float(r["bytes_per_edge"])) for r in rows if r["measure"] == "load"}
        imp = lambda key: seconds_label(loads[key][0]) if key in loads else "--"
        kuzu_bpe = f"{loads['kuzu'][1]:.0f}" if "kuzu" in loads else "--"
        lines.append(f"{name} & {edges / 1e6:.1f}\\,M & {seconds_label(statistics.median(builds) if builds else None)} & "
                     f"{bpe:.1f} & {imp('kuzu')} & {kuzu_bpe} & {imp('neo4j')} & {imp('memgraph')} & {imp('falkordb')} \\\\")
    lines.append(r"\bottomrule\end{tabular}\end{table}")
    write_table(filename, lines)


UPDATE_ROWS = [("index_auto", "DuckPGQ index, automatic folds"), ("index_nofold", "DuckPGQ index, no fold"),
               ("index_per_row", "DuckPGQ index, visibility per row"), ("build", "DuckPGQ build")]
BASELINE_UPDATE = [("kuzu", "Kuzu"), ("memgraph", "Memgraph"), ("neo4j", "Neo4j"), ("falkordb", "FalkorDB")]


def update_metrics(path, config=None):
    """(query median ms, query p99 ms, commit median ms) over an update-stream file, optionally one DuckPGQ config"""
    if path is None:
        return None
    rows = [r for r in csv.DictReader(open(path)) if config is None or r["config"] == config]
    queries = [1000 * float(r["query_s"]) for r in rows if r.get("query_s")]
    commits = [1000 * float(r["update_s"]) for r in rows if r.get("update_s")]
    if not queries or not commits:
        return None
    return statistics.median(queries), p99(queries), statistics.median(commits)


def update_stream_table(filename):
    """Table: the SF10 update stream (190 commits of 2000 edge changes, one query after each) for the DuckPGQ routes
    and the baselines: query latency for one source to 1024 destinations and for 1024 random pairs, commit latency."""
    directory = RESULTS / "sf10"

    def newest(pattern):
        files = newest_files(directory, pattern)
        return files[-1] if files else None

    def fmt(metrics):
        return f"{metrics[0]:.0f} & {metrics[1]:.0f}" if metrics else "-- & --"

    def commit_of(a, b):
        value = a[2] if a else (b[2] if b else None)
        return f"{value:.0f}" if value is not None else "--"

    lines = [r"\begin{table}[t]\centering\scriptsize\setlength{\tabcolsep}{3pt}",
             r"\caption{The update stream at SF10: a writer commits 2000 edge changes (about 1770 inserts and 230 deletes) "
             r"190 times, and one query runs after each commit; 32 threads. Query latency in ms (median and p99 over the "
             r"190 queries) for one source to 1024 destinations and for 1024 random pairs, and the median commit.}"
             r"\label{tab:updates}",
             r"\begin{tabular}{lrrrrr}\toprule",
             r"system & \multicolumn{2}{c}{1 source, 1024 dst.} & \multicolumn{2}{c}{1024 pairs} & commit \\",
             r" & median & p99 & median & p99 & median \\ \midrule"]
    ours_sssp = newest("update_sssp_pairs1024_threads32_batch2000x190_*.csv")
    ours_p2p = newest("update_point_to_point_pairs1024_threads32_batch2000x190_*.csv")
    for config, label in UPDATE_ROWS:
        a, b = update_metrics(ours_sssp, config), update_metrics(ours_p2p, config)
        lines.append(f"{label} & {fmt(a)} & {fmt(b)} & {commit_of(a, b)} \\\\")
    lines.append(r"\addlinespace")
    for sysname, label in BASELINE_UPDATE:
        a = update_metrics(newest(f"update_{sysname}_sssp_pairs1024_threads32_batch2000x*.csv"))
        b = update_metrics(newest(f"update_{sysname}_point_to_point_pairs1024_threads32_batch2000x*.csv"))
        lines.append(f"{label} & {fmt(a)} & {fmt(b)} & {commit_of(a, b)} \\\\")
    lines.append(r"\bottomrule\end{tabular}\end{table}")
    write_table(filename, lines)


def concurrent_series(sf):
    """route -> readers -> (query median ms, query p99 ms, commit median ms, peak RSS MiB) for the readers-under-a-writer
    runs with the default fold ratio and the 200 ms writer pause; the newest file per (route, readers) wins"""
    series = {"query_index": {}, "query_build": {}}
    for path in newest_files(RESULTS / f"sf{sf}", "concurrent_sssp_pairs1024_threads32_readers*_pause200_fold0.1_*.csv"):
        rows = list(csv.DictReader(open(path)))
        if not rows or not rows[0].get("readers"):
            continue
        readers = int(rows[0]["readers"])
        commits = [1000 * float(r["elapsed_s"]) for r in rows if r["kind"] == "commit"]
        peaks = [float(r["peak_rss_mib"]) for r in rows if r.get("peak_rss_mib")]
        for kind in series:
            values = [1000 * float(r["elapsed_s"]) for r in rows if r["kind"] == kind]
            if values:
                series[kind][readers] = (statistics.median(values), p99(values),
                                         statistics.median(commits) if commits else None, max(peaks) if peaks else None)
    return series


def concurrency_figure(filename):
    """Figure 6.5: top, single-pair throughput against clients (index route against build route), with the peak
    memory at 32 clients; bottom, query latency of readers under a writer committing 2000 changes every 200 ms,
    median as the line and the p99 as a hollow marker above it, with the writer's commit latency at the largest reader
    count. One column per scale factor."""
    columns = []
    for sf in SCALE_FACTORS:
        points = {}
        for path in newest_files(RESULTS / f"sf{sf}", "throughput_sf*_threads32_*.csv"):
            for r in csv.DictReader(open(path)):
                if r["queries_per_s"]:
                    points[(r["route"], int(r["clients"]))] = (float(r["queries_per_s"]), float(r["median_ms"]),
                                                               float(r["p99_ms"]), float(r.get("peak_rss_mib") or 0))
        series = concurrent_series(sf)
        if points or series["query_index"]:
            columns.append((sf, points, series))
    if not columns:
        print("no throughput or concurrent results yet, skipping", filename)
        return
    fig, axes = plt.subplots(2, len(columns), figsize=(7.2, 4.6), squeeze=False, sharey="row")
    for col, (sf, points, series) in enumerate(columns):
        top, bottom = axes[0][col], axes[1][col]
        for ax in (top, bottom):
            ax.set_xscale("log", base=2)
            ax.set_yscale("log")
            style_axes(ax)
        clients = sorted({c for _, c in points})
        peaks = {}
        for (system, name, color), route in zip(ROUTES, ("index", "build")):
            xs = [c for c in clients if (route, c) in points]
            if not xs:
                continue
            rates = [points[(route, c)][0] for c in xs]
            linewidth, alpha, zorder = emphasis(system)
            plot_series(top, xs, rates, [], color, linewidth=linewidth, alpha=alpha, zorder=zorder)
            top.annotate(f"{rates[-1]:.0f}/s", (xs[-1], rates[-1]), xytext=(4, 0), textcoords="offset points",
                         fontsize=6.5, color=color, va="center", annotation_clip=False)
            peaks[route] = points[(route, xs[-1])][3] / 1024
        if peaks:
            top.text(0.03, 0.96, "peak memory at 32 clients\n" + ", ".join(f"{r} {peaks[r]:.1f} GiB" for r in ("index", "build") if r in peaks),
                     transform=top.transAxes, fontsize=6, color=INK2, va="top", ha="left", linespacing=1.3)
        top.set_title(f"LDBC SF{sf}", loc="left")
        top.set_xticks(clients)
        top.set_xticklabels([str(c) for c in clients], fontsize=6.5)
        top.set_xlabel("clients, one random pair per query" if col == 1 else "clients", fontsize=7.5)
        readers = sorted(set(series["query_index"]) | set(series["query_build"]))
        for (system, name, color), kind in zip(ROUTES, ("query_index", "query_build")):
            xs = [r for r in readers if r in series[kind]]
            if not xs:
                continue
            medians = [series[kind][r][0] for r in xs]
            tails = [series[kind][r][1] for r in xs]
            linewidth, alpha, zorder = emphasis(system)
            plot_series(bottom, xs, medians, [], color, linewidth=linewidth, alpha=alpha, zorder=zorder)
            bottom.vlines(xs, medians, tails, color=color, alpha=0.5 * alpha, linewidth=0.7, zorder=zorder - 1)
            bottom.plot(xs, tails, linestyle="none", marker="o", markersize=3.6, markerfacecolor=SURFACE,
                        markeredgecolor=color, markeredgewidth=1.0, alpha=alpha, zorder=zorder)
            bottom.annotate(f"{ms_label(medians[-1])} ms", (xs[-1], medians[-1]), xytext=(4, 0), textcoords="offset points",
                            fontsize=6.5, color=color, va="center", annotation_clip=False)
        both = [r for r in readers if r in series["query_index"] and r in series["query_build"]
                and series["query_index"][r][2] is not None and series["query_build"][r][2] is not None]
        if both:
            r = both[-1]
            bottom.text(0.03, 0.96, f"writer's commit at {r} readers\nindex {ms_label(series['query_index'][r][2])} ms, "
                                    f"build {ms_label(series['query_build'][r][2])} ms",
                        transform=bottom.transAxes, fontsize=6, color=INK2, va="top", ha="left", linespacing=1.3)
        bottom.set_xticks(readers)
        bottom.set_xticklabels([str(r) for r in readers], fontsize=6.5)
        bottom.set_xlabel("readers under a writer, one source to 1024 destinations" if col == 1 else "readers", fontsize=7.5)
    axes[0][0].set_ylabel("queries per second")
    axes[1][0].set_ylabel("query latency (ms)")
    handles = [Line2D([], [], color=c, linewidth=1.6, marker="o", markersize=3.6, markeredgecolor=SURFACE) for _, _, c in ROUTES]
    handles.append(Line2D([], [], linestyle="none", marker="o", markersize=3.6, markerfacecolor=SURFACE,
                          markeredgecolor=INK2, markeredgewidth=1.0))
    fig.legend(handles, [n for _, n, _ in ROUTES] + ["p99 of the same queries"], loc="lower center", ncol=3,
               bbox_to_anchor=(0.5, -0.01), handlelength=2.4, columnspacing=1.2)
    fig.tight_layout(rect=(0, 0.06, 0.93, 1.0))
    save(fig, filename)


def crossover_sf100_figure(filename, sf="100"):
    """Section 3.4: the three ways of running k distinct sources on the index route at one scale factor, k random
    pairs (left) and k sources x 200 destinations (right); the dotted line is the smallest k at which the frontier
    traversal is no longer within 10% of the best."""
    kernels = {c for c, _, _ in CROSSOVER_KERNELS}
    latest = {}
    for path in newest_files(RESULTS / f"sf{sf}", "microbench_sf*_threads32_*.csv"):
        for r in csv.DictReader(open(path)):
            if r["config"] not in kernels or r["status"] != "ok":
                continue
            k = int(r["pairs"]) if r["shape"] == "random_pairs" else int(r["sources"] or 0)
            latest.setdefault((r["config"], r["shape"], k, path.name), []).append(r)
    series = {}
    for (config, shape, k, _), rows in sorted(latest.items(), key=lambda item: item[0][3]):
        series[(config, shape, k)] = statistics.median(float(r["query_s"]) for r in rows)
    if not series:
        print("no crossover results yet, skipping", filename)
        return
    shapes = [("random_pairs", "k random pairs"), ("all_pairs", "k sources × 200 destinations")]
    fig, axes = plt.subplots(1, 2, figsize=(3.6, 2.3), sharey=True, squeeze=False)
    for ax, (shape, title) in zip(axes[0], shapes):
        ks = sorted({k for (c, s_, k) in series if s_ == shape})
        last_winning = None
        for k in ks:
            times = {c: series[(c, shape, k)] for c, _, _ in CROSSOVER_KERNELS if (c, shape, k) in series}
            if "single_source_kernel" in times and len(times) >= 2 and times["single_source_kernel"] <= 1.1 * min(
                    t for c, t in times.items() if c != "single_source_kernel"):
                last_winning = k
        later = [k for k in ks if last_winning is None or k > last_winning]
        crossover = later[0] if later else None
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        style_axes(ax)
        for config, name, color in CROSSOVER_KERNELS:
            xs = [k for k in ks if (config, shape, k) in series]
            plot_series(ax, xs, [series[(config, shape, k)] for k in xs], [], color,
                        linewidth=1.6 if config == "single_source_kernel" else 1.3,
                        zorder=5 if config == "single_source_kernel" else 3)
        if crossover is not None:
            ax.axvline(crossover, color=INK2, linestyle=(0, (1.5, 2)), linewidth=0.8, zorder=2)
            ax.annotate(f"k = {crossover}", (crossover, ax.get_ylim()[1]), xytext=(3, -2), textcoords="offset points",
                        fontsize=6.5, color=INK2, va="top", ha="left")
        ax.set_xticks(ks)
        ax.set_xticklabels([str(k) if k in (1, 16, 256, 4096) else "" for k in ks], fontsize=6.5)
        ax.set_title(title, loc="left", fontsize=7.5)
        ax.set_xlabel("distinct sources k", fontsize=7.5)
    axes[0][0].set_ylabel("seconds per query")
    handles = [Line2D([], [], color=c, linewidth=1.6, marker="o", markersize=3.6, markeredgecolor=SURFACE)
               for _, _, c in CROSSOVER_KERNELS]
    fig.legend(handles, ["frontier traversal", "lanes, one per distinct source", "lanes, one per pair"],
               loc="lower center", ncol=1, bbox_to_anchor=(0.5, -0.02), handlelength=2.0, fontsize=6.5)
    fig.tight_layout(rect=(0, 0.2, 1, 1))
    save(fig, filename)



ABLATION_ROWS = [
    ("baseline", "default, 32 threads"),
    ("single_source_kernel", "frontier traversal for every query"),
    ("multi_source_dedup", "lanes for every query"),
    ("lane_per_pair", "one lane per pair"),
    ("no_source_grouping", "pairs not grouped by source"),
    ("no_grouped_batches", "batches not grouped by the scheduler"),
    ("sched_g1_w32", "one batch at a time"),
    ("no_frontier_bitmap", "no frontier summary bitmap"),
    ("no_sparse_levels", "no sparse levels"),
    ("partitions_16384", "destination ranges of 16\\,384"),
    ("partitions_4096", "destination ranges of 4096"),
    ("reverse_index", "with reverse partitions"),
    ("pair_mode", "sources $\\times$ all as a pair list"),
    ("reach_per_source", "reach as one search per source"),
    ("paths", "paths instead of lengths"),
    ("threads_16", "16 threads"),
    ("threads_4", "4 threads"),
    ("threads_1", "1 thread"),
]
ABLATION_COLUMNS = [("random_pairs", "1"), ("random_pairs", "2048"), ("random_pairs", "65536"),
                    ("multi_source", "1"), ("multi_source", "1024"), ("same_dst", "1024")]


def ablation_table(filename, sf="100", since_ns=1793830000000000000, system="duckpgq_index"):
    """Table: the operator's mechanisms switched off or changed one at a time, index route at SF100, six query
    shapes; medians of the microbenchmark repeats on the frozen build (files from the rc13 run only)."""
    by = defaultdict(list)
    for path in newest_files(RESULTS / f"sf{sf}", "microbench_sf*_threads32_*.csv"):
        if int(path.stem.rsplit("_", 1)[1]) < since_ns:
            continue
        for r in csv.DictReader(open(path)):
            if r["system"] != system or r["status"] != "ok":
                continue
            count = r["pairs"] if r["shape"] != "multi_source" else r["sources"]
            by[(r["config"], r["shape"], count)].append(float(r["query_s"]))
    if not by:
        print("no rc13 microbenchmark rows, skipping", filename)
        return

    def fmt(config, shape, count):
        values = by.get((config, shape, count))
        if not values:
            return "--"
        v = statistics.median(values)
        return f"{v * 1000:.0f}\\,ms" if v < 1 else f"{v:.1f}\\,s"

    lines = [r"\begin{table}[t]\centering\scriptsize\setlength{\tabcolsep}{2.6pt}",
             r"\caption{One mechanism switched off or changed at a time, over the index at SF100, 32 threads unless stated: "
             r"median seconds per query of three runs for one random pair, 2048 and 65\,536 random pairs, one source and 1024 "
             r"sources to every reachable person, and 1024 sources to one destination. The index of the sweep has no reverse "
             r"partitions; the row that adds them is the exception.}\label{tab:ablation}",
             r"\begin{tabular}{lrrrrrr}\toprule",
             r" & \multicolumn{3}{c}{random pairs} & \multicolumn{2}{c}{sources $\times$ all} & $\to$ 1 \\",
             r"configuration & 1 & 2048 & 65\,536 & 1 & 1024 & 1024 \\ \midrule"]
    for config, label in ABLATION_ROWS:
        if not any((config, shape, count) in by for shape, count in ABLATION_COLUMNS):
            continue
        lines.append(f"{label} & " + " & ".join(fmt(config, shape, count) for shape, count in ABLATION_COLUMNS) + r" \\")
    lines.append(r"\bottomrule\end{tabular}\end{table}")
    write_table(filename, lines)


def save(fig, filename):
    FIGURES.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIGURES / f"{filename}.pdf")
    fig.savefig(FIGURES / f"{filename}.png", dpi=220)
    plt.close(fig)
    print(f"wrote {FIGURES / filename}.pdf/.png")


if __name__ == "__main__":
    sweep_figure("random_pairs", "pairs per query", "random_pairs",
                 "Bound pairs: n random pairs per query; baselines on their best path")
    sweep_figure("multi_source", "sources per query (each to every person)", "sources_all",
                 "Sources × all persons: distances to every reachable vertex from k sources")
    ldbc_figure("ldbc_queries")
    client_threads_figure("client_threads")
    speedup_figure("speedup")
    teaser_figure("teaser_pairs")
    teaser_bi19_figure("teaser_bi19_sf300")
    teaser_bi19_scales_figure("teaser")
    scaling_figure("scaling")
    throughput_figure("throughput")
    first_query_figure("first_query")
    updates_figure("updates")
    updates_timeline_figure("updates_timeline")
    graphalytics_figure("graphalytics")
    concurrent_figure("concurrent")
    fold_timeline_figure("fold_timeline")
    # W35 clients figure; a second def named throughput_figure used to overwrite throughput.pdf (the sweep's
    # pairs-per-second view that the paper includes) until 2 Nov
    client_throughput_figure("throughput_clients")
    crossover_figure("crossover")
    sweep_table("random_pairs", "random_pairs",
                r"Shortest-path lengths for $n$ random pairs per query, median of three runs, 32 threads, 600\,s timeout; "
                r"rows marked conn.\ split the pairs over 32 client connections.", "tab:random-pairs", "system / pairs")
    sweep_table("multi_source", "sources_all",
                r"Distances from $k$ sources to every reachable person, median of three runs; n/r: not run above 500M pairs.",
                "tab:sources-all", "system / sources")
    ldbc_table("ldbc_queries")
    scaling_grid_figure("scaling_grid")
    graphalytics_table("graphalytics")
    fixed_costs_table("fixed_costs")
    update_stream_table("update_stream")
    concurrency_figure("concurrency")
    crossover_sf100_figure("crossover_sf100")
    ablation_table("ablation")
