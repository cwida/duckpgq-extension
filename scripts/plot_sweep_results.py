#!/usr/bin/env python3
"""Figures for the W27/W17 sweeps: random pairs, sources x all, LDBC queries, client-side concurrency.

Reads every sweep_pairs_*.csv and ldbc_queries_*.csv under data/ldbc-pathfinding/results/sf{10,100,300}; when a
(system, shape, count) point appears in several files the newest file wins, so reruns replace earlier numbers.
Writes PDF and PNG to paper/figures/. Colours follow the entity (one colour per system across every figure), six
categorical slots in the validated order of the data-viz palette; the three slots with low contrast on the light
surface get direct labels at the line ends (relief rule) and every number is also in the plan's tables.
"""
import csv
import os
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
RESULTS = Path(os.environ.get("DUCKPGQ_RESULTS_DIR", str(REPO / "data" / "ldbc-pathfinding" / "results")))
OUT = REPO / "paper" / "figures"
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
    files = sorted((RESULTS / f"sf{sf}").glob(f"{prefix}_*.csv"), key=lambda p: int(p.stem.rsplit("_", 1)[1]))
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
                points[key] = {"mean": statistics.mean(ok), "median": statistics.median(ok), "status": "ok",
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


def series_legend(fig, systems, ncol, y=-0.04, marker="o", linewidth=1.6):
    handles = [Line2D([], [], color=c, linewidth=linewidth, marker=marker, markersize=3.6, markeredgecolor=SURFACE)
               for _, _, c in systems]
    handles.append(Line2D([], [], linestyle="none", marker="^", markersize=5, markerfacecolor=SURFACE, markeredgecolor=INK2))
    labels = [n for _, n, _ in systems] + ["timed out"]
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
    axes[0].set_ylabel("seconds per query (mean of 3)")
    series_legend(fig, systems, ncol=5)
    fig.suptitle(title, x=0.01, ha="left", fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.06, 0.9, 0.95))
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
    axes[0].set_ylabel("seconds per query (mean of 3)")
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
    axes[0].set_ylabel("seconds per query (mean of 3)")
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


TABLES = REPO / "paper" / "tables"
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


def save(fig, filename):
    OUT.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT / f"{filename}.pdf")
    fig.savefig(OUT / f"{filename}.png", dpi=220)
    plt.close(fig)
    print(f"wrote {OUT / filename}.pdf/.png")


if __name__ == "__main__":
    sweep_figure("random_pairs", "pairs per query", "random_pairs",
                 "Bound pairs: n random pairs per query; baselines on their best path")
    sweep_figure("multi_source", "sources per query (each to every person)", "sources_all",
                 "Sources × all persons: distances to every reachable vertex from k sources")
    ldbc_figure("ldbc_queries")
    client_threads_figure("client_threads")
    speedup_figure("speedup")
    teaser_figure("teaser")
    scaling_figure("scaling")
    throughput_figure("throughput")
    first_query_figure("first_query")
    updates_figure("updates")
    sweep_table("random_pairs", "random_pairs",
                r"Shortest-path lengths for $n$ random pairs per query, mean seconds over three runs, 32 threads, 600\,s timeout; "
                r"rows marked conn.\ split the pairs over 32 client connections.", "tab:random-pairs", "system / pairs")
    sweep_table("multi_source", "sources_all",
                r"Distances from $k$ sources to every reachable person, mean seconds over three runs; n/r: not run above 500M pairs. The one sampled source at SF300 has no outgoing edge.",
                "tab:sources-all", "system / sources")
    ldbc_table("ldbc_queries")
