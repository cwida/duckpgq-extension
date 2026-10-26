#!/usr/bin/env python3
"""Figures for the W27/W17 sweeps: random pairs, sources x all, LDBC queries, client-side concurrency.

Reads every sweep_pairs_*.csv and ldbc_queries_*.csv under data/ldbc-pathfinding/results/sf{10,100,300}; when a
(system, shape, count) point appears in several files the newest file wins, so reruns replace earlier numbers.
Writes PDF and PNG to paper/figures/. Colours follow the entity (one colour per system across every figure), six
categorical slots in the validated order of the data-viz palette; the three slots with low contrast on the light
surface get direct labels at the line ends (relief rule) and every number is also in the plan's tables.
"""
import csv
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / "data" / "ldbc-pathfinding" / "results"
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
    files = sorted((RESULTS / f"sf{sf}").glob(f"{prefix}_*.csv"), key=lambda p: p.stat().st_mtime)
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
                points[key] = {"mean": statistics.mean(ok), "status": "ok"}
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


def sweep_figure(shape, xlabel, filename, title):
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.6), sharey=True)
    for ax, sf in zip(axes, SCALE_FACTORS):
        points = load("sweep_pairs", sf)
        counts = sorted({k[2] for k in points if k[1] == shape})
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.axhline(TIMEOUT_S, color=AXIS, linewidth=0.6)
        ax.text(counts[0], TIMEOUT_S * 1.25, "600 s timeout", color=MUTED, fontsize=6.5, va="bottom")
        label_positions = []
        for system, name, color in SYSTEMS:
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
    series_legend(fig, SYSTEMS, ncol=7)
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
                        values.append(point["mean"])
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
    fig.suptitle("LDBC queries as hop-count shortest paths on undirected knows, mean over 5 parameter sets; baselines on their best path",
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
        for system, name, color in SYSTEMS:
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
        ax.set_xlim(1.2, 90)
        ax.set_xlabel("scale factor, edges")
        ax.set_title(title, loc="left")
        style_axes(ax)
    axes[0].set_ylabel("seconds per query (mean of 3)")
    series_legend(fig, SYSTEMS, ncol=7)
    fig.suptitle("Scaling with the graph: the largest point of each shape from SF10 to SF300", x=0.01, ha="left",
                 fontsize=9.5, color=INK)
    fig.tight_layout(rect=(0, 0.06, 0.9, 0.95))
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
    scaling_figure("scaling")
