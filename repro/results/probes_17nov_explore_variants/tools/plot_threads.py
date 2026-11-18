"""Plots of the explore-variant thread sweep: one panel per point, one line per variant.

  threads_time.png      query seconds over the thread count (log-log)
  threads_speedup.png   speed-up over the variant's own single-thread time, with the ideal line
  threads_slowdown.png  each flat variant's time divided by the partitioned explore's at the same thread count

usage: plot_threads.py <microbench csv of --configs explore_threads> <output directory>
"""
import csv
import statistics
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

THREADS = (1, 2, 4, 8, 16, 32)
# variant: (label, colour, marker); colours in fixed order, validated for colour-vision deficiencies; the marker and
# the label at the line's end carry the identity as well
VARIANTS = {
    "partitioned": ("partitioned", "#2a78d6", "o"),
    "flat_serial": ("flat, one owner", "#eb6834", "s"),
    "flat_ranges": ("flat, owned ranges", "#1baf7a", "^"),
    "flat_atomic": ("flat, atomics", "#eda100", "D"),
    "flat_locked": ("flat, locks", "#e87ba4", "v"),
}
SURFACE, INK, MUTED, GRID = "#fcfcfb", "#1f1f1e", "#6b6a63", "#e1e0d9"


def load(path):
    samples, points = {}, []
    for r in csv.DictReader(open(path)):
        if r["status"] != "ok":
            continue
        config = r["config"]
        if config == "baseline":
            variant, threads = "partitioned", 32
        else:
            name = config[len("explore_"):]
            head, _, tail = name.rpartition("_t")
            variant, threads = (head, int(tail)) if head and tail.isdigit() else (name, 32)
        point = f"{int(r['pairs']):,} random pairs" if r["shape"] == "random_pairs" else f"{r['sources']} sources x all"
        if point not in points:
            points.append(point)
        samples.setdefault((point, variant, threads), []).append(float(r["query_s"]))
    return points, {key: statistics.median(values) for key, values in samples.items()}


def end_labels(ax, ends):
    """Labels right of the last marker, moved apart (in log space) where they would overlap"""
    low, high = ax.get_ylim()
    import math
    span = math.log(high / low)
    gap = 0.062 * span
    placed = []
    for value, label in sorted(ends):
        position = math.log(value)
        if placed and position - placed[-1][0] < gap:
            position = placed[-1][0] + gap
        placed.append((position, value, label))
    for position, value, label in placed:
        ax.annotate(label, xy=(32, value), xytext=(41, math.exp(position)), fontsize=8, color=INK, va="center",
                    arrowprops=dict(arrowstyle="-", color=MUTED, lw=0.5, shrinkA=0, shrinkB=3))


def figure(points, title, ylabel, series, out, variants, reference=None, share_y=False, number_format="{:.2f}"):
    fig, axes = plt.subplots(1, len(points), figsize=(13.5, 4.6), sharey=share_y, facecolor=SURFACE)
    for ax, point in zip(axes, points):
        ax.set_facecolor(SURFACE)
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        if reference:
            xs, ys, text, offset, align = reference(point)
            ax.plot(xs, ys, color=MUTED, lw=1, ls=(0, (4, 3)), zorder=1)
            ax.annotate(text, xy=(xs[2], ys[2]), xytext=offset, textcoords="offset points", fontsize=8,
                        color=MUTED, ha=align)
        ends = []
        for variant in variants:
            label, colour, marker = VARIANTS[variant]
            values = [series(point, variant, t) for t in THREADS]
            ax.plot(THREADS, values, color=colour, lw=2, marker=marker, ms=6.5, markeredgecolor=SURFACE,
                    markeredgewidth=1, label=label, zorder=3)
            ends.append((values[-1], f"{label}  {number_format.format(values[-1])}"))
        ax.set_xticks(THREADS)
        ax.set_xticklabels([str(t) for t in THREADS])
        ax.set_xlim(0.85, 170)
        ax.minorticks_off()
        ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(base=10, subs=(1, 2, 5)))
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}"))
        low, high = ax.get_ylim()
        ax.set_ylim(low / 1.15, high * 1.15)
        end_labels(ax, ends)
        ax.grid(True, axis="y", color=GRID, lw=0.8)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(GRID)
        ax.tick_params(colors=MUTED, labelsize=9, length=0)
        ax.set_title(point, fontsize=11, color=INK, loc="left")
        ax.set_xlabel("threads", fontsize=9, color=MUTED, loc="left")
    axes[0].set_ylabel(ylabel, fontsize=9, color=MUTED)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper right", ncol=len(variants), frameon=False, fontsize=9,
               bbox_to_anchor=(0.99, 0.945), labelcolor=INK)
    fig.suptitle(title, fontsize=13, color=INK, x=0.012, ha="left", y=0.985)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    fig.savefig(out, dpi=170, facecolor=SURFACE)
    plt.close(fig)


def main():
    points, median = load(sys.argv[1])
    out = sys.argv[2].rstrip("/")
    everyone = list(VARIANTS)
    flat = everyone[1:]
    figure(points, "Query time over the thread count, LDBC SF300 knows, CSR index route (median of 3; lower is better)",
           "seconds (log scale)", lambda p, v, t: median[(p, v, t)], f"{out}/threads_time.png", everyone,
           number_format="{:.2f} s")
    figure(points, "Speed-up over each variant's own single-thread time, LDBC SF300 knows (higher is better)",
           "speed-up (log scale)", lambda p, v, t: median[(p, v, 1)] / median[(p, v, t)], f"{out}/threads_speedup.png",
           everyone, reference=lambda p: (list(THREADS), list(THREADS), "ideal", (-4, 6), "right"), share_y=True,
           number_format="{:.1f}x")
    figure(points, "Time relative to the partitioned explore at the same thread count, LDBC SF300 knows "
                   "(above 1: slower than partitioned)",
           "times slower than partitioned (log scale)",
           lambda p, v, t: median[(p, v, t)] / median[(p, "partitioned", t)], f"{out}/threads_slowdown.png", flat,
           reference=lambda p: (list(THREADS), [1] * len(THREADS), "partitioned = 1", (0, -12), "center"), share_y=True,
           number_format="{:.1f}x")


main()
