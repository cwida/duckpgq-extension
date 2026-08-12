#!/usr/bin/env python3
"""Plot the cached one-source hash-grouping versus zero-copy A-B-A sweep."""

import argparse
import csv
import html
import math
from pathlib import Path


INK = "#202124"
MUTED = "#5f6368"
GRID = "#e4e7eb"
HASH = "#7a8491"
ZERO_COPY = "#167c68"
REMOVED = "#d29a38"


def esc(value):
    return html.escape(str(value))


def text(x, y, value, size=12, anchor="start", color=INK, weight=400):
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" text-anchor="{anchor}" '
        f'fill="{color}" font-weight="{weight}">{esc(value)}</text>'
    )


def fmt_vertices(value):
    value = int(value)
    return f"{value / 1_000_000:.1f}M"


def read_rows(path):
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows.sort(key=lambda row: int(row["vertices"]))
    for row in rows:
        row["vertices"] = int(row["vertices"])
        row["hash_ms"] = float(row["baseline_wall_mean_s"]) * 1000
        row["zero_a1_ms"] = float(row["a1_wall_mean_s"]) * 1000
        row["zero_a2_ms"] = float(row["a2_wall_mean_s"]) * 1000
        row["zero_ms"] = (row["zero_a1_ms"] + row["zero_a2_ms"]) / 2
        row["speedup"] = row["hash_ms"] / row["zero_ms"]
        row["hash_group_ms"] = float(row["baseline_grouping_mean_s"]) * 1000
        row["group_share"] = row["hash_group_ms"] / row["hash_ms"] * 100
    return rows


def render(rows):
    width, height = 1360, 650
    top, row_h = 142, 69
    left_label = 175
    left_x0, left_x1 = 205, 780
    right_x0, right_x1 = 930, 1300
    min_ms, max_ms = 30.0, 2500.0

    def time_x(value):
        return left_x0 + (
            (math.log10(value) - math.log10(min_ms))
            / (math.log10(max_ms) - math.log10(min_ms))
            * (left_x1 - left_x0)
        )

    def share_x(value):
        return right_x0 + value / 55.0 * (right_x1 - right_x0)

    body = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;letter-spacing:0}</style>',
        text(width / 2, 34, "Exact One-Source Zero-Copy Fast Path", 23, "middle", INK, 700),
        text(width / 2, 60, "Cached CSR, 18 threads, five warm repeats per A-B-A stage", 12, "middle", MUTED),
        text((left_x0 + left_x1) / 2, 96, "Warm query time", 15, "middle", INK, 600),
        text((left_x0 + left_x1) / 2, 116, "milliseconds, log scale; lower is better", 11, "middle", MUTED),
        text((right_x0 + right_x1) / 2, 96, "Hash grouping cost removed", 15, "middle", INK, 600),
        text((right_x0 + right_x1) / 2, 116, "share of hash-path query time", 11, "middle", MUTED),
    ]

    for tick in [30, 50, 100, 250, 500, 1000, 2500]:
        x = time_x(tick)
        body.append(f'<line x1="{x:.1f}" y1="128" x2="{x:.1f}" y2="{height - 72}" stroke="{GRID}"/>')
        label = f"{tick / 1000:g}s" if tick >= 1000 else f"{tick}ms"
        body.append(text(x, height - 50, label, 10, "middle", MUTED))
    for tick in [0, 10, 20, 30, 40, 50]:
        x = share_x(tick)
        body.append(f'<line x1="{x:.1f}" y1="128" x2="{x:.1f}" y2="{height - 72}" stroke="{GRID}"/>')
        body.append(text(x, height - 50, f"{tick}%", 10, "middle", MUTED))

    for index, row in enumerate(rows):
        y = top + index * row_h
        label = row["dataset"].replace("datagen-", "d-")
        body.append(text(left_label, y + 4, label, 12, "end", INK, 600))
        body.append(text(left_label, y + 21, fmt_vertices(row["vertices"]) + " vertices", 10, "end", MUTED))

        zero_x = time_x(row["zero_ms"])
        hash_x = time_x(row["hash_ms"])
        body.append(f'<line x1="{zero_x:.1f}" y1="{y:.1f}" x2="{hash_x:.1f}" y2="{y:.1f}" stroke="#b8bec6" stroke-width="3"/>')
        body.append(f'<circle cx="{hash_x:.1f}" cy="{y:.1f}" r="7" fill="{HASH}"/>')
        body.append(f'<circle cx="{zero_x:.1f}" cy="{y:.1f}" r="8" fill="{ZERO_COPY}" stroke="#ffffff" stroke-width="2"/>')
        for sample in [row["zero_a1_ms"], row["zero_a2_ms"]]:
            sx = time_x(sample)
            body.append(f'<line x1="{sx:.1f}" y1="{y - 11:.1f}" x2="{sx:.1f}" y2="{y + 11:.1f}" stroke="{ZERO_COPY}" stroke-width="1.5"/>')
        body.append(text(hash_x + 10, y - 8, f'{row["speedup"]:.2f}x', 11, "start", ZERO_COPY, 700))
        body.append(text(hash_x + 10, y + 10, f'{row["hash_ms"]:.0f} to {row["zero_ms"]:.0f} ms', 9, "start", MUTED))

        bar_x1 = share_x(row["group_share"])
        body.append(f'<rect x="{right_x0:.1f}" y="{y - 11:.1f}" width="{bar_x1 - right_x0:.1f}" height="22" fill="{REMOVED}" rx="2"/>')
        body.append(text(bar_x1 + 8, y + 4, f'{row["hash_group_ms"]:.0f} ms', 10, "start", INK, 600))

    legend_y = height - 18
    body.append(f'<circle cx="{left_x0}" cy="{legend_y - 4}" r="6" fill="{ZERO_COPY}"/>')
    body.append(text(left_x0 + 12, legend_y, "zero-copy mean; thin ticks are A1 and A2 means", 10, "start", MUTED))
    body.append(f'<circle cx="{left_x0 + 305}" cy="{legend_y - 4}" r="6" fill="{HASH}"/>')
    body.append(text(left_x0 + 317, legend_y, "hash path", 10, "start", MUTED))
    body.append(text(right_x0, legend_y, "Zero-copy grouping: <=0.001 ms", 10, "start", MUTED))
    body.append("</svg>")
    return "\n".join(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("output_svg", type=Path)
    args = parser.parse_args()
    rows = read_rows(args.input_csv)
    args.output_svg.parent.mkdir(parents=True, exist_ok=True)
    args.output_svg.write_text(render(rows))


if __name__ == "__main__":
    main()
