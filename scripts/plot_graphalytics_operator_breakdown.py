#!/usr/bin/env python3
import argparse
import csv
import html
from pathlib import Path


def fmt_int(value):
    number = int(float(value))
    if number >= 1_000_000_000:
        return f"{number / 1_000_000_000:.1f}B"
    if number >= 1_000_000:
        return f"{number / 1_000_000:.1f}M"
    if number >= 1_000:
        return f"{number / 1_000:.1f}k"
    return str(number)


def fmt_seconds(value):
    return f"{value:.2f}s" if value >= 1 else f"{value:.3f}s"


def read_rows(path):
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows.sort(key=lambda row: (int(row["targets"]), int(row["traversal_edges"])))
    return rows


def text(x, y, content, size=12, fill="#222222", anchor="middle", weight="400", rotate=None):
    attrs = [
        f'x="{x:.1f}"',
        f'y="{y:.1f}"',
        f'font-size="{size}"',
        f'fill="{fill}"',
        f'text-anchor="{anchor}"',
        f'font-weight="{weight}"',
    ]
    if rotate is not None:
        attrs.append(f'transform="rotate({rotate:.1f} {x:.1f} {y:.1f})"')
    return f"<text {' '.join(attrs)}>{html.escape(str(content))}</text>"


def rect(x, y, width, height, fill, stroke="none"):
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" height="{height:.1f}" '
        f'fill="{fill}" stroke="{stroke}"/>'
    )


def line(x1, y1, x2, y2, stroke="#666666", width=1):
    return f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{stroke}" stroke-width="{width}"/>'


def render_svg(rows, title, subtitle):
    margin_left = 82
    margin_right = 36
    margin_top = 82
    margin_bottom = 150
    bar_width = 42
    gap = 30
    plot_height = 360
    width = margin_left + margin_right + len(rows) * bar_width + max(0, len(rows) - 1) * gap
    height = margin_top + plot_height + margin_bottom
    max_total = max(float(row["query_s"]) for row in rows)
    y_max = max_total * 1.12

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}</style>',
        text(width / 2, 32, title, size=20, weight="700"),
        text(width / 2, 56, subtitle, size=12, fill="#555555"),
    ]

    axis_x = margin_left
    axis_y = margin_top + plot_height
    parts.append(line(axis_x, margin_top, axis_x, axis_y, "#444444"))
    parts.append(line(axis_x, axis_y, width - margin_right, axis_y, "#444444"))

    for tick in range(0, 5):
        value = y_max * tick / 4
        y = axis_y - (value / y_max) * plot_height
        parts.append(line(axis_x - 5, y, width - margin_right, y, "#e6e6e6" if tick else "#444444"))
        parts.append(text(axis_x - 10, y + 4, f"{value:.1f}", size=11, fill="#555555", anchor="end"))
    parts.append(text(22, margin_top + plot_height / 2, "seconds", size=12, fill="#555555", rotate=-90))

    path_color = "#2f7d62"
    other_color = "#b8bcc4"
    edge_color = "#1f5e49"
    for idx, row in enumerate(rows):
        x = margin_left + idx * (bar_width + gap)
        total = float(row["query_s"])
        pathfinding = float(row["pathfinding_operator_s"])
        other = float(row["other_query_s"])
        path_h = pathfinding / y_max * plot_height
        other_h = other / y_max * plot_height
        path_y = axis_y - path_h
        other_y = path_y - other_h
        parts.append(rect(x, other_y, bar_width, other_h, other_color))
        parts.append(rect(x, path_y, bar_width, path_h, path_color))
        parts.append(rect(x, other_y, bar_width, other_h + path_h, "none", "#333333"))
        parts.append(text(x + bar_width / 2, other_y - 8, fmt_seconds(total), size=10, fill="#222222"))

        label = row["dataset"]
        if len(label) > 14:
            label = label.replace("datagen-", "")
        parts.append(text(x + bar_width / 2, axis_y + 18, label, size=11, fill="#222222", rotate=35))
        parts.append(text(x + bar_width / 2, axis_y + 66, f"V {fmt_int(row['vertices'])}", size=9, fill="#666666", rotate=35))
        parts.append(text(x + bar_width / 2, axis_y + 102, f"E {fmt_int(row['traversal_edges'])}", size=9, fill="#666666", rotate=35))

        if total:
            pct = pathfinding / total * 100
            if path_h > 18:
                parts.append(text(x + bar_width / 2, path_y + path_h / 2 + 4, f"{pct:.0f}%", size=10, fill="#ffffff"))

    legend_x = width - margin_right - 320
    legend_y = 82
    parts.append(rect(legend_x, legend_y, 14, 14, path_color))
    parts.append(text(legend_x + 20, legend_y + 12, "instrumented pathfinding operator", size=12, anchor="start"))
    parts.append(rect(legend_x, legend_y + 22, 14, 14, other_color))
    parts.append(text(legend_x + 20, legend_y + 34, "other query work", size=12, anchor="start"))
    parts.append(text(width / 2, height - 18, "Datasets sorted by target vertex count. V/E labels show materialized vertices/traversal edges.", size=11, fill="#555555"))
    parts.append("</svg>")
    return "\n".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Render a Graphalytics BFS pathfinding-vs-other timing SVG.")
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("output_svg", type=Path)
    parser.add_argument("--title", default="Graphalytics BFS Operator Breakdown")
    parser.add_argument(
        "--subtitle",
        default="DuckPGQ native operator, fixed 16 threads, repeat-3 mean; pathfinding time is instrumented internal phases",
    )
    args = parser.parse_args()

    rows = read_rows(args.input_csv)
    args.output_svg.parent.mkdir(parents=True, exist_ok=True)
    args.output_svg.write_text(render_svg(rows, args.title, args.subtitle))


if __name__ == "__main__":
    main()
