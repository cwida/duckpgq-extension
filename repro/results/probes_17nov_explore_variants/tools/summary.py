"""Median seconds per configuration and point of the explore-variant files, with the ratio to a reference
configuration ("x slower" when the configuration takes longer than the reference, "x faster" when shorter).

usage: summary.py <reference config> <microbench csv>...
"""
import csv
import statistics
import sys


def main():
    reference = sys.argv[1]
    for path in sys.argv[2:]:
        rows = [r for r in csv.DictReader(open(path)) if r["status"] == "ok"]
        points = []
        configs = []
        samples = {}
        for r in rows:
            point = f"{r['pairs']} pairs" if r["shape"] == "random_pairs" else f"{r['sources']} sources x all"
            if point not in points:
                points.append(point)
            if r["config"] not in configs:
                configs.append(r["config"])
            samples.setdefault((r["config"], point), []).append(float(r["query_s"]))
        print(f"\n{path.split('/')[-1]}  (median of the repeats, seconds; ratio to {reference})")
        print(f"{'config':32s}" + "".join(f"{p:>30s}" for p in points))
        for config in configs:
            cells = []
            for point in points:
                values = samples.get((config, point))
                base = samples.get((reference, point))
                if not values:
                    cells.append(f"{'-':>30s}")
                    continue
                median = statistics.median(values)
                note = f"({len(values)} of 3)" if len(values) < 3 else ""
                if base and config != reference:
                    ratio = median / statistics.median(base)
                    note += f" {ratio:.2f}x slower" if ratio >= 1 else f" {1 / ratio:.2f}x faster"
                cells.append(f"{median:10.3f} {note:>19s}")
            print(f"{config:32s}" + "".join(cells))


main()
