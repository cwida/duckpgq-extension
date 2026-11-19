#!/usr/bin/env python3
"""misfire_<graph>.csv (harness microbench, index route, random pairs) as one line per graph and pair count: median
seconds with the probe (baseline) and without (no_shape_probe), the ratio, the probe's verdict and time, and whether
both configurations returned the same counts."""
import collections
import csv
import glob
import os
import re
import statistics

here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
order = ["sf10", "sf100", "sf300", "cit-Patents", "datagen-8_4-fb", "graph500-24"]
print("%-15s %6s | %9s %9s %7s | %-9s %-28s %8s | %-11s | %s" % (
    "graph", "pairs", "probe_s", "off_s", "ratio", "verdict", "outcomes", "probe_us", "kernel", "same answer"))
for graph in order:
    path = f"{here}/misfire_{graph}.csv"
    if not os.path.exists(path):
        print(f"{graph}: no result")
        continue
    groups = collections.OrderedDict()
    for row in csv.DictReader(open(path)):
        groups.setdefault((int(row["pairs"]), row["config"]), []).append(row)
    for pairs in sorted({key[0] for key in groups}):
        probe, off = groups.get((pairs, "baseline"), []), groups.get((pairs, "no_shape_probe"), [])

        def median(rows):
            times = [float(row["query_s"]) for row in rows if row["status"] == "ok" and row["query_s"]]
            return statistics.median(times) if times else None

        def field(pattern, rows):
            values = [m.group(1) for m in (re.search(pattern, row["strategy"]) for row in rows) if m]
            return values[0] if values else "-"

        a, b = median(probe), median(off)
        micros = [int(m.group(1)) for m in (re.search(r"shape_probe_us=(\d+)", row["strategy"]) for row in probe) if m]
        same = {(row["reachable_count"], row["total_len"]) for row in probe} == \
               {(row["reachable_count"], row["total_len"]) for row in off}
        print("%-15s %6d | %9s %9s %7s | %-9s %-28s %8s | %-11s | %s" % (
            graph, pairs, "%.4f" % a if a is not None else "-", "%.4f" % b if b is not None else "-",
            "%.3f" % (a / b) if a and b else "-", field(r"shape_probe=(\w+)", probe),
            field(r"shape_probe_outcomes=([\w:/]+)", probe), max(micros) if micros else "-",
            field(r"source_group_kernel=(\w+)", probe), same))
