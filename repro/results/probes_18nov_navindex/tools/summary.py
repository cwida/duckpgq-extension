#!/usr/bin/env python3
"""Medians of timings.csv, one line per (system, graph, route, setting, workload, pairs, result). Usage: summary.py [graph]"""
import collections
import csv
import os
import re
import statistics
import sys

here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
groups = collections.OrderedDict()
for row in csv.DictReader(open(f"{here}/timings.csv")):
    if len(sys.argv) > 1 and row["graph"] != sys.argv[1]:
        continue
    key = tuple(row[k] for k in ("system", "graph", "route", "setting", "workload", "pairs", "result"))
    groups.setdefault(key, []).append(row)
print("%-24s %-10s %-11s %-14s %-12s %5s %-7s %10s %2s %9s %9s  %s" % (
    "system", "graph", "route", "setting", "workload", "pairs", "result", "median_s", "n", "min_s", "max_s",
    "answer (count|sum of hops; Umbra: sum of path nodes)  orientation/kernel  note"))
for key, rows in groups.items():
    times = [float(row["seconds"]) for row in rows if row["seconds"]]
    note = rows[0]["note"]
    kernel = re.search(r"orientation=(\w+).*?source_group_kernel=(\w+)", note)
    problem = note[:160] if (not times or "rror" in note or "timeout" in note) else ""
    print("%-24s %-10s %-11s %-14s %-12s %5s %-7s %10s %2d %9s %9s  %s %s %s" % (
        *key, "%.4f" % statistics.median(times) if times else "-", len(times),
        "%.4f" % min(times) if times else "-", "%.4f" % max(times) if times else "-", rows[0]["answer"],
        "/".join(kernel.groups()) if kernel else "", problem))
