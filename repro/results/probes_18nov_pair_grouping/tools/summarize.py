#!/usr/bin/env python3
"""Medians of the probe: summarize.py <output directory> <scale factor>

Wall time per (pair table, statement) from the CLI's timer (resolution 1 ms), and the operator's phase rows
(pair_analysis, source_group_build, source_group_scatter) per (pair table, variant) in ms.
"""
import csv
import glob
import os
import re
import statistics
import sys

out, sf = sys.argv[1], sys.argv[2]
times = {}
label = None
for line in open(os.path.join(out, f"probe_sf{sf}.out"), errors="replace"):
    if line.startswith("@@ "):
        parts = line.split()
        label = (parts[1], parts[2]) if len(parts) >= 3 else None
        continue
    match = re.match(r"Run Time \(s\): real ([0-9.]+) user ([0-9.]+)", line)
    if match and label:
        times.setdefault(label, []).append((float(match.group(1)), float(match.group(2))))
        label = None
    if "Error" in line:
        print("ERROR LINE:", line.strip())

statements = ["operator", "distinct_below", "sorted_below", "scan_only", "aggregate_src_dst", "aggregate_src_list_dst",
              "sort_src_dst"]
tables = []
for table, _ in times:
    if table not in tables:
        tables.append(table)
print(f"wall time, median of {max(len(v) for v in times.values())} runs, ms (min-max)")
print("| pair table | " + " | ".join(statements) + " |")
print("|---|" + "---|" * len(statements))
for table in tables:
    cells = []
    for statement in statements:
        values = [1000 * real for real, _ in times.get((table, statement), [])]
        cells.append(f"{statistics.median(values):.0f} ({min(values):.0f}-{max(values):.0f})" if values else "")
    print(f"| {table.replace('benchmark_pairs_', '')} | " + " | ".join(cells) + " |")

print()
print("operator phases, median of the runs, ms (source_group_scatter is summed over the threads)")
phases = ["pair_analysis", "source_group_zero_copy", "source_group_build", "source_group_scatter"]
print("| pair table | variant | pairs into operator | groups | " + " | ".join(phases) + " |")
print("|---|---|---|---|" + "---|" * len(phases))
for path in sorted(glob.glob(os.path.join(out, "phases", f"sf{sf}_*_phase_timing.csv"))):
    name = os.path.basename(path)[len(f"sf{sf}_benchmark_pairs_"):-len("_phase_timing.csv")]
    variant = next(v for v in ("distinct_below", "sorted_below", "operator") if name.endswith(v))
    table = name[:-len(variant) - 1]
    rows = {}
    pairs = groups = ""
    for row in csv.DictReader(open(path)):
        if row["RunID"] != "operator":
            continue
        rows.setdefault(row["Phase"], []).append(float(row["Time_ms"]))
        if row["Phase"] in ("source_group_build", "source_group_zero_copy"):
            pairs, groups = row["PairCount"], row["EdgeCount"]
    cells = [f"{statistics.median(rows[p]):.2f}" if p in rows else "" for p in phases]
    print(f"| {table} | {variant} | {pairs} | {groups} | " + " | ".join(cells) + " |")
