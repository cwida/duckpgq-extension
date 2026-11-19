#!/usr/bin/env python3
"""Medians of the two measurements: summary.py <probe directory>"""
import csv
import glob
import statistics
import sys
from collections import defaultdict
from pathlib import Path

root = Path(sys.argv[1])

print("CREATE PROPERTY GRAPH (seconds, median of the runs; create_timings.csv)")
times = defaultdict(list)
outcomes = defaultdict(set)
for row in csv.DictReader((root / "create_timings.csv").open()):
    key = (row["database"], row["build"])
    outcomes[key].add(row["outcome"])
    if row["create_s"]:
        times[key].append(float(row["create_s"]))
for key in times:
    values = times[key]
    print(f"  {key[0]:22s} {key[1]:6s} median {statistics.median(values):.3f}  min {min(values):.3f}  max {max(values):.3f}"
          f"  n={len(values)}  {'/'.join(sorted(outcomes[key]))}")

print("Commit of 2000 edge changes at SF10, one writer, no readers (ms; fk_commit/commit_<variant>_<run>.csv)")
per_variant = defaultdict(list)
for path in sorted(glob.glob(str(root / "fk_commit" / "commit_*.csv"))):
    variant = Path(path).stem.split("_")[1]
    rows = list(csv.DictReader(open(path)))
    commits = [float(row["elapsed_s"]) * 1000 for row in rows if row["kind"] == "commit"]
    errors = sum(row["kind"] == "error" for row in rows)
    ordered = sorted(commits)
    print(f"  {Path(path).stem:18s} n={len(commits)} median {statistics.median(commits):.2f}  "
          f"p99 {ordered[int(0.99 * (len(ordered) - 1))]:.2f}  max {max(commits):.2f}  errors={errors}")
    per_variant[variant].append(statistics.median(commits))
for variant, medians in per_variant.items():
    print(f"  {variant}: median of the run medians {statistics.median(medians):.2f} ms")
