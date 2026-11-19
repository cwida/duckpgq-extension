#!/usr/bin/env python3
"""summarize_ab.py <output directory> <label> [<label> ...]: median wall time per pair table with the source groups
ordered by source id (by_id) and in order of first appearance (arrival), and whether both return the same result."""
import os
import re
import statistics
import sys

out = sys.argv[1]
print("| graph | query | by_id ms (min-max) | arrival ms (min-max) | arrival / by_id | same result |")
print("|---|---|---|---|---|---|")
for label in sys.argv[2:]:
    times, checks, order = {}, {}, []
    current = None
    for line in open(os.path.join(out, f"ab_{label}.out"), errors="replace"):
        if line.startswith("@@ "):
            parts = line.split()
            current = (parts[1], parts[2]) if len(parts) == 4 else None
            if current and current[0] not in order:
                order.append(current[0])
            continue
        if "Error" in line:
            print("ERROR LINE:", label, line.strip())
        numbers = re.findall(r"│\s*(\d+)\s*", line)
        if current and len(numbers) == 3:
            checks.setdefault(current, set()).add(tuple(numbers))
        match = re.match(r"Run Time \(s\): real ([0-9.]+)", line)
        if match and current:
            times.setdefault(current, []).append(1000 * float(match.group(1)))
            current = None
    for table in order:
        a, b = times.get((table, "by_id"), []), times.get((table, "arrival"), [])
        if not a or not b:
            continue
        ma, mb = statistics.median(a), statistics.median(b)
        same = checks.get((table, "by_id")) == checks.get((table, "arrival")) and len(checks[(table, "by_id")]) == 1
        name = table.replace("benchmark_pairs_", "")
        print(f"| {label} | {name} | {ma:.0f} ({min(a):.0f}-{max(a):.0f}) | {mb:.0f} ({min(b):.0f}-{max(b):.0f}) | "
              f"{mb / ma:.2f} | {'yes' if same else 'NO'} |")
