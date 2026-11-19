#!/usr/bin/env python3
"""summarize_control.py <output directory> <label> [<label> ...]

Per label: median wall time per ordering with its ratio to arrival order, the result checksums, and, when the label
has an activity file, per diagnosed ordering the waves' levels and frontier sizes.
"""
import csv
import os
import re
import statistics
import sys

out = sys.argv[1]
for label in sys.argv[2:]:
    times, checks = {}, {}
    current = None
    shape = ""
    order = ""
    section = None
    for line in open(os.path.join(out, f"control_{label}.out"), errors="replace"):
        if line.startswith("@@ "):
            parts = line.split()
            section = parts[1]
            current = parts[1] if len(parts) == 3 and parts[2].isdigit() else None
            continue
        if "Error" in line:
            print("ERROR LINE:", line.strip())
        numbers = re.findall(r"│\s*(\d+)\s*", line)
        if section == "shape" and len(numbers) == 3:
            shape = f"{numbers[0]} pairs, {numbers[1]} sources, {numbers[2]} vertices"
        if section == "arrival_order" and re.search(r"│\s*[0-9.]+\s*│", line):
            order = f"; arrival order: {line.strip().strip('│').strip()} of adjacent rows have ascending sources"
        if current and len(numbers) == 3:
            checks.setdefault(current, set()).add(tuple(numbers))
        match = re.match(r"Run Time \(s\): real ([0-9.]+)", line)
        if match and current:
            times.setdefault(current, []).append(1000 * float(match.group(1)))
            current = None
    print(f"## {label}: {shape}{order}")
    all_checks = set().union(*checks.values()) if checks else set()
    print(f"results (pairs, reachable, sum of lengths): {sorted(all_checks)}"
          + ("" if len(all_checks) == 1 else "  <-- DIFFER"))
    base = statistics.median(times["arrival"]) if "arrival" in times else None
    print("| ordering | median ms | min-max | arrival / ordering |")
    print("|---|---|---|---|")
    for variant, values in times.items():
        median = statistics.median(values)
        ratio = f"{base / median:.2f}" if base else ""
        print(f"| {variant} | {median:.0f} | {min(values):.0f}-{max(values):.0f} | {ratio} |")
    activity = os.path.join(out, f"activity_{label}.csv")
    if os.path.exists(activity):
        waves = {}
        for row in csv.DictReader(open(activity)):
            wave = waves.setdefault((row["variant"], row["wave"]), {"ms": row["wave_ms"], "levels": []})
            wave["levels"].append((int(row["level"]), int(row["active_lanes"]), int(row["active_vertices"]),
                                   int(row["lane_frontier_sum"])))
        print()
        print("waves under the lane-activity switch (untimed runs; the switch itself adds a scan per level)")
        print("| ordering | waves | levels per wave: median (min-max) | active vertices summed over a wave's levels: "
              "median | lane frontiers summed over a wave: median | active lanes per level: mean |")
        print("|---|---|---|---|---|---|")
        for variant in sorted({v for v, _ in waves}):
            selected = [w for (v, _), w in waves.items() if v == variant]
            levels = [len(w["levels"]) for w in selected]
            vertices = [sum(l[2] for l in w["levels"]) for w in selected]
            frontier = [sum(l[3] for l in w["levels"]) for w in selected]
            lanes = [l[1] for w in selected for l in w["levels"]]
            print(f"| {variant} | {len(selected)} | {statistics.median(levels):.0f} ({min(levels)}-{max(levels)}) | "
                  f"{statistics.median(vertices):,.0f} | {statistics.median(frontier):,.0f} | "
                  f"{statistics.mean(lanes):.0f} |")
        print()
        print("per level, mean over the waves that reach it: waves, active lanes, active vertices")
        for variant in sorted({v for v, _ in waves}):
            per_level = {}
            for (v, _), wave in waves.items():
                if v != variant:
                    continue
                for level, lanes_active, vertices_active, _ in wave["levels"]:
                    per_level.setdefault(level, []).append((lanes_active, vertices_active))
            cells = [f"L{level}: {len(rows)}w {statistics.mean(r[0] for r in rows):.0f}l "
                     f"{statistics.mean(r[1] for r in rows):,.0f}v" for level, rows in sorted(per_level.items())]
            print(f"  {variant}: " + " | ".join(cells))
    deciles = False
    for line in open(os.path.join(out, f"control_{label}.out"), errors="replace"):
        if line.startswith("@@ "):
            deciles = line.startswith("@@ deciles")
            if deciles:
                print()
                print("per decile of the source's rank: decile, pairs, reachable, mean length, max length")
            continue
        if deciles and line.startswith("│") and re.search(r"\d", line):
            print("  " + " ".join(cell.strip() for cell in line.strip().strip("│").split("│")))
    print()
