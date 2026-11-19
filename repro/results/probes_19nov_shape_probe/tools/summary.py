#!/usr/bin/env python3
"""timings.csv of this probe as one line per (graph, route, workload, pairs, result): median seconds of the default
(with the kernel it chose, the probe's verdict, its time in microseconds and the most vertices a deep probe had
discovered), of no_shape_probe and of single_source, and whether the three returned the same answer."""
import collections
import csv
import os
import re
import statistics

here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
groups = collections.OrderedDict()
for row in csv.DictReader(open(f"{here}/timings.csv")):
    key = (row["graph"], row["route"], row["workload"], row["pairs"], row["result"], row["setting"])
    groups.setdefault(key, []).append(row)
median, info = {}, {}
for key, rows in groups.items():
    times = [float(row["seconds"]) for row in rows if row["seconds"]]
    median[key] = statistics.median(times) if times else None
    note = rows[0]["note"]

    def field(pattern):
        found = re.search(pattern, note)
        return found.group(1) if found else "-"

    info[key] = (field(r"source_group_kernel=(\w+)"), field(r"shape_probe=(\w+)"), field(r"shape_probe_us=(\d+)"),
                 field(r"deep_discovered=(\d+)"), rows[0]["answer"])
print("graph      route wl pairs result  | default_s (kernel, probe, probe_us, deep_discovered) | no_shape_probe_s | "
      "single_source_s | same answers")
done = []
for key in groups:
    base = key[:5]
    if base in done:
        continue
    done.append(base)
    settings = [s for s in ("default", "no_shape_probe", "single_source") if base + (s,) in info]
    same = len({info[base + (s,)][4] for s in settings}) == 1

    def seconds(setting):
        value = median.get(base + (setting,))
        return "%.4f" % value if value is not None else "-"

    kernel = info.get(base + ("default",), ("-",) * 5)
    print("%-10s %-5s %-2s %5s %-7s | %s (%s, %s, %s, %s) | %s | %s | %s" % (
        *base, seconds("default"), *kernel[:4], seconds("no_shape_probe"), seconds("single_source"), same))
