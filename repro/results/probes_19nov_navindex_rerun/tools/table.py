#!/usr/bin/env python3
"""timings.csv of this probe as one table per graph: median seconds by pair count for the operator's settings (index
route) and Umbra's modes, then the other operator rows (build route, W1, paths). Usage: table.py [timings.csv]"""
import collections
import csv
import os
import statistics
import sys

here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
path = sys.argv[1] if len(sys.argv) > 1 else f"{here}/timings.csv"
groups = collections.OrderedDict()
answers = {}
for row in csv.DictReader(open(path)):
    key = (row["graph"], row["system"], row["route"], row["setting"], row["workload"], row["pairs"], row["result"])
    if row["seconds"]:
        groups.setdefault(key, []).append(float(row["seconds"]))
        answers[key] = row["answer"] or answers.get(key, "")
    else:
        groups.setdefault(key, [])
        answers[key] = row["note"][:60]
median = {key: (statistics.median(times) if times else None) for key, times in groups.items()}
graphs = list(collections.OrderedDict.fromkeys(key[0] for key in groups))


def cell(key):
    value = median.get(key)
    if key not in median:
        return "-"
    return "%.4f" % value if value is not None else "fail"


for graph in graphs:
    operator = [k for k in groups if k[0] == graph and k[1] != "umbra"]
    system = operator[0][1] if operator else "duckpgq"
    settings = list(collections.OrderedDict.fromkeys(k[3] for k in operator))
    modes = list(collections.OrderedDict.fromkeys(k[2] for k in groups if k[0] == graph and k[1] == "umbra"))
    counts = sorted({int(k[5]) for k in groups if k[0] == graph and k[4] == "w2" and k[6] == "lengths" and k[5]})
    print(f"\n{graph}: N pairs, lengths, median seconds ({system} on the index route | Umbra)")
    print("%6s " % "pairs" + " ".join("%22s" % s for s in settings) + " | " + " ".join("%12s" % m for m in modes))
    for n in counts:
        print("%6d " % n + " ".join("%22s" % cell((graph, system, "index", s, "w2", str(n), "lengths")) for s in settings)
              + " | " + " ".join("%12s" % cell((graph, "umbra", m, "", "w2", str(n), "lengths")) for m in modes))
    for mode in modes:
        build = median.get((graph, "umbra", mode, "", "create_index", "", "lengths"))
        if build is not None:
            print(f"  Umbra {mode}: index build {build:.2f} s")
    print("  other operator rows (route workload pairs result: " + ", ".join(settings) + ")")
    for route, workload, pairs, result in collections.OrderedDict.fromkeys(
            (k[2], k[4], k[5], k[6]) for k in operator if not (k[2] == "index" and k[4] == "w2" and k[6] == "lengths")):
        print("  %-6s %-3s %5s %-8s " % (route, workload, pairs, result)
              + " ".join("%10s" % cell((graph, system, route, s, workload, pairs, result)) for s in settings))
    # Answers: count|sum of hops for the operator, count|sum of path nodes for Umbra (hops = nodes - count)
    problems = []
    for n in counts:
        ours = answers.get((graph, system, "index", settings[0], "w2", str(n), "lengths"), "")
        for s in settings[1:]:
            other = answers.get((graph, system, "index", s, "w2", str(n), "lengths"), "")
            if other and ours and other != ours and median.get((graph, system, "index", s, "w2", str(n), "lengths")) is not None:
                problems.append(f"{n} pairs: {settings[0]} {ours} != {s} {other}")
        for m in modes:
            theirs = answers.get((graph, "umbra", m, "", "w2", str(n), "lengths"), "")
            if ours and theirs and "|" in ours and "|" in theirs:
                c1, h = (int(x) for x in ours.split("|"))
                c2, nodes = (int(x) for x in theirs.split("|"))
                if c1 != c2 or h != nodes - c2:
                    problems.append(f"{n} pairs: operator {ours} != umbra {m} {theirs}")
    print("  answers: " + ("all agree" if not problems else "; ".join(problems)))
