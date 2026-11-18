"""perf counters of one warm query per explore variant (my-beans-rvnn).

For every config the harness's CLI script is run under `perf stat` twice per repeat: with the query once, and with the
query twice (the harness's warm-up form). The difference of the two is one warm query: the index load, the flat-CSR
conversion and the process start cancel. Medians over the repeats.

usage: perf_variants.py <sf N | dataset NAME> <shape: random:<pairs> | multi:<sources>> <out.csv> <configs...>
"""
import csv
import importlib.util
import re
import statistics
import subprocess
import sys

WT = "/local/home/dwolde/bootstrap/duckpgq-extension/.claude/worktrees/explore-variants"
spec = importlib.util.spec_from_file_location("pfb", WT + "/scripts/pathfinding_benchmark.py")
pfb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pfb)

EVENTS = ["cycles:u", "instructions:u", "cache-references:u", "cache-misses:u", "L1-dcache-load-misses:u",
          "dTLB-load-misses:u"]
REPEATS = 3


def perf(script):
    result = subprocess.run(["perf", "stat", "-x", ",", "-e", ",".join(EVENTS), str(pfb.BENCH_DUCKDB), "-unsigned",
                             "-csv"], cwd=WT, text=True, input=script, capture_output=True, timeout=1800)
    if result.returncode != 0 or "Error" in result.stdout:
        raise RuntimeError(result.stdout[-1500:] + result.stderr[-1500:])
    counts = {}
    for line in result.stderr.splitlines():
        parts = line.split(",")
        # perf prints some events without the :u suffix
        name = parts[2] if len(parts) > 2 else ""
        name = name if name.endswith(":u") else name + ":u"
        if name in EVENTS:
            counts[name] = float(parts[0]) if re.match(r"^[0-9.]+$", parts[0]) else float("nan")
    for event in EVENTS:
        counts.setdefault(event, float("nan"))
    timers = re.findall(r"Run Time \(s\): real ([0-9.]+)", result.stdout + result.stderr)
    counts["query_s"] = float(timers[-1])
    return counts


def main():
    kind, target, shape, out = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    configs = sys.argv[5:]
    scale_factor = int(target) if kind == "sf" else pfb.graphalytics_name(target)
    shape_kind, count = shape.split(":")
    if shape_kind == "random":
        table = pfb.pair_table_name(int(count), "random")
    else:
        table = pfb.ensure_multi_source_table(scale_factor, int(count))
    rows = []
    for config in configs:
        entry = pfb.MICROBENCH_CONFIGS[config]
        system = pfb.DuckPGQSystem("index", scale_factor, 32, 1800, settings=entry[0], index_with=entry[1],
                                   multi_source_pair_mode=entry[2],
                                   index_suffix=entry[3] if len(entry) > 3 else None)
        system.load(None, scale_factor)
        forms = system.pair_forms(table)
        once = [perf(system.script(*forms, warm=False)) for _ in range(REPEATS)]
        twice = [perf(system.script(*forms, warm=True)) for _ in range(REPEATS)]
        row = {"target": target, "shape": shape, "config": config,
               "query_s": statistics.median(r["query_s"] for r in twice)}
        for event in EVENTS:
            row[event] = statistics.median(r[event] for r in twice) - statistics.median(r[event] for r in once)
        rows.append(row)
        print(config, {k: (f"{v:.4g}" if isinstance(v, float) else v) for k, v in row.items()}, flush=True)
    with open(out, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


main()
