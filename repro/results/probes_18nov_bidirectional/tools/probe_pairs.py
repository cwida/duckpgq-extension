#!/usr/bin/env python3
"""Single pairs, each as its own query, searched from both ends and from the source only (my-beans-zlnm).

One DuckDB CLI process per scale factor. The pairs are PAIRS persons with an outgoing knows edge and PAIRS persons
with an incoming one, each list ordered by a hash of the person id, paired by position. Every pair is run REPEATS
times per mode, the modes alternating; a query's time is the difference between the transaction timestamps of the
statements before and after it (microseconds; it includes the CLI's handling of one statement, measured by the
`empty` rows: two timestamp statements with nothing between them).

Usage: probe_pairs.py <duckdb cli> <extension> <database> <scale factor> <threads> <out.csv> [lengths|paths]
PROBE_SETTINGS="name=value ..." adds SET experimental_path_finding_operator_<name>=<value> to the session.
"""
import csv
import os
import statistics
import subprocess
import sys

PAIRS = 200
REPEATS = 5
INDEX = "pathfinding_benchmark_csr_reverse_true"
SWITCH = "experimental_path_finding_operator_bidirectional_pair"


def main():
    cli, extension, database, scale_factor, threads, out_path = sys.argv[1:7]
    what = sys.argv[7] if len(sys.argv) > 7 else "lengths"
    function = "shortestpathoperator" if what == "paths" else "iterativelengthoperator"
    result = "(len(r) - 1) // 2" if what == "paths" else "r"
    key = f"csr-index|ldbc|main|person_knows_person|{INDEX}"
    lines = [
        f"LOAD '{extension}';",
        f"SET threads={threads};",
        "SET experimental_path_finding_operator=true;",
        *[f"SET experimental_path_finding_operator_{setting};"
          for setting in os.environ.get("PROBE_SETTINGS", "").split()],
        f"ATTACH '{database}' AS ldbc (READ_ONLY);",
        f"""CREATE TEMP TABLE probe_pairs AS
SELECT s.pos AS pair, s.id AS src, d.id AS dst
FROM (SELECT id, row_number() OVER (ORDER BY hash(id), id) - 1 AS pos
      FROM (SELECT DISTINCT person1id AS id FROM ldbc.person_knows_person)) s
JOIN (SELECT id, row_number() OVER (ORDER BY hash(id + 1), id) - 1 AS pos
      FROM (SELECT DISTINCT person2id AS id FROM ldbc.person_knows_person)) d USING (pos)
WHERE s.pos < {PAIRS};""",
        "SELECT 'pair', pair, src, dst FROM probe_pairs ORDER BY pair;",
    ]

    def query(pair):
        return (f"SELECT 'len', {pair}, {result} FROM (SELECT {function}(src, dst, 0::BIGINT, 0::BIGINT, '{key}') AS r "
                f"FROM probe_pairs WHERE pair = {pair});")

    stamp = "SELECT 'ts', epoch_us(get_current_timestamp());"
    # Warm-up: the index load and both modes once
    for mode in ("true", "false"):
        lines.append(f"SET {SWITCH}={mode};")
        lines.append(query(0))
    for _ in range(20):
        lines += ["SELECT 'tag', 'empty', -1;", stamp, stamp]
    for repeat in range(REPEATS):
        for pair in range(PAIRS):
            for mode in ("true", "false") if repeat % 2 == 0 else ("false", "true"):
                lines.append(f"SET {SWITCH}={mode};")
                lines.append(f"SELECT 'tag', '{'both_ends' if mode == 'true' else 'one_end'}', {pair};")
                lines += [stamp, query(pair), stamp]
    lines.append("SELECT 'strategy', duckpgq_path_finding_strategy();")
    out = subprocess.run([cli, "-unsigned", "-csv", "-noheader"], input="\n".join(lines), text=True,
                         capture_output=True, check=False)
    if out.returncode != 0:
        sys.stderr.write(out.stderr)
        sys.exit(1)

    pairs = {}
    times = {}
    lengths = {}
    tag = None
    stamps = []
    for row in csv.reader(out.stdout.splitlines()):
        if not row:
            continue
        if row[0] == "pair":
            pairs[int(row[1])] = (row[2], row[3])
        elif row[0] == "tag":
            tag = (row[1], int(row[2]))
            stamps = []
        elif row[0] == "ts":
            stamps.append(int(row[1]))
            if len(stamps) == 2 and tag:
                times.setdefault(tag, []).append((stamps[1] - stamps[0]) / 1000.0)
                tag = None
        elif row[0] == "len" and tag:
            lengths.setdefault(tag, set()).add("" if row[2] in ("NULL", "") else row[2])
        elif row[0] == "strategy":
            print("last strategy:", row[1])

    mismatches = 0
    with open(out_path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["scale_factor", "what", "pair", "src", "dst", "len", "both_ends_ms", "one_end_ms",
                         "both_ends_runs", "one_end_runs"])
        for pair in sorted(pairs):
            both = times.get(("both_ends", pair), [])
            one = times.get(("one_end", pair), [])
            len_both = lengths.get(("both_ends", pair), set())
            len_one = lengths.get(("one_end", pair), set())
            if len_both != len_one or len(len_both) != 1:
                mismatches += 1
            writer.writerow([scale_factor, what, pair, *pairs[pair], "|".join(sorted(len_both)) or "",
                             f"{statistics.median(both):.3f}", f"{statistics.median(one):.3f}",
                             " ".join(f"{t:.3f}" for t in both), " ".join(f"{t:.3f}" for t in one)])

    def summary(name, values):
        values = sorted(values)
        if not values:
            return f"{name}: no runs"
        pick = lambda q: values[min(len(values) - 1, int(q * len(values)))]
        return (f"{name}: n={len(values)} median={statistics.median(values):.2f} ms mean={statistics.mean(values):.2f} "
                f"p90={pick(0.9):.2f} p99={pick(0.99):.2f} max={values[-1]:.2f}")

    both_medians = [statistics.median(times[("both_ends", p)]) for p in pairs if ("both_ends", p) in times]
    one_medians = [statistics.median(times[("one_end", p)]) for p in pairs if ("one_end", p) in times]
    reachable = sum(1 for p in pairs if lengths.get(("one_end", p), {""}) != {""})
    print(f"SF{scale_factor} {what}: {len(pairs)} pairs, {reachable} reachable, length mismatches {mismatches}")
    print(summary("empty statement", times.get(("empty", -1), [])))
    print(summary("both ends (median of each pair's runs)", both_medians))
    print(summary("one end   (median of each pair's runs)", one_medians))
    slower = sum(1 for p in pairs if statistics.median(times[("both_ends", p)]) > statistics.median(times[("one_end", p)]))
    print(f"pairs where both ends is slower than one end: {slower}")
    by_len = {}
    for p in pairs:
        length = next(iter(lengths.get(("one_end", p), {""})))
        by_len.setdefault(length, []).append(p)
    for length in sorted(by_len, key=lambda v: (v == "", int(v) if v else 0)):
        group = by_len[length]
        b = statistics.median(statistics.median(times[("both_ends", p)]) for p in group)
        o = statistics.median(statistics.median(times[("one_end", p)]) for p in group)
        print(f"  length {length or 'none'}: {len(group)} pairs, both ends {b:.2f} ms, one end {o:.2f} ms")
    sys.exit(1 if mismatches else 0)


if __name__ == "__main__":
    main()
