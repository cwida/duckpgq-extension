#!/usr/bin/env python3
"""Merge fold against full fold of a CSR index (prototype, bean my-beans-p65v).

Loads the edges of an LDBC database into an in-memory table with two CSR indexes on it. Every batch of changes
reaches both indexes through the table; one index is then folded by a scan of the table (the full fold) and the
other by a merge of its newest epoch with the overlay (duckpgq_csr_index_merge_fold). The fold times come from
duckpgq_csr_index_fold_stats. After the batches both indexes answer the same pairs, which checks the merged epochs
and times a traversal over ids that were not renumbered.

Scenarios, each a fraction of the edge count in changes:
  uniform   half deletes of random edges, half inserts between random existing vertices
  skewed    the same, with every changed edge's destination among the lowest 1/64 of the keys
  newv      inserts only, from existing vertices to new keys above every key (one new vertex per ten edges)

Usage: merge_fold_benchmark.py --duckdb build/release/duckdb --db ldbc_sf10.duckdb --label sf10 --out results.csv
"""
import argparse
import csv
import re
import subprocess
import sys

SCENARIOS = [
    ("uniform", 0.001),
    ("uniform", 0.01),
    ("uniform", 0.1),
    ("skewed", 0.01),
    ("newv", 0.01),
]


def batch_sql(kind, fraction, seed):
    """One batch of changes; __E__, __V__ and __MAXKEY__ are replaced once the table is loaded"""
    changes = f"greatest(2, (__E__ * {fraction})::BIGINT)"
    if kind == "uniform":
        return f"""
DELETE FROM knows WHERE hash(rowid, {seed}) % greatest(1, (2 / {fraction})::BIGINT) = 0;
INSERT INTO knows
SELECT a.k, b.k FROM range({changes} / 2) r
JOIN keys a ON a.rn = hash(r.range, {seed}, 1) % __V__
JOIN keys b ON b.rn = hash(r.range, {seed}, 2) % __V__;
"""
    if kind == "skewed":
        # About E / 64 edges end in the range, so the delete takes fraction * 32 of them
        return f"""
DELETE FROM knows WHERE dst <= (SELECT max(k) FROM keys WHERE rn < __V__ / 64)
  AND hash(rowid, {seed}) % greatest(1, (1 / ({fraction} * 32))::BIGINT) = 0;
INSERT INTO knows
SELECT a.k, b.k FROM range({changes} / 2) r
JOIN keys a ON a.rn = hash(r.range, {seed}, 1) % __V__
JOIN keys b ON b.rn = hash(r.range, {seed}, 2) % (__V__ / 64);
"""
    if kind == "newv":
        return f"""
INSERT INTO knows
SELECT a.k, __MAXKEY__ + 1 + {seed} * 1000000007 + r.range % greatest(1, {changes} / 10) FROM range({changes}) r
JOIN keys a ON a.rn = hash(r.range, {seed}, 1) % __V__;
"""
    raise ValueError(kind)


def run(duckdb, sql):
    result = subprocess.run([duckdb, "-unsigned", ":memory:"], input=sql, capture_output=True, text=True)
    if result.returncode != 0 or "Error" in result.stderr:
        sys.stderr.write(result.stdout[-4000:])
        sys.stderr.write(result.stderr[-4000:])
        raise SystemExit(f"duckdb failed ({result.returncode})")
    return result.stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--threads", type=int, default=0)
    parser.add_argument("--pairs", type=int, default=1024)
    args = parser.parse_args()

    # Sizes first: the batches are sized by them
    sizes = run(args.duckdb, f"""
.mode csv
.headers off
ATTACH '{args.db}' AS ldbc (READ_ONLY);
SELECT count(*), (SELECT count(*) FROM (SELECT person1id FROM ldbc.person_knows_person UNION SELECT person2id FROM ldbc.person_knows_person)),
       greatest(max(person1id), max(person2id)) FROM ldbc.person_knows_person;
""").strip().splitlines()[-1].split(",")
    edges, vertices, max_key = (int(x) for x in sizes)

    sql = [".mode csv", ".headers off", ".timer on"]
    if args.threads > 0:
        sql.append(f"SET threads = {args.threads};")
    sql.append(f"""
SET experimental_path_finding_operator = true;
SET duckpgq_csr_index_fold_ratio = 0;
ATTACH '{args.db}' AS ldbc (READ_ONLY);
CREATE TABLE knows AS SELECT person1id AS src, person2id AS dst FROM ldbc.person_knows_person;
DETACH ldbc;
CREATE TABLE keys AS SELECT k, (row_number() OVER (ORDER BY k) - 1)::BIGINT AS rn
  FROM (SELECT src AS k FROM knows UNION SELECT dst FROM knows);
CREATE TABLE pairs AS SELECT a.k AS s, b.k AS d FROM range({args.pairs}) r
  JOIN keys a ON a.rn = hash(r.range, 7) % {vertices} JOIN keys b ON b.rn = hash(r.range, 8) % {vertices};
SELECT 'MARK|create_full';
CREATE INDEX full_idx ON knows USING CSR (src, dst);
SELECT 'MARK|create_merge';
CREATE INDEX merge_idx ON knows USING CSR (src, dst);
SELECT delete_csr(2147483647);
SELECT 'MARK|pairs_full_initial';
SELECT 'ROWS|' || count(len) FROM (SELECT iterativelengthoperator(s, d, 0::BIGINT, 0::BIGINT, 'csr-index|memory|main|knows|full_idx') AS len FROM pairs);
""")
    seed = 0
    for kind, fraction in SCENARIOS:
        for repeat in range(args.repeats):
            seed += 1
            tag = f"{kind}|{fraction}|{repeat}"
            sql.append(batch_sql(kind, fraction, seed))
            sql.append(f"""
SELECT 'LOG|{tag}|' || duckpgq_csr_index_stats('knows', 'full_idx');
SET duckpgq_csr_index_merge_fold = false;
SELECT duckpgq_csr_index_fold('knows', 'full_idx');
SELECT 'FOLD|{tag}|full|' || duckpgq_csr_index_fold_stats('knows', 'full_idx');
SET duckpgq_csr_index_merge_fold = true;
SELECT duckpgq_csr_index_fold('knows', 'merge_idx');
SELECT 'FOLD|{tag}|merge|' || duckpgq_csr_index_fold_stats('knows', 'merge_idx');
""")
    # Both indexes hold the same edges now; the merge index never renumbered its vertices
    sql.append("""
SELECT 'CHECK|' || count(*) FROM (
  SELECT s, d, iterativelengthoperator(s, d, 0::BIGINT, 0::BIGINT, 'csr-index|memory|main|knows|full_idx') AS len FROM pairs
  EXCEPT ALL
  SELECT s, d, iterativelengthoperator(s, d, 0::BIGINT, 0::BIGINT, 'csr-index|memory|main|knows|merge_idx') AS len FROM pairs);
""")
    for index in ("full_idx", "merge_idx", "full_idx", "merge_idx", "full_idx", "merge_idx"):
        sql.append(f"""
SELECT 'MARK|pairs_{index}';
SELECT 'ROWS|' || count(len) FROM (SELECT iterativelengthoperator(s, d, 0::BIGINT, 0::BIGINT, 'csr-index|memory|main|knows|{index}') AS len FROM pairs);
""")
    text = "\n".join(sql)
    text = text.replace("__E__", str(edges)).replace("__V__", str(vertices)).replace("__MAXKEY__", str(max_key))
    output = run(args.duckdb, text)

    rows = []
    mark = None
    timings = []
    check = None
    for line in output.splitlines():
        line = line.strip().strip('"')
        if line.startswith("MARK|"):
            mark = line.split("|", 1)[1]
        elif line.startswith("Run Time") and mark:
            # The MARK statement itself reports a run time first; the statement after it is the timed one
            timings.append((mark, float(re.search(r"real ([0-9.]+)", line).group(1))))
        elif line.startswith("ROWS|") or line.startswith("LOG|"):
            pass
        elif line.startswith("CHECK|"):
            check = int(line.split("|")[1])
        elif line.startswith("FOLD|"):
            _, kind, fraction, repeat, mode, stats = line.split("|", 5)
            fields = dict(item.split("=") for item in stats.split(";"))
            rows.append({
                "dataset": args.label, "edges": edges, "vertices": vertices, "scenario": kind,
                "fraction": fraction, "repeat": repeat, "fold": mode,
                "fold_ms": fields["last_fold_ms"], "scan_ms": fields["last_fold_scan_ms"],
                "rebuilt": fields["last_fold_rebuilt"], "partitions": fields["last_fold_partitions"],
                "merge_folds": fields["merge_folds"],
            })
    if check != 0:
        raise SystemExit(f"the two indexes disagree on {check} pairs")
    # Per mark: the first Run Time belongs to the MARK select, the next two to the statement(s) after it
    seen = {}
    traversal = []
    for name, seconds in timings:
        seen[name] = seen.get(name, 0) + 1
    position = {}
    for name, seconds in timings:
        position[name] = position.get(name, 0) + 1
        if name.startswith("pairs_") and position[name] % 2 == 0:
            traversal.append((name, seconds))
        if name.startswith("create_") and position[name] == 2:
            traversal.append((name, seconds))
    with open(args.out, "a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        if handle.tell() == 0:
            writer.writeheader()
        writer.writerows(rows)
    with open(args.out.replace(".csv", "_timings.csv"), "a", newline="") as handle:
        writer = csv.writer(handle)
        if handle.tell() == 0:
            writer.writerow(["dataset", "what", "seconds"])
        for name, seconds in traversal:
            writer.writerow([args.label, name, seconds])
    print(f"{args.label}: {len(rows)} folds, {edges} edges, {vertices} vertices, indexes agree on {args.pairs} pairs")


if __name__ == "__main__":
    main()
