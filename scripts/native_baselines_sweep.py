#!/usr/bin/env python3
"""The two DuckDB-native path-finding baselines in the pair-count / source-count sweep, next to the operator:

  duckpgq_recursive  WITH RECURSIVE ... USING KEY (DuckDB's key-based recursive CTE; shortest lengths by keyed
                     minimum, no depth bound beyond a safety cap of 1000 levels)
  duckpgq_scalar     the CIDR 2023 UDF route: create_csr_edge / create_csr_vertex build the CSR as scalar UDFs, and
                     iterativelength() answers each pair (MS-BFS lanes per vector of pairs)

Same pair tables, repeat policy, timeout and result CSV as `pathfinding_benchmark.py sweep-pairs`; the operator's
index route runs in the same sweep so the harness's agreement check covers the native routes. Kept out of the harness
file so the frozen matrix runs (which re-read the harness per step) are not touched; to be folded in afterwards.
"""
import argparse
import importlib.util
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("pathfinding_benchmark", HERE / "pathfinding_benchmark.py")
hb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hb)

RECURSIVE_DEPTH_CAP = 1000  # never reached on LDBC knows (longest shortest path seen: 26 at SF300)


class DuckPGQNativeSystem(hb.DuckPGQSystem):
    """DuckDB-native routes through the same CLI-per-query runner as the operator routes."""

    def __init__(self, route, scale_factor, threads, timeout_s):
        super().__init__("build", scale_factor, threads, timeout_s)
        self.native_route = route
        self.name = f"duckpgq_{route}"

    def script(self, rowid_pairs, key_pairs, warm):
        # rowid_pairs is a relation of (src, dst) person row ids; the reach forms carry a /*reach*/ marker
        reach = "/*reach*/" in rowid_pairs
        if self.native_route == "recursive":
            query = self.recursive_query(rowid_pairs, reach)
        else:
            query = self.scalar_query(rowid_pairs, reach)
        # No warm-up: a native query of several minutes would otherwise count twice against the timeout; the CSR
        # (scalar) and the recursion (recursive) are rebuilt per query by construction
        return f"""LOAD {hb.sql_string(hb.DUCKPGQ_EXTENSION)};
SET threads={self.threads};
ATTACH {hb.sql_string(self.database)} AS ldbc (READ_ONLY);
.timer on
{query}
"""

    def recursive_query(self, rowid_pairs, reach):
        edges = """edges AS (
    SELECT a.rowid::BIGINT AS src, c.rowid::BIGINT AS dst
    FROM ldbc.person_knows_person k
    JOIN ldbc.person a ON a.id = k.person1id
    JOIN ldbc.person c ON c.id = k.person2id
)"""
        if reach:
            # Every vertex each source reaches, the source itself at length 0, as the operator's reach mode counts
            return f"""WITH RECURSIVE
sources AS (SELECT DISTINCT src FROM {rowid_pairs} s),
{edges},
reach(source, here, len) USING KEY (source, here) AS (
    SELECT sources.src AS source, sources.src AS here, 0::BIGINT AS len FROM sources
    UNION ALL (
        SELECT reach.source, edges.dst, reach.len + 1 AS len
        FROM reach
        JOIN edges ON edges.src = reach.here
        LEFT JOIN recurring.reach rec ON rec.source = reach.source AND rec.here = edges.dst
        WHERE reach.len < {RECURSIVE_DEPTH_CAP}
          AND reach.len + 1 < coalesce(rec.len, 9223372036854775807)
        ORDER BY len DESC
    )
)
SELECT count(*), coalesce(sum(len), 0) FROM reach;"""
        return f"""WITH RECURSIVE
pairs AS (SELECT src, dst FROM {rowid_pairs} p),
targets AS (SELECT DISTINCT dst FROM pairs),
{edges},
dvr(here, there, len) USING KEY (here, there) AS (
    SELECT edges.src, edges.dst, 1::BIGINT AS len
    FROM edges JOIN targets ON targets.dst = edges.dst
    UNION ALL (
        SELECT edges.src, dvr.there, dvr.len + 1 AS len
        FROM dvr
        JOIN edges ON edges.dst = dvr.here
        LEFT JOIN recurring.dvr rec ON rec.here = edges.src AND rec.there = dvr.there
        WHERE edges.src <> dvr.there
          AND dvr.len < {RECURSIVE_DEPTH_CAP}
          AND dvr.len + 1 < coalesce(rec.len, 9223372036854775807)
        ORDER BY len DESC
    )
),
lengths AS (
    SELECT CASE WHEN pairs.src = pairs.dst THEN 0 ELSE dvr.len END AS len
    FROM pairs LEFT JOIN dvr ON dvr.here = pairs.src AND dvr.there = pairs.dst
)
SELECT count(len), coalesce(sum(len), 0) FROM lengths;"""

    def scalar_query(self, rowid_pairs, reach):
        pairs = (f"(SELECT s.src AS src, p.rowid::BIGINT AS dst FROM {rowid_pairs} s, ldbc.person p)" if reach
                 else rowid_pairs)
        return f"""{hb.csr_cte("ldbc")},
csr_ready AS (SELECT multiply(0, count(csr_cte.csr_id)) AS temp FROM csr_cte)
SELECT count(len), coalesce(sum(len), 0) FROM (
    SELECT (__x.temp * 0 + iterativelength(0, (SELECT count(*) FROM ldbc.person), p.src, p.dst)) AS len
    FROM {pairs} p CROSS JOIN csr_ready __x
);"""


def make_systems(args, scale_factor, symmetric=False):
    systems = []
    for name in args.systems:
        if name in ("duckpgq_recursive", "duckpgq_scalar"):
            systems.append(DuckPGQNativeSystem(name.split("_", 1)[1], scale_factor, args.threads, args.timeout_s))
        else:
            systems.extend(original_make_systems(argparse.Namespace(**{**vars(args), "systems": [name]}), scale_factor,
                                                 symmetric))
    return systems


original_make_systems = hb.make_systems
hb.make_systems = make_systems

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scale-factor", required=True)
    parser.add_argument("--threads", type=int, default=os.cpu_count())
    parser.add_argument("--systems", nargs="+", default=["duckpgq_index", "duckpgq_recursive", "duckpgq_scalar"])
    parser.add_argument("--pair-counts", nargs="*", type=int, default=hb.SWEEP_PAIR_COUNTS)
    parser.add_argument("--source-counts", nargs="*", type=int, default=hb.SWEEP_SOURCE_COUNTS)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--timeout-s", type=float, default=600)
    parser.add_argument("--client-threads", type=int, default=1)
    parser.add_argument("--max-pairs", type=int, default=500_000_000)
    parser.add_argument("--memgraph-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7688")
    parser.add_argument("--neo4j-database", default="pathfinding")
    parser.add_argument("--neo4j-runtime", default="parallel")
    parser.add_argument("--falkordb-host", default="localhost")
    parser.add_argument("--falkordb-port", type=int, default=6379)
    parser.add_argument("--notes", default="")
    hb.sweep_pairs(parser.parse_args())
