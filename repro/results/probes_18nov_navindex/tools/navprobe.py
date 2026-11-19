#!/usr/bin/env python3
"""The path-finding operator, the community DuckPGQ and Umbra's Nav-Index on the two graphs of the Nav-Index paper
(Reif and Neumann, PVLDB 19(11), 2026), every system measured on this machine (my-beans-24hi).

Subcommands (GRAPH is oberbayern or twitter; the TSV files of the artifact's data archive lie in $NAV_DATA):

  prepare GRAPH     DuckDB database $NAV_WORK/GRAPH.duckdb: nodes(id), edges(src, dst), queries(pos, src, dst) with
                    pos the rank by (src, dst) as in the artifact's `order by src, dest limit N`, and a CSR index
                    WITH (reverse = true). Prints counts and the CREATE INDEX time.
  duckpgq GRAPH     The operator (extension $NAV_EXT, CLI $NAV_CLI): routes index and build, settings default and
                    single_source (few_sources_limit=1000000), workloads W1 (1000 sources, one destination), W2
                    (N pairs, lengths; 1000 pairs, paths) and the artifact's MATCH query unchanged.
  community GRAPH   The community extension on DuckDB 1.4.1 ($NAV_CLI141), loaded and queried as in the artifact's
                    src/systems/duckdb.js.
  umbra GRAPH       Umbra from the artifact ($NAV_UMBRA/bin/sql): Dijkstra only, ad-hoc CH, adaptive, table index.

A measurement is one warm-up execution followed by REPEATS timed ones; the CSV has every timed run. DuckDB times
are the difference between the transaction timestamps of the statements before and after the query (it includes
the CLI's handling of one statement). Umbra times are execution plus compilation as its CLI reports them.
Per-pair hop counts go to <out>/lengths_<system>_<graph>_<workload>.csv for the cross-check (subcommand check).
"""
import csv
import os
import re
import subprocess
import sys
import time

NAV = os.environ.get("NAV", os.path.expanduser("~/navindex"))
DATA = os.environ.get("NAV_DATA", f"{NAV}/data/data")
WORK = os.environ.get("NAV_WORK", f"{NAV}/work")
MAIN = "/local/home/dwolde/bootstrap/duckpgq-extension"
CLI = os.environ.get("NAV_CLI", f"{MAIN}/build/release/duckdb")
EXT = os.environ.get("NAV_EXT", f"{MAIN}/data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension")
CLI141 = os.environ.get("NAV_CLI141", f"{NAV}/duckdb-1.4.1/duckdb")
EXTDIR141 = os.environ.get("NAV_EXTDIR141", f"{NAV}/duckdb-1.4.1/ext")
UMBRA = os.environ.get("NAV_UMBRA", f"{NAV}/artifact/umbra")
THREADS = int(os.environ.get("NAV_THREADS", os.cpu_count()))
REPEATS = int(os.environ.get("NAV_REPEATS", "3"))
TIMEOUT = int(os.environ.get("NAV_TIMEOUT", "900"))
OUT = os.environ.get("NAV_OUT", os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PAIR_COUNTS = [int(n) for n in os.environ.get("NAV_PAIR_COUNTS", "1 10 100 1000 2000").split()]
FILES = {"oberbayern": ("oberbayern.tsv", "queriesoberbayern.tsv"), "twitter": ("twitter.tsv", "queriestwitter.tsv")}
INDEX = "nav_csr"
STAMP = "SELECT 'ts', epoch_us(get_current_timestamp());"
PREFIX = "experimental_path_finding_operator"
SETTINGS = {"default": [], "single_source": [f"SET {PREFIX}_few_sources_limit=1000000;"],
            # Builds with the graph shape probe (my-beans-83ld): the probe off, which is the default of rc13
            "no_shape_probe": [f"SET {PREFIX}_deep_graph_levels=0;"]}
FIELDS = ["system", "graph", "route", "setting", "workload", "pairs", "result", "run", "seconds", "answer", "note"]


def append_rows(rows):
    path = f"{OUT}/timings.csv"
    new = not os.path.exists(path)
    with open(path, "a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
            print(" ".join(f"{k}={row.get(k, '')}" for k in FIELDS), flush=True)


def duckdb_cli(cli, lines, timeout=TIMEOUT, args=()):
    """Runs the statements in one CLI process. Returns (rows, error): CSV rows of the output, None or a message."""
    try:
        out = subprocess.run([cli, "-unsigned", "-csv", "-noheader", *args], input="\n".join(lines) + "\n", text=True,
                             capture_output=True, check=False, timeout=timeout)
    except subprocess.TimeoutExpired:
        return [], f"timeout after {timeout} s"
    rows = [row for row in csv.reader(out.stdout.splitlines()) if row]
    error = None
    if out.returncode != 0 or "Error" in out.stderr:
        error = " ".join(out.stderr.split())[:300] or f"exit {out.returncode}"
    return rows, error


def timed(tag, statement, repeats=REPEATS):
    """One warm-up, then the timed executions, each between two timestamp statements and tagged."""
    lines = [statement]
    for run in range(repeats):
        lines += [f"SELECT 'tag', '{tag}', {run};", STAMP, statement, STAMP]
    return lines


def parse_timed(rows):
    """{tag: [(seconds, answer row)]} from the output of timed() blocks; answers are rows that follow the first stamp."""
    result, tag, stamps, answer = {}, None, [], None
    for row in rows:
        if row[0] == "tag":
            tag, stamps, answer = row[1], [], None
        elif row[0] == "ts" and tag is not None:
            stamps.append(int(row[1]))
            if len(stamps) == 2:
                result.setdefault(tag, []).append(((stamps[1] - stamps[0]) / 1e6, answer))
                tag = None
        elif tag is not None and len(stamps) == 1:
            answer = "|".join(row)
    return result


# ---------------------------------------------------------------------------------------------------------------
def prepare(graph):
    edges, queries = FILES[graph]
    os.makedirs(WORK, exist_ok=True)
    database = f"{WORK}/{graph}.duckdb"
    for suffix in ("", ".wal"):
        if os.path.exists(database + suffix):
            os.remove(database + suffix)
    read = "delim='\\t', header=false, columns={{'src': 'BIGINT', 'dest': 'BIGINT'{extra}}}"
    lines = [
        f"LOAD '{EXT}';", f"SET threads={THREADS};", f"ATTACH '{database}' AS g;", "USE g;",
        "CREATE TABLE edges AS SELECT src, dest AS dst FROM read_csv('%s', %s);"
        % (f"{DATA}/{edges}", read.format(extra=", 'cost': 'DOUBLE'")),
        "CREATE TABLE nodes AS SELECT id FROM (SELECT src AS id FROM edges UNION SELECT dst FROM edges) ORDER BY id;",
        "CREATE TABLE queries AS SELECT row_number() OVER (ORDER BY src, dest) - 1 AS pos, src, dest AS dst "
        "FROM read_csv('%s', %s);" % (f"{DATA}/{queries}", read.format(extra="")),
        "ANALYZE;",
        "SELECT 'count', 'nodes', count(*) FROM nodes;",
        "SELECT 'count', 'edges', count(*) FROM edges;",
        "SELECT 'count', 'distinct_edges', count(*) FROM (SELECT DISTINCT src, dst FROM edges);",
        "SELECT 'count', 'self_loops', count(*) FROM edges WHERE src = dst;",
        "SELECT 'count', 'edges_with_reverse', count(*) FROM (SELECT DISTINCT src, dst FROM edges) e "
        "SEMI JOIN (SELECT DISTINCT src, dst FROM edges) r ON r.src = e.dst AND r.dst = e.src;",
        "SELECT 'count', 'max_out_degree', max(c) FROM (SELECT count(*) AS c FROM edges GROUP BY src);",
        "SELECT 'count', 'queries', count(*) FROM queries;",
        "SELECT 'count', 'distinct_queries', count(*) FROM (SELECT DISTINCT src, dst FROM queries);",
        "SELECT 'count', 'query_sources', count(DISTINCT src) FROM queries;",
        "SELECT 'count', 'query_destinations', count(DISTINCT dst) FROM queries;",
        "SELECT 'count', 'queries_src_eq_dst', count(*) FROM queries WHERE src = dst;",
        "SELECT 'tag', 'create_index', 0;", STAMP,
        f"CREATE INDEX {INDEX} ON edges USING CSR (src, dst) WITH (reverse = true);", STAMP,
        "SELECT 'tag', 'checkpoint', 0;", STAMP, "CHECKPOINT g;", STAMP,
    ]
    rows, error = duckdb_cli(CLI, lines)
    if error:
        sys.exit(f"prepare {graph}: {error}")
    with open(f"{OUT}/prepare_{graph}.txt", "w") as handle:
        for row in rows:
            if row[0] == "count":
                handle.write(f"{row[1]} {row[2]}\n")
        for tag, runs in parse_timed(rows).items():
            handle.write(f"{tag}_seconds {runs[0][0]:.3f}\n")
        handle.write(f"database_bytes {os.path.getsize(database)}\n")
    print(open(f"{OUT}/prepare_{graph}.txt").read())


# ---------------------------------------------------------------------------------------------------------------
def pair_tables():
    """Key pairs for the index route, row-id pairs (suffix _rows) for the build route."""
    lines = []
    names = {f"w2_{n}": f"SELECT pos, src, dst FROM g.queries WHERE pos < {n}" for n in sorted(set(PAIR_COUNTS + [1000]))}
    names["w1_1000"] = ("SELECT pos, src, (SELECT dst FROM g.queries WHERE pos = 0) AS dst "
                        "FROM g.queries WHERE pos < 1000")
    for name, select in names.items():
        lines.append(f"CREATE TEMP TABLE {name} AS {select};")
        lines.append(f"CREATE TEMP TABLE {name}_rows AS SELECT p.pos, a.rowid::BIGINT AS src, b.rowid::BIGINT AS dst "
                     f"FROM {name} p JOIN g.nodes a ON a.id = p.src JOIN g.nodes b ON b.id = p.dst;")
    return lines


def operator_call(function, route, table, counts):
    if route == "index":
        return (f"SELECT pos, {function}(src, dst, 0::BIGINT, 0::BIGINT, 'csr-index|g|main|edges|{INDEX}') AS r "
                f"FROM {table}")
    edge_id = ", id := e_id" if function == "shortestpathoperator" else ""
    return (f"SELECT pos, {function}(src, dst, struct_pack(src := e_src, dst := e_dst{edge_id}), "
            f"{counts['nodes']}::BIGINT, {counts['edges']}::BIGINT, 'graph:navprobe') AS r "
            f"FROM {table}_rows AS pathfinding_pairs, (SELECT a.rowid::BIGINT AS e_src, c.rowid::BIGINT AS e_dst, "
            f"k.rowid::BIGINT AS e_id FROM g.edges k JOIN g.nodes a ON a.id = k.src JOIN g.nodes c ON c.id = k.dst) "
            f"pathfinding_edges")


def match_query(columns):
    """The artifact's query (src/systems/duckdb.js, limit 1000) with the sources' ids optionally in the output."""
    return (f"FROM GRAPH_TABLE (road_graph MATCH p = ANY SHORTEST (a:nodes WHERE a.id in (select src from g.queries "
            f"order by src, dst limit 1000))-[r:edges]->*(b:nodes where b.id = (SELECT dst FROM g.queries WHERE pos = 0)) "
            f"COLUMNS ({columns})) gt")


def session(setting, extra=()):
    return [f"LOAD '{EXT}';", f"SET threads={THREADS};", f"SET {PREFIX}=true;", *SETTINGS[setting], *extra,
            f"ATTACH '{WORK}/GRAPH.duckdb' AS g (READ_ONLY);", "SELECT delete_csr(2147483647) AS duckpgq_state_init;"]


def duckpgq(graph):
    counts = dict(line.split() for line in open(f"{OUT}/prepare_{graph}.txt"))
    workloads = [("w1", 1000, "lengths", "w1_1000")]
    workloads += [("w2", n, "lengths", f"w2_{n}") for n in PAIR_COUNTS]
    workloads += [("w2", 1000, "paths", "w2_1000")]
    extra = [f"SET {PREFIX}_{setting};" for setting in os.environ.get("NAV_SETTINGS", "").split()]
    routes = os.environ.get("NAV_ROUTES", "index build").split()
    settings = os.environ.get("NAV_CONFIGS", "default single_source").split()
    for route in routes:
        for setting in settings:
            for workload, pairs, result, table in workloads:
                function = "shortestpathoperator" if result == "paths" else "iterativelengthoperator"
                value = "(len(r) - 1) // 2" if result == "paths" else "r"
                call = operator_call(function, route, table, counts)
                tag = f"{route}.{setting}.{workload}.{pairs}.{result}"
                lines = [line.replace("GRAPH", graph) for line in session(setting, extra)] + pair_tables()
                lines += timed(tag, f"SELECT count(v), coalesce(sum(v), 0) FROM (SELECT {value} AS v FROM ({call}));")
                lines.append("SELECT 'strategy', duckpgq_path_finding_strategy();")
                dump = route == "index" and setting == "default" and pairs in (1000, max(PAIR_COUNTS))
                if dump:
                    lines.append(f"SELECT 'len', pos, {value} FROM ({call}) ORDER BY pos;")
                record(graph, "duckpgq-rc13", route, setting, workload, pairs, result, tag, lines,
                       f"lengths_duckpgq_{graph}_{workload}_{pairs}_{result}.csv" if dump else None)
    # The artifact's own query text, on the operator
    for setting in settings:
        tag = f"match.{setting}.w1.1000.lengths"
        lines = [line.replace("GRAPH", graph) for line in session(setting, extra)]
        lines += ["SET active_grammar_extensions = ['duckpgq'];", "CREATE PROPERTY GRAPH road_graph VERTEX TABLES (g.nodes) EDGE TABLES (g.edges SOURCE KEY (src) "
                  "REFERENCES g.nodes (id) DESTINATION KEY (dst) REFERENCES g.nodes (id));"]
        lines += timed(tag, f"SELECT count(l), coalesce(sum(l), 0) FROM ({match_query('path_length(p) AS l')});")
        lines.append("SELECT 'strategy', duckpgq_path_finding_strategy();")
        record(graph, "duckpgq-rc13", "match", setting, "w1", 1000, "lengths", tag, lines, None)


def record(graph, system, route, setting, workload, pairs, result, tag, lines, dump, cli=None, args=()):
    start = time.time()
    rows, error = duckdb_cli(cli or CLI, lines, args=args)
    runs = parse_timed(rows).get(tag, [])
    strategy = next((row[1] for row in rows if row[0] == "strategy"), "")
    note = "; ".join(x for x in (error, strategy) if x)
    base = dict(system=system, graph=graph, route=route, setting=setting, workload=workload, pairs=pairs,
                result=result)
    if not runs:
        append_rows([dict(base, run=-1, seconds="", answer="", note=note or f"no result ({time.time() - start:.0f} s)")])
    append_rows([dict(base, run=i, seconds=f"{seconds:.6f}", answer=answer, note=note if i == 0 else "")
                 for i, (seconds, answer) in enumerate(runs)])
    if dump:
        with open(f"{OUT}/{dump}", "w") as handle:
            handle.write("pos,hops\n")
            for row in rows:
                if row[0] == "len":
                    handle.write(f"{row[1]},{row[2]}\n")


# ---------------------------------------------------------------------------------------------------------------
def community(graph):
    edges, queries = FILES[graph]
    load = [
        f"SET extension_directory='{EXTDIR141}';", "LOAD duckpgq;", f"SET threads={THREADS};",
        "create table nodes (id int primary key);",
        "create table edges_raw (src int, dest int, cost float);",
        f"copy edges_raw from '{DATA}/{edges}';",
        "create table queries (src int, dest int);",
        f"copy queries from '{DATA}/{queries}';",
        "insert into nodes from (select distinct src from edges_raw union select distinct dest from edges_raw);",
        "create table edges (src int references nodes (id), dest int references nodes (id), cost float);",
        "insert into edges select src, dest, cost from edges_raw;",
        "CREATE PROPERTY GRAPH road_graph VERTEX TABLES (nodes) EDGE TABLES (edges SOURCE KEY (src) REFERENCES "
        "nodes (id) DESTINATION KEY (dest) REFERENCES nodes (id));",
        "SELECT 'version', version(), extension_version FROM duckdb_extensions() WHERE extension_name = 'duckpgq';",
    ]
    destination = "(select dest from queries order by src, dest limit 1)"

    def query(columns, limit):
        return (f"FROM GRAPH_TABLE (road_graph MATCH p = ANY SHORTEST (a:nodes WHERE a.id in (select src from queries "
                f"order by src, dest limit {limit}))-[r:edges]->*(b:nodes where b.id = {destination}) "
                f"COLUMNS ({columns})) gt")

    for limit in [int(n) for n in os.environ.get("NAV_COMMUNITY_LIMITS", "1000").split()]:
        tag = f"match.community.w1.{limit}.lengths"
        lines = load + timed(tag, f"SELECT count(l), coalesce(sum(l), 0) FROM ({query('path_length(p) AS l', limit)});")
        lines.append(f"SELECT 'len', src, l FROM ({query('a.id AS src, path_length(p) AS l', limit)}) ORDER BY src;")
        record(graph, "duckpgq-community-1.4.1", "match", "as-in-artifact", "w1", limit, "lengths", tag, lines,
               f"lengths_community_{graph}_w1_{limit}.csv", cli=CLI141)


# ---------------------------------------------------------------------------------------------------------------
UMBRA_TIME = re.compile(r"execution:\s+\(([^)]*)\)\s+compilation:\s+\(([^)]*)\)")


def umbra(graph):
    edges, queries = FILES[graph]
    modes = os.environ.get("NAV_UMBRA_MODES", "dijkstra adhoc_ch adaptive table_index").split()
    flag = {"dijkstra": "n", "adhoc_ch": "y", "adaptive": "a", "table_index": "a"}
    for mode in modes:
        load = [
            f"\\set nav.buildindex {flag[mode]}",
            "create table e (src int, dest int, cost float);", f"copy e from '{DATA}/{edges}';",
            "create table q0 (src int, dest int);", f"copy q0 from '{DATA}/{queries}';",
            "create table q as select row_number() over (order by src, dest) - 1 as pos, src, dest from q0;",
        ]
        if mode == "table_index":
            load += ["select 'MARK build' as mark;", "create index on e using navigational(src, dest, cost);"]
        script = list(load)
        for pairs in PAIR_COUNTS:
            statement = ("select count(l), coalesce(sum(l), 0) from (select array_length(shortest_path(table "
                         f"(select * from e), src, dest), 1) as l from q where pos < {pairs}) x;")
            script += [statement]
            for run in range(REPEATS):
                script += [f"select 'MARK w2 {pairs} {run}' as mark;", statement]
        dump = f"{OUT}/lengths_umbra_{graph}_{mode}.csv"
        script += ["select 'MARK dump' as mark;",
                   "select 'len' as k, pos, array_length(shortest_path(table (select * from e), src, dest), 1) as l "
                   f"from q where pos < {max(PAIR_COUNTS)} order by pos;"]
        path = f"{OUT}/umbra_{graph}_{mode}.sql"
        with open(path, "w") as handle:
            handle.write("\n".join(script) + "\n")
        env = dict(os.environ)
        if "NAV_UMBRA_PARALLEL" in os.environ:
            env["PARALLEL"] = os.environ["NAV_UMBRA_PARALLEL"]
        start = time.time()
        try:
            out = subprocess.run([f"{UMBRA}/bin/sql", "", path], text=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, check=False, timeout=TIMEOUT, env=env)
            text, error = out.stdout, None if out.returncode == 0 else f"exit {out.returncode}"
        except subprocess.TimeoutExpired as expired:
            text = expired.stdout.decode(errors="replace") if isinstance(expired.stdout, bytes) else (expired.stdout or "")
            error = f"timeout after {TIMEOUT} s"
        with open(f"{OUT}/umbra_{graph}_{mode}.out", "w") as handle:
            handle.write(text)
        parse_umbra(graph, mode, text, error, dump, time.time() - start)


def parse_umbra(graph, mode, text, error, dump, wall):
    """Timing lines follow each statement's result; a MARK row names the statement that comes next."""
    rows, lengths, mark, answer, expect = [], [], None, None, False
    base = dict(system="umbra", graph=graph, route=mode, setting="", result="lengths")
    for line in text.splitlines():
        stripped = line.strip()
        found = re.search(r"MARK (\w+)(?: (\d+) (\d+))?", stripped)
        timing = UMBRA_TIME.search(stripped)
        if found and not timing:
            mark, answer, expect = found.groups(), None, False
            continue
        if stripped.startswith("len") and mark and mark[0] == "dump":
            parts = stripped.split()
            lengths.append((parts[1], parts[2] if len(parts) > 2 and parts[2].isnumeric() else ""))
        if timing and mark:
            if not expect:  # the timing line of the MARK select itself
                expect = True
                continue
            seconds = sum(float(re.search(r"([\d.]+) median", part).group(1)) for part in timing.groups())
            if mark[0] == "build":
                rows.append(dict(base, workload="create_index", pairs="", run=0, seconds=f"{seconds:.6f}", answer=""))
            elif mark[0] == "w2":
                rows.append(dict(base, workload="w2", pairs=mark[1], run=mark[2], seconds=f"{seconds:.6f}",
                                 answer=answer or ""))
            mark = None if mark[0] != "dump" else mark
        elif mark and expect and stripped and not stripped.startswith("len") and re.match(r"^[\d.\s|]+$", stripped):
            answer = "|".join(stripped.replace("|", " ").split())
    if error or not rows:
        rows.append(dict(base, workload="", pairs="", run=-1, seconds="", answer="",
                         note=f"{error or 'nothing parsed'} ({wall:.0f} s)"))
    append_rows(rows)
    with open(dump, "w") as handle:
        handle.write("pos,array_length\n")
        for pos, length in lengths:
            handle.write(f"{pos},{length}\n")


# ---------------------------------------------------------------------------------------------------------------
def read_lengths(path, offset=0):
    """{key: hops or None}; offset -1 turns Umbra's array length (nodes of the path) into hops."""
    if not os.path.exists(path):
        return None
    with open(path) as handle:
        rows = list(csv.reader(handle))[1:]
    return {int(row[0]): (int(row[1]) + offset if len(row) > 1 and row[1].isnumeric() else None) for row in rows}


def check(graph):
    """Hop counts per pair: the operator against every Umbra mode (W2) and against the community extension (W1)."""
    queries = sorted(tuple(int(x) for x in line.split()) for line in open(f"{DATA}/{FILES[graph][1]}"))
    top = max(PAIR_COUNTS)
    ours = read_lengths(f"{OUT}/lengths_duckpgq_{graph}_w2_{top}_lengths.csv")
    lines = []
    if ours is not None:
        reached = [h for h in ours.values() if h is not None]
        lines.append(f"operator W2 {len(ours)} pairs: {len(reached)} reachable, mean {sum(reached) / len(reached):.2f} "
                     f"hops, max {max(reached)}, self pairs {sum(1 for s, d in queries[:top] if s == d)}")
        for mode in ("dijkstra", "adhoc_ch", "adaptive", "table_index"):
            theirs = read_lengths(f"{OUT}/lengths_umbra_{graph}_{mode}.csv", -1)
            if not theirs:
                lines.append(f"umbra {mode}: no lengths")
                continue
            differ = [(pos, queries[pos], ours.get(pos), theirs[pos]) for pos in sorted(theirs) if ours.get(pos) != theirs[pos]]
            lines.append(f"umbra {mode}: {len(theirs)} pairs compared, {len(differ)} differ" +
                         "".join(f"\n    pos {p} pair {q}: operator {a}, umbra {b}" for p, q, a, b in differ[:10]))
    w1 = read_lengths(f"{OUT}/lengths_duckpgq_{graph}_w1_1000_lengths.csv")
    theirs = read_lengths(f"{OUT}/lengths_community_{graph}_w1_1000.csv")
    if w1 is not None and theirs:
        by_source = {queries[pos][0]: hops for pos, hops in w1.items() if hops is not None}
        differ = [(s, by_source.get(s), theirs.get(s)) for s in sorted(set(by_source) | set(theirs))
                  if by_source.get(s) != theirs.get(s)]
        lines.append(f"community W1: operator reaches the destination from {len(by_source)} distinct sources, "
                     f"community returns {len(theirs)} rows, {len(differ)} differ" +
                     "".join(f"\n    source {s}: operator {a}, community {b}" for s, a, b in differ[:10]))
    else:
        lines.append("community W1: no lengths")
    with open(f"{OUT}/check_{graph}.txt", "w") as handle:
        handle.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    command, graph = sys.argv[1], sys.argv[2]
    {"prepare": prepare, "duckpgq": duckpgq, "community": community, "umbra": umbra, "check": check}[command](graph)
