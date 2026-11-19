Probes of 19 Nov 2026: incomplete edges after the creation-time check (my-beans-0vpk, for my-beans-9h7h)
=========================================================================================================

Build: branch incomplete-edge-check, commit 7912a619 (main's CheckEdgeKeysReferenceVertices ported without
vertex-join elimination), -DDUCKPGQ_LANE_LIMIT=256, extension sha256 03fb22c4..., CLI sha256 12a9d71e... (DuckDB
45f4fdf315). "rc13" is data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension (sha256 06240e53...), loaded into
the same CLI. Machine: m7a.8xlarge, 32 threads; the timed parts ran in the resource monitor's benchmark slot
(09:36-09:42 UTC, no other build, test or database process).

None of this is in the test suite. tools/run.sh reruns the probes, tools/check_graphs.py the creation check,
tools/fk_commit.sh the commit probe, tools/summary.py prints the medians (summary.txt).

Routes. "index": MATCH ... ANY SHORTEST over an edge table with a CSR index (strategy csr=index). "build": the same
graph over an identical edge table without an index, the query builds its CSR (csr=built). "SQL translation":
experimental_path_finding_operator = false. x = 1, y = 2, m = 9; edges 1 -> 9 and 9 -> 2 are the only path from 1 to 2.

(a) After creation, INSERT of 1 -> 9 and 9 -> 2 where 9 has no vertex row (probe_a.out)
  The inserts are accepted. Before the fold and after it (duckpgq_csr_index_fold), the same answers:
    index route        1 -> 2 found: path_length 2, vertices [1, 9, 2], edges [1, 2]; lengths-only query: 2;
                       "all destinations from 1" lists 2 at distance 2 (9 itself is never a source or a destination)
    build route        no row for 1 -> 2 (lengths and paths); "all destinations from 1" lists 1 and 3 only
    SQL translation    Constraint Error "Non-existent/non-unique vertices detected", on both tables
    one-hop MATCH      (a)-[e]->(b) returns 1 -> 3 only (the join with the vertex table drops both new edges)
  CREATE OR REPLACE PROPERTY GRAPH on the table then fails with "Incomplete edge: ... source key (src) = (9)" and the
  previous definition keeps answering.

(b) DELETE of the vertex row of 9 while 1 -> 9 and 9 -> 2 stay (probe_b.out)
  The delete is accepted. Same three answers as (a): index route 1 -> 9 -> 2 (length 2), build route no row, SQL
  translation the constraint error. Unchanged by a fold of the unchanged table, and by a fold after a further edge
  insert (an epoch built after the vertex row was gone still holds key 9).

(c) FOREIGN KEY on both key columns of the edge table (REFERENCES pv(id), pv.id PRIMARY KEY) with a CSR index, on a
    file database (probe_c.out)
  CREATE INDEX ... USING CSR succeeds; CREATE PROPERTY GRAPH succeeds and records verified = true (the check skips
  its scan). The insert of (a) is rejected by DuckDB ("Violates foreign key constraint because key "id: 7" does not
  exist in the referenced table"), the delete of (b) too ("key "src: 9" is still referenced by a foreign key in a
  different table"). A path query over the index, a vertex and its edges inserted in one transaction, an edge delete,
  a fold, a CHECKPOINT and a reload in a new process all work (strategy csr=index before and after the reload, the
  constraints still reject after it).
  One limit of DuckDB's foreign keys seen here: deleting the last edge of a vertex and the vertex row in ONE
  transaction is rejected (the constraint is checked against the committed edges); in two transactions it works.

Creation check on the benchmark databases (create_new.log, create_rc13.log, create_timings.csv)
  The benchmark's statement (vertex table person / nodes, edge table person_knows_person / edges), the database
  attached read-only, the graph in the in-memory catalog as in the harness. Edge rows whose key matches no vertex row,
  counted with an anti-join independent of the extension: 0 sources and 0 destinations on all eight; no NULL keys, no
  foreign keys. CREATE PROPERTY GRAPH succeeds on all eight. Seconds, median of three processes (warm page cache):

    database              vertices      edge rows      with the check   rc13
    LDBC SF10               68,673      1,839,354      0.031            0.009
    LDBC SF100             473,001     18,655,515      0.076            0.009
    LDBC SF300           1,193,579     55,656,915      0.151            0.010
    cit-Patents          3,774,768     16,518,947      0.105            0.008
    graph500-24          8,870,942    520,759,040      1.245            0.009
    datagen-8_4-fb       3,809,084    538,958,354      1.249            0.010
    Nav-Index Oberbayern   495,357        805,743      0.048            0.009
    Nav-Index Twitter       81,306      2,420,766      0.032            0.009

  SF300: 0.151 s with the check against 0.010 s on rc13, 0.14 s more (15 times slower); once per graph creation.

Commit of 2000 edge changes at SF10 with and without the foreign key (fk_commit/, tools/fk_commit.sh)
  One writer, no readers: the concurrent driver with --readers 0, 190 commits of 2000 operations of the staged LDBC
  update log (inserts and deletes), one CSR index (reverse = true), fold ratio 0.001, 32 threads. Two private copies
  built the same way (the edge table re-created and re-filled in the stored order); "fk" adds PRIMARY KEY (id) on
  person and REFERENCES person(id) on both edge columns. Five runs each, alternating, each from a fresh copy.

    variant   median commit (median of the five run medians)   run medians          p99 of a run
    plain      8.79 ms                                          8.76 - 8.93 ms       13.7 - 18.1 ms
    fk        15.87 ms                                         15.78 - 17.10 ms      21.9 - 23.7 ms

  The commit is 1.8 times slower with the foreign key (7.1 ms more per 2000 changes). No commit failed: the log only
  holds edges between persons of the snapshot. The first commit of each fk run takes 263-275 ms (plain: 20-24 ms);
  not investigated (my guess: the first write loads the primary-key index of person).

Both-routes check at SF10 (concurrent_sf10_r4_check.log and the concurrent_*.csv)
  The matrix's step concurrent_sf10_r4_check (4 readers, writer paused 200 ms, fold ratio 0.01, 190 commits of 2000):
  4416 index answers compared with the build route's, 0 mismatches, 0 errors. The driver calls the operator functions
  directly and creates no property graph, so this run does not exercise the check; it shows the build is sound.

Compatibility note
  The check's result is stored in a new column of __duckpgq_internal (verified), added to an existing table on first
  use. In a database FILE whose own catalog has held a graph under this build, rc13 can no longer create a graph
  ("table __duckpgq_internal has 23 columns but 22 values were supplied"; with CREATE OR REPLACE the old rows are
  already deleted by then). The benchmark is not affected: it attaches its databases and keeps the graph in the
  in-memory catalog. A/B runs against rc13 must not share a database file that stores a property graph.
