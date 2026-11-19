Probe of 18 Nov 2026: the path-finding operator against Umbra's Nav-Index on the two graphs of its paper (my-beans-24hi)
====================================================================================================================

Paper: Reif and Neumann, Nav-Index, PVLDB 19(11): 3344-3356, 2026. Artifact:
https://gitlab.db.in.tum.de/max.reif/nav-index-reproducibility (commit 1f341922, 29 Jan 2026), data archive
https://db.in.tum.de/~reif/uploads/data.tar.gz (13.25 GB). Every number below was measured on this machine; none is
taken from the paper.

Setup
-----
Machine: m7a.8xlarge (AMD EPYC 9R14, 32 threads, 123 GB), Amazon Linux 2023. Parts A and B (the operator and Umbra)
ran in quiet slots (load 1.4 and 1.5 at the start, run.log); part C (the community extension) ran beside compiler
jobs of other sessions and is a coarse number.

Systems
- duckpgq-rc13: extension data/ldbc-pathfinding/builds/rc13/duckpgq.duckdb_extension (sha256 06240e53...), DuckDB CLI
  build/release/duckdb of the main checkout (sha256 c016d5fe..., v2.0.0-dev1 45f4fdf315), 32 threads.
  Routes: index (CSR index WITH (reverse = true)), build (CSR built by the query). Settings: default, and
  single_source (experimental_path_finding_operator_few_sources_limit=1000000: one single-source search per source).
- umbra: the artifact's binary, v0.2-1423-g7c87fba6e (2026-06-01), all cores. Modes: dijkstra (nav.buildindex n),
  adhoc_ch (y: a contraction hierarchy built inside every execution), adaptive (a: its cost model chooses),
  table_index (create index on e using navigational(src, dest, cost); queries with the default mode).
  Tables loaded with COPY, as in the artifact's preloadedQuery.
- duckpgq-community-1.4.1: DuckDB 1.4.1 CLI with `INSTALL duckpgq FROM community` (extension build 7705c5c on
  18 Nov 2026; the paper's run may have had an earlier build), in-memory, loaded and queried as in the artifact's
  src/systems/duckdb.js.

Graphs (prepare_<graph>.txt)
- oberbayern.tsv: 495,357 nodes, 805,743 edges, all costs 1, no duplicate edges, no edge with its reverse present,
  largest out-degree 5. This is the file the artifact's system comparison loads. The paper's 516,569 nodes and
  827,682 edges are those of maps/oberbayern.tsv in the same archive (827,682 lines, real-valued costs, another
  node numbering), which the system comparison does not use.
- twitter.tsv: 81,306 nodes, 2,420,766 edge rows (1,768,149 distinct), all costs 1, as in the paper.
- Both are directed for every system (Umbra: 1 -> 335540 is one hop, 335540 -> 1 is eight).
- Query files: 10,000 (src, dest) rows each; pair N is the N-th row by (src, dest), as in the artifact's
  `order by src, dest limit N`.

Workloads
- W2: N pairs, N = 1, 10, 100, 1000, 2000; result count and sum of hop counts (lengths), and for the operator also
  paths at N = 1000. Umbra's shortest_path always returns the path (an array of nodes; hops = length - 1).
- W1: the artifact's DuckPGQ query: the sources of the first 1000 pairs to the destination of the first pair,
  path_length. For the operator as a pair table (1000 rows) and as the artifact's MATCH text unchanged (the IN list
  has 995 distinct sources on Oberbayern, 692 reach the destination on Twitter).

Protocol: one warm-up execution, then three timed ones; the tables give the median. DuckDB: difference between the
timestamps of the statements before and after the query, one CLI process per measurement. Umbra: execution plus
compilation time as its CLI reports them. Index build times are separate.

Results (seconds, median of three; timings.csv has every run, tools/summary.py prints all groups)
--------------------------------------------------------------------------------------------------

1. OSM Oberbayern, N pairs, lengths

  pairs   operator index     operator build     Umbra        Umbra      Umbra      Umbra
          default  single    default  single    table index  adaptive   Dijkstra   ad-hoc CH
      1   0.0535   0.0532    0.0846   0.0826    0.0018       0.4408     0.4362     2.2345
     10   0.0825   0.0809    0.1086   0.1022    0.0023       0.4801     0.4763     2.1148
    100   1.2178   0.2579    1.1793   0.2692    0.0033       1.0201     1.0238     2.1850
   1000   4.2560   1.5778    4.2140   1.4147    0.0130       2.1991     5.1529     2.2062
   2000   7.1780   3.2921    7.7938   2.6966    0.0230       2.2047    10.5087     2.1831

  Index builds: CSR index 0.33 s, Umbra table index 2.15 s.
  1000 pairs with paths: operator 1.80 s (index, default), 1.68 s (build, default); both settings use single-source
  searches for paths.
  - Umbra's table index is 121 times faster than the operator's best configuration at 1000 pairs (0.0130 s against
    1.5778 s on the index route with single_source; 109 times against the build route's 1.4147 s), and 30 times
    faster for one pair.
  - Against Umbra's modes without a stored index, the operator with single_source is faster up to 1000 pairs
    (1000 pairs: 1.58 s against 2.20 s adaptive and 5.15 s Dijkstra) and slower at 2000 pairs (3.29 s against
    2.20 s), where Umbra's adaptive mode builds a contraction hierarchy inside the query at a flat 2.2 s.
  - The operator's default setting is 2.7 times slower than single_source at 1000 pairs and 4.7 times slower at 100
    pairs: above 16 sources it runs multi-source batches, which this graph punishes (see 4).

2. Twitter Social Circles, N pairs, lengths

  pairs   operator index     operator build     Umbra        Umbra      Umbra
          default  single    default  single    table index  adaptive   Dijkstra
      1   0.0024   0.0018    0.0267   0.0256    0.0011       0.2469     0.2597
     10   0.0067   0.0065    0.0299   0.0295    0.0025       0.2980     0.2933
    100   0.0101   0.0261    0.0363   0.0453    0.0036       0.3950     0.3755
   1000   0.0223   0.1102    0.0477   0.1239    0.0153       1.2171     1.2153
   2000   0.0290   0.2105    0.0521   0.2245    0.0264       2.1252     2.1267

  Index builds: CSR index 0.55 s, Umbra table index 191.3 s (348 times slower to build).
  1000 pairs with paths: operator 0.126 s (index), 0.136 s (build).
  - Lengths: Umbra's table index is 1.5 times faster than the operator on the index route at 1000 pairs (0.0153 s
    against 0.0223 s) and 1.1 times faster at 2000 (0.0264 s against 0.0290 s).
  - Paths (what Umbra's function returns): Umbra's table index is 8.2 times faster at 1000 pairs (0.0153 s against
    0.126 s).
  - The operator is 54 times faster than Umbra's Dijkstra mode at 1000 pairs on the index route (0.0223 s against
    1.2153 s) and 25 times faster on the build route, which needs no index (0.0477 s); with paths, 9.6 times.
  - Forced ad-hoc CH was not run on Twitter: every execution would rebuild the hierarchy (minutes).

3. The artifact's DuckPGQ query (W1: 1000 sources, one destination, lengths)

                                               Oberbayern   Twitter
  operator, index, pair table                  0.0865       0.0049     one search from the destination (reverse side)
  operator, index, the artifact's MATCH text   0.0959       0.0103     same plan
  operator, build, default                     3.8788       0.0386     forward, multi-source batches
  operator, build, single_source               2.1311       0.0347     forward, one search per source
  community DuckPGQ on DuckDB 1.4.1            no result    22.94      Oberbayern: process stopped after 900 s

  On Twitter the operator answers the paper's query text 2,200 times faster than the community extension (0.0103 s
  against 22.94 s; the paper reports 18.33 s for the latter on its machine). The community run on Oberbayern (one
  warm-up, three timed executions and one for the hop counts in one process) did not finish in 900 s; with 10
  sources instead of 1000 one execution took 27.6 s in the smoke test.

4. Where the time goes on Oberbayern (diagnostics/, instrumented, not timed)
  - The first 2000 pairs: 1,893 reachable, mean 291 hops, longest 740.
  - Default setting, 100 pairs: one multi-source batch that runs 670 levels, as many as its slowest lane; 1.22 s,
    that is 1.8 ms per level over 495,357 vertices, whatever the frontier size.
  - One single-source search (pair 0, 422 hops): 0.053 s wall on 13 threads; the profile attributes 0.23 s of
    thread time to the operator. 1000 pairs with single_source: 1.58 s wall on 32 threads, about 50 ms of thread
    time per search.
  rc13 has no counter for BFS levels; the level count comes from the lane-activity trace of the instrumented run.

Correctness (check_<graph>.txt)
- Hop counts per pair, first 2000 pairs: the operator equals every Umbra mode on both graphs (0 of 2000 differ;
  Twitter without ad-hoc CH).
- W1 on Twitter: the operator and the community extension return the same hop count for all 692 sources.
- Unreachable pair: NULL in the operator's pair form and in Umbra; no row in MATCH. Self pair (one on Twitter):
  0 hops in the operator, an array of one node in Umbra.

Caveats
- The Umbra binary states "This software is intended strictly for internal evaluation and review purposes and not
  for production purposes"; the artifact has no other licence for it. Publishing these numbers needs the authors'
  agreement.
- Umbra warns at start that the memlock limit (8 MB, hard) is too low to register its io_uring write-back buffers
  ("performance may degrade"); the limit cannot be raised here without root. The measured statements read
  in-memory tables.
- Unweighted only. Nav-Index also answers weighted queries; the operator does not.
- The operator's times include the CLI's handling of one statement (visible at the millisecond scale on Twitter).

Files
- timings.csv: every timed run. run.log: steps with UTC times, checksums and load. prepare_<graph>.txt: counts and
  CSR index build time. check_<graph>.txt: hop-count comparison. lengths_*.csv: per-pair hop counts (Umbra: array
  lengths). umbra_<graph>_<mode>.sql/.out: Umbra's scripts and raw output. <system>_<graph>.out: driver output.
- diagnostics/: phase timing of one pair and of 100 pairs, and the script that produced them.
- tools/navprobe.py (prepare, duckpgq, umbra, community, check), tools/run.sh <graph> [systems], tools/summary.py.
  They expect ~/navindex/artifact (the cloned artifact), ~/navindex/data/data (the five TSV files of the archive),
  ~/navindex/duckdb-1.4.1 (CLI and extension directory); databases are written to ~/navindex/work.
