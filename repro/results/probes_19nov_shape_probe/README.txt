Probe of 19 Nov 2026: the graph shape probe, a shape-aware choice between the two kernels for many sources (my-beans-83ld)
=====================================================================================================================

Why: on OSM Oberbayern (495,357 nodes, mean path 291 hops) the default ran MS-BFS lanes above 16 sources and was 2.7
times slower than single-source searches at 1000 pairs, while on Twitter the single-source setting was 4.9 times
slower than the default (rc13, repro/results/probes_18nov_navindex). One default did not fit both graphs.

The rule (src/core/operator/graph_shape_probe.cpp, setting experimental_path_finding_operator_deep_graph_levels,
default 16, 0 off): when the source count would choose the lanes, a bounded serial search starts from 3 of the
query's sources, spread evenly over them. A search is "deep" when it is still expanding after 16 levels without
having discovered 4096 vertices (a quarter of the vertices on a smaller graph) or scanned 16 edges per vertex of
that budget. Reaching a budget or running out of vertices is not deep. Two searches that agree decide; a deep graph
gets one single-source search per source, anything else keeps the lanes.

Setup: m7a.8xlarge, 32 threads, quiet slots (run.log has the load and the checksums). Build of branch
shape-aware-traversal, which contains pathfindingoperator-forward-port up to aaab7e9d. Three settings on one
build: default (probe on), no_shape_probe (deep_graph_levels = 0, the rule up to rc13), single_source
(few_sources_limit = 1000000). One warm-up, three timed runs, median. Index route unless stated.

1. The Nav-Index graphs (timings.csv, tools/summary.py; extension 939ab57f..., commit d5910d63)

  N pairs, lengths, seconds          default    no_shape_probe   single_source   kernel chosen by the default
  Oberbayern    100                  0.0758     1.2166           0.0801          per source (probe: deep)
  Oberbayern   1000                  0.5965     4.0438           0.5881          per source (probe: deep)
  Oberbayern   2000                  1.1324     7.1101           1.0853          per source (probe: deep)
  Oberbayern   1000, build route     1.4508     4.0720           1.4533          per source (probe: deep)
  Twitter       100                  0.0110     0.0103           0.0194          lanes (probe: not deep)
  Twitter      1000                  0.0224     0.0221           0.0497          lanes (probe: not deep)
  Twitter      2000                  0.0285     0.0293           0.0867          lanes (probe: not deep)
  Twitter      1000, build route     0.0475     0.0470           0.1235          lanes (probe: not deep)

  - Oberbayern: the default is 6.8 times faster with the probe than without at 1000 pairs, 16 times at 100 pairs
    and 6.3 times at 2000 pairs (2.8 times on the build route at 1000 pairs), and equals the forced single-source
    setting within 6%.
  - Twitter: the lanes stay; at 1000 pairs the default with the probe takes 0.0224 s against 0.0221 s without. The
    probe took 0.47 to 0.96 ms in this run, which shows at 100 pairs (0.0110 s against 0.0103 s, 7% slower). This
    run still made all three searches; the build measured in 2 stops after two that agree.
  - A deep search on Oberbayern had discovered at most 750 vertices at the level limit, against the budget of 4096.
  - At most 16 sources, paths, and many sources to one destination (one search from the destination) never reach
    the probe: their rows are the same in all three settings.
  - The answers (count and sum of hops) are identical in the three settings for every row.
  - On this build single-source searches on Oberbayern are faster than on rc13 (1000 pairs 0.59 s against 1.58 s),
    so the comparison with Umbra of probes_18nov_navindex has to be rerun at the next freeze rather than combined
    with these numbers.

2. No misfire on the graphs of Section 5 (misfire_<graph>.csv, tools/misfire_summary.py; extension 2a9a7e30...,
   commit 39cfb2da; harness microbench, random pairs, configurations baseline and no_shape_probe)

  graph            pairs    with probe   without    with / without   probe          probe time
  LDBC SF10           64    0.0150       0.0150     1.000            2 at budget    0.33 ms
  LDBC SF10         1024    0.0210       0.0210     1.000            2 at budget    0.25 ms
  LDBC SF10        65536    0.3370       0.3290     1.024            2 at budget    0.40 ms
  LDBC SF100          64    0.0590       0.0590     1.000            2 at budget    0.19 ms
  LDBC SF100        1024    0.0970       0.1000     0.970            2 at budget    0.26 ms
  LDBC SF100       65536    2.3890       2.3820     1.003            2 at budget    0.30 ms
  LDBC SF300          64    0.1350       0.1320     1.023            2 at budget    0.48 ms
  LDBC SF300        1024    0.1500       0.1620     0.926            2 at budget    0.58 ms
  LDBC SF300       65536    6.7310       6.6690     1.009            2 at budget    0.44 ms
  cit-Patents         64    0.0650       0.0660     0.985            2 exhausted    0.10 ms
  cit-Patents       1024    0.2220       0.2200     1.009            2 exhausted    0.08 ms
  datagen-8_4-fb      64    0.3010       0.3080     0.977            2 at budget    0.51 ms
  datagen-8_4-fb    1024    0.9450       0.9430     1.002            2 at budget    0.47 ms
  graph500-24         64    0.7900       0.7910     0.999            2 at budget    0.52 ms
  graph500-24       1024    3.1700       3.1710     1.000            2 at budget    0.45 ms

  Every graph is "not deep" and keeps the lanes; both configurations return the same counts. The times with the
  probe are between 0.93 and 1.02 times the times without it, which is the run-to-run spread of these points (the
  harness reads the CLI's timer, which has millisecond resolution). The probe reaches its budget in 2 or 3 levels
  on the LDBC, datagen and graph500 graphs and runs out of vertices after 2 levels on cit-Patents (a citation graph:
  a source reaches few vertices), in 0.08 to 0.58 ms.

Tests: test/sql/path_finding/graph_shape_probe.test (a directed ring of 600 vertices through both routes, with a
deleted and an inserted edge in the index overlay, and reach; a ring with chords and a dense graph keep the lanes);
the suite passes (117 files). csr_index_fold_parallel_scan.test fails in about one run of five with or without the
probe (its epochs=1 assertion races with an automatic fold since the fold ratio default of dd49fac8); not touched.

Limits
- The verdict comes from 3 sources of the query. A graph that is deep in one region and shallow in another is
  judged by where those sources lie.
- Thresholds (16 levels, 4096 vertices) are set from one road graph and seven shallow ones. Oberbayern leaves a
  factor of 5 between its largest deep search (750 vertices) and the budget; a denser road graph has less room.
  Larger road graphs (Germany, Europe) were not measured.
- The probe needs the row lookup of the partitions; on streaming segments it reports shape_probe=unavailable and
  the source count decides as before.

Files
- timings.csv, duckpgq_<graph>.out, prepare_<graph>.txt/.out: part 1 (driver of probes_18nov_navindex with
  NAV_CONFIGS; databases in ~/navindex/work_head). misfire_<graph>.csv, misfire_time_<graph>.out,
  misfire_prepare_<graph>.out: part 2. run.log: every step with UTC time, commit, checksum and load.
- tools/run.sh (part 1), tools/misfire.sh (part 2), tools/setup_data.sh (private copies of the six databases, 29 GB;
  removed after the run), tools/summary.py, tools/misfire_summary.py.
