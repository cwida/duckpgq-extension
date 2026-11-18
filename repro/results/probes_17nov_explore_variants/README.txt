Ablation of the partitioned CSR: one partition, partition counts, and a locked or atomic MS-BFS (bean my-beans-rvnn,
Peter Boncz's feedback of 17 Nov 2026). Measured 17 and 18 Nov 2026 on branch pathfinding-explore-variants, code in
1bd6b8e2. The default path is unchanged: src/ was identical to eval-freeze-rc13 (cf4d88ce) before this branch, and
the baseline rows reproduce the rc13 values of the same points (SF100: 65,536 pairs 3.41 s against 3.30 s, 1024
sources x all 0.395 s against 0.415 s). Measurements only; nothing in the sweep archive changed.

Setting experimental_path_finding_operator_explore_variant (default partitioned). The four flat variants run the
EXPLORE phase of the lane kernel on one unpartitioned CSR with 32-bit destinations, derived from the index partitions
in the harness's warm-up query and cached for the process; PREPARE, CHECK and the scheduling are the same code.
- flat_serial: one partition with one owner, a single worker expands a level.
- flat_ranges: every worker owns a destination range and reads every active row for its slice of it (ownership
  without physical partitioning, no synchronisation).
- flat_atomic: workers take morsels of 2048 source vertices (256 frontier entries on a sparse level) and update the
  destination's lane words with fetch_or, after testing that the word would change.
- flat_locked: the same morsels, a plain OR under a striped spinlock (4096 cache-line-padded locks, one stripe per 16
  destinations).
Index route only, 32 threads unless stated, three repeats, median. Every configuration returns the same reachable
count and length sum per point (the harness checks).

Files
- summary.txt: medians and ratios to the baseline at 32 threads for SF100 (with the partition widths), SF300 and
  graph500-24.
- threads_summary.txt: SF300 at 1, 2, 4, 8, 16, 32 threads (the author asked for the thread counts on 17 Nov).
- perf_*.csv: perf stat counters of one warm query (the run with the query twice minus the run with it once, so the
  process start, the index load and the flat conversion cancel), user space, whole query including the result. The
  virtual machine reports 0 for L1-dcache-load-misses; cache-misses is perf's generic event on the AMD EPYC 9R14.
- microbench_*.csv: the harness's files. sf10 is the one-repeat smoke test. The first graph500-24 file is the run
  that was stopped: it has the 65,536-pair point for the baseline (152 s) and flat_serial (294 s, two repeats) only;
  the second graph500-24 file has the other three points for all configurations.
- tools/: the run scripts, summary.py, threads_summary.py, perf_variants.py.

Not measured: the partition widths at SF300 and on graph500-24 (they create four or five persistent CSR indexes in
the shared database files; SF100 has them from rc13); 65,536 pairs on graph500-24 for flat_ranges, flat_atomic and
flat_locked; 65,536 pairs in the thread sweep (flat_atomic takes 75 s there at 32 threads).

Reading notes
- How the points are scheduled (strategy column): 65,536 random pairs run as 256 waves in 32 concurrent groups, each
  level split into 1 task at SF10, 5 at SF100, 14 at SF300 and 32 on graph500-24 (the task_edges rule), so at
  SF100 and above several threads write one wave's lane words. In the thread sweep at t threads: 2048 pairs are 8
  waves in min(t, 8) groups, 1024 sources x all 4 waves in min(t, 4) groups, 256 sources x all one wave; every
  group's level has up to t tasks.
- What separates the cost of the synchronised update from contention is therefore the single-thread column of the
  thread sweep (flat_atomic 1.7 to 2.9 times slower, flat_locked 1.3 to 1.8 times), not the 65,536-pair point.
- flat_ranges gives each task of a wave a destination range and narrows every row by binary search; on 65,536 pairs
  it is slower than flat_serial at SF300.
- figures/: threads_time.png, threads_speedup.png, threads_slowdown.png (tools/plot_threads.py).
- On graph500-24 flat_atomic is the best alternative. A possible reason, not measured: it tests a word before
  updating it and on a dense graph most relaxations add no lane.
