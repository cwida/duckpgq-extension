Few-sources crossover on the three Graphalytics graphs, rerun 12 Nov 2026 on the frozen build eval-freeze-rc13
(cf4d88ce, 256 lanes); measurements only, nothing in the sweep archive changed. The values quoted before 12 Nov
(4 / 16 / 256 sources) came from the runs of 2 Nov on dff0c0eb with 512 lanes.

Two kernels forced, k = 2 to 256 distinct sources, k random pairs and k sources x 200 destinations, index route,
32 threads, three repeats (tools/run.sh). Where the lane waves become faster than the single-source kernel:
- datagen-8_4-fb: at 2 sources already, in both shapes (1.5 and 1.3 times faster at 2; 33 and 63 times at 256).
- graph500-24: at 2 sources for random pairs (1.1 times faster); for k x 200 the two are within 5% at 2 sources
  and the lanes are 1.4 times faster at 4.
- cit-Patents: between 8 and 16 sources in both shapes (2 Nov, 512 lanes: between 64 and 256). No pair of either
  shape is reachable on this graph (reachable_count 0 in every row, also on 2 Nov), so the comparison is between
  searches that find no destination, at 24 to 155 ms.
k = 1 was not rerun, so "at 2 already" means the crossover is at 2 sources or fewer.

- crossover_summary.txt: the medians per k and the ratio, with the 2 Nov runs below for comparison.
- microbench_graphalytics_*.csv: the harness's files. Kept out of repro/results/graphalytics_*/ so that no printer
  takes them for a newer microbenchmark of those graphs.
- tools/: run.sh (the command), summary.py (the table).
The script names the commit the repository was at (8ab7922f); src/ is unchanged since cf4d88ce.
