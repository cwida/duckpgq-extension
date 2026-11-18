CSR index with its memory in DuckDB's buffer pool (branch csr-index-buffer-manager, my-beans-sqnu) against the
rc13 extension, 17 Nov 2026, 17:13 to 17:19 UTC, the machine otherwise idle. Measurements only; nothing in the
sweep archive changed.

Both extensions were loaded into the same DuckDB CLI (the branch's build of DuckDB 45f4fdf3) and read the shared
databases. rc13 extension: build/release of the main checkout, built 4 Nov from cf4d88ce. Branch: 8aaa81ce (the
script records ab597498, the commit that added it; src/ is the same).

1. Traversal at SF300, index route, 32 threads, harness microbench, configuration baseline. Two rounds of three
   repeats per extension, alternating (rc13, branch, rc13, branch); median of the six runs, seconds. Every run
   returned the same reachable count and length sum.

   point                   rc13 extension            branch                    branch against rc13
   65 536 random pairs     10.16 (10.09 to 10.26)    9.93 (9.91 to 9.97)       1.02 times faster
   2048 random pairs       0.364 (0.342 to 0.380)    0.343 (0.329 to 0.366)    1.06 times faster, ranges overlap
   1024 sources x all      0.785 (0.759 to 0.796)    0.762 (0.724 to 0.786)    1.03 times faster, ranges overlap

   The frozen sweep measured 9.87, 10.01 and 10.07 s for 65 536 random pairs on rc13 (6 Nov,
   repro/results/sf300/sweep_pairs_sf300_threads32_1793841659700078410.csv), so the rc13 runs here reproduce it.
   For 65 536 random pairs every branch run is below every rc13 run; why it is faster was not investigated (the
   epoch's arrays now come from DuckDB's block allocator instead of malloc, and the sparse rows have no growth
   slack). The claim these runs support is that the traversal is not slower.

2. CREATE INDEX and the explicit fold on an in-memory copy of knows at SF300 (55 656 915 edges), three runs per
   extension, 32 threads (tools/probe_build_fold.sh).

                                  rc13 extension              branch
   CREATE INDEX, seconds          1.495, 1.495, 1.494         1.372, 1.373, 1.446    (1.09 times faster at the median)
   fold, ms, six folds            1184 to 1300, median 1268   1157 to 1285, median 1233   (ranges overlap)
   index bytes reported           616 640 714                 616 640 714
   EXTENSION bytes, one epoch     0                           616 849 416   (reported + 208 702 of 4 KiB rounding)
   EXTENSION bytes, two epochs    0                           1 233 698 904 (reported 1 233 281 518)
   EXTENSION bytes, third epoch   0                           1 233 698 904 (the oldest epoch was released)

   On rc13 duckdb_memory() shows nothing for the index; on the branch it shows the index within 0.04%.

3. Both-routes correctness check of the update workload at SF10 on the branch (the harness's
   concurrent_sf10_r4_check: 4 readers, 190 commits of 2000 operations, fold ratio 0.01): 4564 index answers
   compared with the build route's, mismatches=0, errors=0.

Files: traversal_*.out and microbench_sf300_*.csv (the harness's output and files, in run order rc13, branch, rc13,
branch), build_fold_*.out, oracle_sf10.out and its concurrent_sssp_*.csv, run.log (start and end of every step),
tools/ (run.sh is the whole command). The CSVs are kept out of repro/results/sf300 and sf10 so that no table
printer takes them for a newer run of the sweep.
