Writer-lock probe of 11 and 12 Nov 2026 on the frozen build eval-freeze-rc13 (cf4d88ce); measurements only, nothing
in the sweep archive and nothing in the build changed. Question: why is the writer's commit at SF10 under 16 readers
slower with the index route's readers (166 ms in the sweep) than with the build route's (83 ms)?

Answer the files support: under the index route's readers the time is in the COMMIT statement (154 of 162 ms, median),
and 90% of the COMMIT time is the wait for the exclusive lock of the index entry (IndexEntry::RemoveFromIndex and
IndexEntry::Append), which the readers hold shared while CSRIndex::Resolve builds their snapshot. The wait grows with
the pending change log and falls after the fold. Under the build route's readers the COMMIT statement takes 27 ms,
the lock wait is 0.1 ms, and the DELETE statement (48 ms) is the larger part.

- writer_statements.txt: median and p90 per writer statement (BEGIN, INSERT, DELETE, COMMIT), both routes.
- commit_and_reader_medians.txt: COMMIT, whole write transaction and reader query medians in windows of 19 commits,
  with the index's log counters, both routes.
- lock_wait.txt: exclusive-lock wait inside each COMMIT statement, measured 12 Nov with a preloaded shim around
  StorageLock::GetExclusiveLock; acquisitions per commit, wait by caller. Per-commit rows in
  raw/*_shim_lock_per_commit.csv (the event lists, 162 MB each, are not kept).
- stack_samples.txt: where the writer thread and the readers were in 488 and 77 samples of one index-route run.
  The writer-thread samples alone understate the wait (the commit also runs on worker threads); lock_wait.txt is
  the measurement.
- raw/: the driver's event CSVs (kinds w_begin, w_insert, w_delete, w_commit added to the sweep's commit and query
  rows) and the stack samples. The files carry no metadata columns: all are SF10, 32 threads, 16 readers,
  190 commits of 2000 changes, 200 ms pause, fold ratio 0.1, pair table benchmark_pairs_same_src_1024.
- tools/: driver_statement_timing.patch (the probe driver is scripts/tools/update_concurrent_driver.cpp with this
  patch, compiled outside the repository; the runs of 11 Nov had the same patch without the abs_start_ns field),
  lockshim.cpp (g++ -O2 -std=c++17 -fPIC -shared lockshim.cpp -o lockshim.so -ldl), run.sh (paths under
  /tmp/wc_probe as used; BIN and PRELOAD select driver and shim), sample_stacks.sh, sum.py, lock_summary.py.

Not measured: the readers' wait for the shared lock, the SF100 and SF300 runs (their 214 and 153 ms are read as the
same wait from the sweep's numbers only), and whether the snapshot can be built outside the lock (read from the code,
not built or tested).
