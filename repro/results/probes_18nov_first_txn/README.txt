First write transaction of the concurrent update driver (my-beans-kzlq, 18 Nov 2026)

Question: in scripts/tools/update_concurrent_driver.cpp the first write transaction (INSERT, DELETE, COMMIT of 2000
changes) took 15.4 s at SF300 and 13 s at SF100; later ones take about 0.1 s.

Cause: a write to a table loads every CSR index of that table that is not resident yet, inside the commit, one after
another (CSRIndex::Append and CSRIndex::TryDelete call EnsureLoadedLocked). The shared benchmark databases carry more
CSR indexes on person_knows_person than the one the driver measures: pathfinding_benchmark_csr_rb<N> from the
microbenchmark's row-bucket configurations and pathfinding_benchmark_csr_undirected from the sweep's undirected
system (5 others at SF300, 15 at SF100 on 18 Nov). The harness hands the driver a copy of the shared database, and
the driver made only the measured index resident before its clock started.

One connection, no readers, SF300, rc13 CLI and extension, a fresh copy of the database per run (tools/t1.sql,
t2_touch_all.sql, t3_drop_extra.sql):
  database as it is                 INSERT 4 ms, DELETE 1.28 s, COMMIT 14.16 s; second transaction's COMMIT 14 ms
  all six indexes touched before    INSERT 6 ms, DELETE 1.21 s, COMMIT 14 ms (the touches: 1.4, 2.3, 2.3, 2.3, 2.3, 4.7 s)
  five other indexes dropped        INSERT 6 ms, DELETE 1.28 s, COMMIT 6 ms
The 1.2 to 1.3 s of the first DELETE (20 ms in the second transaction) is the first scan of the table for its join.

Driver runs (tools/run_ab.sh 300 100; 32 threads, 16 readers, 60 batches of 2000, pause 200 ms, fold ratio 0.1,
index route only, quiet machine, a fresh copy of the database per run). Files sf<N>_<run>.csv and .out.
  rc13_asis     rc13 driver and extension, database as it is
  rc13_dropped  the same, pathfinding_benchmark_csr_rb0, _rb2, _rb8, _rb64 and _undirected dropped before the driver
                (all the others at SF300; 10 more remain at SF100)
  fix           the patched driver, which drops every other CSR index on the edge table before its clock starts,
                with the extension build of branch bidirectional-single-pair (1e594d52). That extension differs from
                rc13 (index WITH (reverse = true), rebuilt by the driver before the clock, which also reads the
                table), so only its first commit is to be compared with the rc13 rows.

  run                 first commit   commit median   queries   query median   wall     peak RSS
  sf300 rc13_asis     15.45 s        152 ms          3032      84 ms          36.6 s   5996 MiB
  sf300 rc13_dropped   1.39 s        115 ms          2869      85 ms          21.1 s   2378 MiB
  sf300 fix            0.08 s         79 ms          2909      63 ms          17.8 s   4358 MiB
  sf100 rc13_asis     13.48 s        161 ms          4436      46 ms          35.6 s   4625 MiB
  sf100 rc13_dropped   8.54 s        137 ms          4280      44 ms          29.7 s   3398 MiB
  sf100 fix            0.04 s         51 ms          5749      25 ms          16.1 s   2022 MiB

The frozen run (rc13, 6 Nov) has the effect at SF100, where run_all.sh runs the microbenchmark before the concurrent
step: first commit 9.25 s in repro/results/sf100/concurrent_sssp_pairs1024_threads32_readers16_batch2000x100_*.csv.
At SF300 it ran no row-bucket configurations and its first commit took 1.03 s. Query and commit medians of the frozen
run are little affected (rc13 at SF300: query median 84 against 85 ms, commit median 152 against 115 ms); the peak
RSS column of the concurrent rows includes the other indexes (5996 against 2378 MiB at SF300).
