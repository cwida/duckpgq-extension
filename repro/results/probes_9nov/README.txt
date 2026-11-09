Post-sweep checks of 9 Nov 2026 on the frozen build eval-freeze-rc13 (cf4d88ce); measurements only, nothing in the
sweep archive changed. Details and numbers: docs/PATHFINDING_PAPER_PLAN.md, entry "9 Nov (2)".
- sf100/oracle, sf300/oracle: both-routes correctness check (update-concurrent --routes both) at SF100 and SF300 with
  16 readers under the writer, 20 commits of 2000 changes, 200 ms pause, fold ratio 0.1, and SF300 with 0.0005 (one
  fold under the comparison). All mismatches=0 errors=0. Kept out of the sf*/ top level so the concurrent-table
  printer does not take them for the sweep's 16-reader rows.
- q1q2: phase-column stats of 6 Nov (65536 pairs and a single pair at SF300, both routes).
- perf_single_pair_sf300_top_symbols.txt: perf profile of 60 warm single-pair queries at SF300 on the index route.
- rowid_index.txt, load_after_restart.txt: the row-id-keyed index comparison and the index load after a restart.
