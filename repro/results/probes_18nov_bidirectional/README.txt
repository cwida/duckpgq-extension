One pair searched from both ends (my-beans-zlnm, branch bidirectional-single-pair), 18 Nov 2026. Measurements only;
nothing in the sweep archive changed, and Neo4j and Kuzu were not rerun (their figures below are the frozen run's).

What is measured. A source group with exactly one destination is searched from both ends (BidirectionalPairSearch:
forward from the source, backward from the destination over the index's reverse side, the smaller frontier first,
stop at the first vertex both sides have seen). It needs a CSR index WITH (reverse = true) on a directed graph; the
frozen sweep's index has no reverse side, so its pairs keep the search from the source.

Setup. 32 threads, index route, the branch's DuckDB CLI (45f4fdf3) and extension, private copies of the benchmark
databases (the reverse index and the pair tables are created in the copies). Machine quiet for every timed run
(slots given by the session that sequences the box). The files at the top level are the final run, 12:24 to 12:30
UTC, commit 2c379d44. Three configurations of the harness's microbench (group `bidirectional`):
  baseline               the directed index of the frozen run (no reverse side): search from the source
  reverse_index          index WITH (reverse = true): search from both ends
  reverse_index_one_end  the same index, experimental_path_finding_operator_bidirectional_pair = false
and the rc13 extension on the directed index (microbench_rc13_*).

1. The sweep's pair (pairs=1 of random_pairs) and 16 pairs with distinct sources, harness microbench, median of 5
   repeats, ms (the CLI timer has a resolution of 1 ms). Every configuration returned the same reachable count and
   length sum.

                         one pair                                   16 pairs, distinct sources
   scale  pair           source   both ends            rc13         source   both ends           rc13
   SF10   4 hops         3        1  (3 times faster)   3           8        4  (2.0 x faster)    7
   SF100  unreachable    15       3  (5 times faster)   15          56       17 (3.3 x faster)    59
   SF300  5 hops         23       3  (7.7 x faster)     23          146      61 (2.4 x faster)    144
   ("source" is reverse_index_one_end; baseline on the directed index measured 3, 15, 23 and 8, 60, 129.)

   Frozen run (eval-freeze-rc13, repro/results/sweep_pairs_table.txt), one pair: duckpgq_index 3 / 15 / 22 ms,
   Neo4j 12 / 15 / 4 ms, Kuzu 7 / 36 / 52 ms at SF10 / SF100 / SF300. With the search from both ends DuckPGQ's one
   pair is 1 / 3 / 3 ms: 12, 5 and 1.3 times faster than Neo4j, 7, 12 and 17 times faster than Kuzu. The baselines'
   times include their driver's round trip, DuckPGQ's are the CLI's statement time, as in the sweep.
   The SF300 search: 5 levels over both sides, 8866 vertices discovered, no sweep (strategy keys
   bidirectional_levels, bidirectional_discovered, bidirectional_sweeps).

2. 200 single pairs, each as its own query (tools/probe_pairs.py: sources with an outgoing edge and destinations
   with an incoming one, ordered by a hash of the person id; 5 runs per pair and mode, alternating; the time is
   between statement timestamps, microseconds, and includes about 0.3 ms of CLI work per statement). Median over
   the pairs of each pair's median, ms. The lengths of both searches agree on all 200 pairs at every scale, for
   lengths and for paths (the path's length).

   lengths              reachable  median               mean          p90           max           both ends slower
   scale                pairs      source  both ends    src   both    src   both    src   both    than source
   SF10                 85         2.45    1.53         2.50  1.69    3.39  2.42    3.89  2.89    4 of 200
   SF100                82         8.30    2.39         8.53  3.57    14.2  6.63    15.9  8.81    4 of 200
   SF300                96         14.07   2.48         14.5  5.77    22.8  13.7    28.0  19.0    4 of 200

   paths
   SF10                 85         2.70    1.70         2.73  1.92    3.57  2.79    4.08  3.46    12 of 200
   SF100                82         14.22   2.86         14.6  4.33    20.2  8.13    21.5  11.0    0 of 200
   SF300                96         29.43   3.15         29.7  7.19    38.0  17.9    44.8  24.5    0 of 200

   By outcome at SF300, lengths, median ms: 3 hops 1.52 against 8.34 from the source, 4 hops 1.70 against 14.48,
   5 hops 2.00 against 15.26; unreachable 8.95 against 14.80. A reachable pair costs about 1.2 ms of fixed query
   cost plus well under 1 ms of search. An unreachable pair is the weak case: when the forward side of the source
   and the backward side of the destination are both large, both are explored to the end (the pairs that are
   slower than the search from the source are of this kind). About half of the probe's pairs are unreachable in
   the directed knows graph.

3. The sweep threshold (sweep_threshold/, tools/run_sweep_threshold.sh): a level is swept over the partitions when
   its frontier holds at least 1 / sparse_frontier_divisor of the vertices. SF300, both ends:

   divisor                 200 single pairs: mean / max, ms    unreachable, median ms    16 pairs, ms
   64 (default)            5.81 / 19.4                         9.00                      58
   8                       5.50 / 20.3                         7.15                      142
   1 (never sweeps)        5.70 / 26.0                         7.13                      191
   0 (sweeps every level)  8.07 / 23.1                         12.60                     85

   The default stays at 64: a search that has all 32 threads loses 2 ms on a median unreachable pair to the sweep,
   16 searches with 2 workers each gain a factor of 2.4 to 3.3. The setting also moves the threshold of the search
   from the source, so the "one end" columns of these files do not compare across divisors.

4. Cost of the reverse side: peak RSS of the one-pair query 1241 MiB with the directed index and 1569 MiB with the
   index WITH (reverse = true) at SF300 (455 and 552 MiB at SF100, 103 and 114 MiB at SF10).

Earlier versions, kept for the record:
  v1/  first version (commit 89cb0500): lookups only, per-vertex parent arrays. Paths of a 4-hop pair at SF300 took
       9.9 ms (two fresh 12-byte-per-vertex arrays, a page fault per discovery; now a log of the discoveries, 2.0
       ms); 16 pairs at SF300 took 180 ms, slower than the 140 ms from the source.
  v2/  second version (commit 4229d09c): the sweep with one task
       per partition and a stamp array; unreachable single pairs at SF300 10.4 ms median. Its sweep_threshold/
       files use other divisor labels: 2000000000 sweeps every level (as 0 does), 2 sweeps from half the vertices.

Files: microbench_sf*.out and microbench_sf*_threads32_*.csv (harness output and rows; the second CSV per scale is
the rc13 extension), pairs_lengths_sf*.{out,csv} and pairs_paths_sf*.{out,csv} (summary and one row per pair with
every run), run.log (start and end of every step), tools/ (run.sh is the whole command). The CSVs are kept out of
repro/results/sf10, sf100 and sf300 so that no table printer takes them for a newer run of the sweep.
