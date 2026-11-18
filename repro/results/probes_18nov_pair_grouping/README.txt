Pair grouping in the operator against DuckDB's own operators (my-beans-ij12, 18 Nov 2026)

Question (Peter Boncz, 17 Nov): let DuckDB's operators group the (source, destination) pairs instead of the
operator's own code. Would that be simpler or faster?

What the operator does today (src/core/operator/physical_path_finding_operator.cpp, read at 8faa9659):
  pair_analysis          FinalizePathFindingPhase: the sunk pairs are copied out of the collection by a parallel scan,
                         keys are translated to vertex ids (index route), and two HyperLogLog sketches estimate the
                         distinct sources and destinations (they choose the orientation, forward or reverse)
  source_group_build     FinalizeSourceGroupedPathFindingPhase: one thread, one std::unordered_map from source id to
                         group number, one group number per input row
  (schedule event)       every row's destination is registered with its group (a set per group, so a duplicate pair
                         adds nothing); the groups, in order of first appearance, are cut into waves of 256 lanes
  source_group_scatter   GetSourceGroupedData: every input row, in input order with its pass-through columns, reads
                         the distance of its destination from its group

Run: tools/run.sh <checkout> <out> 300 5, then tools/summarize.py <out> 300. One CLI process, rc13 CLI and
extension (sha256 06240e53...), SF300 database attached read-only, index route, 32 threads, quiet machine (slot
from the resource monitor session), 5 runs per statement interleaved; the phase rows come from 3 further runs with
the benchmark switch on. Files: probe_sf300.sql, probe_sf300.out, phases/, summary_sf300.md.

Pair tables: 1, 1024 and 65,536 random pairs (every source distinct); 65536_twice, the 65,536 pairs with every
pair twice; all_pairs_s1024_t200, 1024 sources with 200 destinations each (204,800 pairs).
Statements: operator, the harness's query; distinct_below, the operator over SELECT DISTINCT src, dst, joined back
to the pairs on (src, dst); sorted_below, the operator over the pairs ORDER BY src, dst; and without the operator
the scan, GROUP BY src, dst, GROUP BY src with list(dst), and ORDER BY src, dst. All three operator statements
return the same count, reachable count and sum of lengths per table.

Wall time, median of 5, ms:
  pair table              operator   distinct_below   sorted_below   GROUP BY src,dst   GROUP BY src   ORDER BY
  1                             23               20             19                  1              1          1
  1024                         205              232            162                  1              1          1
  65536                     10,087           10,099          6,641                  4              7          8
  65536_twice               10,158           10,126          6,764                  5              7         17
  all_pairs_s1024_t200         265              303            304                  6              4         21

Operator phases at 65,536 pairs, ms: pair_analysis 84, source_group_build 4.3, source_group_scatter 9.5 (summed
over the threads), of a query of 10,087. At 204,800 pairs in 1024 groups: 26, 2.1 and 25 of 265.

Findings
1. The grouping is not a cost. The operator's map takes 4.3 ms for 65,536 pairs; DuckDB's hash aggregate takes 4 ms
   on (src, dst) and 7 ms on src with list(dst), its sort 8 ms. Replacing one by the other moves nothing in a query
   of 10 s. At one pair every statement is at the timer's resolution (1 ms) and the three operator statements take
   19 to 23 ms: a planned operator adds no measurable overhead there.
2. Deduplication planned below the operator gains nothing: with every pair twice the operator takes 10,158 ms and
   distinct_below 10,126 ms, because a group already holds its destinations as a set. On 1024 pairs and on
   1024 x 200 the join back makes the query 1.13 and 1.14 times slower (232 against 205 ms, 303 against 265 ms).
3. The order of the pairs is a cost. Sorted by source below the operator, 65,536 random pairs take 6,641 ms
   against 10,087 ms (1.52 times faster) and 1024 pairs 162 against 205 ms (1.27 times faster); 1024 x 200 is 1.15
   times slower (304 against 265 ms; its sort alone takes 21 ms). The time is in the waves: the median wave takes
   760 ms sorted against 1,192 ms unsorted (768 wave rows per variant, 3 runs of 256 waves). Not established: why.
   The sorted input changes which sources share a wave (256 consecutive vertex ids instead of 256 in arrival
   order); the probe does not separate that from any other effect of the order, and it is one graph.

Control for finding 3 (tools/control.sh, tools/summarize_control.py, files under control/, summary in
control/summary.md). Same binaries, quiet machine, 3 runs per ordering interleaved, every ordering returns the same
result. The orderings sort the pairs below the operator; arrival is the table as the harness builds it.

  SF300, directed index, 65,536 pairs (256 waves)        median ms   arrival / ordering
  arrival (0.503 of adjacent rows have ascending sources)   10,182        1.00
  by hash(source), a random order                           10,254        0.99 (slower)
  by destination                                             9,933        1.03 (faster)
  by source                                                  6,774        1.50 (faster)
  the waves of "by source" in random order                   6,729        1.51 (faster)
  random within windows of 1024 consecutive sources          6,752        1.51 (faster)
  random within windows of 4096                              7,059        1.44 (faster)
  random within windows of 16,384                            7,750        1.31 (faster)

  other graphs, arrival / by source / by hash, median ms
  SF100, directed index, 65,536 pairs             3,409 / 2,293 / 3,431   by source 1.49 times faster
  SF300, undirected index, 16,384 pairs           2,700 / 2,672 / 2,679   no difference (1.01)
  graph500-24, 2048 pairs (8 waves)               5,956 / 5,897 / 5,878   no difference (1.01)

Lane activity at SF300 (one untimed run per ordering with experimental_path_finding_operator_benchmark_lane_activity,
control/activity_sf300.csv: one row per ordering, wave and level):
                                                              arrival      by hash     by source
  levels per wave, median                                          19           19            17
  lane frontiers summed over a wave's levels, median      115,961,260  115,808,964   125,262,094
  vertices with any active lane, summed over the levels     4,074,689    4,071,998     2,634,224
  the same at level 4 / level 5, mean (of 1,193,579)   1,086K / 1,109K  1,086K / 1,109K  619K / 621K

Cause. The arrival order is a random order. What matters is which sources share a wave, not the order of the waves
or of the destinations. Every knows edge has person1id < person2id (checked at SF10: 1,839,354 of 1,839,354), so
the directed graph is acyclic along the person id and a search reaches only persons with a larger id than its
source (reachable pairs fall from 85% in the lowest decile of the source's rank to 7% in the highest). 256 sources
with neighbouring ids search the same part of the graph: their lanes do the same work (lane frontiers 125M against
116M) on 1.55 times fewer vertices with any active lane (2.63M against 4.07M per wave), and the wave's time follows
that count (1.50 times faster). A wave of random sources has an active lane on 93% of the vertices at its widest
level, a wave of neighbouring sources on 52%. Where the ids carry no such structure (the undirected index, graph500)
the order changes nothing.

Not measured: an in-operator ordering of the groups before the waves are cut (the same wave composition without a
sort of the pairs); cit-Patents and datagen-8_4-fb; 65,536 pairs on the undirected index and on graph500 (the
control used 16,384 and 2048); why 1024 sources x 200 destinations were 1.15 times slower sorted in the first run
(the sort of 204,800 pairs takes 21 of the 39 ms).
