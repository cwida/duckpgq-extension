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

Not measured: sorting by source only, by destination, or by a hash (the control that would show whether wave
composition is the cause); SF100 and the Graphalytics graphs; an in-operator sort of the groups before the waves
are cut.
