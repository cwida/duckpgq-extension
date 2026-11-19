wall time, median of 5 runs, ms (min-max)
| pair table | operator | distinct_below | sorted_below | scan_only | aggregate_src_dst | aggregate_src_list_dst | sort_src_dst |
|---|---|---|---|---|---|---|---|
| 1 | 23 (19-24) | 20 (19-30) | 19 (17-19) | 1 (1-1) | 1 (1-1) | 1 (1-1) | 1 (1-1) |
| 1024 | 205 (197-208) | 232 (202-246) | 162 (159-195) | 1 (1-1) | 1 (1-18) | 1 (1-1) | 1 (1-1) |
| 65536 | 10087 (9980-10205) | 10099 (9985-10139) | 6641 (6590-6753) | 1 (1-1) | 4 (3-59) | 7 (5-9) | 8 (8-8) |
| 65536_twice | 10158 (10111-10483) | 10126 (10033-10450) | 6764 (6572-6774) | 2 (1-2) | 5 (4-6) | 7 (7-8) | 17 (16-18) |
| all_pairs_s1024_t200 | 265 (240-293) | 303 (294-306) | 304 (293-318) | 2 (1-2) | 6 (6-18) | 4 (3-9) | 21 (20-22) |

operator phases, median of the runs, ms (source_group_scatter is summed over the threads)
| pair table | variant | pairs into operator | groups | pair_analysis | source_group_zero_copy | source_group_build | source_group_scatter |
|---|---|---|---|---|---|---|---|
| 1024 | distinct_below | 1024 | 1024 | 20.29 |  | 0.10 | 0.29 |
| 1024 | operator | 1024 | 1024 | 0.50 |  | 0.09 | 0.18 |
| 1024 | sorted_below | 1024 | 1024 | 0.48 |  | 0.09 | 0.16 |
| 1 | distinct_below | 1 | 1 | 0.02 | 0.00 |  | 0.01 |
| 1 | operator | 1 | 1 | 0.02 | 0.00 |  | 0.01 |
| 1 | sorted_below | 1 | 1 | 0.02 | 0.00 |  | 0.01 |
| 65536 | distinct_below | 65536 | 65536 | 109.14 |  | 3.87 | 9.47 |
| 65536 | operator | 65536 | 65536 | 84.11 |  | 4.30 | 9.55 |
| 65536 | sorted_below | 65536 | 65536 | 79.71 |  | 2.92 | 8.04 |
| 65536_twice | distinct_below | 65536 | 65536 | 122.11 |  | 3.84 | 9.10 |
| 65536_twice | operator | 131072 | 65536 | 125.67 |  | 5.76 | 16.75 |
| 65536_twice | sorted_below | 131072 | 65536 | 85.66 |  | 3.38 | 10.51 |
| all_pairs_s1024_t200 | distinct_below | 204800 | 1024 | 3.71 |  | 2.62 | 27.52 |
| all_pairs_s1024_t200 | operator | 204800 | 1024 | 25.97 |  | 2.14 | 25.34 |
| all_pairs_s1024_t200 | sorted_below | 204800 | 1024 | 14.47 |  | 1.50 | 5.76 |
