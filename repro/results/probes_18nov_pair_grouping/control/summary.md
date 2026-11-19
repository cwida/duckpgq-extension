## sf300: 65536 pairs, 65536 sources, 1193579 vertices; arrival order: 0.503 of adjacent rows have ascending sources
results (pairs, reachable, sum of lengths): [('65536', '31747', '122902')]
| ordering | median ms | min-max | arrival / ordering |
|---|---|---|---|
| arrival | 10182 | 10139-10200 | 1.00 |
| src | 6774 | 6658-6810 | 1.50 |
| dst | 9933 | 9913-10179 | 1.03 |
| hash | 10254 | 10227-10714 | 0.99 |
| win1024 | 6752 | 6714-6975 | 1.51 |
| win4096 | 7059 | 7013-7171 | 1.44 |
| win16384 | 7750 | 7634-7838 | 1.31 |
| waves_shuf | 6729 | 6685-6831 | 1.51 |

waves under the lane-activity switch (untimed runs; the switch itself adds a scan per level)
| ordering | waves | levels per wave: median (min-max) | active vertices summed over a wave's levels: median | lane frontiers summed over a wave: median | active lanes per level: mean |
|---|---|---|---|---|---|
| arrival | 256 | 19 (15-26) | 4,074,689 | 115,961,260 | 110 |
| hash | 256 | 19 (15-26) | 4,071,998 | 115,808,964 | 111 |
| src | 256 | 17 (14-26) | 2,634,224 | 125,262,094 | 124 |

per level, mean over the waves that reach it: waves, active lanes, active vertices
  arrival: L1: 256w 256l 256v | L2: 256w 256l 15,761v | L3: 256w 256l 589,936v | L4: 256w 254l 1,086,143v | L5: 256w 224l 1,108,878v | L6: 256w 146l 917,986v | L7: 256w 132l 270,919v | L8: 256w 131l 57,189v | L9: 256w 130l 17,274v | L10: 256w 123l 6,477v | L11: 256w 101l 2,642v | L12: 256w 68l 1,116v | L13: 256w 38l 476v | L14: 256w 18l 208v | L15: 256w 9l 97v | L16: 254w 4l 48v | L17: 232w 3l 25v | L18: 196w 2l 16v | L19: 153w 2l 10v | L20: 114w 1l 8v | L21: 83w 1l 6v | L22: 55w 1l 5v | L23: 40w 1l 4v | L24: 20w 1l 3v | L25: 9w 1l 2v | L26: 3w 1l 1v
  hash: L1: 256w 256l 256v | L2: 256w 256l 15,758v | L3: 256w 256l 590,014v | L4: 256w 254l 1,086,316v | L5: 256w 224l 1,108,950v | L6: 256w 146l 916,411v | L7: 256w 132l 270,767v | L8: 256w 131l 57,162v | L9: 256w 130l 17,261v | L10: 256w 123l 6,475v | L11: 256w 101l 2,643v | L12: 256w 68l 1,118v | L13: 256w 38l 477v | L14: 256w 18l 208v | L15: 256w 9l 97v | L16: 248w 5l 49v | L17: 236w 3l 25v | L18: 201w 2l 15v | L19: 149w 2l 11v | L20: 104w 1l 8v | L21: 74w 1l 7v | L22: 53w 1l 5v | L23: 39w 1l 4v | L24: 19w 1l 3v | L25: 9w 1l 2v | L26: 3w 1l 1v
  src: L1: 256w 256l 256v | L2: 256w 256l 14,540v | L3: 256w 256l 414,772v | L4: 256w 254l 618,918v | L5: 256w 224l 621,085v | L6: 256w 146l 595,536v | L7: 256w 132l 182,520v | L8: 256w 131l 41,453v | L9: 256w 130l 13,264v | L10: 256w 123l 5,102v | L11: 256w 101l 2,081v | L12: 256w 68l 863v | L13: 256w 38l 358v | L14: 256w 18l 151v | L15: 249w 9l 71v | L16: 206w 5l 43v | L17: 146w 4l 30v | L18: 101w 4l 24v | L19: 60w 4l 22v | L20: 37w 4l 21v | L21: 23w 4l 20v | L22: 19w 3l 13v | L23: 14w 3l 10v | L24: 9w 2l 6v | L25: 5w 2l 3v | L26: 3w 1l 1v

per decile of the source's rank: decile, pairs, reachable, mean length, max length
  int64 int64 int64 double int64
  0 6554 5541 3.78 7
  1 6554 5080 3.82 9
  2 6553 4592 3.86 8
  3 6554 4173 3.86 7
  4 6553 3450 3.89 7
  5 6554 2965 3.93 11
  6 6554 2467 3.95 9
  7 6553 1857 3.96 7
  8 6554 1142 3.98 7
  9 6553 480 4.15 8

## sf300_undirected: 16384 pairs, 16384 sources, 1193579 vertices; arrival order: 0.491 of adjacent rows have ascending sources
results (pairs, reachable, sum of lengths): [('16384', '16384', '56183')]
| ordering | median ms | min-max | arrival / ordering |
|---|---|---|---|
| arrival | 2700 | 2671-2707 | 1.00 |
| src | 2672 | 2616-2677 | 1.01 |
| hash | 2679 | 2650-2699 | 1.01 |

## sf100: 65536 pairs, 65536 sources, 473001 vertices; arrival order: 0.529 of adjacent rows have ascending sources
results (pairs, reachable, sum of lengths): [('65536', '31274', '118174')]
| ordering | median ms | min-max | arrival / ordering |
|---|---|---|---|
| arrival | 3409 | 3401-3416 | 1.00 |
| src | 2293 | 2277-2296 | 1.49 |
| hash | 3431 | 3403-3564 | 0.99 |

## graph500_24: 2048 pairs, 2048 sources, 8870942 vertices; arrival order: 0.417 of adjacent rows have ascending sources
results (pairs, reachable, sum of lengths): [('2048', '2043', '6991')]
| ordering | median ms | min-max | arrival / ordering |
|---|---|---|---|
| arrival | 5956 | 5860-6082 | 1.00 |
| src | 5897 | 5804-5952 | 1.01 |
| hash | 5878 | 5831-6043 | 1.01 |

