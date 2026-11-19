#!/usr/bin/env python3
"""reduce_activity.py <directory with <variant>/w_lane_activity_<run>.csv> <output csv>

One row per (variant, wave, level): lanes with a non-empty frontier, vertices with any active lane (lane -1 of the
operator's file), the sum of the lanes' frontiers, and the largest single frontier. The wave's time comes from the
bfs_* row of the variant's phase file with the same run id.
"""
import csv
import glob
import os
import sys

root, out = sys.argv[1], sys.argv[2]
with open(out, "w", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow(["variant", "wave", "wave_ms", "level", "active_lanes", "active_vertices", "lane_frontier_sum",
                     "lane_frontier_max"])
    for variant in sorted(os.listdir(root)):
        wave_ms = {}
        phase_file = os.path.join(root, variant, "w_phase_timing.csv")
        if os.path.exists(phase_file):
            for row in csv.DictReader(open(phase_file)):
                if row["Phase"].startswith("bfs"):
                    wave_ms[row["RunID"]] = row["Time_ms"]
        for path in sorted(glob.glob(os.path.join(root, variant, "w_lane_activity_*.csv"))):
            run = os.path.basename(path)[len("w_lane_activity_"):-len(".csv")]
            levels = {}
            for row in csv.DictReader(open(path)):
                level = levels.setdefault(int(row["Iter"]), [0, 0, 0, 0])
                lane, count = int(row["Lane"]), int(row["ActiveVertices"])
                if lane < 0:
                    level[1] = count
                elif count:
                    level[0] += 1
                    level[2] += count
                    level[3] = max(level[3], count)
            for level in sorted(levels):
                writer.writerow([variant, run, wave_ms.get(run, ""), level] + levels[level])
