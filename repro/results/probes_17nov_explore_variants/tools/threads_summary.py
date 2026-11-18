"""Thread sweep of the explore variants: median seconds per variant and thread count, then each flat variant's time
divided by the partitioned explore's at the same thread count (above 1: the flat variant is that many times slower).

usage: threads_summary.py <microbench csv of --configs explore_threads>
"""
import csv
import statistics
import sys

THREADS = (1, 2, 4, 8, 16, 32)
VARIANTS = ["partitioned", "flat_serial", "flat_ranges", "flat_atomic", "flat_locked"]


def main():
    rows = [r for r in csv.DictReader(open(sys.argv[1])) if r["status"] == "ok"]
    samples = {}
    points = []
    for r in rows:
        config = r["config"]
        if config == "baseline":
            variant, threads = "partitioned", 32
        else:
            name = config[len("explore_"):]
            head, _, tail = name.rpartition("_t")
            variant, threads = (head, int(tail)) if head and tail.isdigit() else (name, 32)
        point = f"{r['pairs']} pairs" if r["shape"] == "random_pairs" else f"{r['sources']} sources x all"
        if point not in points:
            points.append(point)
        samples.setdefault((point, variant, threads), []).append(float(r["query_s"]))

    def median(point, variant, threads):
        return statistics.median(samples[(point, variant, threads)])

    for point in points:
        print(f"\n{point}: median seconds")
        print(f"  {'threads':22s}" + "".join(f"{t:>9d}" for t in THREADS))
        for variant in VARIANTS:
            print(f"  {variant:22s}" + "".join(f"{median(point, variant, t):9.3f}" for t in THREADS))
        print("  times slower than partitioned at the same thread count")
        for variant in VARIANTS[1:]:
            print(f"  {variant:22s}" + "".join(
                f"{median(point, variant, t) / median(point, 'partitioned', t):9.2f}" for t in THREADS))
        print("  speed-up over the variant's own single-thread time")
        for variant in VARIANTS:
            print(f"  {variant:22s}" + "".join(
                f"{median(point, variant, 1) / median(point, variant, t):9.2f}" for t in THREADS))


main()
