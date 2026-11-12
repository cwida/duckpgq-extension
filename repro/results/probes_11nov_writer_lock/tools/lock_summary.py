# usage: lock_summary.py <locks.csv> <driver.csv>: exclusive StorageLock waits inside each writer COMMIT statement
import csv, sys, statistics as st, collections, subprocess, bisect
def dem(s):
    return subprocess.run(['c++filt', s], capture_output=True, text=True).stdout.strip().split('(')[0]
ev = list(csv.DictReader(open(sys.argv[1])))
ex = sorted((int(r['start_ns']), int(r['wait_ns']), r['caller'], r['tid']) for r in ev if r['kind'] == 'exclusive')
starts = [e[0] for e in ex]
rows = list(csv.DictReader(open(sys.argv[2])))
com = [(int(r['stats'].split('=')[1]), float(r['elapsed_s']) * 1e9) for r in rows if r['kind'] == 'w_commit']
print('exclusive acquisitions in the run: %d; writer commits: %d' % (len(ex), len(com)))
by = collections.defaultdict(lambda: [0, 0]); per = []; n_in = 0
for s, d in com:
    inside = ex[bisect.bisect_left(starts, s):bisect.bisect_right(starts, s + d)]
    n_in += len(inside)
    per.append((d / 1e6, sum(e[1] for e in inside) / 1e6, len(inside)))
    for e in inside:
        by[e[2]][0] += 1; by[e[2]][1] += e[1]
med = lambda i: st.median(p[i] for p in per)
print('COMMIT statement: median %.1f ms' % med(0))
print('exclusive-lock wait inside it: median %.1f ms per commit, %.0f%% of all COMMIT time' % (med(1), 100 * sum(p[1] for p in per) / sum(p[0] for p in per)))
print('acquisitions per commit: median %d (min %d, max %d)' % (med(2), min(p[2] for p in per), max(p[2] for p in per)))
print('by caller (acquisitions inside commits, total wait):')
for c, (n, w) in sorted(by.items(), key=lambda x: -x[1][1]):
    print('  %7d  %9.0f ms  %s' % (n, w / 1e6, dem(c)))
print('acquisitions outside the COMMIT statements: %d' % (len(ex) - n_in))
w = sorted(e[1] / 1e6 for e in ex)
print('single acquisition wait: median %.3f ms, p90 %.3f, p99 %.3f, max %.1f; acquisitions that waited over 1 ms: %d' % (st.median(w), w[int(.9 * len(w))], w[int(.99 * len(w))], w[-1], sum(1 for x in w if x > 1)))
print('per-commit count of acquisitions that waited over 1 ms: median %d' % st.median(sum(1 for e in ex[bisect.bisect_left(starts, s):bisect.bisect_right(starts, s + d)] if e[1] > 1e6) for s, d in com))
print('shared acquisitions by caller (count, total wait, longest):')
for r in sorted((r for r in ev if r['kind'] == 'shared_sum'), key=lambda r: -int(r['wait_ns']))[:6]:
    print('  %7s  %9.0f ms  %7.1f ms  %s' % (r['start_ns'], int(r['wait_ns']) / 1e6, int(r['tid']) / 1e6, dem(r['caller'])))
