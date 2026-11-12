# usage: summary.py <microbench csv>...: median ms per k for the single-source kernel and the lane waves, and the ratio
import csv, sys, statistics as st, collections
for f in sys.argv[1:]:
    rows = list(csv.DictReader(open(f)))
    d = collections.defaultdict(list); strat = {}
    for r in rows:
        k = int(r['sources']) if r['shape'] == 'all_pairs' else int(r['pairs'])
        key = (r['shape'], k, r['config'])
        d[key].append(float(r['query_s']) * 1000 if r['status'] == 'ok' else float('inf'))
        strat[key] = r['strategy']
    print('==', f.split('/')[-1], '|', rows[0]['benchmark_notes'], '|', rows[0]['repo_commit'][:8])
    for shape in ('random_pairs', 'all_pairs'):
        ks = sorted({k[1] for k in d if k[0] == shape})
        if not ks: continue
        print(' %s%s' % (shape, ' (k sources x 200 destinations)' if shape == 'all_pairs' else ' (k pairs)'))
        print('  %6s %14s %12s %28s %8s  %s' % ('k', 'single-source', 'lane waves', 'lanes vs single-source', 'repeats', 'kernels chosen'))
        for k in ks:
            s = d.get((shape, k, 'single_source_kernel')); l = d.get((shape, k, 'multi_source_dedup'))
            if not s or not l: continue
            ms, ml = st.median(s), st.median(l)
            word = '%.2fx faster' % (ms / ml) if ml < ms else '%.2fx slower' % (ml / ms)
            kern = lambda x: dict(p.split('=', 1) for p in x.split(';') if '=' in p).get('source_group_kernel', '?')
            print('  %6d %14.1f %12.1f %28s %8s  %s / %s' % (k, ms, ml, 'lanes ' + word, '%d/%d' % (len(s), len(l)), kern(strat[(shape, k, 'single_source_kernel')]), kern(strat[(shape, k, 'multi_source_dedup')])))
