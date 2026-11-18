"""summ_policies.py <directory>: one line per concurrent run of the fold policy benchmark."""
import csv
import glob
import re
import statistics
import sys


def p99(values):
    values = sorted(values)
    return values[int(0.99 * (len(values) - 1))]


rows_out = []
for path in sorted(glob.glob(sys.argv[1].rstrip('/') + '/concurrent_*policy_*.csv')):
    rows = list(csv.DictReader(open(path)))
    if 'scale_factor' not in rows[0]:
        continue
    sf = int(rows[0]['scale_factor'])
    tag = re.search(r'_policy_([a-z]+)_\d+\.csv', path).group(1)
    ratio = rows[0]['fold_ratio']
    name = {'full': 'full fold at a tenth', 'merge': f'merge + fast path at {ratio}', 'cost': 'merge + fast path, by cost',
            'fullcost': 'full fold, by cost'}[tag]
    q = [float(r['elapsed_s']) * 1000 for r in rows if r['kind'] == 'query_index']
    c = [float(r['elapsed_s']) * 1000 for r in rows if r['kind'] == 'commit']
    ends = [float(r['start_s']) + float(r['elapsed_s']) for r in rows if r['kind'] in ('query_index', 'commit')]
    wall = max(ends)
    folds = {}
    for r in rows:
        if r['kind'] == 'query_index':
            st = dict(i.split('=') for i in r['stats'].split(';') if '=' in i)
            folds[int(st.get('folds', 0))] = float(st.get('last_fold_ms', 0))
    fold_ms = [v for k, v in sorted(folds.items()) if k > 0]
    rows_out.append((sf, ['full', 'merge', 'cost', 'fullcost'].index(tag), -float(ratio), name, len(q), wall, len(q) / wall,
                     statistics.median(q), p99(q), max(q), statistics.median(c), p99(c), max(folds) if folds else 0,
                     statistics.median(fold_ms) if fold_ms else 0, max(fold_ms) if fold_ms else 0,
                     float(rows[0]['peak_rss_mib']) / 1024))
print('sf  | policy                           | queries | wall s | q/s    | q med | q p99 | q max  | c med | c p99 | folds | fold med | fold max | rss GiB')
for r in sorted(rows_out):
    print(f"{r[0]:3} | {r[3]:32} | {r[4]:7} | {r[5]:6.1f} | {r[6]:6.0f} | {r[7]:5.1f} | {r[8]:5.0f} | {r[9]:6.0f} | {r[10]:5.0f} | {r[11]:5.0f} | {r[12]:5} | {r[13]:8.0f} | {r[14]:8.0f} | {r[15]:5.1f}")
