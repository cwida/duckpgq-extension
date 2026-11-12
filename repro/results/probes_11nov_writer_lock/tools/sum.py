import csv,sys,statistics as st,collections
for f in sys.argv[1:]:
    d=collections.defaultdict(list)
    for r in csv.DictReader(open(f)): d[r['kind']].append(float(r['elapsed_s'])*1000)
    print(f.split('/')[-1],' '.join('%s n=%d med=%.1f p90=%.1f'%(k,len(v),st.median(v),sorted(v)[int(.9*(len(v)-1))]) for k,v in sorted(d.items())))
