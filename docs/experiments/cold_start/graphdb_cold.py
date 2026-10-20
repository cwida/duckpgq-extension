import importlib, shutil, sys, time, os
engine, phase, path = sys.argv[1], sys.argv[2], sys.argv[3]
mod = importlib.import_module(engine)
THREADS = 18
def t():
    return time.perf_counter()
if phase == "import":
    if os.path.isdir(path): shutil.rmtree(path)
    elif os.path.exists(path): os.remove(path)
    for extra in (path + ".wal", path + ".lock"):
        if os.path.exists(extra): os.remove(extra)
    s = t(); db = mod.Database(path); conn = mod.Connection(db, num_threads=THREADS)
    conn.execute("CREATE NODE TABLE V(id INT64 PRIMARY KEY)")
    conn.execute("CREATE REL TABLE E(FROM V TO V)")
    s1 = t(); conn.execute("COPY V FROM (LOAD FROM '/tmp/pf_v.parquet' RETURN id)"); s2 = t()
    conn.execute("COPY E FROM (LOAD FROM '/tmp/pf_e.parquet' RETURN src, dst)"); s3 = t()
    print(f"{engine} {mod.__version__}: schema+open {s1-s:.3f} s, copy vertices {s2-s1:.3f} s, copy edges {s3-s2:.3f} s, total import {s3-s:.3f} s")
    del conn; del db
else:
    s = t(); db = mod.Database(path, read_only=True); conn = mod.Connection(db, num_threads=THREADS); s1 = t()
    q1 = "MATCH (s:V {id: 7})-[e:E* SHORTEST 1..8]->(d:V {id: 1999999}) RETURN count(e)"
    r = conn.execute(q1); rows = r.get_all(); s2 = t()
    r = conn.execute(q1); rows = r.get_all(); s3 = t()
    q2 = "MATCH (s:V)-[e:E* SHORTEST 1..8]->(d:V) WHERE s.id < 20 AND d.id >= 1999800 RETURN count(*), sum(length(e))"
    r = conn.execute(q2); rows2 = r.get_all(); s4 = t()
    r = conn.execute(q2); rows2 = r.get_all(); s5 = t()
    print(f"{engine} {mod.__version__}: open {s1-s:.3f} s | 1-pair first {s2-s1:.3f} s, second {s3-s2:.3f} s ({rows[0][0]} paths) | 20-source first {s4-s3:.3f} s, second {s5-s4:.3f} s ({rows2[0]})")
