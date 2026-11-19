"""lock_owners.py <stack dump>...: for every thread blocked on a mutex, which thread owns it and where that one is.
The dumps come from gdb: 'info threads', 'thread apply all bt 30', 'thread apply all p *(int*)($rdi+8)' (the owner
field of the mutex a thread in __lll_lock_wait is waiting for)."""
import re
import sys


def duck(frames, n=5):
    return ' < '.join(f.replace('duckdb::', '')[:44] for f in [f for f in frames if 'duckdb::' in f][:n])


for path in sys.argv[1:]:
    txt = open(path).read()
    lwp = {int(m.group(1)): int(m.group(2))
           for m in re.finditer(r'^\*?\s*(\d+)\s+Thread 0x[0-9a-f]+ \(LWP (\d+)\)', txt, re.M)}
    by_lwp = {v: k for k, v in lwp.items()}
    stacks = {}
    for block in re.split(r'\n(?=Thread \d+ \(Thread)', txt):
        m = re.match(r'Thread (\d+) ', block)
        if not m:
            continue
        frames = []
        for line in block.split('\n'):
            if line.startswith('#'):
                found = re.search(r' in (.*?) \(', line)
                frames.append(found.group(1) if found else line)
        if frames:
            stacks.setdefault(int(m.group(1)), frames)
    owners = {int(m.group(1)): int(m.group(2))
              for m in re.finditer(r'Thread (\d+) \(Thread[^\n]*\n\$\d+ = (-?\d+)', txt)}
    print('==', path.split('/')[-1], '-', len(stacks), 'threads')
    seen = set()
    for thread, frames in sorted(stacks.items()):
        if 'lll_lock_wait' in frames[0]:
            holder = by_lwp.get(owners.get(thread))
            line = f"  waits on a mutex: {duck(frames, 4)}\n      held by: {duck(stacks.get(holder, []), 6) or 'unknown'}"
            if line not in seen:
                seen.add(line)
                print(line)
        elif any('StorageLock' in f for f in frames[:6]):
            print(f"  waits for the index entry's lock: {duck(frames, 6)}")
