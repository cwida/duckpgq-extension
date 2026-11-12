#!/bin/bash
# Stack sampling of 11 Nov, run while `run.sh idxstk 16 --no-build` was in its first seconds: the writer thread alone
# about every 0.13 s (eu-stack -1), and every 25th round all threads of the process.
pid=$(pgrep -x probe | head -1)
eu-stack -p $pid > full0.txt 2>/dev/null
tid=$(python3 -c "
import re
for th in re.split(r'\nTID ', open('full0.txt').read()):
    if 'WriterBody' in th: print(th.split(':')[0]); break")
i=0
while kill -0 $pid 2>/dev/null; do
  i=$((i+1)); eu-stack -1 -p $tid > w_$i.txt 2>/dev/null
  if [ $((i%25)) = 0 ]; then eu-stack -p $pid > stk_$i.txt 2>/dev/null; fi
  sleep 0.08
done
