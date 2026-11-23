#!/usr/bin/env bash
# The frozen sweep of eval-freeze-rc14 on the dedicated machine; resumable (finished steps are skipped)
source ~/sweep_env.sh
cd ~/duckpgq-extension/repro || exit 1
echo "$(date -u +%FT%TZ) start commit=$(git rev-parse --short HEAD) load=$(cut -d" " -f1-3 /proc/loadavg)" >> ~/sweep/run_sweep.log
./containers.sh start > ~/sweep/containers_start.out 2>&1
sleep 90   # Memgraph needs about a minute after a start before it answers
SCALE_FACTORS="10 100 300" ./run_all.sh all >> logs/run_all.out 2>&1
echo "$(date -u +%FT%TZ) end exit=$? load=$(cut -d" " -f1-3 /proc/loadavg)" >> ~/sweep/run_sweep.log
