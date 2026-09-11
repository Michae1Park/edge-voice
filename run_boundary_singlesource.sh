#!/bin/bash
# Single-source (17s.wav, cropped to 9-15s) length sweep at two extreme
# density/rate combos, with early-stop enabled: a cell that hits a
# sustained latency plateau or a merge event stops ~10s later instead of
# always running the full 300s.
set -uo pipefail
cd /home/ai/workspace/edge-voice
source venv/bin/activate
mkdir -p characterization/results/boundary_singlesource

python3 -c "
import json
m = json.load(open('characterization/testaudio/metadata_boundary_singlesource.json'))
for label, meta in m.items():
    cycle = meta['atomic_segment_duration_s'] + meta['gap_s']
    rate = meta['rate']
    print(f'{label} {rate} {cycle:.4f}')
" > /tmp/ss_cells.txt

while read -r cell rate cycle; do
  echo "=== STARTING $cell (rate=${rate}x, atomic_cycle=${cycle}s) at $(date -u +%FT%TZ) ==="
  MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \
    --wav characterization/testaudio/boundary_${cell}.wav --channels rx \
    --duration-s 300 --disable-reliability --min-silence-duration-ms 50 \
    --token-budget-multiplier ${rate} \
    --early-stop --early-stop-grace-s 10 --merge-atomic-cycle-s ${cycle} \
    --csv-out characterization/results/boundary_singlesource/${cell}.csv \
    > characterization/results/boundary_singlesource/${cell}_log.txt 2>&1
  echo "=== FINISHED $cell at $(date -u +%FT%TZ) ==="
  python characterization/scripts/analyze_boundary_run.py "$cell" \
    characterization/results/boundary_singlesource/${cell}.csv \
    characterization/results/boundary_singlesource/${cell}_log.txt \
    characterization/testaudio/metadata_boundary_singlesource.json \
    | tee -a characterization/results/boundary_singlesource/verdicts.txt
done < /tmp/ss_cells.txt
echo "=== ALL SINGLE-SOURCE CELLS DONE ==="
