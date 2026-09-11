#!/bin/bash
# 9s.wav length-gap cells, queued to run AFTER run_boundary_fine.sh's 10
# cells finish (waits on that PID first) so they never share the Pi's
# single core with another sustained run.
set -uo pipefail
cd /home/ai/workspace/edge-voice
source venv/bin/activate
mkdir -p characterization/results/boundary_fine

while pgrep -f '[r]un_boundary_fine.sh' >/dev/null; do
  sleep 15
done

declare -A RATE=( [fine_len9_max]=2.00 [fine_len9_r175]=1.75 [fine_len9_d95]=2.00 )
CELLS="fine_len9_max fine_len9_r175 fine_len9_d95"

for cell in $CELLS; do
  echo "=== STARTING $cell (rate=${RATE[$cell]}x) at $(date -u +%FT%TZ) ==="
  MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \
    --wav characterization/testaudio/boundary_${cell}.wav --channels rx \
    --duration-s 300 --disable-reliability --min-silence-duration-ms 50 \
    --token-budget-multiplier ${RATE[$cell]} \
    --csv-out characterization/results/boundary_fine/${cell}.csv \
    > characterization/results/boundary_fine/${cell}_log.txt 2>&1
  echo "=== FINISHED $cell at $(date -u +%FT%TZ) ==="
  python characterization/scripts/analyze_boundary_run.py "$cell" \
    characterization/results/boundary_fine/${cell}.csv \
    characterization/results/boundary_fine/${cell}_log.txt \
    characterization/testaudio/metadata_boundary_fine.json \
    | tee -a characterization/results/boundary_fine/verdicts.txt
done
echo "=== ALL LEN9 CELLS DONE ==="
