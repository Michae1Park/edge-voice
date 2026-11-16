#!/bin/bash
# Sequential sustained-load limit search over combo1-9. Each combo gets up
# to 300s (5 min); if it doesn't show clogging by then, move on. Uses the
# validated min_silence_duration_ms=50 fix + reliability disabled, so any
# clogging found here is a genuine capacity signal, not the already-fixed
# VAD-threshold artifact from the earlier combo5 investigation.
set -uo pipefail
cd /home/ai/workspace/edge-voice
source venv/bin/activate
mkdir -p characterization/results/limit_search

declare -A SPEED=( [combo1]=1.0 [combo2]=1.0 [combo3]=1.0 [combo4]=1.0 [combo5]=2.0 [combo6]=1.0 [combo7]=2.0 [combo8]=1.0 [combo9]=2.0 )

for combo in combo1 combo2 combo3 combo4 combo5 combo6 combo7 combo8 combo9; do
  echo "=== STARTING $combo (speed=${SPEED[$combo]}x) at $(date -u +%FT%TZ) ==="
  MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \
    --wav characterization/testaudio/${combo}.wav --channels rx \
    --duration-s 300 --disable-reliability --min-silence-duration-ms 50 \
    --token-budget-multiplier ${SPEED[$combo]} \
    --csv-out characterization/results/limit_search/${combo}.csv \
    > characterization/results/limit_search/${combo}_log.txt 2>&1
  echo "=== FINISHED $combo at $(date -u +%FT%TZ) ==="
done
echo "=== ALL COMBOS DONE ==="
