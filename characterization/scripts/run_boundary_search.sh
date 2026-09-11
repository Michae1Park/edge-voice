#!/bin/bash
# Phase 1 + Phase 2 of the systematic boundary-search experiment. Each cell
# gets 300s (5 min) sustained/looped playback, then is automatically
# analyzed against the objective pass/fail criteria.
set -uo pipefail
cd /home/ai/workspace/edge-voice
source venv/bin/activate
mkdir -p characterization/results/boundary

declare -A RATE=( [p1_baseline]=1.0 [p1_density_95]=1.0 [p1_density_97]=1.0 [p1_rate_1.5]=1.5 [p1_rate_2.0]=2.0 [p1_length_7]=1.0 [p1_length_10]=1.0 [p2_a]=2.0 [p2_b_max]=2.0 [p2_c]=1.0 [p2_d]=2.0 )

CELLS="p1_baseline p1_density_95 p1_density_97 p1_rate_1.5 p1_rate_2.0 p1_length_7 p1_length_10 p2_a p2_b_max p2_c p2_d"

for cell in $CELLS; do
  echo "=== STARTING $cell (rate=${RATE[$cell]}x) at $(date -u +%FT%TZ) ==="
  MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \
    --wav characterization/testaudio/boundary_${cell}.wav --channels rx \
    --duration-s 300 --disable-reliability --min-silence-duration-ms 50 \
    --token-budget-multiplier ${RATE[$cell]} \
    --csv-out characterization/results/boundary/${cell}.csv \
    > characterization/results/boundary/${cell}_log.txt 2>&1
  echo "=== FINISHED $cell at $(date -u +%FT%TZ) ==="
  python characterization/scripts/analyze_boundary_run.py "$cell" \
    characterization/results/boundary/${cell}.csv \
    characterization/results/boundary/${cell}_log.txt \
    | tee -a characterization/results/boundary/verdicts.txt
done
echo "=== ALL CELLS DONE ==="
