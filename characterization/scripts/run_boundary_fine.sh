#!/bin/bash
# Dense follow-up grid around the p2_b_max boundary (97% density, 2x rate,
# 10s length). Each cell gets 300s sustained/looped playback, same
# convention as the original 11-cell sweep, then automatic analysis
# against capture_rate / backlog / merge-onset criteria.
set -uo pipefail
cd /home/ai/workspace/edge-voice
source venv/bin/activate
mkdir -p characterization/results/boundary_fine

declare -A RATE=( [fine_r125]=1.25 [fine_r150]=1.50 [fine_r175]=1.75 \
  [fine_d90]=2.00 [fine_d93]=2.00 [fine_d95]=2.00 [fine_d96]=2.00 \
  [fine_int_d95_r150]=1.50 [fine_int_d95_r175]=1.75 [fine_int_d93_r175]=1.75 )

CELLS="fine_r125 fine_r150 fine_r175 fine_d90 fine_d93 fine_d95 fine_d96 fine_int_d95_r150 fine_int_d95_r175 fine_int_d93_r175"

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
echo "=== ALL FINE-GRID CELLS DONE ==="
