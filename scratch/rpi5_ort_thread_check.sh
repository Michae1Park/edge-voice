#!/bin/bash
set -e

EVDIR=./rpi5-bench-results
mkdir -p "$EVDIR"

vcgencmd measure_temp && vcgencmd get_throttled

# --- Part A: isolated decoder, single-thread vs multi-thread ---
for tag in unset 0 1; do
  mpstat -P ALL 1 > "$EVDIR/mpstat-$tag.log" 2>&1 &
  MPID=$!
  if [ "$tag" = "unset" ]; then
    EDGE_VOICE_STT__USE_PROCESSES=false python3 scratch/bench_ort_threads.py 2>&1 | tee "$EVDIR/ort-threads-$tag.log"
  else
    EDGE_VOICE_STT__USE_PROCESSES=false MOONSHINE_ORT_SINGLE_THREAD=$tag python3 scratch/bench_ort_threads.py 2>&1 | tee "$EVDIR/ort-threads-$tag.log"
  fi
  kill "$MPID" 2>/dev/null
done

for tag in unset 0 1; do
  echo "=== $tag ===" | tee -a "$EVDIR/mpstat-summary.txt"
  awk '$3 ~ /^[0-9]+$/ {busy=100-$NF; if (busy>50) seen[$3]=1} END{n=0; for (c in seen) n++; print n, "cores >50% busy"}' "$EVDIR/mpstat-$tag.log" | tee -a "$EVDIR/mpstat-summary.txt"
done

# --- Part B: full pipeline, 3 configs x 3 rotated rounds ---
DUR=150

run_unpinned() {
  python3 scratch/bench_pipeline_load.py --duration-s $DUR --grace-s 5 \
    --csv-out "$EVDIR/pipe-unpinned-r$1.csv" 2>&1 | tee "$EVDIR/pipe-unpinned-r$1.log"
}
run_pinned_single() {
  MOONSHINE_ORT_SINGLE_THREAD=1 python3 scratch/bench_pipeline_load.py \
    --stt-cores 2,3 --other-cores 0,1 --duration-s $DUR --grace-s 5 \
    --csv-out "$EVDIR/pipe-pinned-r$1.csv" 2>&1 | tee "$EVDIR/pipe-pinned-r$1.log"
}
run_unpinned_single() {
  MOONSHINE_ORT_SINGLE_THREAD=1 python3 scratch/bench_pipeline_load.py \
    --duration-s $DUR --grace-s 5 \
    --csv-out "$EVDIR/pipe-unpinned-single-r$1.csv" 2>&1 | tee "$EVDIR/pipe-unpinned-single-r$1.log"
}

run_unpinned 1; run_pinned_single 1; run_unpinned_single 1
run_pinned_single 2; run_unpinned_single 2; run_unpinned 2
run_unpinned_single 3; run_unpinned 3; run_pinned_single 3

vcgencmd measure_temp && vcgencmd get_throttled

echo "Done. Results in $EVDIR/. tar it up with:"
echo "  tar czf rpi5-bench-results.tar.gz $EVDIR"
