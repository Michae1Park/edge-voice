# Benchmarks (edge-voice)

Real numbers from the target hardware, not vendor claims. This doc grows over
time as new measurements come in — each entry is dated and self-contained,
so old numbers stay readable even after the pipeline around them changes.

---

## STT: per-update decode latency — tiny-en / tiny-ko / tiny-zh

**Date:** 2026-08-07
**Hardware:** Raspberry Pi 5
**Script:** `[scratch/bench_streaming_cost.py](../scratch/bench_streaming_cost.py)`
**Model:** Moonshine `tiny`, non-streaming `Transcriber`, `update_interval=1.0s`,
`max_tokens_per_second=18.0`, `vad_threshold=0` (matches `configs/default.yaml`'s
`stt:` block)

### Method

- WAV file fed in small chunks via `add_audio()`; chunks below the update
interval just buffer.
- Once the buffer crosses a decode threshold, the model re-decodes the  
**entire accumulated buffer from scratch** (non-streaming — no incremental  
state between updates). The elapsed time for that call is what's logged.
- Each language was run multiple times (separate process invocations); the
table below averages the runs to smooth out measurement noise.

### Results — update latency, averaged across runs

| audio (s) | en (ms) | en chars | ko (ms) | ko chars | zh (ms) | zh chars |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1         | 199.2   | 20       | 467.0   | 9        | 510.8   | 17       |
| 2         | 432.7   | 50       | 897.0   | 18       | 578.3   | 13       |
| 3         | 645.5   | 68       | 1339.7  | 28       | 999.0   | 20       |
| 4         | 909.0   | 94       | 1541.8  | 39       | 1317.1  | 27       |
| 5         | 1161.0  | 131      | 1686.8  | 39       | 1692.6  | 32       |
| 6         | 1415.3  | 139      | 2097.4  | 46       | 2066.8  | 36       |
| 7         | 1499.9  | 161      | 2886.7  | 61       | 2374.0  | 42       |
| 8         | 1709.7  | 179      | 3075.7  | 66       | 2818.5  | 49       |
| 9         | 1923.0  | 191      | 3499.4  | 70       | 3385.8  | 57       |
| 10        | 2004.1  | 204      | 4069.6  | 78       | 3866.3  | 62       |

Notes:

- zh's chars dip from 17 (1s) to 13 (2s) — the model revised an earlier
guess shorter with more context. Expected non-streaming behavior, not a
data error.
- Chars matched exactly across each language's own runs, so only latency
(not output) is averaged above.

### Reproducing this

```bash
python scratch/bench_streaming_cost.py
```

Edit `WAV`, `MODEL_LN`, `MODEL_SIZE` at the top of the script to point at a different clip/language/model size (`WAV="wav/english.wav", MODEL_LN="en"` for the Chinese run above).

### Why Korean and Chinese are ~2x slower than English — not a bug

`tiny-en`, `tiny-ko`, and `tiny-zh` share the same architecture *label* but
are **different trained checkpoints**, confirmed by inspecting the installed
`moonshine-voice` 0.0.69 cache directly
(`~/.cache/moonshine_voice/download.moonshine.ai/model/`):

|                                      | `tiny-en`                            | `tiny-ko` | `tiny-zh`   |
| ------------------------------------ | ------------------------------------ | --------- | ----------- |
| Decoder (`decoder_model_merged.ort`) | 30.4 MB                              | 58.3 MB   | **58.3 MB** |
| Encoder                              | 13.28 MB                             | 13.24 MB  | 13.24 MB    |
| `decoder_with_attention.ort`         | present (30.1 MB)                    | absent    | absent      |
| Tokenizer (`tokenizer.bin`)          | same md5, all three: `1373d589…f969` |           |             |

- `tiny-zh`'s decoder is byte-for-byte almost the same size as `tiny-ko`'s
— the general shape of every non-English `tiny` checkpoint, not a
Korean-specific quirk.
- A ~1.9x larger decoder doing token-by-token autoregressive generation on
CPU accounts for most of the ~2x latency gap on its own.
- English alone ships `decoder_with_attention.ort`, possibly a faster
incremental-decode path the other languages lack — inferred from the
asset list, not confirmed from source.
- Compounding factor: all three share one BPE tokenizer trained
predominantly on Latin-script text, which typically needs more decode
steps per second of speech for Korean/Chinese (multi-byte UTF-8 per
character/syllable, poor byte-level merge coverage). Consistent with
Korean's and Chinese's steeper latency growth and lower character
throughput for the same audio duration.
- `src/edge_voice/stt/stt_worker.py` has no language-conditional decode
logic (`max_tokens_per_second`, `repetitive_ratio`, etc. are global) — the
gap is entirely upstream, in Useful Sensors' per-language checkpoints.

---

## Pipeline end-to-end: dual-channel conversational audio (post-multiprocessing)

**Date:** 2026-08-10
**Hardware:** Raspberry Pi 5
**Script:** `[scratch/bench_pipeline_load.py](../scratch/bench_pipeline_load.py)`
**Config:** `settings.stt.use_processes=true` (one dedicated STT process per
channel, not the old single shared decoder), `vad.segment_limits_enabled=true`,
`max_segment_s=5.0`, `soft_cut_enabled=false` — i.e. `configs/default.yaml`
as currently committed. `--stt-cores 2,3 --other-cores 0,1`,
`MOONSHINE_ORT_SINGLE_THREAD=1` (see that script's docstring for why the
latter is required alongside core pinning). A same-session comparison run
with `soft_cut_enabled=true` (`soft_cut_s=3.0`) is kept in the Notes below
to document the effect that setting has.

### Method

Real `PipelineOrchestrator`, real MQTT broker, real Moonshine `tiny-ko`
model — not a mock. `wav/rx_recorded_1.wav` + `wav/tx_recorded_1.wav` (a
recorded call-leg pair with natural conversational pauses/turn-taking)
looped for 190s (6 loops) on both channels concurrently. Run immediately
after a fresh reboot — see the Notes below for why that matters here.

```bash
MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \
    --stt-cores 2,3 --other-cores 0,1 \
    --wav wav/rx_recorded_1.wav wav/tx_recorded_1.wav \
    --channels rx tx --duration-s 180
```

### Results

54 segments over 6 loops (190s), fresh-reboot baseline, current committed
config (`soft_cut_enabled=false`):

| Metric | p50 | p95 | max |
| --- | ---: | ---: | ---: |
| Full latency (mic → transcript) | 1948 ms | 3971 ms | 4035 ms |

| RSS mean | RSS peak | CPU mean | CPU median | CPU max |
| ---: | ---: | ---: | ---: | ---: |
| 1242 MB | 1243 MB | 64% | 62% | 155% (of 400% available) |

| Queue depth | first half | second half | peak | verdict |
| --- | ---: | ---: | ---: | --- |
| combined (ingest+routed+segment) | 0.15 | 0.19 | 2 | flat/draining |

By channel:

| Channel | n | Full latency mean | STT decode mean |
| --- | ---: | ---: | ---: |
| rx | 24 | 2779.4 ms | 2750.1 ms |
| tx | 30 | 1640.7 ms | 1549.7 ms |

`hit_cap` was `False` for all 54 segments — natural pauses in this
recording never come close to the 5.0s hard cap, so `segment_limits_enabled`
costs nothing here (see the soft-cut note below for the one way it *used to*
cost something even with `hit_cap` false throughout).

### Notes

- **Healthy with real margin**: queue depth peaks at 2 (against a
configured capacity of 256+128+64=448), and CPU never exceeds 155% of the
400% available — nowhere close to falling behind real-time for this
content.
- rx runs measurably higher latency than tx on this file — a property of
the source recording (rx's turns are longer on average, see `dur_s` in the
per-segment CSV), not a channel-handling asymmetry in the pipeline.

#### Soft-cut latency (confirmed fixed by disabling it)

A soft cut (`vad_worker.py`'s `_maybe_cut`) doesn't
cut at the live edge; it scans back up to `soft_cut_lookahead_s` (1.0s) for
the best pause and backdates the emitted segment's `.end` to that point:

```python
cut_ts = state.segment_start_ts + cut_idx * chunk_s   # a point in the past
self._finalize_segment(channel_id, state, end_ts=cut_ts)
```

Since `full_latency_ms = created_at - end` is measured against that
backdated `.end`, part of the "latency" on a soft-cut segment is really
just "how far back the chosen pause was," not the pipeline falling
behind — and the queued *tail* piece then genuinely waits behind the
head's own decode on top of that (each channel has exactly one STT
decoder). A same-session run with `soft_cut_enabled=true` (`soft_cut_s=3.0`)
showed exactly this: an ordinary ~3.35s utterance (`rx`, recurring at the
same point each loop) got split into two pieces every time it recurred,
each paying real cost:

| Piece | dur_s | chars | pre_ms, soft-cut on | pre_ms, soft-cut off (this run) |
| --- | ---: | ---: | ---: | ---: |
| (whole utterance, uncut) | 3.36-3.40s | 34 | *(split below)* | **-1.5 / 17.7 / 20.2** |
| head | 2.27s | 22 | 695 – 722 ms | *(no longer split)* |
| tail | 1.08s | 12 | 1106 – 1348 ms | *(no longer split)* |

Added `vad.soft_cut_enabled` (default `true`, matching the prior behavior
whenever `segment_limits_enabled` was on) so the pause search can be
turned off independently of the hard cap, which is what actually bounds
worst-case backlog and is unaffected by this flag. `configs/default.yaml`
now ships `soft_cut_enabled=false`.

Confirmed by this run, three ways:
- The recurring utterance lands as one clean segment all three times
  (elapsed≈26s/88s/150s), `pre_ms` back to single-digit-to-tens, matching
  the original pre-`segment_limits` baseline (`pre_ms=16.9`) almost exactly.
- Segment count dropped from 57 (soft-cut on) to 54 (soft-cut off) — exactly
  the 3 fewer segments expected from 3 recurring splits no longer happening.
- Aggregate `pre_ms` p95 (VAD wait + queueing, across all segments) dropped
  from **1105.9 ms to 51.7 ms** run-over-run; both runs' `pre_ms` **max**
  stayed near 2020ms from the same, unrelated end-of-run drain artifact (the
  final segment caught mid-grace-period at shutdown) — confirming the p95
  improvement is specifically the soft-cut fix, not a general latency shift.

Re-enable `soft_cut_enabled` if mid-word chops on a rare
`max_segment_s` overrun turn out to matter more than this latency does.

#### Thermal/DVFS drift (not a regression)

An earlier same-day run on identical audio, taken mid-session after a long
string of back-to-back stress tests (no reboot in between), measured
~35-70% slower per segment on this exact same hardware/config. This
fresh-reboot run lands back in line with the very first post-multiprocessing
measurement (taken before any of that session's stress testing) —
confirming the mid-session numbers were inflated by sustained heat/clock
throttling on a board with no active cooling, not a real regression from the
segment-cap or stall-detection fixes:

| Run | Latency p50 | Latency p95 | CPU mean |
| --- | ---: | ---: | ---: |
| First post-multiprocessing (session start, `segment_limits_enabled=false`) | 1817 ms | 3976 ms | 67% |
| Mid-session (after a long run of back-to-back stress tests) | 3448 ms | 6498 ms | 88% |
| Fresh-reboot, soft-cut on (same session, `soft_cut_enabled=true`) | 1903 ms | 3778 ms | 64% |
| **Fresh-reboot, soft-cut off (this run, current config)** | **1948 ms** | **3971 ms** | **64%** *(= Results above)* |

Same conclusion holds per-segment, on identical audio (`stt_ms`):

| Segment (duration, chars) | Session start | Mid-session | Reboot, soft-cut on | Reboot, soft-cut off (this run) |
| --- | ---: | ---: | ---: | ---: |
| 2.37s, 27 chars | 1866 ms | 3220 ms | 1999 ms | 2030 ms |
| 1.92s, 18 chars | 1262 ms | 2111 ms | 1168 ms | 1133 ms |
| 1.69s, 13 chars | 1067 ms | 1443 ms | 1062 ms | 1074 ms |
| 4.83s, 43 chars | 4238 ms | 5916 ms | 4182 ms | 3936 ms |

The two reboot runs (soft-cut on vs. off) agree closely on these four
*unsplit* segments, as expected — soft-cut only touches segments that
actually run past `soft_cut_s`, so its cost is isolated to those, not a
blanket slowdown. Both reboot runs sit well below the mid-session numbers
either way, reinforcing that the thermal effect and the soft-cut effect
are two independent, now both-understood findings, not one conflated one.

**Practical takeaway:** benchmark numbers taken deep into a long, uncooled
RPi5 test session run measurably pessimistic — a cold-boot (or at least
cooled-down) baseline is the number to trust for capacity planning, and the
mid-session one is now understood as a thermal artifact rather than a
separate finding.

---

## Pipeline end-to-end: dual-channel stress test — near-continuous speech exceeds real-time capacity

**Date:** 2026-08-10
**Hardware:** Raspberry Pi 5
**Script:** `[scratch/bench_pipeline_load.py](../scratch/bench_pipeline_load.py)`
**Config:** same as above, with `max_segment_s` swept across 2.0s/3.0s/5.0s
(`soft_cut_s` 1.0s/2.0s/3.0s respectively) to isolate the cap's effect.

### Method

Same harness, but `wav/conversation_60s.wav` looped on **both** rx and tx —
a single mono clip of dense, near-continuous Korean radio/news speech
(~8% measured silence, vs. the natural-pause recording above) chosen
specifically to stress-test near-100% dual-channel speech duty cycle. This
is **not representative of real usage** (estimated <0.1% likelihood for
this product's actual two-party conversational/walkie-talkie traffic,
which has natural turn-taking pauses) — kept here as a documented hardware
ceiling, not a target to optimize toward.

```bash
MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \
    --stt-cores 2,3 --other-cores 0,1 \
    --wav wav/conversation_60s.wav wav/conversation_60s.wav \
    --channels rx tx --duration-s 180
```

### Results

This input surfaced two real reliability bugs before it settled into a
pure capacity finding — noted here since the numbers only make sense in
that order:

1. **False-positive stall/restart loop (fixed).** With `STTWorker`'s
   liveness (`last_activity`) stamped only at segment dequeue, a single
   legitimately-slow decode (routine on this content) looked identical to a
   hang once it ran past `reliability.stall_timeout_s` (10s default). The
   supervisor SIGKILLed still-working STT children, which re-stalled on the
   next long segment, and repeated until `max_restarts` was exhausted
   (heading to DEGRADED). Fixed by making the child's heartbeat track real
   CPU progress instead of segment-start time.
2. **Two interpreter-exit hangs on shutdown (fixed).** (a) A still-decoding
   STT child left alive past the shutdown join timeout was never
   force-killed on the plain `stop()` path (only a supervisor-triggered
   restart had that). (b) Once a child *did* exit while its `segment_queue`
   still had unconsumed backlog, the parent's abandoned feeder thread could
   block forever inside `pipe_write()`, and `multiprocessing` joins that
   thread with no timeout at interpreter exit — hanging the whole process.
   Both are now handled in `orchestrator.py`'s shutdown path.

With both fixed, the run completes and reports real numbers — which show a
genuine capacity limit, not a bug. Three `max_segment_s` values were swept,
each a single ~62s pass of the same clip on both channels:

| `max_segment_s` | `soft_cut_s` | First-segment RTF† | Time to queue saturation | Peak combined queue depth | Queue trend |
| ---: | ---: | ---: | ---: | ---: | --- |
| 2.0s | 1.0s | **0.92 – 0.98** (clean start) | ~18s | 399 | GROWING |
| 3.0s | 2.0s | 2.22 – 2.35 (dirty start‡) | ~24s | 391 | GROWING |
| 5.0s | 3.0s | 1.81 – 1.95 (dirty start‡) | ~22s | 388 | GROWING |

RTF = `stt_ms / (dur_s * 1000)`; >1.0 means decode is slower than real-time.

† The 2.0s run is the only clean (queue-empty) start; the other two
inherited backlog from the immediately-preceding sweep iteration (no
cooldown between them), so their RTF numbers are not a clean read on cap
length alone — flagged, not corrected for, pending a re-run with a
cooldown/reboot between iterations (see open items).
‡ Routed queue was already 85-104/128 deep before either run's first
transcript had even landed.

Worst individual segments observed (uncapped run, `segment_limits_enabled=false`):

| Segment duration | Decode time | RTF |
| ---: | ---: | ---: |
| 37.81s | 119.5s | 3.16x |
| 5.02s (capped) | 16.6s | 3.31x |

Note the peak-queue-depth column above is nearly identical across all three
caps (388-399) regardless of setting — that's the configured queue capacity
ceiling (`queues.ingest=256` + `queues.routed=128` = 384), not a real
difference between cap values. The more informative signal is that every
cap value saturates on essentially the same ~20-25s timescale.

### Notes

- Shrinking `max_segment_s` bounds *worst-case single-segment latency*, but
does **not** fix the underlying deficit: aggregate STT decode throughput for
two concurrent channels of this content exceeds what 2 pinned RPi5 cores can
do, regardless of how the audio is chunked. Every cap value tested saturates
the queues on essentially the same timescale.
- Confirmed not a thermal-throttling or power-supply artifact:
`vcgencmd measure_temp` stayed at 56-58°C through testing (well under the
~80°C throttle point), and `vcgencmd get_throttled`'s under-voltage/throttle
flags were isolated to boot-time, not during any test run.
- Conclusion: **not pursued further as a tuning problem.** Given the
confirmed <0.1% real-world likelihood of this input pattern, the fix here is
not a config change but the two reliability fixes above (so this exact
class of extreme input degrades to bounded latency + eventual packet loss
under backpressure, rather than a crash loop) — see the "normal audio" entry
above for the actual target-workload numbers.

---

## `MOONSHINE_ORT_SINGLE_THREAD`: single-thread vs ORT's default multi-thread pool

**Date:** 2026-08-11
**Hardware:** Raspberry Pi 5 (decisive numbers); a 32-core x86 dev box was also run same-day as a same-day mechanism pre-check, noted in Results/Notes for context only
**Scripts:** `[scratch/bench_ort_threads.py](../scratch/bench_ort_threads.py)` (isolated single decoder), `[scratch/bench_pipeline_load.py](../scratch/bench_pipeline_load.py)` (full pipeline)
**Config:** `settings.stt.use_processes=true` (shipped default) for the pipeline runs; `EDGE_VOICE_STT__USE_PROCESSES=false` forces the single in-process decoder the isolated test needs, so it isn't conflated with multiprocessing

### Why this needed measuring

`docs/archived/STT_MULTIPROCESS_PLAN.md` assumed single-thread ORT beats its default multi-threaded pool ("sync overhead exceeds the benefit at this model size") without ever measuring it on the Pi. Disassembling `ort_maybe_force_single_thread` in `libmoonshine.so` confirmed the flag is binary: `getenv("MOONSHINE_ORT_SINGLE_THREAD")` — NULL, empty, or the literal string `"0"` all skip forcing and leave ORT's own default pool (sized to `hardware_concurrency()`); anything else forces `intra_op_num_threads=1`. That settled *what* the flag does; this entry settles whether it's worth setting.

### Method

Two levels:

1. **Isolated decoder** — one thread-backed decoder, not the multiprocess default, fed the same 24s tx/rx clip, one run per env var value.
2. **Full pipeline** — real orchestrator, real MQTT broker, `use_processes=true`, 150s runs, **3 rotated rounds per config**. Rotated (not fixed order) because a same-order dev-box comparison couldn't rule out time-drift aliasing with the config effect — see Notes.

```bash
# isolated decoder
EDGE_VOICE_STT__USE_PROCESSES=false python3 scratch/bench_ort_threads.py
EDGE_VOICE_STT__USE_PROCESSES=false MOONSHINE_ORT_SINGLE_THREAD=0 python3 scratch/bench_ort_threads.py
EDGE_VOICE_STT__USE_PROCESSES=false MOONSHINE_ORT_SINGLE_THREAD=1 python3 scratch/bench_ort_threads.py

# full pipeline, one of three configs (pinned adds --stt-cores 2,3 --other-cores 0,1)
MOONSHINE_ORT_SINGLE_THREAD=1 python3 scratch/bench_pipeline_load.py --duration-s 150 --grace-s 5
```

### Results — isolated decoder (RPi5)

| `MOONSHINE_ORT_SINGLE_THREAD` | `total_latency_s` (24s audio) | Cores >50% busy |
| --- | ---: | ---: |
| unset | 16.43s | 4 |
| `0` | 17.23s | 4 |
| `1` | **13.73s** | 3 |

Single-thread ~16-20% faster. (Dev-box pre-check, same day, same direction at larger margin: 0.94s vs 1.55s/1.34s, 1 core busy vs 31-32 — confirms the mechanism, not a substitute for the Pi number.)

### Results — full pipeline (RPi5, averaged over 3 rotated rounds)

| Config | Full latency mean | p95 | CPU mean | Queue depth peak (3 rounds) |
| --- | ---: | ---: | ---: | --- |
| unpinned, multi-thread (default) | 2849 ms | 5651 ms | 122% | **25, 51, 57** |
| pinned + single-thread (deployed config) | **2112 ms** | **3811 ms** | 65% | 1, 1, 5 |
| unpinned + single-thread | 2170 ms | 3989 ms | 65% | 1, 1, 3 |

### Notes

- **The decisive signal is queue depth, not latency.** Without the flag, every one of 3 rounds built a real backlog (25-57 segments). With it — pinned or not — no round ever exceeded 5. That's the difference between keeping up with live audio and periodically falling behind it, not just "a bit slower."
- **Pinning adds a further, smaller edge on top of single-threading** (2112ms vs 2170ms mean, ~3%) — real, consistent with cache-locality reasoning, but secondary. The flag is the load-bearing decision; pinning is a bonus on top of it.
- **Rounds were rotated, not sequential**, so a monotonic drift (thermal, background load) would hit all three configs equally instead of aliasing into a fake config effect — see the dev-box caution below for why this mattered.
- **The same full-pipeline comparison on the dev box was noisy and pointed the wrong way** (single run each, no rotation): pinned+single-thread measured *slightly slower* than the multi-thread default there. Root cause, not a contradiction: the dev box has 32 idle cores and this recording is sparse/gappy, so neither pinning nor single-threading has any scarcity to defend against — the RPi5's real constraint (4 cores, real memory-bandwidth limits) is what makes the effect show up at all. The isolated-decoder mechanism test *did* transfer from dev box to Pi; the full-pipeline comparison did not, and needed the real hardware.
- Confirms the assumption in `docs/archived/STT_MULTIPROCESS_PLAN.md` §2.1. Ship `MOONSHINE_ORT_SINGLE_THREAD=1`.

---

## STT multiprocessing Gate 0: process-based parallelism on RPi5

**Date:** 2026-08-11
**Hardware:** Raspberry Pi 5
**Script:** `[scratch/probe_mp_speedup.py](../scratch/probe_mp_speedup.py)`
**Config:** `tiny-ko`, `MOONSHINE_ORT_SINGLE_THREAD=1` (set by the script itself), 30 decodes/child, model load excluded from timing

### Why this needed measuring

`docs/archived/STT_MULTIPROCESS_PLAN.md`'s whole premise rests on `multiprocessing.Process` removing the GIL-serialization that capped two threads at 1.02x (measured on the RPi5, `probe_gil_release.py`). The process-based number — 84% overlap, 1.70x speedup — was so far only measured on a 32-core x86 dev box. §11.2 names this **Gate 0**: the cheap check before writing any `src/` code, required to pass on the RPi5 specifically, not inferred from the dev box.

### Method

Two independent `Transcriber` instances, one per `multiprocessing.Process`, each decoding the same 3s clip 30 times. Model load is excluded from the timing — each child signals ready, then blocks on a shared barrier; the clock starts once both are waiting — so spawn+load overhead (3.6-7.3s per instance on the RPi5) doesn't swamp the decode-only comparison.

```bash
python3 scratch/probe_mp_speedup.py --iterations 30
```

### Results

| | Dev-box (x86, 32 cores) | RPi5 |
| --- | ---: | ---: |
| Overlap | 84% | **100%** |
| Speedup | 1.70x | **1.47x** |
| Gate (≥80% overlap, ≥1.4x speedup) | pass | **pass** |

For reference, two *threads* on the RPi5 (`probe_gil_release.py`) measured 1.02x — no benefit, GIL-bound.

### Notes

- **Overlap higher, speedup lower — consistent with the memory-bandwidth hypothesis, not a contradiction.** On the RPi5 the two processes were computing simultaneously effectively the entire time (100%), more cleanly than the dev box's 84%. But the RPi5's far lower memory bandwidth means that concurrent compute contends harder for the same memory bus, capping the throughput gain to 1.47x instead of the dev box's 1.70x. The GIL-removal mechanism is confirmed even more cleanly on target hardware; the payoff is real but smaller — this is the number to plan around, not the dev box's more optimistic 1.70x.
- Repeated `STTWorker: ... final line was repetitive, falling back to best partial` log lines are expected: the probe decodes the identical clip 30 times per child, so the repetition guard firing every time is a property of the synthetic workload, not a defect.
- **Gate 0 passes** — confirms the plan's premise on the actual target hardware. Step 7's remaining work is the full before/after pipeline protocol (`docs/archived/STT_MULTIPROCESS_PLAN.md` §11.3-11.7); this closes only the Gate 0 sub-step (§11.2).

---

## Open items for future entries

- Cap-length sweep (stress-test entry above) re-run with a cooldown/reboot
between each `max_segment_s` value, to get a clean read on whether segment
length itself affects decode efficiency, independent of the thermal drift
and inherited-backlog confounds noted there.
- `tiny` vs `base` latency/accuracy tradeoff, per language.
- Streaming-model (`tiny-streaming-en` etc.) comparison — `bench_streaming_cost.py`
experiment 2/3 have this wired up but commented out (see
`docs/deferred/STREAMING_STT_PLAN.md`, currently on hold).
- Remaining shipped `tiny-*` checkpoints (`ja`, `ar`, `uk`, `vi`) not yet
benchmarked — expected to follow the same ~1.9x-decoder pattern as `ko`/`zh`
based on cache inspection, but unconfirmed against real audio.

