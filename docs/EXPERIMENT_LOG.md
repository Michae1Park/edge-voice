# Experiment Log — 2026-09-08/09 RPi5 benchmarking session

Chronological record of everything measured/built during this session on the
`asr` device (RPi5, `ssh ai@192.168.33.119`, 4 cores, hostname `asr`). This is
a raw lab notebook, not a polished reference — see `docs/BENCHMARK.md` for the
curated numbers that came out of experiments #1 and #10-11. Everything here
used `scratch/bench_pipeline_load.py` unless noted, against the real
`PipelineOrchestrator`, real MQTT broker (mosquitto), real Moonshine `tiny-ko`
quantized model.

## Contents

1. [Full-latency re-measurement, 5x repeated](#1-full-latency-re-measurement-5x-repeated)
2. [Single-channel vs. dual-channel latency](#2-single-channel-vs-dual-channel-latency)
3. [Same-audio concurrency isolation](#3-same-audio-concurrency-isolation)
4. [Strict per-core isolation (`--stt-core-map`)](#4-strict-per-core-isolation---stt-core-map)
5. [RSS/CPU for shared-cores dual-channel](#5-rsscpu-for-shared-cores-dual-channel)
6. [RSS/CPU for single-channel (flawed — see #7)](#6-rsscpu-for-single-channel-flawed--see-7)
7. [Bug found & fixed: single-channel wasn't single-process](#7-bug-found--fixed-single-channel-wasnt-single-process)
8. [True single-process RSS/CPU](#8-true-single-process-rsscpu)
9. [STT throughput (Input/Processing/Rate-Difference)](#9-stt-throughput-inputprocessingrate-difference)
10. [Speed sweep — natural recording](#10-speed-sweep--natural-recording)
11. [Standard synthetic audio + cross-validated ceiling](#11-standard-synthetic-audio--cross-validated-ceiling)

---

## 1. Full-latency re-measurement, 5x repeated

**Goal:** confirm the `docs/BENCHMARK.md` pipeline-load numbers (p50=1948ms,
p95=3971ms, recorded at commit `f8056c8`) still hold after the VAD backend
switch (`1b3a3bf`, torch.hub → onnxruntime for Silero).

**Method:** exact original command, dual-channel (`rx_recorded_1.wav` +
`tx_recorded_1.wav`), `--stt-cores 2,3 --other-cores 0,1`,
`MOONSHINE_ORT_SINGLE_THREAD=1`, `--duration-s 180`, repeated 5x, no reboot
between runs, `--csv-out` capturing all 285 segments (57/run).

**Result:** ~2x faster than the pre-refactor baseline.

| Metric | mean | stdev | p50 | p95 | max |
|---|---:|---:|---:|---:|---:|
| Full latency | 1157.9 ms | 581.2 ms | 912.1 ms | 2039.3 ms | 2925.6 ms |
| STT decode | 1081.6 ms | 556.8 ms | 891.0 ms | 1999.0 ms | 2054.5 ms |
| Pre-STT | 76.3 ms | 278.0 ms | 21.2 ms | 455.5 ms | 1981.8 ms |

Per-run means were extremely stable (1153.5-1168.4ms) — no thermal drift this
time, unlike the mid-session drift noted in the original BENCHMARK.md entry.

**Written up in `docs/BENCHMARK.md`** under "VAD backend switch (torch.hub →
onnxruntime) — ~2x latency improvement."

---

## 2. Single-channel vs. dual-channel latency

**Goal:** isolate whether rx and tx contend with each other, or whether
each channel's own latency is independent of the other channel running.

**Method:** `--channels rx --wav wav/rx_recorded_1.wav` only, same core
pinning, 5x180s runs.

**Result (rx alone):**

| Metric | mean | stdev | p50 | p95 | max |
|---|---:|---:|---:|---:|---:|
| Full latency | 1549.4 ms | 526.1 ms | 1741.3 ms | 2035.8 ms | 2054.9 ms |

**Compared to rx's own numbers within the dual-channel run** (mean 1551.8ms,
p50 1758.7ms) — essentially identical. Initial conclusion: "no cross-channel
contention." **This conclusion was wrong** — see experiment #3, which found
real contention once audio content is controlled for. The reason single vs.
dual looked the same here is a composition effect: dual-channel's *pooled*
p50 (912ms) mixes in tx's much shorter, more numerous segments (165 tx vs.
120 rx, tx averaging 2.04s vs. rx's 3.54s) — rx's own distribution was
unaffected by tx running or not, but that doesn't mean concurrency has zero
cost in general (see #3).

---

## 3. Same-audio concurrency isolation

**Goal:** remove the confound in #2 — test concurrency with *identical*
audio content and timing on both channels, forcing simultaneous decode
demand instead of relying on natural (staggered) turn-taking.

**Method:** `--wav wav/rx_recorded_1.wav wav/rx_recorded_1.wav --channels rx
tx` (same file on both), shared `--stt-cores 2,3`, 5x180s.

**Result:**

| | rx alone (#2) | rx+tx, same audio, synchronized |
|---|---:|---:|
| mean | 1549.4 ms | 2025.4 ms |
| p50 | 1741.3 ms | 2228.7 ms |
| p95 | 2035.8 ms | 2781.6 ms |

**+31% mean, +28% p50, +37% p95** — real concurrency cost once both channels
are forced to decode at the same moments. `#2`'s "no contention" conclusion
only held because the natural rx/tx recording rarely needs simultaneous
decode (one side is usually silent while the other talks).

---

## 4. Strict per-core isolation (`--stt-core-map`)

**Goal:** test whether the #3 slowdown is kernel scheduling contention
(`sched_setaffinity({2,3})` lets either process land on either core) by
giving each channel an *exclusive* core.

**Built:** `--stt-core-map rx=2,tx=3` (and general `CH=C[+C...]` syntax) in
`scratch/bench_pipeline_load.py`, alongside the existing shared `--stt-cores`.
Verified via `ps`: `STTWorker-rx` → core 2 only, `STTWorker-tx` → core 3 only.

**Result:** isolation did **not** help — if anything, marginally worse.

| Condition | mean | p50 | p95 |
|---|---:|---:|---:|
| Single channel | 1549.4 ms | 1741.3 ms | 2035.8 ms |
| Dual, shared `{2,3}` | 2025.4 ms | 2228.7 ms | 2781.6 ms |
| Dual, exclusive `rx→2, tx→3` | 2075.7 ms | 2375.8 ms | 2860.9 ms |

**Ruled out:** CPU saturation (mean 56-61% of 400% available, max ~213%) and
active thermal throttling (`vcgencmd get_throttled` = `0x50000` — historical
bits only, none of the "currently throttling" bits set; temp 47.7°C).

**Leading remaining explanation:** memory-bandwidth or shared-cache
contention between the two decoder processes — each is a separate process
with its own full copy of the model, and the RPi5's DRAM controller/L3 is
shared regardless of which core each process sits on. Not proven directly
(would need `perf stat` cache-miss counters or a synthetic memory-bandwidth
stressor) — this is the strongest available evidence for it so far, since it
survives ruling out CPU and thermal explanations.

---

## 5. RSS/CPU for shared-cores dual-channel

**Goal:** get resource-usage percentiles for the #3 scenario (shared
`{2,3}`, same audio both channels).

**Method:** reused the #3 log (didn't re-run) — the script already prints
per-run RSS/CPU percentile summaries.

**Result** (avg ± std across 5 runs, each run's own mean/median/p95/max):

| | RSS (MB) | CPU (%, 400%=4 cores) |
|---|---|---|
| mean | 1250.16 ± 1.03 | 57.30 ± 1.17 |
| p50 | 1250.94 ± 1.06 | 8.40 ± 0.55 |
| p95 | 1251.62 ± 1.03 | 208.76 ± 0.50 |
| max | 1251.62 ± 1.03 | 212.14 ± 0.88 |

CPU's huge mean/median gap (57% vs 8%) reflects bursty synchronized decode
load (both channels' identical audio means simultaneous speech bursts,
separated by long idle/silence stretches) — not a measurement artifact.

---

## 6. RSS/CPU for single-channel (flawed — see #7)

**Goal:** compare RSS/CPU between single- and dual-channel to find the
per-decoder memory cost.

**Result at the time** (avg ± std across 5 runs):

| | RSS (MB) | CPU (%) |
|---|---|---|
| mean | 1241.10 ± 3.31 | 23.10 ± 0.07 |
| max | 1242.00 ± 3.25 | 105.38 ± 0.53 |

**Surprising finding:** RSS was nearly identical to the dual-channel number
(1241 vs 1250MB) — only ~9MB apart, despite CPU cleanly halving (23% vs
57%). This didn't add up (a whole second decoder process should cost
hundreds of MB, not 9), which triggered the investigation in #7.

---

## 7. Bug found & fixed: single-channel wasn't single-process

**Investigation:** used `ps -eo pid,ppid,rss,cmd` on a live run instead of
trusting the script's own aggregate. Found **two** `multiprocessing-fork`
children present even with `--channels rx` alone (~470MB RSS each) — one
real (decoding), one permanently idle.

**Root cause:** `PipelineOrchestrator.build()` builds one STT worker per
channel in `configs/default.yaml`'s `mqtt.channels` (hardcoded rx+tx),
completely independent of `bench_pipeline_load.py`'s `--channels` flag —
that flag only controls which channel(s) get replayed *audio*, not how many
STT processes the orchestrator builds.

**Fix (`scratch/bench_pipeline_load.py`):** after `Settings.load()`, filter
`settings.mqtt.channels` down to the requested `--channels` before
`orch.build()`, with a validation error if an unknown channel is requested
and a log line confirming the restriction. Verified via `ps`: exactly one
`multiprocessing-fork` child now, for `--channels rx`.

---

## 8. True single-process RSS/CPU

**Goal:** redo #6 with the #7 fix, to get the real per-decoder memory cost.

**Result** (avg ± std across 5 runs):

| | RSS (MB) | CPU (%) |
|---|---|---|
| mean | 774.42 ± 0.04 | 23.02 ± 0.16 |
| p50 | 774.94 ± 0.05 | 4.00 ± 0.00 |
| p95 | 775.32 ± 0.04 | 103.38 ± 0.53 |
| max | 775.34 ± 0.05 | 112.38 ± 4.51 |

**The real 1-process vs. 2-process comparison:**

| | True single-process | 2-process (1 idle) | 2-process (both active) |
|---|---:|---:|---:|
| RSS mean | 774.4 MB | 1241.1 MB | 1250.2 MB |
| CPU mean | 23.0% | 23.1% | 57.3% |

**~467MB per additional decoder process** — matches the `ps`-observed
~463-474MB per fork almost exactly. Confirms essentially no memory sharing
between decoder processes (each loads/allocates its own weights
independently post-fork, not a shared read-only mmap). CPU is unaffected by
an idle second worker, as expected.

---

## 9. STT throughput (Input/Processing/Rate-Difference)

**Goal:** measure "jobs/s" throughput — Input Rate (segments arriving),
Processing Rate (segments completed), Rate Difference — to check whether the
pipeline keeps up with the natural recording's pacing.

**Definitions:**
- **Job** = one VAD-finalized speech segment (one CSV row).
- **Input Rate** = segment arrivals (`elapsed_s - full_latency_ms/1000`)
  per 30s window.
- **Processing Rate** = segment completions (`elapsed_s`) per 30s window.

**Method:** reused the #1 dataset (dual-channel, natural rx/tx audio),
binned into 30s windows, pooled across all 5 runs (35 windows).

**Result:**

| Metric | mean | stdev | p50 | p95 | max | min |
|---|---:|---:|---:|---:|---:|---:|
| Input Rate (jobs/s) | 0.2714 | 0.0776 | 0.3000 | 0.3667 | 0.3667 | 0.1000 |
| Processing Rate (jobs/s) | 0.2714 | 0.0719 | 0.3000 | 0.3667 | 0.3667 | 0.1000 |
| Rate Difference (jobs/s) | 0.0000 | 0.0243 | 0.0000 | 0.0333 | 0.0333 | -0.0667 |

**Important scoping caveat, established through discussion, not
re-measurement:** this is STT throughput specifically (arrival into STT
queue → STT completion), not "full pipeline from raw MQTT ingest" — a job
doesn't exist until VAD finalizes it, and nothing happens after STT
completes, so in this architecture STT throughput and "pipeline" throughput
collapse into the same measurement. VAD's own capacity (a different unit —
audio chunks, not jobs) was estimated separately from the `vad_silero`
metrics-log timing (~0.001s/chunk, coarse/rounded) as ~1000 chunks/s
capacity vs. ~50-100 chunks/s needed, i.e. ~10-20x headroom — not a proper
percentile measurement, just a capacity statement.

Also established: Input≈Processing here is true *by construction* — over
any long-enough window with a flat queue (confirmed separately in every run
so far), arrivals and completions must converge. This says nothing about the
pipeline's actual capacity *ceiling* — only that it keeps up with *this
recording's* pacing (~0.27 jobs/s). That question is what motivated
experiments #10-11.

---

## 10. Speed sweep — natural recording

**Goal:** find the pipeline's actual throughput ceiling (not just "keeps up
with this one recording") by feeding audio faster than real-time.

**Built:**
- `--speed` on `src/edge_voice/utils/audio_generation/wav_source.py` —
  scales the real-time publish pacing (`expected = frame_num *
  frame_duration_s / speed`).
- `--speed` on `scratch/bench_pipeline_load.py`, passed through to
  `wav_source.main()`, surfaced in the TL;DR line along with the existing
  queue-depth-trend verdict.

**Method:** dual-channel (rx+tx, different real audio), shared `{2,3}`,
60s/level, speeds 1x-8x, `--csv-out` per level.

**Result:**

| Speed | Segments | Latency p50 | Latency p95 | CPU mean | Queue 1st→2nd | Peak queue | Verdict |
|---|---:|---:|---:|---:|---:|---:|---|
| 1x | 18 | 992 ms | 1999 ms | 37% | 0.00→0.00 | 0 | stable |
| 2x | 36 | 1133 ms | 2260 ms | 77% | 0.22→0.00 | 3 | stable |
| 3x | 57 | 1151 ms | 2309 ms | 119% | 0.14→0.10 | 2 | stable |
| **4x** | 77 | 1479 ms | 3764 ms | 191% | 0.55→0.59 | 7 | **stable (last)** |
| **5x** | 89 | 2073 ms | 16629 ms | 235% | 1.52→3.67 | 17 | **GROWING (crossover)** |
| 6x | 91 | 7265 ms | 24661 ms | 256% | 3.73→11.63 | 19 | GROWING |
| 7x | 91 | 11467 ms | 29027 ms | 259% | 8.93→20.39 | 31 | GROWING |
| 8x | 86 | 13740 ms | 31727 ms | 268% | 11.88→30.19 | 43 | GROWING |

**Ceiling-effect caveat found in an earlier 16x/32x probe** (not in the table
above): at very extreme speeds, queue depth saturates near its ~448 capacity
almost immediately and can't grow *further* within the run, so the
first-half/second-half verdict heuristic misleadingly reports "flat/draining"
— that's saturation, not health. Not an issue in the 1x-8x table above (all
GROWING rows were still mid-ascent when the run ended), but worth remembering
before trusting the verdict column at extreme multipliers.

**Correction made mid-analysis:** the CSV's `duration_s` field is derived
from wall-clock MQTT-ingest timestamps, so at `--speed N` it's compressed by
N× — it does **not** represent true audio-content duration once sped up.
Recomputed `true_duration = duration_s × speed` to get real content-seconds,
yielding a cleaner throughput metric:

| Speed | True audio-content-s decoded per wall-clock-s |
|---|---:|
| 1x | 0.812 |
| 2x | 1.755 |
| 3x | 2.799 |
| 4x | 3.670 |
| 5x | 4.051 |
| 6x | 3.962 |
| 7x | 4.041 |
| 8x | 3.853 |

Plateaus right at the crossover (~4.0), giving a cleaner capacity number than
"4x of this recording's pacing" — but still tied to this recording's ~2.7s
average utterance length and its own natural density. See #11 for the
cross-validation that made this a defensible general claim.

**Also discussed, not separately measured:** `--speed N` does not
pitch-shift or resample the waveform — it republishes identical PCM chunks
with N× less wall-clock delay between them. Functionally this is "the same
conversation, N times denser," not "a different, faster conversation" — it
scales density of *this* pattern, not utterance-length mix or any other
property a genuinely different denser recording might have.

---

## 11. Standard synthetic audio + cross-validated ceiling

**Goal:** get a density/ceiling number that isn't entangled with one
specific recording's pause structure — build a controlled, reproducible
test input instead.

**Built:** `scratch/make_standard_audio.py` — extracts one real speech
snippet from a source WAV and tiles `[snippet][silence gap]` to hit an exact
target silence ratio at any total duration:

```bash
python scratch/make_standard_audio.py \
    --source wav/tx_recorded_1.wav --snippet-start-s 3.05 --snippet-duration-s 2.35 \
    --silence-ratio 0.20 --total-duration-s 120 \
    --out wav/standard_20pct_silence.wav
```

**Bug caught during setup:** the first attempt extracted `tx_recorded_1.wav`
[0s, 2.37s] as the "utterance," assuming it was the file's opening speech
(based on misremembering an earlier segment's `duration_s` as a file
offset). Result: **zero VAD segments over 25s+** on the generated file.
Debugged by running Silero VAD directly (bypassing the pipeline) against
both the generated file (zero start/end events) and the known-good source
file (first real event: `start` at 3.104s) — the [0, 2.37s] window was a
non-speech connection tone, not the greeting. Re-extracted from the verified
real utterance boundary (Silero-confirmed `start=3.104s, end=5.376s`),
regenerated, and reverified clean, regular ~2.9s-cadence start/end events
before deploying.

**Method:** same standard file (20% silence, ~2.9s utterance+gap unit) on
both channels, shared `{2,3}`, 60s/level, speeds 1x-8x.

**Result:**

| Speed | n | jobs/s | content-s/wall-clock-s | p50 lat | p95 lat | Queue 1st→2nd | Peak q | Verdict |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1x | 87 | 1.450 | 2.948 | 1441 ms | 4587 ms | 1.02→0.23 | 5 | stable |
| **2x** | 76 | 1.267 | 3.071 | 1395 ms | 1554 ms | 0.05→0.34 | 4 | **stable (last)** |
| **3x** | 108 | 1.800 | 4.421 | 15368 ms | 26526 ms | 7.56→21.35 | 29 | **GROWING (crossover)** |
| 4x | 82 | 1.367 | 3.414 | 16540 ms | 31113 ms | 11.66→31.20 | 40 | GROWING |
| 5x | 96 | 1.600 | 3.969 | 23580 ms | 45904 ms | 21.83→67.15 | 192 | GROWING |
| 6x | 78 | 1.300 | 3.333 | 23309 ms | 44113 ms | 26.13→176.77 | 448 | GROWING |
| 7x | 88 | 1.467 | 3.729 | 26791 ms | 50844 ms | 35.75→406.00 | 448 | GROWING |
| 8x | 80 | 1.333 | 3.443 | 26354 ms | 49312 ms | 55.38→447.05 | 448 | GROWING |

**The finding:** this file starts denser at 1x (2.948 content-s/wall-clock-s,
since it's 80% speech vs. the natural recording's much sparser pacing), so it
collapses after just one more speed step (2x→3x) instead of the natural
recording's gradual climb. But the actual break happens between **2.948
(stable) and 4.421 (collapsed)** — the *same* ~4.0 content-s/wall-clock-s
ceiling found independently in #10 (which broke between 3.670 stable and
4.051 collapsed).

**Conclusion:** two structurally different audio inputs — a real bilingual
conversation with natural pauses, and a synthetic 20%-silence repeated
utterance — hit the same ceiling. That cross-validation is what makes "~3-4
content-seconds decoded per wall-clock-second" a defensible general claim
for this hardware/model/config (RPi5, 2 dedicated STT cores, `tiny-ko`
quantized), for utterances in the ~2-3s range — rather than a property of
one specific recording's pacing (which "4x real-time" alone would have been).

**Still scoped, not universal:** would still shift with a different
utterance-length distribution (per-call decode overhead vs. content-
proportional cost wasn't isolated), a different channel count (concurrent-
decoder scaling was already shown to be sub-linear in #3-4), or a different
model/language/hardware.

---

## Files touched this session

- `scratch/bench_pipeline_load.py` — added `--stt-core-map`, the
  `settings.mqtt.channels` filtering fix (#7), `--speed`, and the
  queue-depth verdict in the TL;DR line.
- `src/edge_voice/utils/audio_generation/wav_source.py` — added `--speed`.
- `scratch/make_standard_audio.py` — new; synthetic silence-ratio test audio
  generator.
- `wav/standard_20pct_silence.wav` — generated (gitignored, not committed;
  regenerate via the command in #11).
- `docs/BENCHMARK.md` — updated with experiment #1's results (VAD backend
  switch section).

## Open threads / natural follow-ups (not done this session)

- Confirm the memory-bandwidth-contention hypothesis from #4 directly (e.g.
  `perf stat` cache-miss counters, or a synthetic memory-bandwidth stressor
  run alongside one decoder).
- Isolate utterance-length dependence of the ~4.0 content-s/wall-clock-s
  ceiling (repeat #11 with a much shorter and a much longer snippet).
- Extend `--stt-core-map` findings / #11's standard-audio method to a 3rd or
  4th concurrent channel, to see how the ceiling scales past 2 decoders.
