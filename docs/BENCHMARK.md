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
| --------- | ------- | -------- | ------- | -------- | ------- | -------- |
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

---



## Open items for future entries

- End-to-end latency (mic → transcript) under the real pipeline, not this
isolated STT-only harness.
- RAM/CPU footprint on RPi5 under sustained dual-channel load.
- `tiny` vs `base` latency/accuracy tradeoff, per language.
- Streaming-model (`tiny-streaming-en` etc.) comparison — `bench_streaming_cost.py`
experiment 2/3 have this wired up but commented out (see
`docs/STREAMING_STT_PLAN.md`, currently on hold).
- Remaining shipped `tiny-*` checkpoints (`ja`, `ar`, `uk`, `vi`) not yet
benchmarked — expected to follow the same ~1.9x-decoder pattern as `ko`/`zh`
based on cache inspection, but unconfirmed against real audio.

---



## Analysis



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

