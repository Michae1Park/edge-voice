# STT Multiprocessing — Design Plan

**Status:** Not started. Scoping only — no code written.
**Companion to:** `ARCHITECTURE.md` (per-channel STT decision), `RELIABILITY.md`
(supervisor/restart mechanics), `BENCHMARK.md` (the RPi5 numbers this plan is
built on)

## Goal

Replace the two per-channel `STTWorker` **threads** with per-channel STT
**processes**, so the two channels' decode calls genuinely run in parallel
instead of taking turns on the GIL.

---

## Why — confirmed, not assumed

The per-channel-decoder change already shipped (one `STTWorker` thread +
`Transcriber` per channel, see `ARCHITECTURE.md`) fixed the unbounded queue
growth that one shared decoder caused. But it did **not** deliver genuine
dual-channel throughput, and this is measured, not suspected:

1. **On the real dual-channel RPi5 pipeline** (`scratch/bench_pipeline_load.py`),
   decode calls alternate in lockstep: the next channel's decode starts the
   instant the other channel's decode ends, even though each channel has its
   own dedicated `Transcriber` and its own dedicated CPU cores
   (`--stt-cores 2,3`). A concrete example from that run: an `rx` decode ran
   ≈15.9s→19.8s; the very next `tx` segment (whose audio had already fully
   finished being spoken by ≈18.26s — 1.5s *before* its decode started) didn't
   start decoding until ≈19.75s, the moment `rx`'s decode finished. Since the
   two channels' speech is half-duplex (never overlaps), that gap can't be
   explained by audio timing — only by decode contention.

2. **Isolated confirmation, no pipeline involved**
   (`scratch/probe_gil_release.py`, run on the RPi5, 10 iterations/thread):
   two independent `Transcriber` instances, two threads, same call shape as
   production. Sequential (no threading at all): 24.98s for 20 calls.
   Concurrent (2 threads): 24.45s for the same 20 calls. **Speedup: 1.02x —
   no benefit.** (The script's own "overlap" metric read misleadingly high,
   23.92s of the 24.45s run — that turned out to measure whether the two
   threads' call *spans* overlap in wall-clock time, not whether they're
   actually computing concurrently; frequent GIL handoffs at Python/native
   boundaries during streaming decode make spans overlap even when the
   underlying compute is fully serialized moment-to-moment. Speedup is the
   number that answers the actual question.)

Both point to the same conclusion: the GIL (or an equivalent lock inside
`moonshine_voice`) serializes decode compute regardless of thread count, core
count, or `Transcriber` instance count.

**Why the current fix still works, and why it's not enough on its own:** it
works because total STT demand dropped under 100% of the real-time budget —
measured at ~65% (`200.4s` of decode work over a `310s` run) on the test
clip, mostly from removing CPU contention between STT and VAD via core
pinning. That's a margin win, not a capacity win. A denser conversation, or
a longer/more talkative session, can push demand back over 100% and
reproduce the exact same unbounded backlog, because two decoders taking
turns is not meaningfully more capacity than one decoder doing the same
total work.

---

## Design decisions (recommendations, not yet finalized)

**1. How orchestrator/Supervisor keep working with process-backed workers.**
`orchestrator.py` and `pipeline/supervisor.py` currently duck-type on
`threading.Thread`-shaped objects (`is_alive()`, `.stop()`, `.stopping`,
`.last_activity`, `_get_workers() -> list[threading.Thread]`). A
`multiprocessing.Process` has a different, narrower API. Recommend a thin
wrapper class (`STTProcessHandle` or similar) presenting the same
Thread-shaped interface but backed by a `multiprocessing.Process` — keeps
`orchestrator.py`'s existing `_w()`/`_get_workers()`/`SupervisedTarget`
wiring almost unchanged, rather than teaching those generic mechanisms
about two different worker shapes.

**2. IPC for segments (parent → child).** A `multiprocessing.Queue` per
channel, replacing that channel's `queue.Queue` at the STT boundary only —
VAD, router, and ingest stay exactly as they are (in-process threads,
`queue.Queue`). `SpeechSegment`'s `audio` field is raw PCM bytes for a few
seconds of speech at most; pickling cost is negligible next to
multi-hundred-millisecond-to-multi-second decode times, so no need for
shared memory.

**3. IPC for transcripts (child → parent).** A second `multiprocessing.Queue`
per channel, drained by a small receiver thread in the parent that
republishes into `TranscriptHub` — mirrors the current `_on_transcript`
callback, just fed by a queue-drain loop instead of a direct call.

**4. Process start method: `spawn`, not `fork`.** Linux defaults to `fork`,
which is cheap but unsafe here — `fork()`-ing a process that has already
loaded ONNX Runtime/native library state can corrupt that state in the
child. Use `multiprocessing.get_context("spawn")` explicitly. Costs more
startup time (a fresh interpreter, full re-import) — worth measuring against
the RPi5's boot/restart-time expectations (see `deploy/edge-voice.service`'s
restart policy in `RELIABILITY.md`).

**5. Model loading happens inside the child.** The `Transcriber`/ONNX
session can't be pickled from the parent — the child process's own entry
point must do its own model resolution and `Transcriber` construction at
startup, same work `STTWorker.__init__`/`_new_transcriber` already do today,
just triggered inside the subprocess instead of the parent.

**6. Cross-process logging needs its own design.** Today everything logs
in-process through one structured JSON logger (`observability/logging.py`).
A child process needs either a `QueueHandler` forwarding log records back to
the parent for formatting/output, or its own independent log
stream/aggregation. Not solved by this plan — flagged as a real subtask.

**7. Keep a thread-backed option for tests.** `STTWorker`'s existing
`transcriber_factory` injection point is what makes `tests/test_stt_worker.py`
fast and synchronous (`worker._handle_segment(segment)` called directly, no
thread, no process). That should keep working — recommend the process
wrapper be an *additional* way to run an `STTWorker`, not a replacement, so
unit tests keep testing the same pure `_handle_segment`/`_transcribe` logic
directly, and only integration-level tests need to exercise the real
process/IPC path.

---

## What does NOT need to change

`STTWorker._handle_segment`/`_transcribe`/`_is_repetitive`/`_pcm_to_float32`
— the actual decode logic — stays exactly as it is: pure, synchronous,
already unit-tested in isolation (`tests/test_stt_worker.py`). This plan
only changes *how* an `STTWorker` instance is driven (thread reading a
`queue.Queue` vs. process reading a `multiprocessing.Queue`) and how its
output gets back to the rest of the pipeline — not what happens once a
segment is handed to it.

---

## Likely blast radius

- New: a process wrapper/entry-point module (name TBD — maybe
  `stt/stt_process.py`) owning the `spawn`, the child-side model load, and
  the two `multiprocessing.Queue`s.
- `pipeline/orchestrator.py`: `_build_stt` builds a process-backed worker
  instead of (or alongside, if kept configurable) a thread-backed one; a
  new receiver thread drains each channel's outbound transcript queue.
  `_get_workers()`/`_build_stt_supervisor_targets()`/`_restart_dict_worker`
  need to work with the wrapper's Thread-shaped interface (see decision 1)
  — ideally little to no change if the wrapper is faithful enough.
  `_stt_latency_s`/`_stt_channel_latencies_s` need their data to arrive via
  IPC too (today they read worker attributes directly in-process).
  `docs/ARCHITECTURE.md`/`docs/RELIABILITY.md` will need another rewrite
  pass, same as the per-channel-decoder change needed.
- `config/settings.py`: possibly a toggle (`stt.use_processes` or similar)
  if decision 7's thread-backed test path stays configurable rather than
  hardcoded to tests only.
- Nothing in `vad/vad_worker.py`, `channel/router.py`, or
  `audio_ingest/mqtt_client.py` — this is scoped to the STT boundary only.

---

## Verification plan

1. **Isolated first, before touching the orchestrator.** Adapt
   `scratch/probe_gil_release.py` to use `multiprocessing.Process` instead
   of `threading.Thread` for the same two-`Transcriber` test. Confirm
   speedup lands near 2x (genuine parallelism) before integrating —
   cheap, fast feedback, same script shape already proven useful.
2. **Then integrate**, and rerun `scratch/bench_pipeline_load.py` on the
   RPi5 the same way as before. Two things to check, not just one:
   queue depth trend should stay flat (already true today, so this is a
   regression check, not a new result), **and** CPU utilization during
   concurrent speech should show sustained >150-200% (real two-core work),
   not the spiky-but-mostly-single-core pattern the current thread-based
   version shows.
3. Re-check memory: two full processes (not just two `Transcriber`
   instances in one process) means duplicated Python interpreter + torch +
   onnxruntime runtime overhead on top of the per-model cost already
   measured (`scratch/probe_decoder_memory.py`, ~56MB marginal per
   `Transcriber`). That per-process baseline overhead hasn't been measured
   yet and matters more here than it did for the threaded version.

---

## A nice side-effect, not the goal

`RELIABILITY.md` documents that Python threads can't be force-killed — a
wedged `STTWorker` thread becomes a permanent zombie until a full process
restart (the OS watchdog layer). A `multiprocessing.Process` *can* be
force-killed (`SIGKILL`) if it stops responding, which would let the
in-process supervisor actually clear a wedged STT worker instead of
degrading and waiting on the OS watchdog. Worth calling out when this gets
built, not a reason to build it on its own.

---

## Open questions to resolve during implementation, not before

- Exact wrapper class shape (decision 1) — design once the real
  `SupervisedTarget`/`_w()` call sites are being touched, not speculatively
  now.
- Whether `spawn`'s startup cost is acceptable for the systemd restart
  policy, or whether it needs a startup-time budget check first.
- Logging design (decision 6) — genuinely unsolved, needs its own short
  scoping pass.
