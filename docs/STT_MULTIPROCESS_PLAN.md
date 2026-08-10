# STT Multiprocessing — Build Plan

**Status: BUILT (2026-08-10).** Implemented in `src/edge_voice/stt/stt_process.py` plus the orchestrator/logging/UI changes below; `settings.stt.use_processes` defaults to `True`. Steps 0-6 are done and verified on the dev box. **Step 7 — the RPi5 before/after measurement in §11 — has NOT been run and is the remaining work.**

Dev-box results so far, for reference (x86, 32 cores — validates the mechanism, does not predict the Pi):
- Gate 0: **84% compute overlap, 1.70x** speedup with processes, vs 1.02x with threads on the RPi5.
- End to end: two children spawn, transcribe correctly, per-channel latencies flow back over IPC, child logs reach the parent's JSON handler, Ctrl-C shuts down in **2.5s** with no child tracebacks and no join stalls.
- A SIGSTOPped (wedged) child is **reclaimed** by `kill()` — the reliability upgrade over threads, which could only ever abandon a wedged worker.
- RSS rose from **~699MB (threads) to ~1215MB (processes)** — two extra interpreters plus duplicated ONNX Runtime. Fits the RPi5's ~2.6GB idle headroom, but is a real cost to re-measure there (§11.6).

Everything below is the plan as approved; decisions are settled, not to be relitigated.
**Goal:** Use all 4 RPi5 cores by running **one OS process per channel's STT worker**, so the two channels decode genuinely in parallel instead of taking turns on the GIL.
**Companion docs:** `ARCHITECTURE.md` (per-channel STT decision), `RELIABILITY.md` (supervisor/restart), `BENCHMARK.md` (the RPi5 numbers this is built on), `THREADS.md` (thread inventory).

---

## 1. Settled decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | **STT only** moves to processes. Ingest, router, and VAD stay as threads in the main process. | Everything upstream of STT is far under budget; see §3. |
| D2 | **One process per channel** (2 processes for rx/tx), each owning its own `Transcriber`. | Matches the already-shipped per-channel worker split; each process is a known-good single-channel workload. |
| D3 | IPC is **`multiprocessing.Queue`**, one inbound + one outbound per channel. | Duck-types `queue.Queue`, so `vad_worker.py` needs **zero changes**. Measured cost: 0.039ms per 5s segment. |
| D4 | Start method is **`spawn`**, set explicitly. | `fork()` after ONNX Runtime has loaded native state is unsafe. |
| D5 | Transcript ordering is **not** enforced in the pipeline. The UI inserts by timestamp. | Buffering to enforce order would trade away the throughput this whole change buys. See §7. |
| D6 | VAD **stays a thread for now**, re-measured after the STT move lands. | The STT move is expected to make VAD faster for free; measuring before it would measure the wrong thing. See §8. |
| D7 | The existing thread-backed `STTWorker` class **stays**, behind a config toggle. | Keeps unit tests fast and synchronous, and gives a one-line rollback if the Pi surprises us. |

---

## 2. Why — confirmed by measurement, not assumed

The already-shipped per-channel **thread** split fixed unbounded queue growth but did **not** deliver parallelism:

1. **On the real RPi5 pipeline** (`scratch/bench_pipeline_load.py`): decode calls alternate in lockstep. An `rx` decode ran ≈15.9s→19.8s; the next `tx` segment — whose audio finished being spoken at ≈18.26s, 1.5s *before* its decode began — didn't start decoding until ≈19.75s, the exact moment `rx` finished. The two channels are half-duplex (speech never overlaps), so that gap can only be decode contention.

2. **Isolated confirmation** (`scratch/probe_gil_release.py`, on the RPi5, 10 iterations/thread): two independent `Transcriber` instances, two threads, production call shape.

   | | Wall clock, 20 calls |
   |---|---|
   | Sequential (no threads) | 24.98s |
   | Concurrent (2 threads) | 24.45s |
   | **Speedup** | **1.02x — no benefit** |

   *(That script's "overlap" metric reads misleadingly high — it measures whether call **spans** overlap in wall-clock time, not whether compute is concurrent. Frequent GIL handoffs at Python/native boundaries during streaming decode make spans overlap even when compute is fully serialized. Speedup is the number that answers the question.)*

**Conclusion:** the GIL (or an equivalent lock inside `moonshine_voice`) serializes decode compute regardless of thread count, core count, or `Transcriber` instance count. Separate OS processes are the only mechanism that removes it. Free-threaded Python is not a viable alternative today — ONNX Runtime force re-enables the GIL on 3.13t builds (open upstream issue), and `moonshine-voice` ships no free-threaded wheel.

### What the current fix actually bought, and why it isn't enough

It works by keeping total STT demand **under** the real-time budget — measured at 65% (200.4s of decode over a 310s run), mostly from removing STT/VAD CPU contention via core pinning. That is a **margin**, not capacity. A denser conversation pushes demand back over 100% and reproduces the same unbounded backlog, because two decoders taking turns is not more capacity than one.

### The capacity this change unlocks (same RPi5 run, recomputed per core)

| | Decode work | Utilization if truly parallel |
|---|---:|---:|
| rx (40 segments × 3091.7ms) | 123.7s | **40%** of its core |
| tx (52 segments × 1475.0ms) | 76.7s | **25%** of its core |
| Both, serialized (today) | 200.4s | 65% of the single shared budget |

Post-split the busiest STT core sits near 40% — roughly **2.5x headroom** on the worst channel, versus the razor-thin margin today.

### Target core allocation (4 cores)

| Cores | Occupant |
|---|---|
| 0-1 | Main process: MQTT ingest, router, VAD, webui, supervisor, metrics |
| 2 | STT process — channel `rx` |
| 3 | STT process — channel `tx` |

Set `MOONSHINE_ORT_SINGLE_THREAD=1` so each STT process stays on its one core rather than trying to spread across both (measured on the RPi5: multi-threading one decoder was *slower* than single-threaded — sync overhead exceeds the benefit at this model size).

---

## 3. Why not a fully separate pipeline per channel

Considered and rejected as the first move. Splitting *everything* per channel (own ingest + router + VAD + STT per process) would remove cross-channel serialization completely — but there is almost none left to remove outside STT:

| Stage | Measured per-call cost (dev box) | Runs | Aggregate |
|---|---:|---|---|
| Router repacketize | ~6µs | per packet | negligible |
| VAD (Silero) | ~293µs | per packet, ~52% skipped by the RMS gate | small fraction of a core |
| **STT decode** | **64-125ms (dev) / 1475-3092ms (Pi)** | **per segment** | **the entire bottleneck** |

Costs of the full split, against ~zero throughput gain:

- **Memory roughly doubles.** An STT-only child needs `numpy` + `moonshine_voice` + ONNX Runtime — it imports **no torch** (verified: `stt_worker.py` imports only `numpy` and, lazily, `moonshine_voice`). A full-pipeline child would additionally load torch + torchaudio + Silero. Current whole-process RSS on the Pi is ~546MB with a ~484MB floor; two full pipelines land near 1GB, two STT-only children add far less.
- Two MQTT connections, two routers, two of every failure mode, for no measured benefit.
- Ordering is *not* the differentiator — cross-channel inversion is possible either way (§7).

**Non-obvious bonus that makes STT-only the better boundary:** because moonshine's decode holds the GIL, `VADWorker` — a thread in the same process — is currently **blocked for the entire duration of every decode**. Moving STT out doesn't just parallelize STT; it hands the main process's GIL back to VAD/router/ingest. Splitting further buys little on top of that.

**Keep the full split as a documented fallback** if the channel count ever grows past 2, or if §8's post-move VAD measurement shows VAD saturating.

---

## 4. Scope

### Changes

| File | Change |
|---|---|
| `src/edge_voice/stt/stt_process.py` | **New.** Child entry point + `STTProcessHandle` + IPC message types. |
| `src/edge_voice/pipeline/orchestrator.py` | Build mp queues, spawn handles, run per-channel receiver threads, source metrics from received results. |
| `src/edge_voice/observability/logging.py` | Add child-side `QueueHandler` setup + parent-side `QueueListener`. |
| `src/edge_voice/pipeline/transcript_hub.py` | Replay backlog sorted by `start` (§7). |
| `src/edge_voice/config/settings.py` | `stt.use_processes: bool = True`. |
| `src/edge_voice/webui/templates/console.html` | Insert by timestamp instead of always appending (§7). |
| `docs/ARCHITECTURE.md`, `docs/RELIABILITY.md`, `docs/THREADS.md` | Rewrite the STT sections once built. |

### Explicitly unchanged

- **`src/edge_voice/vad/vad_worker.py` — zero changes.** `multiprocessing.Queue` duck-types `queue.Queue` for `put`/`put_nowait`/`get`/`qsize` and raises the same `queue.Full`. The orchestrator simply hands it mp queues instead.
- **`STTWorker._handle_segment` / `_transcribe` / `_is_repetitive` / `_pcm_to_float32`** — the decode logic itself. Pure, synchronous, already unit-tested. This plan changes only *how a worker is driven* and *how its output gets home*.
- `channel/router.py`, `audio_ingest/mqtt_client.py`.

---

## 5. Component specs

### 5.1 IPC contract (per channel)

| Object | Type | Direction | Purpose |
|---|---|---|---|
| `segment_queue` | `mp.Queue[SpeechSegment]` | parent → child | Work in. Replaces this channel's `queue.Queue`. |
| `transcript_queue` | `mp.Queue[SttResult]` | child → parent | Results out. |
| `stop_event` | `mp.Event` | parent → child | Graceful stop signal. |
| `heartbeat` | `mp.Value("d")` | child → parent | `time.monotonic()` of last loop iteration, for supervisor stall detection. |
| `ready_event` | `mp.Event` | child → parent | **Set once the model is loaded and the consume loop is entered.** Load-bearing — see §5.6. |
| `log_queue` | `mp.Queue[logging.LogRecord]` | child → parent | Shared by both children (§5.4). |

Both payload types are already `@dataclass(slots=True)` and verified picklable. A 5s segment pickles to 160KB in **0.039ms** round-trip — ~0.001% of a 3s decode. No shared memory needed.

```python
@dataclass(slots=True)
class SttResult:
    event: TranscriptEvent
    # None for partials, matching today's rule that _handle_partial never
    # touches last_latency_s (see stt_worker.py). Finals always carry it.
    stt_latency_s: float | None
```

### 5.2 `STTProcessHandle` — the Thread-shaped wrapper

`orchestrator.py` and `supervisor.py` duck-type on `threading.Thread`. This wrapper presents that exact surface so `_get_workers()`, `_signal()`, `_join()`, `_restart_dict_worker()`, and `SupervisedTarget` keep working with minimal change.

| Member | Backed by | Notes |
|---|---|---|
| `name` | `f"STTWorker-{channel_id}"` | Must match `SupervisedTarget.name`, or `get_status()` won't merge supervisor state. |
| `ident` | `self._proc.pid` | `orchestrator._join()` checks `ident is None` to detect never-started — mapping to pid keeps that unchanged. |
| `start()` | `Process.start()` | |
| `join(timeout)` | `Process.join(timeout)` | |
| `is_alive()` | `Process.is_alive()` | |
| `stop()` | `stop_event.set()` | |
| `stopping` | `stop_event.is_set()` | |
| `last_activity` | `heartbeat.value`, **but see §5.6** | **Cross-process safe:** verified `time.monotonic()` is comparable across processes on Linux (CLOCK_MONOTONIC is system-wide). Linux-only, consistent with existing repo assumptions. |
| `ready` | `ready_event.is_set()` | New. Gates stall detection during model load (§5.6). |
| `kill()` | `Process.kill()` | **New capability** — see §10. |
| `exitcode` | `Process.exitcode` | New. Log it on unexpected death — a segfault shows as `-11`, a diagnostic threads never gave us. |

Widen `_get_workers() -> list[threading.Thread]` to a small `Protocol` (or `list[Any]`) covering `name`/`ident`/`start`/`join`/`is_alive`/`stop`.

### 5.3 Child entry point

Must be a **module-level function** — `spawn` pickles the target by qualified name. A closure, lambda, or bound method will fail at `Process.start()`. *(This is not hypothetical: it was hit while validating this plan — `spawn` could not resolve a target defined in a `python -c` `__main__`.)*

```python
def stt_child_main(
    channel_id: str,
    config: STTWorkerConfig,     # picklable dataclass of plain values
    segment_queue,               # mp.Queue[SpeechSegment]
    transcript_queue,            # mp.Queue[SttResult]
    stop_event,                  # mp.Event
    heartbeat,                   # mp.Value("d")
    log_queue,                   # mp.Queue[logging.LogRecord]
) -> None:
```

Child responsibilities, in order:
1. Install the `QueueHandler` on the root logger (§5.4). Do this **first**, so model-load failures are visible.
2. Construct an `STTWorker` locally — this resolves and loads the model in-process (`Transcriber`/ONNX sessions are not picklable, so the parent cannot pass one in). Same work `STTWorker.__init__`/`_new_transcriber` does today.
3. Loop: `segment_queue.get(timeout=…)` → update `heartbeat.value` → `worker._handle_segment(segment)` → put `SttResult` on `transcript_queue`.
4. Exit cleanly when `stop_event` is set or a `None` sentinel arrives.

The module must have **no import-time side effects** — `spawn` re-imports it in the child.

### 5.4 Cross-process logging

Standard `logging.handlers` pattern, no invention required:

- **Child:** root logger gets a single `QueueHandler(log_queue)`. Records are pickled to the parent unformatted.
- **Parent:** one `QueueListener(log_queue, *existing_handlers)` started in `configure_logging()`, fanning records into the existing console/file/JSON handlers.

This preserves `JsonFormatter`'s structured fields (`stage`, `channel_id`, `segment_id`, `stt_latency_s`) because formatting still happens in the parent, against the same handlers. One shared `log_queue` for both children is fine.

**Keep the `TRANSCRIPT` log line in the parent**, emitted by the receiver thread (§5.5) — not the child. It is documented in `orchestrator.py` as the segment-lifecycle closing line, it must stay adjacent and in-order with the `TranscriptHub.publish()` call, and keeping it there preserves `test_final_transcript_still_emits_the_closing_log_line` unchanged.

### 5.5 Receiver thread + metrics

One thread per channel in the parent, draining `transcript_queue`:

```
result = transcript_queue.get(timeout=…)
  ├─ if result.stt_latency_s is not None:
  │     update parent-side _last_latency_s + _channel_latency_s[channel_id]
  ├─ emit the TRANSCRIPT / PARTIAL log line (moved verbatim from _on_transcript)
  └─ self._transcript_hub.publish(result.event)
```

This is why `SttResult` carries the latency: `_stt_latency_s()` and `_stt_channel_latencies_s()` currently read worker attributes in-process, which is impossible across a process boundary. Piggybacking on the result needs no extra IPC and keeps latency naturally paired with the segment it belongs to.

`queue_depths()`: `mp.Queue.qsize()` works on Linux (verified) and is approximate; it raises `NotImplementedError` on macOS. Acceptable — already documented as context, not a precise reading.

### 5.6 Startup readiness — prevents a supervisor crash-loop

**This is a real bug if skipped, not a nicety.** The numbers collide:

| Quantity | Value | Source |
|---|---|---|
| `reliability.stall_timeout_s` | **10.0s** | `config/settings.py` default |
| Child `Transcriber` load, RPi5 | **3.6s / 7.3s** (1st / 2nd instance) | `scratch/probe_decoder_memory.py` output |
| Plus `spawn`: fresh interpreter + import numpy + moonshine + ORT dlopen | **unmeasured, seconds on a Pi** | — |

The supervisor stall-restarts a target when `is_alive() and not stopping and input_pending() and now - last_activity() > stall_timeout_s`. During a child's model load: it *is* alive, it is *not* stopping, segments *are* piling up in its queue (so `input_pending()` is True), and `heartbeat` has not advanced. If load + spawn overhead exceeds 10s, the supervisor kills it, the replacement takes just as long, and it trips again — **an infinite restart loop terminating in DEGRADED, on first boot.**

**Required handling:**
- Child sets `ready_event` immediately after its model is loaded and before entering the consume loop.
- `STTProcessHandle.last_activity` returns `time.monotonic()` (i.e. "active right now") while `not ready_event.is_set()`, so stall detection cannot fire during load.
- Add a separate, generous **startup timeout** so a child that never becomes ready is still caught rather than hidden forever. Log distinctly from a stall — "never became ready" and "stopped making progress" are different faults.
- `orchestrator.start()` should wait for both children's `ready_event` (with that timeout) before returning, preserving today's invariant that models are loaded off the real-time path.

**Knock-on:** `build()` no longer loads models eagerly (the child does, after `start()`). `tests/test_orchestrator.py::test_build_warms_up_stt_transcriber` asserts `w._transcriber is not None` after `build()` — it must be rewritten against `ready_event` after `start()` instead. The *intent* it protects (no model load on the real-time path) is preserved by the readiness wait; only the mechanism moves.

### 5.7 Shutdown ordering — prevents a join deadlock

**Empirically confirmed on this machine, not theoretical.** A child that has put items on an `mp.Queue` **will not exit until those items are flushed**, so `join()` on an undrained queue hangs:

```
3. join() HUNG 5.0s with 20 undrained items -> DEADLOCK CONFIRMED
   after draining, join() returned
```

`orchestrator.stop()` signals and joins each worker in `_get_workers()` order with `WORKER_JOIN_TIMEOUT_S = 10`. If an STT child still has undrained transcripts, that join burns the full 10s and then falsely logs a zombie warning — per child.

**Required handling:**
- The per-channel **receiver thread must outlive its child.** It is the consumer of the child's output, so `stop()`'s existing "producers before their consumers" rule puts the child first and the receiver *after* it — the opposite of the intuitive "stop the reader first."
- Drain `transcript_queue` until the child has exited, then stop the receiver thread.
- **Do NOT call `cancel_join_thread()` on the transcript queue.** An earlier draft of this plan recommended it as belt-and-braces; building Step 0 proved that wrong. It lets the process exit *without flushing*, which silently **discards** queued results and hangs the parent's `get()` — it cost a 300s timeout to diagnose in `scratch/probe_mp_speedup.py`. Draining before joining is the correct and sufficient fix. Reserve `cancel_join_thread()` for emergency teardown paths where losing data is already accepted.
- `log_queue` deadlocks identically. Same fix: keep the `QueueListener` running until the children have exited.

---

## 6. Build steps

Each step is independently verifiable. **Do not skip Step 0** — it is the cheap gate on the whole plan.

### Step 0 — Prove it in isolation (no `src/` changes)
Adapt `scratch/probe_gil_release.py` into `scratch/probe_mp_speedup.py`: same two-`Transcriber` test, `multiprocessing.Process` instead of `threading.Thread`.

**Acceptance:** speedup ≥ 1.7x on the RPi5. If it comes back near 1.0x, **stop** — the contention is not the GIL and this plan's premise is wrong.

### Step 1 — `stt/stt_process.py`: message types + child entry point
`SttResult`, `stt_child_main`. Includes `ready_event` signalling (§5.6), `SIGINT` ignore (§12), and `cancel_join_thread()` on exit (§5.7). No orchestrator wiring yet.

**Acceptance:** a standalone script spawns one child, pushes a `SpeechSegment`, receives a matching `SttResult` with non-empty text and a plausible `stt_latency_s`. Record the wall-clock time from `start()` to `ready_event` — that number sizes the readiness timeout in Step 4.

### Step 2 — `STTProcessHandle`
The Thread-shaped wrapper (§5.2).

**Acceptance:** unit test asserting `start`/`is_alive`/`stop`/`join` lifecycle, that `ident` is `None` before start and a pid after, that `last_activity` advances while the child loops, and — specifically — that `last_activity` reports "just active" while `ready_event` is unset, so a slow model load cannot look like a stall (§5.6).

### Step 3 — Cross-process logging
`QueueHandler`/`QueueListener` (§5.4).

**Acceptance:** a child-emitted log line reaches the parent's configured handlers with its structured fields intact under `JsonFormatter`.

### Step 4 — Orchestrator wiring
mp queues in `build()`, handles via `_build_stt(channel_id)`, per-channel receiver threads, metrics from `SttResult`, `stt.use_processes` toggle, readiness wait in `start()` (§5.6), and shutdown ordering in `stop()` (§5.7).

Also in this step, not later:
- **Fix `scratch/bench_pipeline_load.py`** — pin by `Process.pid` not `Thread.native_id`, and read `orch._stt` as handles. It is the tool that verifies everything downstream, and it broke the same way on the last STT refactor.
- **Decide the production pinning path** — settings knob applied in `start()`, or systemd `CPUAffinity=`. The core allocation in §2 must exist somewhere other than a benchmark flag.

**Acceptance:** full test suite green with `stt.use_processes` both `True` and `False`; `edge-voice` runs end to end against the wav pair; `/api/status` shows both `STTWorker-rx`/`STTWorker-tx` running with distinct per-channel latencies; Ctrl-C shuts down cleanly with no child tracebacks and no 10s join stalls.

### Step 5 — Supervisor integration
Wire `SupervisedTarget` to the handle. Add kill-escalation to `_restart_dict_worker`: after `join(timeout)` fails, call `kill()` instead of logging a zombie warning. Log `exitcode` on unexpected death.

**Acceptance:** `scratch/demo_supervisor.py` still demonstrates stall → restart, and the wedged STT worker is now genuinely **reclaimed** rather than leaked (§10). Separately: start the pipeline with audio already flowing and confirm **no** stall-restart fires during the children's model load (§5.6) — this is the crash-loop regression test.

### Step 6 — UI ordering
Timestamp insertion + sorted backlog replay (§7). Preserve scroll position when inserting above the viewport: adjust `scrollTop` by the inserted element's height, or the view jumps.

### Step 7 — Measure on the RPi5
Run the full before/after protocol in §11, including the §11.5 lockstep check — not just the summary numbers.

---

## 7. Transcript ordering

**Within a channel, order is guaranteed** — one worker, one FIFO queue, before and after this change. Only *cross-channel* order can invert.

That is **already true today** with two independent thread workers; multiprocessing only makes inversions more likely, since decodes genuinely overlap. Concretely: a long `rx` utterance decoding for 3s can be overtaken by a short `tx` utterance decoding in 1s, landing the later-spoken line first.

**Decision (D5): do not fix this in the pipeline.** Buffering to enforce global order reintroduces exactly the head-of-line waiting this change exists to remove — the UI would stall on the slowest channel, converting a throughput win back into latency.

**Fix it in the UI instead — insert by timestamp, don't append.**

- Every `TranscriptEvent` already carries `start`, `end`, and `created_at`. No new data needed.
- `console.html`'s `appendMessage()` currently always does `inner.insertBefore(wrap, cursorRow)`. Change to: store `start` on the element, then walk back from `cursorRow` to find the first message with a smaller `start` and insert after it.
- **Partials make this work better than it sounds.** A partial for a long utterance arrives ~1s in (`vad.partial_interval_s`) and claims its chronological slot early; the final then replaces it *in place* via the existing `segment_id` keying in `applyMessage()`. So the common case is resolved before an inversion can even become visible.
- `transcript_hub.py`: replay the backlog sorted by `start` on `subscribe()`, so a reconnecting client sees the same order as a live one.

**Honest limits** (worth knowing, not blockers):
- A very late arrival inserts *above* content the reader may have already passed. Mitigate with a brief highlight on inserted-not-appended messages, and/or cap insertion depth — beyond N messages back, append instead.
- Auto-scroll logic keys on "near bottom"; an insertion above the fold must not trigger a scroll jump.
- Logs and any future persistence are unaffected — every event carries `channel_id` + timestamps, so any consumer can sort.

---

## 8. VAD: measure after, not before

**Do not give VAD its own process in this change.** Two reasons, one of which is easy to miss:

1. **Cost is small.** Per call VAD is ~293µs against STT's 64-125ms (dev box) — and the RMS gate already skips ~52% of Silero forward passes on duplex audio. Per *call* VAD is indeed the second-heaviest module, but core allocation should follow **aggregate CPU share**, and by that measure VAD is a small fraction of one core. Dedicating a core to it would leave that core mostly idle.

2. **Measuring now would measure the wrong thing.** VAD is currently GIL-blocked for the whole duration of every STT decode (§3). Moving STT out raises VAD's effective throughput *without touching VAD*. Any pre-move measurement is contaminated by contention that is about to disappear.

**Known gap, stated honestly:** there is no clean RPi5 measurement of VAD's aggregate CPU share. The 300s run showed 125.5% mean CPU against ~65% wall-clock STT decode, and the remainder is not cleanly attributed — that run did not set `MOONSHINE_ORT_SINGLE_THREAD`, so ORT intra-op threading could account for much of it. This is a measurement to take, not a number to assume.

**After Step 7, decide with data.** If VAD shows sustained high utilization, the natural next step is **per-channel VAD processes** — VAD state is already fully per-channel (`_channels` dict keyed by `channel_id`, own Silero instance each), so that split is clean. Track it as a follow-up, not part of this build.

---

## 9. Testing

| Test file | Change |
|---|---|
| `tests/test_stt_worker.py` | **None.** Keeps testing `_handle_segment` directly via `transcriber_factory` — the whole reason for D7. |
| `tests/test_stt_process.py` | **New.** `SttResult` round-trip; `STTProcessHandle` lifecycle; one real spawn→segment→result integration test (mark `integration` — it loads a model). |
| `tests/test_orchestrator.py` | Update for handles instead of threads. Add: receiver thread updates per-channel latency from `SttResult`. |
| `tests/test_vad_worker.py`, `tests/test_partial_transcripts.py` | **None expected** — mp queues duck-type `queue.Queue`. Confirm rather than assume. |
| `tests/test_webui_app.py` | Worker-name set unchanged (`STTWorker-rx`/`-tx`); check `queue_depths` keys still resolve. |

Keep `stt.use_processes=False` for the default unit-test path so the suite stays fast and does not spawn interpreters.

---

## 10. Reliability upgrade (a real win, not just a side-effect)

`RELIABILITY.md` documents that a wedged Python thread **cannot be force-killed** — it becomes a permanent zombie holding its input queue until the OS watchdog restarts the whole process. `scratch/demo_supervisor.py` demonstrates exactly this today.

A `multiprocessing.Process` **can** be killed. Step 5 should escalate: `stop()` → `join(timeout)` → `kill()`. That converts the worst STT failure mode from "degrade and wait for a full process restart" into "reclaim the worker in-process," which is a genuine reliability improvement over the thread design and should be called out in `RELIABILITY.md` when built.

---

## 11. Verification — exact before/after protocol

All runs on the **RPi5**, never a dev box. Reboot first (`sudo reboot`) so swap is clear and RSS baselines are honest — see `BENCHMARK.md` for why a dirty swap made an earlier "idle" reading meaningless.

### 11.1 Capture the BEFORE baseline

Run on the current thread-backed HEAD. Tag it first so returning is trivial:

```bash
cd ~/workspace/edge-voice
git tag baseline-threads          # return anytime with: git checkout baseline-threads
git rev-parse --short HEAD | tee /tmp/ev-before-sha.txt
mkdir -p /tmp/ev

# B0. Idle memory floor (after reboot, nothing running)
free -h | tee /tmp/ev/before-mem-idle.txt

# B1. Isolated: what do two THREADS buy? (known answer: ~1.02x)
python scratch/probe_gil_release.py --iterations 10 2>&1 | tee /tmp/ev/before-probe.log

# B2. Full pipeline, UNPINNED — the fair apples-to-apples baseline
python scratch/bench_pipeline_load.py --duration-s 300 --grace-s 5 \
    --csv-out /tmp/ev/before-unpinned.csv 2>&1 | tee /tmp/ev/before-unpinned.log

# B3. Full pipeline, best-known thread config (pinned + single-thread ORT)
MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \
    --stt-cores 2,3 --other-cores 0,1 --duration-s 300 --grace-s 5 \
    --csv-out /tmp/ev/before-pinned.csv 2>&1 | tee /tmp/ev/before-pinned.log
```

Budget ~15 min. **Already-known baseline numbers** (from the 2026-08-10 pinned run, ORT *not* single-threaded) if a rerun isn't worth the time — but a fresh capture with logs on disk is better for diffing:

| Metric | Value |
|---|---|
| Segments / p50 / p95 / max latency | 92 / 2379ms / 4316ms / 4504ms |
| `stt_ms` mean | 2177.9ms |
| `pre_ms` mean / p95 | 437.8ms / 1931.1ms |
| CPU mean / max | 125.5% / 269.8% |
| RSS mean / peak | 546MB / 548MB |
| Queue trend | 0.52 → 0.72, peak 6 (flat) |

### 11.2 Gate 0 — before writing any `src/` code

```bash
python scratch/probe_mp_speedup.py --iterations 30 2>&1 | tee /tmp/ev/after-probe.log
```

The probe sets `MOONSHINE_ORT_SINGLE_THREAD=1` itself, matching the deployed
config. **This is load-bearing, not tidiness:** without it each process spawns
its own full ORT thread pool and they oversubscribe the machine — measured
**0.73x** (worse than sequential) on a 32-core dev box versus **1.69x** with
it. Running the gate without the env var produces a false FAIL. `--no-single-thread`
reproduces that if you want to see it.

Judge on **two** signals, because they answer different questions:

| Signal | Question | Threshold |
|---|---|---|
| **Overlap %** | *Did* the two processes compute simultaneously? Direct evidence the GIL is gone. | ≥80% |
| **Speedup** | How much throughput did that buy? | ≥1.4x |

| Result | Action |
|---|---|
| Overlap ≥80% **and** speedup ≥1.4x | **Proceed to Step 1.** |
| Overlap ≥80%, speedup <1.4x | Parallelism works but something below the CPU (memory bandwidth, shared cache) is the real limit. Investigate; payoff will be small. |
| Overlap <80% | **STOP.** Still serializing. Check `MOONSHINE_ORT_SINGLE_THREAD` first, then re-scope. |

**Dev-box result, 2026-08-10** (x86, 32 cores — *not* the target, but it validates the mechanism and the script): **84% overlap, 1.70x**, versus 1.02x for two threads. Note it plateaus near 1.6-1.7x rather than 2x even at high overlap: each call runs ~24% slower when concurrent, consistent with memory-bandwidth/shared-cache contention on a small memory-bound model. **Expect real-world gains to track ~1.6-1.7x, not 2x** — still a large win over 1.02x, but the §2 capacity table's "perfect parallelism" numbers should be read as an optimistic bound. The RPi5 (4 cores, far less memory bandwidth) may show this more strongly; its number is the one that counts.

### 11.3 Capture the AFTER runs

```bash
# A1. Full pipeline, UNPINNED — isolates the multiprocessing effect vs B2
python scratch/bench_pipeline_load.py --duration-s 300 --grace-s 5 \
    --csv-out /tmp/ev/after-unpinned.csv 2>&1 | tee /tmp/ev/after-unpinned.log

# A2. Production config: pinned processes + single-thread ORT (vs B3)
MOONSHINE_ORT_SINGLE_THREAD=1 python scratch/bench_pipeline_load.py \
    --stt-cores 2,3 --other-cores 0,1 --duration-s 300 --grace-s 5 \
    --csv-out /tmp/ev/after-pinned.csv 2>&1 | tee /tmp/ev/after-pinned.log

# A3. Memory under load — parent + both children, not just this process
free -h | tee /tmp/ev/after-mem-load.txt
ps -o pid,rss,comm -p $(pgrep -d, -f edge.voice) | tee /tmp/ev/after-mem-procs.txt
```

> `bench_pipeline_load.py`'s `--stt-cores` currently pins **threads** via `native_id`. It must be updated to pin **processes** via `pid`, and to read `orch._stt` as handles — this is a required deliverable of Step 4, not an afterthought. The same script broke on the last STT refactor for the same reason.

### 11.4 What to compare, and what each number means

Compare B2↔A1 (unpinned, isolates multiprocessing) and B3↔A2 (production config).

| Metric | Before | Pass | Why this number |
|---|---|---|---|
| **CPU mean** | 125.5% | **sustained >150%**, ideally ~200% during concurrent speech | **The number that proves parallelism.** Flat queues alone do not — the thread build already has flat queues. |
| **`pre_ms` mean** | 437.8ms | large drop, → low hundreds | This *is* the lockstep wait. It is what the change targets. |
| **`pre_ms` p95** | 1931.1ms | **< ~500ms** | The p95 is almost entirely "waiting for the other channel's decode." It should nearly vanish. |
| `stt_ms` mean | 2177.9ms | ≈unchanged or better | Per-call decode shouldn't change. **If it gets worse, contention moved rather than disappeared** — investigate before declaring success. |
| Full p50 / p95 | 2379 / 4316ms | both drop | Follows from `pre_ms`. |
| Queue trend | flat | **still flat** | Regression check. |
| RSS (all procs) | 546MB, 1 proc | measure; budget vs ~2.6GB idle-available | See 11.5. |
| Segment count | 92 | ≈92 | Sanity: same audio, same VAD. A big change means something else moved. |

### 11.5 The decisive check: is the lockstep pattern gone?

Aggregate means can improve for boring reasons. This is the direct test that the *mechanism* changed.

In `before-pinned.csv`, nearly every row with `pre_ms > 1000` is a `tx` segment landing while a long `rx` decode was still running — the §2 signature. Concretely, before:

```
elapsed_s  ch  dur_s  stt_ms   pre_ms
     19.8  rx   4.80  3936.4     33.1     <- long rx decode running
     21.2  tx   2.06  1454.4   1492.4     <- tx waits ~1.5s for it
```

**After, that correlation must be gone**: `tx` rows should show small `pre_ms` regardless of what `rx` is doing. Check by sorting each CSV on `pre_ms` descending and looking at the top ~10 rows:

```bash
for f in /tmp/ev/before-pinned.csv /tmp/ev/after-pinned.csv; do
  echo "== $f"; head -1 "$f"
  tail -n +2 "$f" | sort -t, -k9 -gr | head -10
done
```

If the after-run's worst `pre_ms` rows are still one channel shadowing the other's decode, **the processes are not actually running in parallel** — check pinning, `MOONSHINE_ORT_SINGLE_THREAD`, and that both children really spawned (`ps` should show 3 Python processes, not 1).

### 11.6 Memory

Threads shared one address space; processes do not, and `spawn` gets no copy-on-write. Two competing effects, so **measure rather than predict**:

- **Up:** each child pays a full interpreter + numpy + ONNX Runtime + its own model.
- **Down:** the parent no longer loads Moonshine at all — it keeps only torch/torchaudio/Silero for VAD.

Net could plausibly land near neutral. Sum RSS across all three processes (`after-mem-procs.txt`) and compare to the single-process 546MB before. Budget against ~2.6GB available at genuine idle. Flag if the total exceeds ~1.2GB.

### 11.7 Also required to call it done

1. Full test suite, `ruff`, `mypy` green — **with `stt.use_processes` both `True` and `False`**, since D7 keeps both paths live.
2. `scratch/demo_supervisor.py` still demonstrates stall → restart, and now shows the wedged worker actually **reclaimed** via `kill()` rather than leaked (§10).
3. Cold-start timing recorded: time from `edge-voice` launch to both `ready_event`s set. Must fit comfortably inside the readiness timeout (§5.6), and must not trip systemd (`WatchdogSec=10s` pings come from the parent's supervisor tick, which is unaffected by child loads — confirm this holds in practice).
4. Ctrl-C produces a clean shutdown with no child `KeyboardInterrupt` tracebacks (§12).
5. `docs/ARCHITECTURE.md`, `RELIABILITY.md`, `THREADS.md` updated.

---

## 12. Known hazards

| Hazard | Severity | Handling |
|---|---|---|
| **Supervisor crash-loop during child model load** | **High** | `stall_timeout_s=10s` vs 3.6-7.3s load + spawn overhead. `ready_event` gating — see §5.6. |
| **`join()` deadlock on undrained queues** | **High** | Confirmed empirically. Receiver thread must outlive its child; `cancel_join_thread()` — see §5.7. |
| `spawn` target must be importable by qualified name | Medium | Module-level `stt_child_main`; no closures/lambdas/bound methods. *Hit for real while validating this plan.* |
| **SIGINT reaches children directly** | Medium | Ctrl-C goes to the whole process group, so children raise `KeyboardInterrupt` mid-decode and spew tracebacks. Child should `signal.signal(SIGINT, SIG_IGN)` and exit only via `stop_event`, letting the parent (which already catches `KeyboardInterrupt` in `cli.py`) orchestrate shutdown. |
| **`bench_pipeline_load.py` will break** | Medium | `_pin_thread` uses `Thread.native_id`; processes need `Process.pid`, and `orch._stt` values become handles. Fix as part of Step 4 — this is the tool that verifies the change, and it broke the same way on the last STT refactor. |
| **Core pinning has no production path** | Medium | The plan pins cores 2/3, but only `bench_pipeline_load.py` can pin today. Decide at Step 4: a settings knob applied in `start()`, or systemd `CPUAffinity=` in `deploy/edge-voice.service`. Don't ship a plan whose core allocation only exists in a benchmark script. |
| `MOONSHINE_ORT_SINGLE_THREAD` must reach the children | Low | `spawn` inherits the parent env, so systemd `Environment=` works. State it explicitly in the unit file rather than relying on the operator's shell. |
| Module re-imported in child | Low | No import-time side effects in `stt_process.py`. |
| `mp.Queue.qsize()` approximate; unavailable on macOS | Low | Verified working on Linux; already documented as context, not precise. |
| Queues must be passed at `Process` construction | Low | Cannot be pickled arbitrarily later; pass via `args=`. |
| Orphaned children if the parent dies hard | Low | `daemon=True`, and/or verify systemd's `KillMode` reaps the whole cgroup. |
| Restart now costs a model reload | Low | Already true today (`STTWorker.__init__` builds its `Transcriber` eagerly), plus spawn overhead. Slightly worse, not new. |

**Checked and explicitly *not* problems** — recorded so they aren't rediscovered mid-build:

- **`partial_stats()` needs no IPC.** Grep confirms it is referenced only by `tests/test_partial_transcripts.py`, never surfaced in metrics or health. The thread-backed `STTWorker` those tests use is unchanged (D7).
- **`/api/stop` → `/api/start` was already broken.** `Process` cannot be restarted after termination — but neither can a `Thread` (`RuntimeError: threads can only be started once`, verified). No test does stop-then-start. This is a pre-existing limitation, not a multiprocessing regression; don't let it get misattributed later.
- **`vad_worker.py` genuinely needs zero changes.** Verified `mp.Queue` raises the stdlib `queue.Full` that `_emit_partial` already catches, and duck-types `put`/`put_nowait`/`qsize`.
- **`partial_max_queue_depth` backpressure still works.** It reads `self._segment_queue.qsize()` from inside the child; `qsize()` is available on Linux.
