# Thread Lifecycle (edge-voice)

Complements `ARCHITECTURE.md` §4 (Concurrency Model), which covers *what*
each worker does. This covers *how* they start, run, and stop.

## TL;DR

- Python threads can't be force-killed. A thread only stops when its `run()`
  method returns — always cooperatively.
- `stop()` just flips a flag; it does **not** stop anything by itself.
- `join()` just **waits** for a thread to finish; it does **not** cause it to.
- Shutdown flow: *signal → worker notices on its own → cleans up → exits →
  orchestrator's `join()` unblocks.*
- A worker that never notices the flag becomes an unkillable zombie — only a
  full process restart (OS watchdog) recovers it.
- Not all workers notice equally fast: queue-driven ones notice only when
  their `queue.get()` next times out; the tick-driven `MetricsCollector`
  notices **immediately** (Rule 2).

---

## The threads

| Thread | Role | Optional? |
|---|---|---|
| `MqttAudioIngest` | Pulls audio in from MQTT | No |
| `ChannelRouter` | Re-packetizes, routes by channel | No |
| `VADWorker` | Segments speech per channel | No |
| `STTWorker-<channel>` | Runs transcription, one per channel. **A separate OS process, not a thread**, when `stt.use_processes` (the default) — see `STT_MULTIPROCESS_PLAN.md`. The orchestrator drives it through `STTProcessHandle`, which presents the same Thread-shaped surface, so everything in this doc still applies. | No |
| `STTReceiver-<channel>` | Drains one STT child's transcript queue and republishes into `TranscriptHub`. Process mode only. Must outlive its child on shutdown — an undrained queue keeps the child alive. | `stt.use_processes` |
| `Supervisor` | Watches the four above, restarts on crash/stall | `reliability.enabled` |
| `MetricsCollector` | Aggregates queue depth / STT latency / restart budget / MQTT status on a timer | `metrics.enabled` |
| `AudioDumpWorker` / `SegmentAudioDumpWorker` | Debug-only raw/segment audio capture | `dump.enabled` / `segment_dump.enabled` |

Owned by `PipelineOrchestrator` (`orchestrator.py:252-268`). The **main
thread** only runs `PipelineOrchestrator.run()` (build/start/wait/stop) — no
pipeline work, which is why tuning its idle-loop sleep isn't a lever for
transcription speed.

`observability/logging.py` is **not** a thread — just one-time formatter
setup (`configure_logging`) called at startup. Noted here only so it isn't
mistaken for one.

---

## Diagram: start/stop call chains

`build()` (`orchestrator.py:84`) only constructs — no threads exist yet:

```text
_build_mqtt_subscriber() → MqttAudioIngest(...)   ┐
_build_router()          → ChannelRouter(...)     │  __init__ only,
_build_vad()             → VADWorker(...)         │  nothing running
_build_stt(channel)      → STTProcessHandle(...)  ┘  (or STTWorker if
                            one per channel          stt.use_processes
                                                     is False)
_build_supervisor()      → Supervisor(...)          (if reliability.enabled)
_build_metrics()         → MetricsCollector(...)    (if metrics.enabled)
```

**`start()`** (`orchestrator.py:122`) — workers first, observers layered on
after, each depending on the thing before it being alive:

```text
for w in _get_workers():   # Mqtt, Router, VAD, STT[, dump workers]
    w.start() ──▶ threading.Thread.start() ──▶ w.run()   # own loop, own thread

if supervisor: supervisor.start() ──▶ Supervisor.run()        # after workers exist
if metrics:    metrics.start()    ──▶ MetricsCollector.run()  # after supervisor exists
```

**`stop()`** (`orchestrator.py:140`) — observers unwind first, then workers
upstream-first; each `_signal`/`_join` pair (`orchestrator.py:184-198`)
drives one thread through its own exit path:

```text
1. _signal(metrics)    ─▶ MetricsCollector.stop() ─▶ sets _stop_event
   _join(metrics)      ─▶ blocks until MetricsCollector.run() returns:
                            Event.wait() → True ─▶ while exits ─▶ run() returns

2. _signal(supervisor) ─▶ Supervisor.stop() ─▶ sets _stop_event
   _join(supervisor)   ─▶ blocks until Supervisor.run() returns

3. for w in [ingest, router, vad, stt, ...]:   # upstream-first, one at a time
     _signal(w) ─▶ _join(w)   # each worker fully stops before the next is touched

   finally: _signal(w) again, every worker   # backstop if a 2nd Ctrl-C
                                              # interrupts step 3 mid-loop
```

### Worked example: exact call order for `VADWorker`

"One at a time" in step 3 means VAD isn't touched until ingest and router
have each *fully* stopped — not just signalled. Full order, orchestrator
calls and `VADWorker`'s own calls interleaved:

```text
 1. orchestrator.build()          → self._vad = self._build_vad()
                                     → VADWorker.__init__()            (orchestrator.py:106)
 2. orchestrator.start()          → self._vad.start()
                                     → threading.Thread.start()  (stdlib)
                                     → VADWorker.run() begins on its own OS thread
 3. VADWorker.run()               → loops: routed_queue.get(timeout=0.5) → _handle_packet() → …
 4. orchestrator.stop()           → _signal(audio_source) → _join(audio_source)   [fully drained first]
 5.                                 _signal(router)        → _join(router)         [fully drained next]
 6.                                 _signal(self._vad)     → VADWorker.stop() → _stop_event.set()
 7.                                 _join(self._vad)       → blocks on VADWorker.join()
 8. VADWorker.run() (concurrently)→ routed_queue.get() times out → while-check fails
                                     → self.flush("shutdown")  → run() returns
 9. orchestrator.stop() (cont.)   → _join(self._vad) unblocks
10.                                 _signal(self._stt) → _join(self._stt)   [still alive to receive step 8's flush]
```

Steps 4-5 finishing before step 6 is why VAD's flush at step 8 is safe: STT
(step 10) hasn't been touched yet, so it's still consuming when VAD's
shutdown segment lands on the segment queue.

---

## Rule 1 — A thread lives exactly as long as `run()` runs

> There's no Python equivalent of `pthread_kill`. Nothing outside a thread
> can force it to stop.

The OS thread ends only when `run()` returns — loop exit, exception, or (for
a one-shot `run()`) just finishing. "Stopping" a thread always means asking
it to return, then waiting.

---

## Rule 2 — The `stop_event` pattern

Every long-running worker follows the same shape:

```python
def __init__(self, ...):
    self._stop_event = threading.Event()

def stop(self) -> None:
    self._stop_event.set()

def run(self) -> None:
    while not self._stop_event.is_set():
        try:
            item = self._queue.get(timeout=SOME_TIMEOUT)
        except queue.Empty:
            continue
        if item is None:  # shutdown sentinel
            break
        ...  # do work
    self.flush("shutdown")  # VADWorker only — see Rule 4
```

- **`stop()` is instant but inert** — flips a lock-guarded flag, does
  nothing to the running thread by itself.
- **The loop can't react until `queue.get()` returns** — blocked there, not
  polling `_stop_event`. It only rechecks `while` when an item arrives or
  the timeout raises `queue.Empty`. So worst-case notice delay = that
  worker's queue timeout: `VADWorker` 0.5s (`vad_worker.py:176`), `STTWorker`
  0.2s (`QUEUE_GET_TIMEOUT_S`, `stt_worker.py:59`).

(Without the loop, this machinery would be dead code — the thread would just
run once and exit, `_stop_event` never consulted.)

### Variant: tick-driven workers (no queue)

`MetricsCollector` (`observability/metrics.py:52-90`) has no queue — it
wakes on a fixed interval and uses `_stop_event` itself as the sleep:

```python
def run(self) -> None:
    while not self._stop_event.wait(self._emit_interval_s):
        self._tick()
```

`Event.wait(timeout)` returns `True` the **instant** the event is set
(loop exits immediately), or `False` after the full interval with the flag
still unset (normal tick). So unlike the queue-based workers, it notices
`stop()` essentially immediately — even though its normal tick interval
(`emit_interval_s`, default 10s) is much longer. Same flag, but `Event.wait()`
reacts to it directly, where `queue.get()` only reacts to its own timeout or
an incoming item.

### Variant: the orchestrator's own loop

`PipelineOrchestrator` isn't a `threading.Thread` — its `run()`
(`orchestrator.py:231-245`) executes on whichever thread calls it (normally
the CLI's main thread), not a dedicated one:

```python
while self._running:
    if end is not None and time.time() >= end:
        break
    self._stop_event.wait(1.0)
```

- **No queue, no work.** Same `Event.wait()`-as-sleep idiom as
  `MetricsCollector` — `stop()` wakes it almost instantly, and the 1.0s is
  just the `duration_s` deadline-check interval, not a notice-delay bound.
  (This is the loop from the earlier question about whether widening that
  interval speeds up transcription — it can't, since it does no pipeline
  work.)
- **Gated on `_running`, not the event.** `stop()` sets both, but
  `get_status()` (`:201`) and `_restart`'s guard (`:387`) read `_running`
  directly, not `_stop_event`.
- **Usually cross-thread.** A web UI handler calling `orchestrator.stop()`
  wakes the main thread out of `wait(1.0)` from a different thread entirely
  — same cooperative signal every worker uses, just aimed at the main
  thread's own loop instead of a background one.

---

## Rule 3 — `join()` is a spectator, not an actor

`PipelineOrchestrator._signal` / `_join` (`orchestrator.py:184-198`):

- **`_signal(worker)`** — calls `worker.stop()`, swallowing `AttributeError`
  defensively.
- **`_join(worker)`** — guards `worker.ident is None` (never `.start()`ed —
  legal in tests; `.join()` raises otherwise), then
  `worker.join(timeout=WORKER_JOIN_TIMEOUT_S)` (10s), warning if still alive.

`join()` blocks the *calling* thread waiting to be told the target finished
— it has no power to make that happen. Two independent things run in
parallel:

```
orchestrator thread                    worker thread
────────────────────                   ─────────────
_signal(w) → w.stop()                  blocked in queue.get(timeout=...)
  (sets the flag, returns
   immediately)
                                        queue.get times out
                                        while-condition rechecked → False
                                        cleanup (e.g. flush) runs
                                        run() returns → OS thread exits itself
_join(w) → w.join(timeout=10)  ───────► unblocks here, or after 10s, whichever first
```

**Failure mode:** a worker that never notices the flag (wedged in a
no-timeout blocking call, an infinite loop, etc.) becomes a **zombie** —
`join()` times out, warns, and nothing in this process can kill it.
`_restart` (`orchestrator.py:363-388`) only swaps the *attribute* to a fresh
worker; the old thread keeps running orphaned if still alive. The OS-level
watchdog (§5 Reliability, `ARCHITECTURE.md`) is the real remedy — a full
process restart.

---

## Rule 4 — Start/stop order is deliberate

Two distinct ordering concerns, two distinct reasons.

### 4a. Pipeline workers: producer before consumer

`stop()` (`orchestrator.py:140-175`) signals and joins the four pipeline
workers **upstream-first**, draining each stage before the next. Load-
bearing: `VADWorker.run()` calls `self.flush("shutdown")` on exit to emit
any in-progress segment rather than drop it (same failure class as
`flush()`/`reset_channel()`). If STT were signalled/joined with or before
VAD, it could already be gone when the shutdown flush tries to reach it.

> Producer before consumer, every stage — keeps the final flush from
> landing on a queue nobody's reading anymore.

### 4b. Supervisor and Metrics: observers first out, last in

Both watch the pipeline rather than sit in the data path, so they bracket
the four workers in opposite order on each side:

| Phase | Order |
|---|---|
| **start()** (`orchestrator.py:122-138`) | 4 workers → `Supervisor` → `MetricsCollector` |
| **stop()** (`orchestrator.py:140-175`) | `MetricsCollector` → `Supervisor` → 4 workers (upstream-first) |

- **Start last** — `Supervisor` shouldn't scan before the workers it watches
  are running (else "not started yet" reads as "crashed"). `MetricsCollector`
  starts even later since it also reads `Supervisor.status()`.
- **Stop first** — each observer must stop reading before what it reads
  disappears. A `Supervisor` that outlived the workers being torn down would
  see them "crashing" and race to restart them mid-shutdown.

> 4a protects **data** from being dropped; 4b protects **observers** from
> reading a system mid-teardown. Different failure mode, same instinct: know
> what depends on what before deciding who goes first.

---

## Quick reference

| Question | Answer |
|---|---|
| Does `stop()` block? | No — sets a flag, returns immediately |
| Does `stop()` guarantee the thread has stopped? | No |
| Does `join()` block? | Yes, up to `timeout` |
| Does `join()` cause the thread to stop? | No — only waits for a stop already in progress |
| What actually ends a thread? | Its own `run()` returning |
| Max delay: `stop()` → loop notices | Worker's `queue.get` timeout (0.2–0.5s here); `MetricsCollector` is near-instant |
| Max delay: `stop()` → `_join` gives up | `WORKER_JOIN_TIMEOUT_S` = 10s |
| Can a wedged thread be force-killed? | No — only a full process restart (OS watchdog) |
| Stop order across all threads | `MetricsCollector` → `Supervisor` → workers, upstream-first |
