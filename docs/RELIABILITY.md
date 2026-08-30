# Reliability: Supervisor & Watchdog (edge-voice)

Complements `ARCHITECTURE.md` §5 (Reliability), which covers *what* the two
layers do and *why*. This doc covers *how* — exact call chains, where each
piece of data actually lives, and what runs on which thread.

## TL;DR

- **Two independent layers.** Layer 1 (`Supervisor`) watches individual
  worker threads in-process and restarts crashed/stalled ones. Layer 2
  (systemd, via `deploy/edge-voice.service`) watches the whole process from
  outside and restarts it if layer 1's own thread goes silent.
- `Supervisor` is deliberately generic — it knows nothing about `VADWorker`/
  `STTWorker`/MQTT. It only calls callables the orchestrator wired in.
- **Restarting never repairs a worker.** It always builds a brand-new
  instance on the *same* queues and abandons the old one — Python threads
  can't be repaired, and a crashed/stalled thread's internal state can't be
  trusted anyway.
- **Python threads can't be force-killed.** A thread-backed worker that won't
  respond to a stop signal becomes a permanent zombie; only a full process
  restart (layer 2) actually removes it. **The STT workers are the exception**:
  in the deployed configuration they are child *processes*, so an unresponsive
  one is escalated to `kill()` and reclaimed in place — see the worked example
  below.
- A single stalled worker almost never reaches layer 2 — that's the whole
  point of having layer 1. Layer 2 only matters when the *Supervisor's own
  thread* stops running.

---

## Layer 1: the in-process `Supervisor`

### The two failure modes it detects

| | Crash | Stall |
|---|---|---|
| Condition | `is_alive()` is `False` | Alive, not stopping, `input_pending()` is `True`, and `now - last_activity() > stall_timeout_s` |
| Meaning | Thread exited without being asked to | Wedged — deadlock or a hung native call |
| Why exit-detection alone misses stalls | n/a | Nothing exits; the OS watchdog is also blind to it, since the rest of the process (this heartbeat included) keeps ticking fine |
| False-positive guard | n/a | `input_pending()` — a worker with an empty queue is simply idle, never flagged |

An intentional stop (`is_stopping()` is `True`) is never treated as either — that's the orchestrator tearing the pipeline down on purpose (`supervisor.py:156-173`, `_failure_reason`).

### `SupervisedTarget` — the generic interface (`supervisor.py:60-81`)

```python
@dataclass
class SupervisedTarget:
    name: str
    is_alive: Callable[[], bool]
    is_stopping: Callable[[], bool]
    last_activity: Callable[[], float]
    restart: Callable[[], None]
    input_pending: Callable[[], bool] = field(default=lambda: False)
    pending_loss: Callable[[], str | None] = field(default=lambda: None)
    stall_detection: bool = True
```

Fields are named for **the question `Supervisor` needs answered**, not for something that necessarily exists on the worker by that name. Each one's real source, per worker, wired in `PipelineOrchestrator._build_supervisor_targets()` (`orchestrator.py:263-306`):

| Field | Real source | Notes |
|---|---|---|
| `is_alive` | `threading.Thread.is_alive()` — stdlib, inherited automatically | Same for all 4 workers |
| `is_stopping` | `.stopping` property (`_stop_event.is_set()`) — every worker defines this | Same shape for all 4 |
| `last_activity` | `.last_activity` property — a `time.monotonic()` the worker updates on each unit of work | Same shape for all 4 |
| `restart` | **Not on the worker at all** — `lambda: self._restart("<attr>", self._build_<worker>)`, i.e. the orchestrator's own rebuild logic | See `_restart` below |
| `input_pending` | **Not on the worker** — `lambda: self._queue_pending(<input queue>)`, checking the queue object itself | `MqttAudioIngest` leaves this at the default `lambda: False` (no input queue) |
| `pending_loss` | Only `VADWorker` has a real `pending_loss()` method; every other target leaves the default `lambda: None` | Reports in-progress segment audio a restart would discard |
| `stall_detection` | Plain `bool`, not callable — `False` only for `MqttAudioIngest` | Its liveness contract isn't "consume a queue" |

Per-worker wiring summary:

| Target | `input_pending` checks | `pending_loss` | `stall_detection` | rebuilt via |
|---|---|---|---|---|
| `MqttAudioIngest` | *(default `False`)* | *(default `None`)* | `False` | `_build_mqtt_subscriber` |
| `ChannelRouter` | `self._ingest_queue` | *(default)* | `True` | `_build_router` |
| `VADWorker` | `self._routed_queue` | `self._w("_vad").pending_loss()` | `True` | `_build_vad` |
| `STTWorker-{cid}` (one target per channel — see `_build_stt_supervisor_targets`) | `self._segment_queues[cid]` | *(default)* | `True` | `_build_stt(cid)` |

### `_TargetState` — the mutable bookkeeping (`supervisor.py:84-90`)

```python
@dataclass
class _TargetState:
    target: SupervisedTarget
    restarts: deque[float] = field(default_factory=deque)  # monotonic times, windowed
    degraded: bool = False
    restarting: bool = False
    state: str = STATE_RUNNING
```

`SupervisedTarget` is fixed and read-only once built. `_TargetState` is the running ledger layered around it — one per worker, created once in `Supervisor.__init__` (`supervisor.py:107`).

| Field | Meaning | Who sets it |
|---|---|---|
| `restarts` | Timestamps of past restart attempts, pruned to the trailing `restart_window_s` | `_trigger_restart` (append + prune) |
| `degraded` | Permanently given up on this target — a one-way door, nothing ever resets it to `False` | `_trigger_restart`, on budget exhaustion |
| `restarting` | Gate: is a restart currently in flight for this target *right now* | Set `True` in `_trigger_restart`, cleared in `_restart_worker`'s `finally` |
| `state` | Human-readable mirror of the two booleans (`"running"`/`"restarting"`/`"degraded"`), read by `status()` | Both methods above |

`_scan()`'s very first check is `if ts.degraded or ts.restarting: continue` — these two booleans are the actual control-flow gates; `state` is display-only.

### The tick loop (`supervisor.py:124-142`)

```python
if self._watchdog_enabled:
    systemd_watchdog.notify("READY=1")          # once, before the loop

while not self._stop_event.wait(self._tick_interval_s):
    if self._watchdog_enabled:
        systemd_watchdog.notify("WATCHDOG=1")   # every tick, unconditionally
    self._scan()

self._await_restarts()
```

Same `Event.wait()`-as-sleep idiom used throughout this codebase's worker loops: `wait()` returns `False` on a normal timeout (keep looping), `True` the instant `stop()` is called (exit). The ping happens **before** `_scan()`, deliberately — detection/dispatch must never be able to delay the heartbeat past `WatchdogSec`.

### `_scan()` → `_failure_reason()` → `_trigger_restart()` (`supervisor.py:146-173`)

```python
def _scan(self) -> None:
    now = time.monotonic()
    for ts in list(self._states.values()):
        with self._lock:
            if ts.degraded or ts.restarting:
                continue
        reason = self._failure_reason(ts.target, now)
        if reason is not None:
            self._trigger_restart(ts, reason)
```

One `now` per scan, shared across all targets. The lock scope is deliberately tiny — just the boolean gate read — so a slow `_failure_reason()` call (it invokes arbitrary orchestrator-supplied callables) never blocks other threads from touching `_TargetState`.

### `_trigger_restart` — budget check, then dispatch (`supervisor.py:177-214`)

Runs entirely on the **tick thread**. Two phases:

**Under lock — decide degrade vs. proceed:**
```python
while ts.restarts and (now - ts.restarts[0]) > self._restart_window_s:
    ts.restarts.popleft()                  # prune, oldest-first

if len(ts.restarts) >= self._max_restarts:
    ts.degraded = True; ts.state = STATE_DEGRADED
    return                                  # no restart dispatched

ts.restarts.append(now)
ts.restarting = True; ts.state = STATE_RESTARTING
```

**Outside lock — log and spawn:**
```python
t = threading.Thread(target=self._restart_worker, args=(ts,),
                      name=f"restart-{ts.target.name}", daemon=True)
with self._lock:
    self._restart_threads.append(t)
t.start()
```

`self._restart_threads` holds references to these **agent threads** (the ones doing the restarting), not the pipeline workers themselves. It exists purely so `_await_restarts()` can find and bound-join them on shutdown — nothing ever prunes finished entries from it.

### `_restart_worker` — the actual work, on its own thread (`supervisor.py:216-241`)

```python
try:
    loss = ts.target.pending_loss()
except Exception:
    loss = None
if loss:
    logger.error(...)                       # only ever non-None for VADWorker

try:
    ts.target.restart()                     # → PipelineOrchestrator._restart(...)
    with self._lock:
        if not ts.degraded:
            ts.state = STATE_RUNNING
except Exception:
    logger.exception(...)                   # state left as-is; retried next tick
finally:
    with self._lock:
        ts.restarting = False               # ALWAYS reopens the gate
```

`ts.target.restart()` is a **zero-argument** call — the two arguments `PipelineOrchestrator._restart` actually needs (`attr`, `build_fn`) are hardcoded into the lambda's own source, not passed in here:

```python
restart=lambda: self._restart("_vad", self._build_vad),
```

`Supervisor` never sees `"_vad"` or `self._build_vad` — it just calls a lambda that already knows.

---

## `PipelineOrchestrator._restart` — the kill/rebuild mechanics (`orchestrator.py:308-333`)

```python
old = getattr(self, attr)
self._signal(old)                  # old.stop() — sets old's own _stop_event
self._join(old)                    # old.join(timeout=10) — WAITS, forces nothing
if old.is_alive():
    logger.warning(...)            # zombie: joined timed out, still running
new = build_fn()                   # brand-new instance, SAME queues
setattr(self, attr, new)
if self._running and not self._stop_event.is_set():
    new.start()                    # this is the moment it actually restarts
```

### Crash vs. stall — two very different `old`s

| | Crashed worker | Stalled worker |
|---|---|---|
| `old.is_alive()` before `_signal` | Already `False` | `True` |
| What `_signal`/`_join` does | `_join` returns almost instantly — nothing to wait for | Waits up to 10s hoping the worker notices its stop flag |
| If unresponsive after 10s | n/a (already dead) | **Zombie** — still running, unreachable, un-killable. Logged, then abandoned |
| Does the pipeline recover anyway? | Yes | Yes — a fresh worker is built and started regardless |

### Worked example: `STTWorker-rx` stalls

STT has one dedicated worker per channel (`self._stt`, keyed by
`channel_id` — see `docs/ARCHITECTURE.md`'s per-channel STT decision), so
restarting one doesn't touch the other channel's worker at all. Since it's a
dict entry rather than a bare attribute, this goes through
`_restart_dict_worker` instead of `_restart` (same shape, `workers[key] = new`
in place of `setattr`):

```
_restart_dict_worker(self._stt, "rx", lambda: self._build_stt("rx"))
  old = self._stt["rx"]                        # the stalled STTWorker-rx
  self._signal(old) → old.stop()
  self._join(old)   → waits ≤10s
    (if it never responds → kill() it; see below)
  new = self._build_stt("rx")                  # wired to the SAME self._segment_queues["rx"]
  self._stt["rx"] = new
  new.start()
  new.wait_ready(...)                          # process mode: let the model load
                                               # before the supervisor watches it
```

**This is the one place a wedged worker is genuinely recoverable.** In the
deployed configuration (`stt.use_processes`, the default) an STT worker is a
child *process*, not a thread — so when it ignores the graceful stop,
`_restart_dict_worker` escalates to `kill()` and the OS reclaims it.
Verified against a `SIGSTOP`-ped child: graceful stop times out, `kill()`
returns it with `exitcode=-9`. Every other supervised target in this
document is a thread and still degrades to the zombie case below, because
Python cannot kill a thread.

`new.wait_ready(...)` is not optional politeness: a replacement child has to
load its model (3.6-7.3s on the RPi5, plus spawn), and the supervisor's
`stall_timeout_s` is 10s. Without gating on readiness, the load itself looks
like a stall, and the restart loops. See `STT_MULTIPROCESS_PLAN.md` §5.6.

The reason this actually fixes the stall: `self._segment_queues["rx"]` is never recreated. Whatever segments piled up (the very thing `input_pending()` detected) are still sitting there, untouched — the new worker's `run()` loop starts consuming them the moment it starts. The "restart" only ever changes *who's reading that channel's queue*, never the queue itself, and never touches `self._stt["tx"]`.

### The shutdown-race guard

```python
if self._running and not self._stop_event.is_set():
    new.start()
```

`_restart` runs on a `restart-<name>` thread, potentially concurrently with `orchestrator.stop()` running on a different one. `Supervisor.run()`'s own shutdown (`_await_restarts()`, bounded by `tick_interval_s` per thread — not the full `WORKER_JOIN_TIMEOUT_S`) is only *best-effort* at waiting for in-flight restarts to finish first. This check is the backstop: don't `.start()` a new thread that nothing will ever signal or join again if the pipeline is already tearing down.

---

## Layer 2: the OS-level watchdog

### `systemd_watchdog.notify(state)` (`pipeline/systemd_watchdog.py`)

- Reads `$NOTIFY_SOCKET` from the environment — set by systemd only when launched as a `Type=notify` unit. Unset (e.g. dev, CI, manual run) → `notify()` is a no-op, always returns `False`.
- Translates systemd's `@`-prefixed abstract-socket notation into the NUL-byte-prefixed form the kernel actually expects (`@name` → `"\0" + name`); a `/`-prefixed path is used as-is.
- Sends the given string (`"READY=1"`, `"WATCHDOG=1"`) as one raw UDP-style datagram over an `AF_UNIX`/`SOCK_DGRAM` socket. No framing, no library — the entire sd_notify protocol is one short write.
- Never raises — any failure is logged at `debug` and swallowed, since a failed heartbeat *is* the signal this mechanism exists to surface, not an error to crash over.

### `READY=1` vs `WATCHDOG=1`

| | `READY=1` | `WATCHDOG=1` |
|---|---|---|
| Sent | Once, before the tick loop starts | Every tick, unconditionally, forever |
| Meaning | "Startup finished" | "Still alive, right now" |
| Effect | Unblocks anything waiting on this unit; **starts** systemd's watchdog countdown | Resets that countdown back to `WatchdogSec` |

Other sd_notify states exist (`STATUS=`, `STOPPING=1`, `RELOADING=1`, `MAINPID=`, ...) but this codebase only ever sends these two.

### `deploy/edge-voice.service`

```ini
Type=notify
WatchdogSec=10s      # 5x margin over the default 2.0s tick_interval_s
Restart=always
RestartSec=3s
```

If `WATCHDOG=1` stops arriving for a full `WatchdogSec`, systemd kills and restarts the **entire process** — not any single thread. Nothing in this process gets a chance to run cleanup; the kill is external and total. `Restart=always` + `RestartSec=3s` is what brings a fresh process up afterward, which rebuilds everything (`Supervisor` included) from scratch.

Install placeholders that must be edited before copying to `/etc/systemd/system/`: `User=`, `WorkingDirectory=`, `ExecStart=`.

### Why a single stalled worker almost never reaches this layer

`Supervisor`'s tick thread and a stalled `VADWorker`/`STTWorker` are separate OS threads. As long as the stall doesn't hold the GIL hostage (ordinary I/O/lock blocks release it fine), `Supervisor` keeps ticking, keeps pinging, and handles the stall entirely via layer 1 — layer 2 never sees anything wrong. Layer 2 only matters when whatever's wrong takes the **Supervisor's own thread** down with it — e.g. a native call that hangs without releasing the GIL, or the whole process frozen (`SIGSTOP`, severe OOM/thrashing). That's a structural boundary, not a tuning knob: layer 1 can only ever fix what it's still alive to detect.

---

## Config knobs (`ReliabilitySettings`, `configs/default.yaml`)

| Key | Default | Meaning |
|---|---|---|
| `reliability.enabled` | `True` | Whether `Supervisor` is built/started at all |
| `reliability.tick_interval_s` | `2.0` | Scan + ping cadence |
| `reliability.stall_timeout_s` | `10.0` | No-progress threshold with input queued → stalled |
| `reliability.max_restarts` | `3` | Restarts allowed per worker per window before `degraded` |
| `reliability.restart_window_s` | `60.0` | Rolling window the budget above is measured over |
| `reliability.watchdog_enabled` | `True` | Whether `sd_notify` pings are sent at all |
| `WatchdogSec` (unit file) | `10s` | systemd's own countdown; independent of the config above |
| `RestartSec` (unit file) | `3s` | Delay before systemd relaunches after a kill |

---

## Named patterns, for communicating this design

Useful for code review / design-doc language — calibrated honestly, not force-fit:

| Pattern | Where | Fit |
|---|---|---|
| **Supervisor / "let it crash"** (Erlang/OTP) | The whole kill-and-rebuild-fresh mechanic | Strong — this is literally what the class is named for |
| **Circuit Breaker** | `max_restarts`/`restart_window_s` → `degraded` | Strong |
| **Dependency Injection** | `Supervisor(targets: list[SupervisedTarget], ...)` | Strong |
| **Command** | `restart` field — a fully-bound action, invoked without the caller knowing what it does | Strong |
| **Adapter** | The lambdas bridging each worker's concrete interface into `SupervisedTarget`'s generic one | Strong |
| **Null Object** | `input_pending`/`pending_loss` defaults (`lambda: False` / `lambda: None`) | Strong |
| **Factory Method** | `_build_vad`/`_build_stt`/etc., passed around as first-class `build_fn` values | Strong |
| **Strategy** | `input_pending` (real check vs. always-`False`) | Genuine fit |
| **Strategy** | `is_alive`/`is_stopping`/`last_activity` | **Weak** — same operation every time, just closed over a different object; this is plain closures/DI, not a real family of algorithms |

For an SRE/distributed-systems audience specifically, the more precise and more immediately legible vocabulary is usually better than GoF names: the tick loop is a **reconciliation/control loop**; `is_alive`/`is_stopping` are a **liveness probe**; the stall check is a **progress/health probe**; `degraded` is **crash-loop backoff**; the two-layer design is **defense in depth** — the same idea as a kubelet failing and the cluster control plane catching it a level up.
