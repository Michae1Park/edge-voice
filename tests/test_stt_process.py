"""Tests for running an STTWorker in its own process (stt/stt_process.py).

The fast tests here deliberately never spawn: they exercise the readiness
gate and the Thread-shaped surface directly, because that logic is what
prevents a supervisor crash-loop and it should be covered without paying a
model load. The one real spawn is marked `integration`.
"""

import pickle
import queue
import threading
import time
from types import SimpleNamespace

import pytest

from edge_voice.pipeline.models import SpeechSegment, TranscriptEvent
from edge_voice.stt.stt_process import (
    STTProcessHandle,
    SttResult,
    _bridge,
    _read_self_cpu_ticks,
    new_mp_context,
)
from edge_voice.stt.stt_worker import STTWorkerConfig

SAMPLE_RATE = 16000


def _handle(channel_id: str = "rx") -> STTProcessHandle:
    """A handle that is never started -- enough to test everything the
    supervisor reads before/without a running child."""
    ctx = new_mp_context()
    return STTProcessHandle(
        channel_id=channel_id,
        segment_queue=ctx.Queue(),
        config=STTWorkerConfig(),
        ctx=ctx,
        log_queue=ctx.Queue(),
        log_level=20,
    )


# -- SttResult (the IPC payload) -----------------------------


def test_sttresult_survives_pickling():
    """It crosses a process boundary, so picklability is a hard requirement,
    not an implementation detail."""
    event = TranscriptEvent(
        channel_id="rx", segment_id="rx-1", text="안녕하세요", start=0.0, end=1.5
    )
    result = SttResult(event=event, stt_latency_s=0.42)

    back = pickle.loads(pickle.dumps(result))

    assert back.event == event
    assert back.stt_latency_s == 0.42


def test_sttresult_carries_no_latency_for_partials():
    """Mirrors STTWorker's rule that _handle_partial never touches
    last_latency_s -- a partial's latency would belong to another segment."""
    event = TranscriptEvent(
        channel_id="rx", segment_id="rx-1", text="안녕", start=0.0, end=1.0, is_final=False
    )
    assert SttResult(event=event, stt_latency_s=None).stt_latency_s is None


# -- the Thread-shaped surface -----------------------------


def test_name_matches_the_supervised_target_name():
    """get_status() merges supervisor state onto workers[name]; a mismatch
    would silently leave the row unrefined."""
    assert _handle("rx").name == "STTWorker-rx"


def test_ident_is_none_before_start():
    """orchestrator._join() uses `ident is None` to skip a never-started
    worker, so pid must map onto it."""
    assert _handle().ident is None


def test_stop_sets_stopping():
    handle = _handle()
    assert handle.stopping is False
    handle.stop()
    assert handle.stopping is True


# -- the readiness gate (the crash-loop guard) -----------------------------


def test_last_activity_reports_active_while_not_ready():
    """The load-bearing one.

    Model load takes 3.6-7.3s on the RPi5 plus spawn/import, against a 10s
    stall_timeout_s, while segments queue up so input_pending() is already
    True. If last_activity went stale during load, the supervisor would
    restart a child that is merely still starting -- and the replacement
    would take just as long, looping forever into DEGRADED.
    """
    handle = _handle()
    assert not handle.ready

    before = time.monotonic()
    reported = handle.last_activity
    after = time.monotonic()

    # "Active right now", not the unset 0.0 heartbeat.
    assert before <= reported <= after


def test_last_activity_reads_the_heartbeat_once_ready():
    handle = _handle()
    stamp = time.monotonic() - 3.0
    handle._heartbeat.value = stamp
    handle._ready_event.set()

    assert handle.ready
    assert handle.last_activity == pytest.approx(stamp)


def test_last_activity_ignores_an_unseeded_heartbeat_even_when_ready():
    """Guards the sliver between ready_event and the bridge's first tick --
    a raw 0.0 would read as 'last active in 1970' and trip stall detection."""
    handle = _handle()
    handle._ready_event.set()  # heartbeat deliberately left at 0.0

    before = time.monotonic()
    reported = handle.last_activity

    assert reported >= before


def test_wait_ready_times_out_when_the_child_never_starts():
    assert _handle().wait_ready(timeout=0.05) is False


# -- _bridge's CPU-progress heartbeat (the false-stall guard) --------------
#
# Reproduces, at unit-test speed, the bug found via bench_pipeline_load.py on
# the RPi5: a near-continuous-speech WAV (no pauses for VAD to cut on) let a
# single _transcribe() call run past stall_timeout_s, and worker.last_activity
# -- stamped only at segment dequeue -- couldn't tell that apart from a real
# hang, so the supervisor SIGKILLed STTWorker-rx/tx mid-decode and looped
# into DEGRADED. See _bridge()'s docstring for the fix.


def test_read_self_cpu_ticks_returns_a_nonnegative_int():
    ticks = _read_self_cpu_ticks()
    assert ticks is not None
    assert ticks >= 0


def _run_bridge_briefly(monkeypatch, cpu_ticks, worker_last_activity: float) -> float:
    """Runs _bridge() for a couple of heartbeat ticks against a fake
    CPU-tick source and a worker whose last_activity never itself advances
    (standing in for one long-running _transcribe() call), then returns the
    last heartbeat value published before shutdown.
    """
    import edge_voice.stt.stt_process as stt_process

    monkeypatch.setattr(stt_process, "HEARTBEAT_INTERVAL_S", 0.02)
    ticks = iter(cpu_ticks)
    monkeypatch.setattr(stt_process, "_read_self_cpu_ticks", lambda: next(ticks))

    worker = SimpleNamespace(last_activity=worker_last_activity, stop=lambda: None)
    stop_event = threading.Event()
    heartbeat = SimpleNamespace(value=0.0)

    t = threading.Thread(target=_bridge, args=(worker, stop_event, heartbeat), daemon=True)
    t.start()
    time.sleep(0.02 * 5)  # several ticks' worth
    stop_event.set()
    t.join(timeout=1.0)
    return heartbeat.value


def test_bridge_keeps_heartbeat_fresh_while_cpu_keeps_advancing(monkeypatch):
    """A worker still genuinely decoding (CPU ticks rising every tick) must
    report as active *now*, not frozen at its stale dequeue timestamp --
    otherwise a slow-but-real decode looks identical to a hang."""
    frozen_dequeue_stamp = 12345.0
    before = time.monotonic()

    reported = _run_bridge_briefly(
        monkeypatch, cpu_ticks=range(0, 10_000, 10), worker_last_activity=frozen_dequeue_stamp
    )

    after = time.monotonic()
    assert before <= reported <= after
    assert reported != frozen_dequeue_stamp


def test_bridge_falls_back_to_worker_last_activity_when_cpu_is_flat(monkeypatch):
    """A worker that stops burning CPU -- blocked on a lock or syscall, the
    actual hang this exists to catch -- must NOT have its heartbeat
    artificially kept fresh; it should read whatever worker.last_activity
    already says, so the supervisor's stall check still fires."""
    frozen_dequeue_stamp = 12345.0

    reported = _run_bridge_briefly(
        monkeypatch, cpu_ticks=[42] * 20, worker_last_activity=frozen_dequeue_stamp
    )

    assert reported == frozen_dequeue_stamp


# -- one real spawn -----------------------------


@pytest.mark.integration
def test_child_process_transcribes_a_segment_end_to_end():
    """Spawns a real child, which loads a real model. Also records the
    start->ready time, which is what sizes the orchestrator's readiness
    timeout (see docs/archived/STT_MULTIPROCESS_PLAN.md §5.6)."""
    ctx = new_mp_context()
    segment_queue = ctx.Queue()
    handle = STTProcessHandle(
        channel_id="rx",
        segment_queue=segment_queue,
        config=STTWorkerConfig(),
        ctx=ctx,
        log_queue=ctx.Queue(),
        log_level=20,
    )

    try:
        t0 = time.monotonic()
        handle.start()
        assert handle.wait_ready(timeout=180), "child never finished loading its model"
        load_s = time.monotonic() - t0
        print(f"\nchild start->ready: {load_s:.2f}s")

        assert handle.ident is not None
        assert handle.is_alive()

        # Silence still produces a (possibly empty-text) final, and the
        # latency/plumbing is what's under test here, not the transcript.
        segment_queue.put(
            SpeechSegment(
                channel_id="rx",
                start=0.0,
                end=1.0,
                audio=bytes(SAMPLE_RATE * 2),
                segment_id="rx-probe-1",
            )
        )

        result = handle.transcript_queue.get(timeout=120)
        assert isinstance(result, SttResult)
        assert result.event.channel_id == "rx"
        assert result.event.segment_id == "rx-probe-1"
        assert result.event.is_final
        assert result.stt_latency_s is not None and result.stt_latency_s >= 0.0
    finally:
        handle.stop()
        # Drain before joining: a child holding undelivered queue items will
        # not exit (see plan section 5.7).
        while True:
            try:
                handle.transcript_queue.get_nowait()
            except queue.Empty:
                break
        handle.join(timeout=30)
        handle.kill()
