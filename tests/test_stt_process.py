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
    ctx = new_mp_context()
    return STTProcessHandle(
        channel_id=channel_id,
        segment_queue=ctx.Queue(),
        config=STTWorkerConfig(),
        ctx=ctx,
        log_queue=ctx.Queue(),
        log_level=20,
    )


def test_sttresult_survives_pickling():
    event = TranscriptEvent(
        channel_id="rx", segment_id="rx-1", text="안녕하세요", start=0.0, end=1.5
    )
    result = SttResult(event=event, stt_latency_s=0.42)

    back = pickle.loads(pickle.dumps(result))

    assert back.event == event
    assert back.stt_latency_s == 0.42


def test_sttresult_carries_no_latency_for_partials():
    event = TranscriptEvent(
        channel_id="rx", segment_id="rx-1", text="안녕", start=0.0, end=1.0, is_final=False
    )
    assert SttResult(event=event, stt_latency_s=None).stt_latency_s is None


def test_name_matches_the_supervised_target_name():
    assert _handle("rx").name == "STTWorker-rx"


def test_ident_is_none_before_start():
    assert _handle().ident is None


def test_stop_sets_stopping():
    handle = _handle()
    assert handle.stopping is False
    handle.stop()
    assert handle.stopping is True


def test_last_activity_reports_active_while_not_ready():
    handle = _handle()
    assert not handle.ready

    before = time.monotonic()
    reported = handle.last_activity
    after = time.monotonic()

    assert before <= reported <= after


def test_last_activity_reads_the_heartbeat_once_ready():
    handle = _handle()
    stamp = time.monotonic() - 3.0
    handle._heartbeat.value = stamp
    handle._ready_event.set()

    assert handle.ready
    assert handle.last_activity == pytest.approx(stamp)


def test_last_activity_ignores_an_unseeded_heartbeat_even_when_ready():
    handle = _handle()
    handle._ready_event.set()

    before = time.monotonic()
    reported = handle.last_activity

    assert reported >= before


def test_wait_ready_times_out_when_the_child_never_starts():
    assert _handle().wait_ready(timeout=0.05) is False


def test_read_self_cpu_ticks_returns_a_nonnegative_int():
    ticks = _read_self_cpu_ticks()
    assert ticks is not None
    assert ticks >= 0


def _run_bridge_briefly(monkeypatch, cpu_ticks, worker_last_activity: float) -> float:
    import edge_voice.stt.stt_process as stt_process

    monkeypatch.setattr(stt_process, "HEARTBEAT_INTERVAL_S", 0.02)
    ticks = iter(cpu_ticks)
    monkeypatch.setattr(stt_process, "_read_self_cpu_ticks", lambda: next(ticks))

    worker = SimpleNamespace(last_activity=worker_last_activity, stop=lambda: None)
    stop_event = threading.Event()
    heartbeat = SimpleNamespace(value=0.0)

    t = threading.Thread(target=_bridge, args=(worker, stop_event, heartbeat), daemon=True)
    t.start()
    time.sleep(0.02 * 5)
    stop_event.set()
    t.join(timeout=1.0)
    return heartbeat.value


def test_bridge_keeps_heartbeat_fresh_while_cpu_keeps_advancing(monkeypatch):
    frozen_dequeue_stamp = 12345.0
    before = time.monotonic()

    reported = _run_bridge_briefly(
        monkeypatch, cpu_ticks=range(0, 10_000, 10), worker_last_activity=frozen_dequeue_stamp
    )

    after = time.monotonic()
    assert before <= reported <= after
    assert reported != frozen_dequeue_stamp


def test_bridge_falls_back_to_worker_last_activity_when_cpu_is_flat(monkeypatch):
    frozen_dequeue_stamp = 12345.0

    reported = _run_bridge_briefly(
        monkeypatch, cpu_ticks=[42] * 20, worker_last_activity=frozen_dequeue_stamp
    )

    assert reported == frozen_dequeue_stamp


@pytest.mark.integration
def test_child_process_transcribes_a_segment_end_to_end():
    ctx = new_mp_context()
    segment_queue = ctx.Queue()
    handle = STTProcessHandle(
        channel_id="rx",
        segment_queue=segment_queue,
        config=STTWorkerConfig(model="moonshine"),
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
        while True:
            try:
                handle.transcript_queue.get_nowait()
            except queue.Empty:
                break
        handle.join(timeout=30)
        handle.kill()
