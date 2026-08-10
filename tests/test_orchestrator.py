"""Tests for edge_voice.pipeline.orchestrator."""

import queue
import threading

import pytest

from edge_voice.config.settings import (
    AudioSettings,
    MQTTChannels,
    MQTTSettings,
    Settings,
    QueuesSettings,
)
from edge_voice.pipeline.orchestrator import PipelineOrchestrator


# -- helpers -----------------------------


def _mock_mqtt_channels():
    return [
        MQTTChannels(topic="stt/audio_chunks_rx", channel_id="rx"),
        MQTTChannels(topic="stt/audio_chunks_tx", channel_id="tx"),
    ]


def _minimal_settings(
    queues: QueuesSettings | None = None, use_processes: bool = False
) -> Settings:
    """Thread-backed STT by default.

    Production runs `stt.use_processes=True`, but spawning two interpreters
    and loading two models per test would make this suite minutes slower for
    no extra coverage: the decode path is identical either way (see
    stt/stt_process.py), and the process plumbing has its own tests in
    tests/test_stt_process.py.
    """
    q = queues or QueuesSettings()
    settings = Settings(
        mqtt=MQTTSettings(
            broker_host="localhost",
            broker_port=1883,
            channels=_mock_mqtt_channels(),
        ),
        audio=AudioSettings(
            sample_rate=16000,
            chunk_samples=320,
        ),
        queues=q,
    )
    settings.stt.use_processes = use_processes
    return settings


# -- __init__ -----------------------------


def test_init_default_state():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    assert orch._ingest_queue is None
    assert orch._routed_queue is None
    assert orch._segment_queues is None
    assert orch._dump_queue is None
    assert orch._segment_dump_queue is None
    assert orch._stop_event.is_set() is False


def test_build_sets_running_false():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert not orch.get_status()["running"]


def test_build_sets_running_true():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    assert orch.get_status()["running"]
    orch.stop()
    orch.wait()


def test_stop_sets_running_false():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    orch.stop()
    orch.wait()
    assert not orch.get_status()["running"]


def test_build_creates_correct_workers():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._audio_source is not None
    assert orch._router is not None
    assert orch._vad is not None
    assert set(orch._stt) == {"rx", "tx"}  # one dedicated STTWorker per channel


def test_build_gives_vad_a_channel_per_configured_channel():
    """Issue #10: model loading moves to VADWorker construction, off the
    real-time path -- see VADWorker.__init__'s docstring.

    Before this, VADWorker._channels was only populated lazily by the first
    packet on each channel_id. _build_vad() now passes
    Settings.mqtt.channels straight into the constructor, so build() alone
    -- no packet, no start() -- already has channel state (and therefore a
    loaded model) for every configured channel.
    """
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert set(orch._vad._channels) == {"rx", "tx"}


def test_build_warms_up_stt_transcriber():
    """_build_stt() -> STTWorker.__init__() resolves each channel's own
    Transcriber eagerly -- see the module docstring. No packet, no segment,
    no start() needed; build() alone is enough, for every channel."""
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert all(w._transcriber is not None for w in orch._stt.values())


def test_build_with_dump_enabled():
    s = _minimal_settings()
    s.dump.enabled = True
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._dump_worker is not None


def test_build_with_segment_dump_enabled():
    s = _minimal_settings()
    s.segment_dump.enabled = True
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._segment_dump_worker is not None


# -- queues -----------------------------


def test_build_creates_queues():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert isinstance(orch._ingest_queue, queue.Queue)
    assert isinstance(orch._routed_queue, queue.Queue)
    assert set(orch._segment_queues) == {"rx", "tx"}
    assert all(isinstance(q, queue.Queue) for q in orch._segment_queues.values())


def test_ingest_queue_maxsize_from_settings():
    s = _minimal_settings(queues=QueuesSettings(ingest=512))
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._ingest_queue.maxsize == 512


def test_routed_queue_maxsize_from_settings():
    s = _minimal_settings(queues=QueuesSettings(routed=256))
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._routed_queue.maxsize == 256


def test_segment_queue_maxsize_from_settings():
    s = _minimal_settings(queues=QueuesSettings(segment=128))
    orch = PipelineOrchestrator(s)
    orch.build()
    assert all(q.maxsize == 128 for q in orch._segment_queues.values())


# -- status / get_status -------------------


def test_worker_status_after_build():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    # After build (before start) workers are created but not running
    status = orch.get_status()
    assert not status["running"]
    workers = list(status["workers"].keys())
    assert "MqttAudioIngest" in workers
    assert "ChannelRouter" in workers
    assert "VADWorker" in workers
    assert "STTWorker-rx" in workers
    assert "STTWorker-tx" in workers


def test_get_status_after_start():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    status = orch.get_status()
    assert status["running"]
    assert len(status["workers"]) > 0
    orch.stop()
    orch.wait()


def test_worker_states_after_stop():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    orch.stop()
    orch.wait()
    assert not orch.get_status()["running"]


# -- _join()'s kill-on-timeout fallback (the shutdown-hang guard) ----------
#
# Reproduces, at unit-test speed, why bench_pipeline_load.py could hang
# indefinitely on the RPi5 (never on faster hardware) under sustained STT
# backlog: a still-alive worker was previously just logged and left running
# past WORKER_JOIN_TIMEOUT_S, with no attempt to reclaim it. A process-backed
# worker (STTProcessHandle) is reclaimable even while wedged inside a long
# native decode call (unlike a thread, which Python has no way to
# force-stop) -- _join() must use that whenever it's available.


class _FakeProcessWorker:
    """Stands in for STTProcessHandle: exposes .kill(), like a real
    process-backed worker, and never stops on its own -- simulates one still
    mid-decode past the join timeout."""

    def __init__(self, name: str = "FakeSTT", dies_on_kill: bool = True) -> None:
        self.name = name
        self.ident: int | None = 123
        self._alive = True
        self._dies_on_kill = dies_on_kill
        self.kill_called = False
        self.join_calls = 0

    def join(self, timeout: float | None = None) -> None:
        self.join_calls += 1

    def is_alive(self) -> bool:
        return self._alive

    def kill(self) -> None:
        self.kill_called = True
        if self._dies_on_kill:
            self._alive = False


class _FakeThreadWorker:
    """Stands in for a plain threading.Thread-based worker: no .kill at
    all, since Python offers no way to force-stop a thread."""

    def __init__(self, name: str = "FakeThread", alive: bool = True) -> None:
        self.name = name
        self.ident: int | None = 456
        self._alive = alive

    def join(self, timeout: float | None = None) -> None:
        pass

    def is_alive(self) -> bool:
        return self._alive


def test_join_kills_a_still_alive_worker_that_exposes_kill():
    worker = _FakeProcessWorker()

    PipelineOrchestrator._join(worker)

    assert worker.kill_called is True
    assert worker.is_alive() is False
    assert worker.join_calls == 2  # once before the kill attempt, once after


def test_join_does_not_call_kill_on_a_worker_that_already_stopped():
    """The common case -- no unnecessary kill on a worker that simply
    finished within the timeout."""
    worker = _FakeProcessWorker(dies_on_kill=True)
    worker._alive = False  # already stopped by the time join() returns

    PipelineOrchestrator._join(worker)

    assert worker.kill_called is False
    assert worker.join_calls == 1


def test_join_falls_back_to_a_warning_when_no_kill_is_available():
    """A wedged thread has no .kill -- _join must not crash trying to call
    a method that doesn't exist, and must leave it running (nothing else it
    can do; the OS watchdog is the real remedy for a thread)."""
    worker = _FakeThreadWorker(alive=True)

    PipelineOrchestrator._join(worker)  # must not raise

    assert worker.is_alive() is True


# -- _abandon_segment_queue_backlog (the interpreter-exit hang guard) ------
#
# Reproduces, at unit-test speed, a second RPi5 hang found right after the
# first: even once a wedged STT child is reaped (_join's kill fallback,
# above), a real backlog left in its segment_queue can still block the
# *parent* forever. `ps -eLo wchan` on the RPi5 showed two threads parked in
# pipe_write (the queue's feeder threads, stuck because the child that would
# have drained them was already gone) and the main thread parked in
# futex_wait_queue (joining them) -- multiprocessing joins a Queue's feeder
# thread with no timeout at interpreter exit, so the whole process hung.


class _FakeMpQueue:
    """Stands in for an mp.Queue -- has .cancel_join_thread(), like the real
    thing, unlike a plain queue.Queue (used in thread mode)."""

    def __init__(self) -> None:
        self.cancelled = False

    def cancel_join_thread(self) -> None:
        self.cancelled = True


def test_abandon_segment_queue_backlog_cancels_join_thread_on_every_queue():
    orch = PipelineOrchestrator(_minimal_settings())
    rx, tx = _FakeMpQueue(), _FakeMpQueue()
    orch._segment_queues = {"rx": rx, "tx": tx}

    orch._abandon_segment_queue_backlog()

    assert rx.cancelled is True
    assert tx.cancelled is True


def test_abandon_segment_queue_backlog_ignores_a_plain_queue():
    """Thread mode's segment_queue is a stdlib queue.Queue with no
    cancel_join_thread at all -- must not crash calling a method that
    doesn't exist on it."""
    orch = PipelineOrchestrator(_minimal_settings())
    orch._segment_queues = {"rx": queue.Queue()}

    orch._abandon_segment_queue_backlog()  # must not raise


def test_abandon_segment_queue_backlog_is_a_noop_before_build():
    orch = PipelineOrchestrator(_minimal_settings())

    orch._abandon_segment_queue_backlog()  # must not raise; _segment_queues is None


# -- reliability / supervisor (Milestone 6) --


def test_status_reports_not_degraded_when_healthy():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    try:
        status = orch.get_status()
        assert "degraded" in status
        assert status["degraded"] is False
    finally:
        orch.stop()
        orch.wait()


def test_supervisor_built_when_reliability_enabled():
    s = _minimal_settings()
    s.reliability.enabled = True
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._supervisor is not None


def test_no_supervisor_when_reliability_disabled():
    s = _minimal_settings()
    s.reliability.enabled = False
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._supervisor is None
    # Pipeline still starts and stops cleanly with no supervisor at all.
    orch.start()
    orch.stop()
    orch.wait()
    assert not orch.get_status()["running"]


def test_supervisor_thread_stops_with_pipeline():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    sup = orch._supervisor
    assert sup is not None and sup.is_alive()
    orch.stop()
    orch.wait()
    assert not sup.is_alive()


def test_restart_swaps_in_fresh_running_worker():
    # Exercises the real restart mechanics the supervisor drives: rebuild the
    # worker via its actual builder, on the same queue, and start it.
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    try:
        old_stt = orch._stt["rx"]
        old_queue = orch._segment_queues["rx"]
        orch._restart_dict_worker(orch._stt, "rx", lambda: orch._build_stt("rx"))
        assert orch._stt["rx"] is not old_stt  # a genuinely new instance
        assert orch._stt["rx"].is_alive()
        assert orch._segment_queues["rx"] is old_queue  # same queue, not rebuilt
        assert orch._stt["tx"] is not None  # the other channel's worker untouched
    finally:
        orch.stop()
        orch.wait()


def test_restart_does_not_start_worker_after_shutdown():
    # A late restart (e.g. one dispatched just as stop() runs) must not
    # resurrect a worker into a pipeline that's already torn down.
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    orch.stop()
    orch.wait()
    old_stt = orch._stt["rx"]
    orch._restart_dict_worker(orch._stt, "rx", lambda: orch._build_stt("rx"))
    assert orch._stt["rx"] is not old_stt
    assert not orch._stt["rx"].is_alive()  # rebuilt but not started


# -- ingest_queue property ---------


def test_ingest_queue_property_raises_before_build():
    orch = PipelineOrchestrator(_minimal_settings())
    with pytest.raises(RuntimeError, match="Pipeline not built"):
        _ = orch.ingest_queue


def test_ingest_queue_property_returns_queue_after_build():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    assert isinstance(orch.ingest_queue, queue.Queue)


# -- run_with_timer --


def test_run_with_timer_shuts_down_cleanly():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()

    def finish_timer():
        import time

        time.sleep(1)
        orch.stop()

    t = threading.Thread(target=finish_timer, daemon=True)
    t.start()
    orch.run_with_timer(duration_s=2)
    t.join(timeout=3)
    assert not orch.get_status()["running"]


# -- stop / build state transitions ----


def test_build_clears_stop_event():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch._stop_event.set()
    orch.build()
    assert orch._stop_event.is_set() is False


def test_multiple_builds_no_error():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.stop()
    orch.build()
    orch.stop()


# -- health accessors (Milestone 7) ----


def test_channel_freshness_before_build_uses_configured_channels():
    # build() hasn't run, so there's no router to ask -- fall back to config
    # so the UI's channel rows don't appear/disappear across lifecycle phases.
    orch = PipelineOrchestrator(_minimal_settings())
    assert orch.channel_freshness() == {"rx": None, "tx": None}


def test_channel_freshness_after_build_reads_router_channel_ids():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    freshness = orch.channel_freshness()
    assert set(freshness) == {"rx", "tx"}
    assert all(v is None for v in freshness.values())  # no packets seen yet


def test_metrics_snapshot_is_none_when_metrics_disabled():
    s = _minimal_settings()
    s.metrics.enabled = False
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._metrics is None
    assert orch.metrics_snapshot() is None


def test_metrics_snapshot_is_none_before_first_tick():
    s = _minimal_settings()
    s.metrics.enabled = True
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._metrics is not None
    assert orch.metrics_snapshot() is None


def test_health_is_a_superset_of_get_status():
    # Detailed assembly is covered in test_health_reporting.py; this only
    # confirms the orchestrator wires the same values through.
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    status = orch.get_status()
    health = orch.health()
    for key, value in status.items():
        assert health[key] == value
    assert health["status"] == "down"  # built but not started
    assert set(health["channels"]) == {"rx", "tx"}


# -- partial transcripts -----------------------------


def _built_orchestrator_and_callback():
    """The _on_transcript closure _build_stt hands to the "rx" STTWorker.

    Every test using this passes channel_id="rx" events, so pointing at
    that one worker keeps them exercising the same closure as before --
    each channel's STTWorker gets its own _on_transcript now, but the
    logic being tested here doesn't depend on which channel it's for.
    """
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    return orch, orch._stt["rx"]._on_transcript


def test_partial_transcript_does_not_emit_the_segment_closing_log_line(caplog):
    """TRANSCRIPT is the one-per-segment lifecycle close (see _on_transcript).
    A revisable prefix must not fire it, or the trace gains N lines per
    segment and each reports a latency belonging to some other segment."""
    from edge_voice.pipeline.models import TranscriptEvent

    orch, on_transcript = _built_orchestrator_and_callback()
    with caplog.at_level("DEBUG"):
        on_transcript(
            TranscriptEvent(
                channel_id="rx",
                segment_id="rx-1",
                text="안녕",
                start=0.0,
                end=1.5,
                is_final=False,
            )
        )

    assert not any("TRANSCRIPT" in r.message for r in caplog.records)
    assert any("PARTIAL" in r.message for r in caplog.records)


def test_partial_transcript_does_not_read_stale_stt_latency(caplog):
    """last_latency_s belongs to the last *final*; _handle_partial leaves it
    alone, so the partial branch must not attach it to this segment."""
    from edge_voice.pipeline.models import TranscriptEvent

    orch, on_transcript = _built_orchestrator_and_callback()
    with caplog.at_level("DEBUG"):
        on_transcript(
            TranscriptEvent(
                channel_id="rx", segment_id="rx-1", text="안녕", start=0.0, end=1.5, is_final=False
            )
        )

    assert not any(hasattr(r, "stt_latency_s") for r in caplog.records)


def test_partial_transcript_still_reaches_the_live_feed():
    from edge_voice.pipeline.models import TranscriptEvent

    orch, on_transcript = _built_orchestrator_and_callback()
    sub = orch.transcripts.subscribe()
    event = TranscriptEvent(
        channel_id="rx", segment_id="rx-1", text="안녕", start=0.0, end=1.5, is_final=False
    )
    on_transcript(event)

    assert sub.get_nowait() is event


def test_final_transcript_still_emits_the_closing_log_line(caplog):
    from edge_voice.pipeline.models import TranscriptEvent

    orch, on_transcript = _built_orchestrator_and_callback()
    # _handle_segment always sets this immediately before publishing a final
    # (see _on_transcript's comment); the log line formats it as %.3f, so
    # standing in for that write is what makes this callable in isolation.
    orch._stt["rx"]._last_latency_s = 0.42
    with caplog.at_level("INFO"):
        on_transcript(
            TranscriptEvent(
                channel_id="rx", segment_id="rx-1", text="안녕하세요", start=0.0, end=2.0
            )
        )

    assert any("TRANSCRIPT" in r.message for r in caplog.records)


# -- per-channel STT latency aggregation -----------------------------


def test_stt_latency_s_is_max_across_channels():
    """No single "most recent across channels" exists anymore with one
    worker per channel -- see _stt_latency_s's docstring. max() is what's
    actually computed.

    Seeds the per-channel dict rather than each worker's `_last_latency_s`:
    the aggregate is derived from the per-channel view precisely so that one
    code path serves both STT modes (a child process's attributes are
    unreachable, so process mode has only the per-channel mirror).
    """
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    orch._stt["rx"]._channel_latency_s["rx"] = 0.10
    orch._stt["tx"]._channel_latency_s["tx"] = 0.42
    assert orch._stt_latency_s() == 0.42


def test_stt_latency_s_is_none_when_no_worker_has_decoded_yet():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    assert orch._stt_latency_s() is None


def test_stt_channel_latencies_s_merges_without_collision():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    orch._stt["rx"]._channel_latency_s["rx"] = 0.10
    orch._stt["tx"]._channel_latency_s["tx"] = 0.42
    assert orch._stt_channel_latencies_s() == {"rx": 0.10, "tx": 0.42}


# -- process-backed STT (the deployed configuration) -----------------------------


@pytest.mark.integration
def test_process_mode_builds_handles_and_reports_them_as_workers():
    """Spawns two real STT child processes (loads two models).

    The unit tests above all run thread-backed for speed; this is the one
    that proves the deployed configuration actually wires up.
    """
    orch = PipelineOrchestrator(_minimal_settings(use_processes=True))
    orch.build()
    try:
        # Handles, not threads -- distinguished by their IPC surface.
        assert set(orch._stt) == {"rx", "tx"}
        assert all(hasattr(h, "transcript_queue") for h in orch._stt.values())
        # Not started yet, so no pids and nothing is ready.
        assert all(h.ident is None for h in orch._stt.values())

        orch.start()
        # start() blocks on readiness, so by here both models are loaded.
        assert all(h.ready for h in orch._stt.values())
        assert all(h.ident is not None for h in orch._stt.values())

        workers = orch.get_status()["workers"]
        assert workers["STTWorker-rx"] == "running"
        assert workers["STTWorker-tx"] == "running"
        assert set(orch.queue_depths()) >= {"segment_rx", "segment_tx"}
    finally:
        orch.stop()
        orch.wait()

    # Shutdown must actually reap the children -- an undrained transcript
    # queue would otherwise hold them open (see the plan's section 5.7).
    assert all(not h.is_alive() for h in orch._stt.values())
