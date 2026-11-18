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


def _mock_mqtt_channels():
    return [
        MQTTChannels(topic="stt/audio_chunks_rx", channel_id="rx"),
        MQTTChannels(topic="stt/audio_chunks_tx", channel_id="tx"),
    ]


def _minimal_settings(
    queues: QueuesSettings | None = None, use_processes: bool = False
) -> Settings:
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
    settings.stt.model = "moonshine"
    return settings


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
    assert set(orch._stt) == {"rx", "tx"}


def test_build_gives_vad_a_channel_per_configured_channel():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert set(orch._vad._channels) == {"rx", "tx"}


def test_build_warms_up_stt_transcriber():
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


def test_worker_status_after_build():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
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


class _FakeProcessWorker:

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
    assert worker.join_calls == 2


def test_join_does_not_call_kill_on_a_worker_that_already_stopped():
    worker = _FakeProcessWorker(dies_on_kill=True)
    worker._alive = False

    PipelineOrchestrator._join(worker)

    assert worker.kill_called is False
    assert worker.join_calls == 1


def test_join_falls_back_to_a_warning_when_no_kill_is_available():
    worker = _FakeThreadWorker(alive=True)

    PipelineOrchestrator._join(worker)

    assert worker.is_alive() is True


class _FakeMpQueue:

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
    orch = PipelineOrchestrator(_minimal_settings())
    orch._segment_queues = {"rx": queue.Queue()}

    orch._abandon_segment_queue_backlog()


def test_abandon_segment_queue_backlog_is_a_noop_before_build():
    orch = PipelineOrchestrator(_minimal_settings())

    orch._abandon_segment_queue_backlog()


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
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    try:
        old_stt = orch._stt["rx"]
        old_queue = orch._segment_queues["rx"]
        orch._restart_dict_worker(orch._stt, "rx", lambda: orch._build_stt("rx"))
        assert orch._stt["rx"] is not old_stt
        assert orch._stt["rx"].is_alive()
        assert orch._segment_queues["rx"] is old_queue
        assert orch._stt["tx"] is not None
    finally:
        orch.stop()
        orch.wait()


def test_restart_does_not_start_worker_after_shutdown():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    orch.stop()
    orch.wait()
    old_stt = orch._stt["rx"]
    orch._restart_dict_worker(orch._stt, "rx", lambda: orch._build_stt("rx"))
    assert orch._stt["rx"] is not old_stt
    assert not orch._stt["rx"].is_alive()


def test_ingest_queue_property_raises_before_build():
    orch = PipelineOrchestrator(_minimal_settings())
    with pytest.raises(RuntimeError, match="Pipeline not built"):
        _ = orch.ingest_queue


def test_ingest_queue_property_returns_queue_after_build():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    assert isinstance(orch.ingest_queue, queue.Queue)


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


def test_channel_freshness_before_build_uses_configured_channels():
    orch = PipelineOrchestrator(_minimal_settings())
    assert orch.channel_freshness() == {"rx": None, "tx": None}


def test_channel_freshness_after_build_reads_router_channel_ids():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    freshness = orch.channel_freshness()
    assert set(freshness) == {"rx", "tx"}
    assert all(v is None for v in freshness.values())


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
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    status = orch.get_status()
    health = orch.health()
    for key, value in status.items():
        assert health[key] == value
    assert health["status"] == "down"
    assert set(health["channels"]) == {"rx", "tx"}


def _built_orchestrator_and_callback():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    return orch, orch._stt["rx"]._on_transcript


def test_partial_transcript_does_not_emit_the_segment_closing_log_line(caplog):
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
    orch._stt["rx"]._last_latency_s = 0.42
    with caplog.at_level("INFO"):
        on_transcript(
            TranscriptEvent(
                channel_id="rx", segment_id="rx-1", text="안녕하세요", start=0.0, end=2.0
            )
        )

    assert any("TRANSCRIPT" in r.message for r in caplog.records)


def test_stt_latency_s_is_max_across_channels():
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


@pytest.mark.integration
def test_process_mode_builds_handles_and_reports_them_as_workers():
    orch = PipelineOrchestrator(_minimal_settings(use_processes=True))
    orch.build()
    try:
        assert set(orch._stt) == {"rx", "tx"}
        assert all(hasattr(h, "transcript_queue") for h in orch._stt.values())
        assert all(h.ident is None for h in orch._stt.values())

        orch.start()
        assert all(h.ready for h in orch._stt.values())
        assert all(h.ident is not None for h in orch._stt.values())

        workers = orch.get_status()["workers"]
        assert workers["STTWorker-rx"] == "running"
        assert workers["STTWorker-tx"] == "running"
        assert set(orch.queue_depths()) >= {"segment_rx", "segment_tx"}
    finally:
        orch.stop()
        orch.wait()

    assert all(not h.is_alive() for h in orch._stt.values())
