"""Tests for edge_voice.pipeline.orchestrator."""

import queue
import threading
from unittest import mock

import pytest

from edge_voice.config.settings import (
    AudioSettings,
    MQTTChannels,
    MQTTSettings,
    Settings,
    QueuesSettings,
)
from edge_voice.pipeline.orchestrator import PipelineOrchestrator


# ── helpers ─────────────────────────────────────


def _mock_mqtt_channels():
    return [
        MQTTChannels(topic="stt/audio_chunks_rx", channel_id="rx"),
        MQTTChannels(topic="stt/audio_chunks_tx", channel_id="tx"),
    ]


def _minimal_settings(queues: QueuesSettings | None = None) -> Settings:
    q = queues or QueuesSettings()
    return Settings(
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


# ── __init__ ─────────────────────────────────────


def test_init_default_state():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    assert orch._ingest_queue is None
    assert orch._router_queue is None
    assert orch._routed_queue is None
    assert orch._dump_queue is None
    assert orch._segment_queue is None
    assert orch._stop_event.is_set() is False


def test_build_sets_status_running_false():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._status.running is False


def test_build_sets_status_running_true():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    assert orch._status.running is True


def test_stop_sets_running_false():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    orch.stop()
    assert orch._status.running is False


def test_build_creates_correct_workers():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert orch._audio_source is not None
    assert orch._router is not None
    assert orch._tracker is not None
    assert orch._vad is not None
    assert orch._stt is not None


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


# ── queues ─────────────────────────────────────


def test_build_creates_queues():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    assert isinstance(orch._ingest_queue, queue.Queue)
    assert isinstance(orch._router_queue, queue.Queue)
    assert isinstance(orch._routed_queue, queue.Queue)
    assert isinstance(orch._segment_queue, queue.Queue)


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
    assert orch._segment_queue.maxsize == 128


# ── status / get_status ─────────────────────────


def test_worker_status_after_build():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    states = {w.name: w.state for w in orch._status.workers}
    assert states["audio_source"] == "built"
    assert states["router"] == "built"
    assert states["vad"] == "built"
    assert states["stt"] == "built"


def test_get_status_after_start():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    status = orch.get_status()
    assert status.running is True
    assert len(status.workers) > 0
    orch.stop()


def test_worker_states_after_stop():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)
    orch.build()
    orch.start()
    orch.stop()
    states = {w.name: w.state for w in orch._status.workers}
    assert all(state == "stopped" for state in states.values())


# ── ingest_queue property ──────────────────────


def test_ingest_queue_property_raises_before_build():
    orch = PipelineOrchestrator(_minimal_settings())
    with pytest.raises(RuntimeError, match="Pipeline not built"):
        _ = orch.ingest_queue


def test_ingest_queue_property_returns_queue_after_build():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    assert isinstance(orch.ingest_queue, queue.Queue)


# ── run_with_timer ─────────────────────────────


@pytest.mark.integration
def test_run_with_timer_shuts_down_cleanly():
    s = _minimal_settings()
    orch = PipelineOrchestrator(s)

    def finish_timer():
        import time

        time.sleep(1)
        with mock.patch.object(orch, "_stop_event"):
            orch._stop_event.is_set.return_value = True

    t = threading.Thread(target=finish_timer, daemon=True)
    t.start()
    orch.run_with_timer(duration_s=2)
    t.join(timeout=3)
    assert orch._stop_event.is_set()


# ── stop / build state transitions ─────────────


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
