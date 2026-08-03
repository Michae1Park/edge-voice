"""Tests for edge_voice.webui.app."""

import asyncio
import json
import time

import pytest
from fastapi.testclient import TestClient

from edge_voice.config.settings import (
    AudioSettings,
    MQTTChannels,
    MQTTSettings,
    QueuesSettings,
    Settings,
)
from edge_voice.pipeline.models import TranscriptEvent
from edge_voice.pipeline.orchestrator import PipelineOrchestrator
from edge_voice.webui.app import _sse_events, create_app


def _minimal_settings() -> Settings:
    return Settings(
        mqtt=MQTTSettings(
            broker_host="localhost",
            broker_port=1883,
            channels=[
                MQTTChannels(topic="stt/audio_chunks_rx", channel_id="rx"),
                MQTTChannels(topic="stt/audio_chunks_tx", channel_id="tx"),
            ],
        ),
        audio=AudioSettings(sample_rate=16000, chunk_samples=320),
        queues=QueuesSettings(),
    )


@pytest.fixture
def orchestrator():
    orch = PipelineOrchestrator(_minimal_settings())
    orch.build()
    yield orch
    orch.stop()
    orch.wait()


@pytest.fixture
def client(orchestrator):
    return TestClient(create_app(orchestrator))


@pytest.fixture
def make_client():
    """Build a client over a fresh orchestrator with settings overrides.

    The plain `client` fixture covers the default case; this one exists for
    tests that need metrics off, or a tick fast enough to observe.
    """
    built: list[PipelineOrchestrator] = []

    def _make(metrics_enabled: bool = True, emit_interval_s: float | None = None) -> TestClient:
        settings = _minimal_settings()
        settings.metrics.enabled = metrics_enabled
        if emit_interval_s is not None:
            settings.metrics.emit_interval_s = emit_interval_s
        orch = PipelineOrchestrator(settings)
        orch.build()
        built.append(orch)
        return TestClient(create_app(orch))

    yield _make

    for orch in built:
        orch.stop()
        orch.wait()


def _wait_until(predicate, timeout: float = 3.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# -- HTTP endpoints -----------------------------


def test_index_serves_console_html(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "EDGE-VOICE" in resp.text


def test_status_before_start(client):
    resp = client.get("/api/status")
    assert resp.status_code == 200
    assert resp.json()["running"] is False


def test_start_then_stop_via_api(client):
    resp = client.post("/api/start")
    assert resp.status_code == 200
    assert resp.json()["running"] is True

    resp = client.post("/api/stop")
    assert resp.status_code == 200
    assert resp.json()["running"] is False


# -- /api/status health object (Milestone 7) -----------------------------


def test_status_is_a_superset_of_get_status(client, orchestrator):
    body = client.get("/api/status").json()
    for key, value in orchestrator.get_status().items():
        assert body[key] == value


def test_status_reports_configured_channels(client):
    channels = client.get("/api/status").json()["channels"]
    assert set(channels) == {"rx", "tx"}
    # Built but never started: no packets have been seen on either channel.
    assert channels["rx"] == {"freshness_s": None, "stale": False}


def test_status_metrics_pending_before_first_tick(client):
    body = client.get("/api/status").json()
    assert body["metrics"] == {"state": "pending", "age_s": None}
    assert body["mqtt_connected"] is None
    assert body["restarts"] is None


def test_status_metrics_disabled(make_client):
    body = make_client(metrics_enabled=False).get("/api/status").json()
    assert body["metrics"]["state"] == "disabled"


def test_status_reports_queue_depths_even_when_metrics_disabled(make_client):
    # Queue depths are read live, not from the metrics snapshot.
    body = make_client(metrics_enabled=False).get("/api/status").json()
    assert set(body["queue_depths"]) == {"ingest", "routed", "segment"}


def test_status_surfaces_snapshot_fields_after_a_tick(make_client):
    """Once a tick lands, the snapshot-derived fields stop being None.

    Deliberately does not assert *which* way mqtt_connected goes: whether a
    broker is reachable is a property of the machine running the tests, not
    of this code. The verdict logic for each case is pinned deterministically
    in test_health_reporting.py; what matters here is that the wiring
    surfaces a real value and that the verdict agrees with it.
    """
    client = make_client(emit_interval_s=0.05)
    client.post("/api/start")

    assert _wait_until(lambda: client.get("/api/status").json()["metrics"]["state"] == "ok")
    body = client.get("/api/status").json()

    assert body["metrics"]["age_s"] is not None
    assert isinstance(body["mqtt_connected"], bool)  # not None: metrics has ticked
    assert body["restarts"]["max"] == 3
    assert body["restarts"]["window_s"] == 60.0
    assert set(body["restarts"]["counts"]) == set(body["workers"])
    assert set(body["latencies"]) == {"router", "vad_silero", "vad_rms_gate", "stt"}
    assert body["status"] == ("warn" if body["mqtt_connected"] is False else "ok")


def test_status_reports_worker_state_even_while_metrics_pending(client):
    """Module state is live; module latency needs a tick. The UI renders the
    two independently, so the payload must too."""
    body = client.get("/api/status").json()
    assert body["metrics"]["state"] == "pending"
    assert body["latencies"] is None
    assert set(body["workers"]) == {
        "MqttAudioIngest",
        "ChannelRouter",
        "VADWorker",
        "STTWorker",
    }


# -- SSE transcript stream -----------------------------


class _FakeRequest:
    async def is_disconnected(self) -> bool:
        return False


@pytest.mark.asyncio
async def test_sse_stream_yields_published_transcript(orchestrator):
    gen = _sse_events(orchestrator, _FakeRequest())
    event = TranscriptEvent(channel_id="rx", segment_id="seg-1", text="hi", start=0.0, end=1.0)

    async def publish_soon():
        await asyncio.sleep(0.05)
        orchestrator.transcripts.publish(event)

    asyncio.get_event_loop().create_task(publish_soon())
    line = await asyncio.wait_for(gen.__anext__(), timeout=2)
    await gen.aclose()

    assert line.startswith("data: ")
    payload = json.loads(line[len("data: ") :].strip())
    assert payload["channel_id"] == "rx"
    assert payload["text"] == "hi"


@pytest.mark.asyncio
async def test_sse_stream_replays_backlog_on_subscribe(orchestrator):
    orchestrator.transcripts.publish(
        TranscriptEvent(channel_id="tx", segment_id="seg-1", text="backlog", start=0.0, end=1.0)
    )
    gen = _sse_events(orchestrator, _FakeRequest())
    line = await asyncio.wait_for(gen.__anext__(), timeout=2)
    await gen.aclose()

    payload = json.loads(line[len("data: ") :].strip())
    assert payload["text"] == "backlog"
