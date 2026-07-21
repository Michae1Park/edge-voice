"""Tests for edge_voice.webui.app."""

import asyncio
import json

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
