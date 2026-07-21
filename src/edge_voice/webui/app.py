"""FastAPI app for the kiosk console UI.

Runs in-process with cli.py/orchestrator (see docs/BUILDPLAN.md Milestone 5)
-- the deployment target has no network at all, so the only client is a
kiosk browser on the device's own attached display, and there is no reverse
proxy or IPC boundary to be gained by running this as a separate process.

Two different data-access patterns on purpose:
  - Transcripts are a stream of discrete events -> SSE, fed by
    TranscriptHub's per-connection subscriber queue (pipeline/transcript_hub.py).
  - Pipeline/worker status is current state, not a stream -> plain GET,
    reading orchestrator.get_status() directly. No queue: a queue would just
    accumulate stale status snapshots a client never drained.

No MQTT anywhere in this module -- MQTT stays scoped to the audio_generation
<-> audio_ingest process boundary it was built for.
"""

from __future__ import annotations

import json
import logging
import queue
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from edge_voice.pipeline.models import TranscriptEvent
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)

_TEMPLATE_PATH = Path(__file__).parent / "templates" / "console.html"

# How long each blocking Queue.get() waits before looping back to check for
# client disconnect. Not a "refresh rate" -- a new transcript interrupts the
# wait immediately; this only bounds how late a disconnect is noticed.
SSE_POLL_TIMEOUT_S = 1.0


def create_app(orchestrator: PipelineOrchestrator) -> FastAPI:
    app = FastAPI(title="edge-voice")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _TEMPLATE_PATH.read_text()

    @app.get("/api/status")
    def status() -> dict:
        return orchestrator.get_status()

    @app.post("/api/start")
    async def start() -> dict:
        # start() itself is near-instant (just spawns threads), but run it
        # off the event loop anyway so a slow model load on first start
        # can't stall other requests (e.g. the status poll) in the meantime.
        await run_in_threadpool(orchestrator.start)
        return orchestrator.get_status()

    @app.post("/api/stop")
    async def stop() -> dict:
        # stop() joins each worker in turn (see orchestrator.stop()) -- real
        # blocking work, so it must not run on the event loop thread.
        await run_in_threadpool(orchestrator.stop)
        await run_in_threadpool(orchestrator.wait)
        return orchestrator.get_status()

    @app.get("/api/transcripts/stream")
    async def transcript_stream(request: Request) -> StreamingResponse:
        return StreamingResponse(_sse_events(orchestrator, request), media_type="text/event-stream")

    return app


async def _sse_events(orchestrator: PipelineOrchestrator, request: Request) -> AsyncIterator[str]:
    sub = orchestrator.transcripts.subscribe()
    try:
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await run_in_threadpool(sub.get, True, SSE_POLL_TIMEOUT_S)
            except queue.Empty:
                continue
            yield f"data: {json.dumps(_serialize(event))}\n\n"
    finally:
        orchestrator.transcripts.unsubscribe(sub)


def _serialize(event: TranscriptEvent) -> dict:
    return {
        "channel_id": event.channel_id,
        "segment_id": event.segment_id,
        "text": event.text,
        "start": event.start,
        "end": event.end,
        "created_at": event.created_at,
    }
