from __future__ import annotations

import json
import logging
import queue
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from edge_voice.pipeline.models import TranscriptEvent
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

logger = logging.getLogger(__name__)

_TEMPLATE_PATH = Path(__file__).parent / "templates" / "console.html"
_STATIC_DIR = Path(__file__).parent / "static"

SSE_POLL_TIMEOUT_S = 1.0


def create_app(orchestrator: PipelineOrchestrator) -> FastAPI:
    app = FastAPI(title="edge-voice")
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _TEMPLATE_PATH.read_text()

    @app.get("/api/status")
    def status() -> dict:
        return orchestrator.health()

    @app.post("/api/start")
    async def start() -> dict:
        await run_in_threadpool(orchestrator.start)
        return orchestrator.get_status()

    @app.post("/api/stop")
    async def stop() -> dict:
        await run_in_threadpool(orchestrator.stop)
        await run_in_threadpool(orchestrator.wait)
        return orchestrator.get_status()

    @app.get("/api/transcripts/stream")
    async def transcript_stream(request: Request) -> StreamingResponse:
        return StreamingResponse(_sse_events(orchestrator, request), media_type="text/event-stream")

    @app.post("/api/transcripts/clear")
    def clear_transcripts() -> dict:
        orchestrator.transcripts.clear()
        return {"cleared": True}

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
        "is_final": event.is_final,
    }
