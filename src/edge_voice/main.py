"""Milestone 0 entry point.

Wires together: FakeAudioSource -> FakeRouter -> FakeVADWorker ->
FakeSTTWorker, connected by bounded queues, and logs TranscriptEvents to
stdout. This is the "prove the skeleton works" milestone — nothing here is
real audio, routing, VAD, or STT yet.

Run with:  python main.py
Stop with: Ctrl-C, or wait — it auto-stops after RUN_SECONDS.
Expect:    clean exit, no orphaned threads (each worker logs "stopped").
"""

from __future__ import annotations

import logging
import time

from pipeline.fake_workers import FakeRouter, FakeSTTWorker, FakeVADWorker
from pipeline.models import TranscriptEvent
from pipeline.queues import make_ingest_queue, make_segment_queue
from utils.audio_generation.fake_source import FakeAudioSource

RUN_SECONDS = 10
CHANNEL_IDS = ["audio-rx", "audio-tx"]
JOIN_TIMEOUT_S = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
logger = logging.getLogger("main")


def on_transcript(event: TranscriptEvent) -> None:
    logger.info(
        "TRANSCRIPT channel=%s segment=%s [%.2f-%.2f] %r",
        event.channel_id,
        event.segment_id,
        event.start,
        event.end,
        event.text,
    )


def main() -> None:
    ingest_queue = make_ingest_queue()
    routed_queue = make_ingest_queue()  # same shape as ingest_queue, separate fake stage
    segment_queue = make_segment_queue()

    source = FakeAudioSource(ingest_queue, CHANNEL_IDS)
    router = FakeRouter(ingest_queue, routed_queue)
    vad = FakeVADWorker(routed_queue, segment_queue)
    stt = FakeSTTWorker(segment_queue, on_transcript)

    workers = [source, router, vad, stt]

    for worker in workers:
        worker.start()

    logger.info(
        "Pipeline running. Ctrl-C to stop early (auto-stops after %ss).",
        RUN_SECONDS,
    )

    start = time.time()
    try:
        while time.time() - start < RUN_SECONDS:
            time.sleep(0.2)
    except KeyboardInterrupt:
        logger.info("Ctrl-C received, shutting down...")
    finally:
        logger.info("Stopping workers...")
        for worker in workers:
            worker.stop()
        for worker in workers:
            worker.join(timeout=JOIN_TIMEOUT_S)
            if worker.is_alive():
                logger.warning("%s did not stop cleanly within %ss", worker.name, JOIN_TIMEOUT_S)
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
