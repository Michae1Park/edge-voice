"""Tests for the per-channel latency tracking added to STTWorker.

Uses transcriber_factory (per the module docstring: "Inject transcriber_factory
to bypass it entirely (tests, benchmarks)") so no real Moonshine model is
needed. The fake transcriber never fires any listener events -- text() comes
back empty and no TranscriptEvent is published -- but that's irrelevant here:
_handle_segment records channel latency before checking whether text is empty.
"""

import queue

from edge_voice.pipeline.models import SpeechSegment
from edge_voice.stt.stt_worker import STTWorker, STTWorkerConfig


class _FakeTranscriber:
    def remove_all_listeners(self) -> None:
        pass

    def add_listener(self, listener) -> None:
        pass

    def start(self) -> None:
        pass

    def add_audio(self, chunk, sample_rate) -> None:
        pass

    def stop(self) -> None:
        pass


def _make_worker() -> STTWorker:
    return STTWorker(
        segment_queue=queue.Queue(),
        on_transcript=lambda event: None,
        config=STTWorkerConfig(),
        transcriber_factory=_FakeTranscriber,
    )


def _segment(channel_id: str, segment_id: str, n_samples: int = 512) -> SpeechSegment:
    return SpeechSegment(
        channel_id=channel_id,
        start=0.0,
        end=n_samples / 16000,
        audio=bytes(n_samples * 2),  # int16 mono, silent
        segment_id=segment_id,
    )


def test_handling_segment_records_per_channel_latency():
    worker = _make_worker()
    worker._handle_segment(_segment("tx", "tx-1"))

    assert "tx" in worker.channel_latencies_s()
    assert worker.channel_latencies_s()["tx"] == worker.last_latency_s


def test_latencies_are_independent_per_channel():
    worker = _make_worker()
    worker._handle_segment(_segment("rx", "rx-1"))
    worker._handle_segment(_segment("tx", "tx-1"))

    assert set(worker.channel_latencies_s()) == {"rx", "tx"}


def test_channel_latency_matches_global_last_latency_after_each_segment():
    worker = _make_worker()
    worker._handle_segment(_segment("rx", "rx-1"))
    first_global = worker.last_latency_s
    assert worker.channel_latencies_s()["rx"] == first_global

    worker._handle_segment(_segment("tx", "tx-1"))
    assert worker.channel_latencies_s()["tx"] == worker.last_latency_s
    # rx's own last reading is untouched by a tx segment landing afterward.
    assert worker.channel_latencies_s()["rx"] == first_global


def test_accessor_returns_copy_not_live_reference():
    worker = _make_worker()
    worker._handle_segment(_segment("rx", "rx-1"))

    snapshot = worker.channel_latencies_s()
    snapshot["rx"] = -1.0
    assert worker.channel_latencies_s()["rx"] != -1.0


# ── eager construction: resolved in __init__, off the real-time path ──


def test_init_resolves_the_transcriber_eagerly():
    worker = _make_worker()

    assert isinstance(worker._transcriber, _FakeTranscriber)


def test_handle_segment_does_not_build_a_new_transcriber():
    worker = _make_worker()
    resolved = worker._transcriber

    worker._handle_segment(_segment("rx", "rx-1"))

    assert worker._transcriber is resolved  # reused, not rebuilt
