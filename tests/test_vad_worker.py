"""Tests for the per-channel latency tracking added to VADWorker.

A fake Silero model avoids any real model load: VADWorker(model=...) short-
circuits _load_model() entirely (vad_worker.py:463), and silero_vad's
VADIterator only ever calls `self.model(x, sr).item()` and
`self.model.reset_states()` -- both trivially satisfied here.
"""

import queue

import torch

from edge_voice.pipeline.models import AudioPacket
from edge_voice.vad.vad_worker import VADWorker, VADWorkerConfig

SAMPLE_RATE = 16000


class _FakeSileroModel:
    """Returns a fixed speech probability; reset_states() is a no-op."""

    def __init__(self, score: float) -> None:
        self.score = score

    def __call__(self, x, sr):
        return torch.tensor(self.score)

    def reset_states(self, batch_size: int = 1) -> None:
        pass


def _make_worker(score: float) -> VADWorker:
    config = VADWorkerConfig(rms_gate_enabled=True, silence_rms_floor=0.01, sample_rate=SAMPLE_RATE)
    return VADWorker(
        routed_queue=queue.Queue(),
        segment_queue=queue.Queue(),
        config=config,
        model=_FakeSileroModel(score),
    )


def _packet(channel_id: str, amplitude: int, n_samples: int = 512) -> AudioPacket:
    """A packet of constant-amplitude int16 PCM. amplitude=0 -> RMS well
    below the default silence_rms_floor; a few thousand -> well above it."""
    samples = (amplitude).to_bytes(2, byteorder="little", signed=True) * n_samples
    return AudioPacket(channel_id=channel_id, timestamp=0.0, samples=samples)


def test_silent_packet_records_rms_gate_latency_not_silero():
    worker = _make_worker(
        score=0.01
    )  # below threshold, irrelevant here -- gate skips Silero anyway
    worker._handle_packet(_packet("rx", amplitude=0))

    assert "rx" in worker.rms_gate_latencies_s()
    assert "rx" not in worker.silero_latencies_s()
    assert worker.rms_gate_latencies_s()["rx"] >= 0.0


def test_loud_packet_records_silero_latency_not_rms_gate():
    worker = _make_worker(
        score=0.01
    )  # below Silero's own threshold -- no trigger, just a normal pass
    worker._handle_packet(_packet("tx", amplitude=5000))

    assert "tx" in worker.silero_latencies_s()
    assert "tx" not in worker.rms_gate_latencies_s()
    assert worker.silero_latencies_s()["tx"] >= 0.0


def test_latencies_are_independent_per_channel():
    worker = _make_worker(score=0.01)
    worker._handle_packet(_packet("rx", amplitude=0))  # gated
    worker._handle_packet(_packet("tx", amplitude=5000))  # reaches Silero

    assert set(worker.rms_gate_latencies_s()) == {"rx"}
    assert set(worker.silero_latencies_s()) == {"tx"}


def test_rms_gate_disabled_always_reaches_silero():
    config = VADWorkerConfig(rms_gate_enabled=False, sample_rate=SAMPLE_RATE)
    worker = VADWorker(
        routed_queue=queue.Queue(),
        segment_queue=queue.Queue(),
        config=config,
        model=_FakeSileroModel(score=0.01),
    )
    worker._handle_packet(_packet("rx", amplitude=0))  # would gate if enabled

    assert "rx" in worker.silero_latencies_s()
    assert "rx" not in worker.rms_gate_latencies_s()


def test_accessors_return_copies_not_live_references():
    worker = _make_worker(score=0.01)
    worker._handle_packet(_packet("rx", amplitude=5000))

    snapshot = worker.silero_latencies_s()
    snapshot["rx"] = -1.0  # mutate the returned copy
    assert worker.silero_latencies_s()["rx"] != -1.0  # internal state unaffected
