"""Tests for VADWorker's per-channel latency tracking and its fixed,
constructor-time channel set (issue #10: model loading moves to __init__,
off the real-time path -- see VADWorker.__init__'s docstring).

A fake Silero model avoids any real model load: VADWorker(model=...) short-
circuits _load_model() entirely (vad_worker.py:463), and silero_vad's
VADIterator only ever calls `self.model(x, sr).item()` and
`self.model.reset_states()` -- both trivially satisfied here.
"""

import queue

import pytest
import torch
from pydantic import ValidationError

from edge_voice.config.settings import VADSettings
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


def _make_worker(score: float, channel_ids: list[str] | None = None) -> VADWorker:
    ids = channel_ids if channel_ids is not None else ["rx", "tx"]
    config = VADWorkerConfig(rms_gate_enabled=True, silence_rms_floor=0.01, sample_rate=SAMPLE_RATE)
    return VADWorker(
        routed_queue=queue.Queue(),
        segment_queues={cid: queue.Queue() for cid in ids},
        channel_ids=ids,
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
        segment_queues={"rx": queue.Queue()},
        channel_ids=["rx"],
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


# ── fixed, constructor-time channel set (issue #10) ─────────────


def test_init_builds_channel_state_for_every_configured_id():
    worker = _make_worker(score=0.01, channel_ids=["rx", "tx"])

    assert set(worker._channels) == {"rx", "tx"}


def test_first_packet_does_not_build_new_channel_state():
    worker = _make_worker(score=0.01, channel_ids=["rx"])
    state_after_init = worker._channels["rx"]

    worker._handle_packet(_packet("rx", amplitude=5000))

    # Same _ChannelState object: _handle_packet does a plain lookup, it never
    # builds one -- __init__ already did, for every configured channel.
    assert worker._channels["rx"] is state_after_init


def test_init_uses_the_injected_model_not_a_real_load():
    worker = _make_worker(score=0.01, channel_ids=["rx"])

    assert worker._channels["rx"].scorer._model is worker.model


def test_packet_on_unconfigured_channel_raises():
    # ChannelRouter has already dropped anything outside channel_ids before
    # it reaches this worker's queue -- a channel_id showing up here that
    # wasn't in the constructor's list means that invariant broke upstream,
    # so this should surface loudly (run()'s own try/except logs it) rather
    # than silently create ad-hoc state for it.
    worker = _make_worker(score=0.01, channel_ids=["rx"])

    with pytest.raises(KeyError):
        worker._handle_packet(_packet("unconfigured", amplitude=5000))


# ── segment-length limits: soft_cut_enabled (docs/BENCHMARK.md) ────
#
# A soft cut backdates the emitted segment's .end to the chosen pause, and
# the queued tail then waits behind the head's own decode on top of that --
# measured on the RPi5 as real, avoidable latency on every ordinary segment
# that runs past soft_cut_s (~700-1750ms per piece on a plain ~3.3s
# utterance, vs ~17ms as one segment with soft-cut off). soft_cut_enabled
# lets that cost be opted out of while keeping the hard cap, which is what
# actually bounds worst-case backlog and is unconditional either way.


def _make_cut_worker(soft_cut_enabled: bool) -> VADWorker:
    config = VADWorkerConfig(
        sample_rate=SAMPLE_RATE,
        segment_limits_enabled=True,
        soft_cut_enabled=soft_cut_enabled,
        max_segment_s=0.6,
        soft_cut_s=0.3,
        soft_cut_lookahead_s=0.2,
        soft_cut_min_dip=0.05,
    )
    return VADWorker(
        routed_queue=queue.Queue(),
        segment_queues={"rx": queue.Queue()},
        channel_ids=["rx"],
        config=config,
        model=_FakeSileroModel(1.0),
    )


def _feed_scored_chunks(worker: VADWorker, channel_id: str, scores: list[float]) -> None:
    """Drives _maybe_cut with one 0.1s chunk per score in `scores`, mirroring
    the real call order (chunk appended to segment_chunks, then _maybe_cut --
    see _continue_segment). Bypasses Silero's own trigger/VADIterator
    machinery entirely: soft/hard-cut logic only reads state.segment_chunks
    and state.scorer.last_score, both poked directly here.
    """
    state = worker._channels[channel_id]
    state.segment_start_ts = 0.0
    state.segment_chunks = []
    state.scores = []
    worker._new_segment_id(channel_id, state)
    packet = _packet(channel_id, amplitude=3000, n_samples=1600)  # 0.1s @ 16kHz
    for score in scores:
        state.scorer.last_score = score
        state.segment_chunks.append(packet.samples)
        worker._maybe_cut(channel_id, state, packet)


def test_soft_cut_enabled_cuts_early_at_a_score_dip():
    """A brief dip in VAD confidence, once confirmed by a subsequent higher
    score, cuts there rather than running to the hard cap -- the whole point
    of soft-cut: land the boundary in a pause, not mid-word."""
    worker = _make_cut_worker(soft_cut_enabled=True)

    _feed_scored_chunks(worker, "rx", scores=[1.0, 0.5, 1.0, 1.0, 1.0, 1.0])

    emitted = worker.segment_queues["rx"].get_nowait()
    assert emitted.end - emitted.start == pytest.approx(0.2)  # cut at the dip, not 0.6s


def test_soft_cut_disabled_ignores_the_dip_and_runs_to_the_hard_cap():
    """Same dip, soft-cut off: must run all the way to the unconditional
    hard cap instead, trading cut quality for zero cost on the common case."""
    worker = _make_cut_worker(soft_cut_enabled=False)

    _feed_scored_chunks(worker, "rx", scores=[1.0, 0.5, 1.0, 1.0, 1.0, 1.0])

    emitted = worker.segment_queues["rx"].get_nowait()
    assert emitted.end - emitted.start == pytest.approx(0.6)  # the dip was ignored


def test_soft_cut_s_may_reach_max_segment_s_when_soft_cut_disabled():
    """The validator exists to stop the hard cap from silently preempting a
    search that would otherwise run -- irrelevant once that search itself
    is switched off (same reasoning as it already skips when
    segment_limits_enabled is off)."""
    VADSettings(
        segment_limits_enabled=True, soft_cut_enabled=False, max_segment_s=5.0, soft_cut_s=5.0
    )


def test_soft_cut_s_still_validated_against_max_segment_s_when_enabled():
    with pytest.raises(ValidationError):
        VADSettings(
            segment_limits_enabled=True, soft_cut_enabled=True, max_segment_s=5.0, soft_cut_s=5.0
        )
