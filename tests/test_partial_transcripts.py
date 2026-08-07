"""Tests for in-progress partial transcripts (vad.partial_interval_s).

The load-bearing property here is isolation: a partial is a throwaway
prefix, so it must not reach the segment dump, must not move any STT
latency metric, and must not fire the one-per-segment TRANSCRIPT log line.
Those three are what the feature is allowed to touch, and everything below
either asserts a partial does its job or asserts it stays out of the way.

Fakes follow the two existing suites: _FakeSileroModel from
test_vad_worker.py (VADWorker(model=...) skips the real load) and a
transcriber_factory from test_stt_worker.py (no Moonshine needed).
"""

import queue

import torch

from edge_voice.pipeline.models import AudioPacket, SpeechSegment, TranscriptEvent
from edge_voice.stt.stt_worker import STTWorker, STTWorkerConfig
from edge_voice.vad.vad_worker import VADWorker, VADWorkerConfig

SAMPLE_RATE = 16000
CHUNK_SAMPLES = 512  # 32ms
CHUNK_S = CHUNK_SAMPLES / SAMPLE_RATE


# ── VAD side: emission cadence and gating ───────────────────────


class _FakeSileroModel:
    def __init__(self, score: float) -> None:
        self.score = score

    def __call__(self, x, sr):
        return torch.tensor(self.score)

    def reset_states(self, batch_size: int = 1) -> None:
        pass


def _vad_worker(
    partial_interval_s: float,
    partial_min_segment_s: float = 1.5,
    dump_queue: "queue.Queue | None" = None,
) -> VADWorker:
    return VADWorker(
        routed_queue=queue.Queue(),
        segment_queues={"rx": queue.Queue()},
        channel_ids=["rx"],
        config=VADWorkerConfig(
            sample_rate=SAMPLE_RATE,
            rms_gate_enabled=False,
            partial_interval_s=partial_interval_s,
            partial_min_segment_s=partial_min_segment_s,
        ),
        model=_FakeSileroModel(0.9),
        dump_queue=dump_queue,
    )


def _packet(amplitude: int = 5000) -> AudioPacket:
    samples = (amplitude).to_bytes(2, byteorder="little", signed=True) * CHUNK_SAMPLES
    return AudioPacket(channel_id="rx", timestamp=0.0, samples=samples)


def _feed_mid_speech(worker: VADWorker, n_chunks: int) -> None:
    """Drive _continue_segment directly with a segment already in progress.

    Going through _handle_packet would make the test depend on Silero's
    trigger state machine; the partial logic lives past that decision.
    """
    state = worker._channels["rx"]
    state.triggered = True
    state.segment_start_ts = 0.0
    state.segment_id = "rx-0.000-1"
    for _ in range(n_chunks):
        state.segment_chunks.append(_packet().samples)
        worker._continue_segment(_packet(), state)


def _drain(q: "queue.Queue") -> list:
    items = []
    while not q.empty():
        items.append(q.get_nowait())
    return items


def test_disabled_by_default_emits_no_partials():
    worker = _vad_worker(partial_interval_s=0.0)
    _feed_mid_speech(worker, n_chunks=200)  # 6.4s of speech

    assert _drain(worker.segment_queues["rx"]) == []


def test_partials_emitted_on_cadence_once_past_minimum():
    worker = _vad_worker(partial_interval_s=1.0, partial_min_segment_s=1.5)
    _feed_mid_speech(worker, n_chunks=157)  # ~5.0s

    partials = _drain(worker.segment_queues["rx"])
    assert partials, "expected at least one partial past the 1.5s minimum"
    assert all(p.is_partial for p in partials)
    # First lands at the minimum, not before it.
    assert partials[0].end >= 1.5
    # ~1s apart: 5.0s of speech, first at 1.5s -> 1.5/2.5/3.5/4.5
    assert len(partials) == 4
    gaps = [b.end - a.end for a, b in zip(partials, partials[1:])]
    assert all(abs(g - 1.0) < CHUNK_S for g in gaps)


def test_partial_shorter_than_minimum_is_never_emitted():
    worker = _vad_worker(partial_interval_s=0.5, partial_min_segment_s=1.5)
    _feed_mid_speech(worker, n_chunks=30)  # ~0.96s, under the minimum

    assert _drain(worker.segment_queues["rx"]) == []


def test_partials_carry_the_in_progress_segment_id():
    """The final replaces the partial in the UI, so the ids must match."""
    worker = _vad_worker(partial_interval_s=1.0)
    _feed_mid_speech(worker, n_chunks=157)

    partials = _drain(worker.segment_queues["rx"])
    assert {p.segment_id for p in partials} == {"rx-0.000-1"}


def test_partials_never_reach_the_dump_queue():
    """Prefixes would bury real segments in segment_audio_dump."""
    dump: queue.Queue = queue.Queue()
    worker = _vad_worker(partial_interval_s=1.0, dump_queue=dump)
    _feed_mid_speech(worker, n_chunks=157)

    assert _drain(worker.segment_queues["rx"]), "sanity: partials were emitted"
    assert _drain(dump) == []


def test_partial_clock_resets_so_a_new_segment_starts_fresh():
    worker = _vad_worker(partial_interval_s=1.0)
    state = worker._channels["rx"]
    state.segment_chunks = [_packet().samples] * 100
    state.segment_start_ts = 0.0
    state.segment_id = "rx-0.000-1"
    state.last_partial_s = 3.0

    worker._finalize_segment("rx", state, end_ts=3.2)

    assert state.last_partial_s == 0.0


# ── STT side: isolation from finals ─────────────────────────────


class _FakeLine:
    def __init__(self, text: str) -> None:
        self.text = text


class _FakeTranscript:
    def __init__(self, text: str) -> None:
        self.lines = [_FakeLine(text)]


class _FakeTranscriber:
    """Records which entry point each call took."""

    def __init__(self, partial_text: str = "안녕하세요") -> None:
        self.partial_text = partial_text
        self.without_streaming_calls = 0
        self.stream_calls = 0

    def transcribe_without_streaming(self, samples, sample_rate):
        self.without_streaming_calls += 1
        return _FakeTranscript(self.partial_text)

    # Streaming path, used by finals only.
    def remove_all_listeners(self) -> None:
        pass

    def add_listener(self, listener) -> None:
        pass

    def start(self) -> None:
        self.stream_calls += 1

    def add_audio(self, chunk, sample_rate) -> None:
        pass

    def stop(self) -> None:
        pass


def _stt_worker(
    transcriber: _FakeTranscriber,
    events: list,
    segment_queue: "queue.Queue | None" = None,
    partial_max_queue_depth: int = 0,
) -> STTWorker:
    return STTWorker(
        segment_queue=segment_queue if segment_queue is not None else queue.Queue(),
        on_transcript=events.append,
        config=STTWorkerConfig(partial_max_queue_depth=partial_max_queue_depth),
        transcriber_factory=lambda: transcriber,
    )


def _segment(segment_id: str, is_partial: bool, n_samples: int = 512) -> SpeechSegment:
    return SpeechSegment(
        channel_id="rx",
        start=0.0,
        end=n_samples / SAMPLE_RATE,
        audio=bytes(n_samples * 2),
        segment_id=segment_id,
        is_partial=is_partial,
    )


def test_partial_emits_a_non_final_transcript_event():
    events: list[TranscriptEvent] = []
    worker = _stt_worker(_FakeTranscriber(), events)

    worker._handle_segment(_segment("rx-1", is_partial=True))

    assert len(events) == 1
    assert events[0].is_final is False
    assert events[0].text == "안녕하세요"
    assert events[0].segment_id == "rx-1"


def test_partial_uses_the_stateless_entry_point_not_the_stream():
    """transcribe_without_streaming can't perturb the final's decoder state."""
    transcriber = _FakeTranscriber()
    worker = _stt_worker(transcriber, [])

    worker._handle_segment(_segment("rx-1", is_partial=True))

    assert transcriber.without_streaming_calls == 1
    assert transcriber.stream_calls == 0


def test_partial_does_not_move_stt_latency_metrics():
    """The isolation this whole feature is gated on."""
    worker = _stt_worker(_FakeTranscriber(), [])

    worker._handle_segment(_segment("rx-1", is_partial=True))

    assert worker.last_latency_s is None
    assert worker.channel_latencies_s() == {}


def test_partial_does_not_disturb_a_previous_final_reading():
    worker = _stt_worker(_FakeTranscriber(), [])
    worker._handle_segment(_segment("rx-final", is_partial=False))
    after_final = worker.last_latency_s
    assert after_final is not None

    worker._handle_segment(_segment("rx-partial", is_partial=True))

    assert worker.last_latency_s == after_final
    assert worker.channel_latencies_s()["rx"] == after_final


def test_final_still_records_latency_and_is_final():
    events: list[TranscriptEvent] = []
    worker = _stt_worker(_FakeTranscriber(), events)

    worker._handle_segment(_segment("rx-1", is_partial=False))

    assert worker.last_latency_s is not None
    # The fake fires no listener events, so no transcript is published --
    # matching test_stt_worker.py's fake. Latency is the assertion here.
    assert all(e.is_final for e in events)


def test_partial_is_dropped_when_real_work_is_waiting():
    backed_up: queue.Queue = queue.Queue()
    backed_up.put(_segment("queued-final", is_partial=False))
    transcriber = _FakeTranscriber()
    events: list[TranscriptEvent] = []
    worker = _stt_worker(transcriber, events, segment_queue=backed_up)

    worker._handle_segment(_segment("rx-1", is_partial=True))

    assert transcriber.without_streaming_calls == 0
    assert events == []
    assert worker.partial_stats() == {"transcribed": 0, "dropped": 1}


def test_partial_is_kept_when_queue_is_within_the_allowance():
    backed_up: queue.Queue = queue.Queue()
    backed_up.put(_segment("queued-final", is_partial=False))
    transcriber = _FakeTranscriber()
    worker = _stt_worker(transcriber, [], segment_queue=backed_up, partial_max_queue_depth=1)

    worker._handle_segment(_segment("rx-1", is_partial=True))

    assert transcriber.without_streaming_calls == 1
    assert worker.partial_stats() == {"transcribed": 1, "dropped": 0}


def test_repetitive_partial_is_suppressed():
    """A looping prefix is noise the user would watch get worse."""
    events: list[TranscriptEvent] = []
    worker = _stt_worker(_FakeTranscriber("잘 말하고 잘 말하고 잘 말하고 잘 말하고"), events)

    worker._handle_segment(_segment("rx-1", is_partial=True))

    assert events == []
