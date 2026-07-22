"""
VADWorker: single thread consuming the mixed (both-channel) routed_queue,
demultiplexing by AudioPacket.channel_id, and emitting finalized
SpeechSegments to segment_queue.

Because this is one thread pulling packets in order, there's no need for
the vad_lock from the two-thread version -- calls into the model are
already serialized by construction. What each channel still needs is its
own VADIterator (own internal state), tracked per channel_id in
self._channels, plus its own preroll buffer and in-progress segment buffer.

Silence is RMS-gated per channel: while a channel isn't mid-speech and its
chunk is quiet, we skip the Silero forward pass entirely and just keep the
chunk in a small preroll ring buffer.

Assumptions:
  - AudioPacket.samples is raw PCM bytes, int16 mono, at settings.audio.sample_rate.
    Adjust `_bytes_to_float_tensor` if your format differs (e.g. int32, float32 already).
  - `None` on routed_queue is the shutdown sentinel (matches orchestrator's
    w.stop() + join(timeout=10) pattern -- adjust if you use something else).
  - segment_id is generated here as f"{channel_id}-{start:.3f}"; swap for
    uuid4 or whatever convention SegmentAudioDumpWorker/STT expect.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import torch

from edge_voice.pipeline.fanout import fanout_put
from edge_voice.pipeline.models import AudioPacket, SpeechSegment

logger = logging.getLogger(__name__)


@dataclass
class VADWorkerConfig:
    threshold: float = 0.5
    sample_rate: int = 16000
    rms_gate_enabled: bool = True
    silence_rms_floor: float = 0.01  # CALIBRATE: normalized float32 RMS, not raw int16
    preroll_chunks: int = 3
    min_silence_duration_ms: int = 100  # silence required before an `end` event fires
    speech_pad_ms: int = 30  # padding Silero appends on both sides of detected speech

    # Wall-clock seconds of *no packets at all* on a channel before its
    # in-progress segment is emitted anyway. Every other boundary rule needs
    # packets to keep arriving -- min_silence_duration_ms needs silent frames
    # to fire `end`, max_segment_s needs frames to accumulate -- so if a
    # sender simply stops (caller mutes, RTP gap, stream ends), the segment
    # would otherwise sit buffered until shutdown. 0 disables.
    idle_flush_s: float = 2.0

    # ── Segment-length limits (off by default) ──────────────────
    # For outlier cases only: speech that runs on with no pause long enough
    # for Silero to fire `end`. Without these such a run buffers unbounded
    # and the pipeline emits nothing until the speaker finally stops, so
    # these bound emission latency as much as segment size. Ordinary
    # turn-taking never reaches soft_cut_s and pays nothing for this.
    segment_limits_enabled: bool = False
    max_segment_s: float = 7.0  # hard cap: cut here regardless of audio
    soft_cut_s: float = 5.0  # past this, start looking for a natural pause
    soft_cut_lookahead_s: float = 1.0  # how far back to scan for that pause
    soft_cut_min_dip: float = 0.10  # dip must be this far below current score


class _ScoreCapturingModel:
    """Records the speech probability VADIterator computes internally.

    Soft-cut needs the per-window confidence score to find a natural pause.
    The scratch script gets it with a second `vad_model(window)` call
    alongside `vad_iter(window)`, which both doubles the forward passes and
    feeds Silero's stateful RNN each window twice. Wrapping the model
    instead yields the exact score VADIterator used for its own decision,
    for free and without perturbing its state.
    """

    def __init__(self, model: Any) -> None:
        self._model = model
        self.last_score = 0.0

    def __call__(self, x: Any, sr: int) -> Any:
        out = self._model(x, sr)
        self.last_score = float(out.item()) if hasattr(out, "item") else float(out)
        return out

    def __getattr__(self, name: str) -> Any:
        # Delegate reset_states() and friends to the wrapped model.
        return getattr(self._model, name)


@dataclass
class _ChannelState:
    vad_iter: Any  # VADIterator instance, own state
    scorer: Any = None  # _ScoreCapturingModel feeding this channel's vad_iter
    triggered: bool = False
    preroll: list = field(default_factory=list)  # list[(timestamp, bytes)]
    segment_chunks: list = field(default_factory=list)  # list[bytes], during active speech
    segment_start_ts: float | None = None
    seg_counter: int = 0
    # time.monotonic() of the last packet seen; drives idle_flush_s.
    last_packet_at: float = 0.0
    # (chunk_index, score) pairs, only recorded once a segment approaches
    # soft_cut_s -- see _maybe_cut. Empty for normal-length segments.
    scores: list = field(default_factory=list)


class VADWorker(threading.Thread):
    """Drop-in replacement for FakeVADWorker(routed_queue, segment_queue)."""

    def __init__(
        self,
        routed_queue: "queue.Queue[AudioPacket]",
        segment_queue: "queue.Queue[SpeechSegment]",
        config: VADWorkerConfig | None = None,
        model=None,
        name: str = "VADWorker",
        dump_queue: "queue.Queue[SpeechSegment] | None" = None,
    ) -> None:
        super().__init__(name=name, daemon=True)
        self.routed_queue = routed_queue
        self.segment_queue = segment_queue
        self.dump_queue = dump_queue
        self.config = config or VADWorkerConfig()

        # Passing `model` makes every channel share one instance. That is only
        # safe for single-channel use (tests, benchmarks) -- see
        # _new_channel_state for why. Left as None, each channel loads its own.
        self.model = model

        self._channels: dict[str, _ChannelState] = {}
        self._stop_event = threading.Event()
        # Monotonic timestamp of the last packet handled, read by the
        # supervisor's stall check (docs/BUILDPLAN.md Milestone 6). A plain
        # float write/read is atomic under the GIL, so no lock is needed.
        self._last_activity = time.monotonic()

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    @property
    def last_activity(self) -> float:
        """Monotonic time of the last packet handled (for supervisor stall check)."""
        return self._last_activity

    def pending_loss(self) -> str | None:
        """Report any in-progress segment audio that a restart would discard.

        The supervisor calls this on a crashed VADWorker before replacing it,
        so a crash that ate a live utterance is logged as its own distinct
        event rather than vanishing silently -- the same drop-the-last-utterance
        class of bug flush() was added to fix, just reached via crash+restart.
        Returns a human-readable summary, or None if nothing was buffered.
        """
        parts = []
        for channel_id, state in self._channels.items():
            if state.segment_start_ts is not None and state.segment_chunks:
                n_samples = sum(len(c) for c in state.segment_chunks) // 2  # int16
                parts.append(f"{channel_id}={n_samples / self.config.sample_rate:.2f}s")
        return ", ".join(parts) if parts else None

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                packet = self.routed_queue.get(timeout=0.5)
            except queue.Empty:
                self._flush_idle()
                continue

            if packet is None:  # shutdown sentinel
                break

            self._last_activity = time.monotonic()

            try:
                self._handle_packet(packet)
            except Exception:
                logger.exception("VADWorker failed on packet from channel=%s", packet.channel_id)

            # Also check after a packet, not just on an empty queue: on a
            # duplex call the other channel can keep the queue busy while
            # this one has gone silent, so `Empty` may never be reached.
            self._flush_idle()

        # Speech in progress when the stream stops would otherwise never be
        # emitted -- segments are only finalized on an `end` event.
        self.flush("shutdown")

    def flush(self, reason: str = "flush") -> int:
        """Emit any in-progress segments; returns how many were flushed.

        Called on shutdown, and safe to call directly when a stream ends
        (e.g. finite audio in tests). Runs on the worker thread from run(),
        so it does not race with _handle_packet.
        """
        return sum(
            self._flush_channel(channel_id, state, reason)
            for channel_id, state in self._channels.items()
        )

    def _flush_idle(self) -> int:
        """Emit segments on channels that have stopped receiving packets.

        Without this the final utterance of a stream is only recovered at
        shutdown -- every other boundary rule depends on packets continuing
        to arrive. See VADWorkerConfig.idle_flush_s.
        """
        if self.config.idle_flush_s <= 0:
            return 0
        now = time.monotonic()
        return sum(
            self._flush_channel(channel_id, state, "idle")
            for channel_id, state in self._channels.items()
            if now - state.last_packet_at >= self.config.idle_flush_s
        )

    def _flush_channel(self, channel_id: str, state: _ChannelState, reason: str) -> int:
        """Finalize one channel's in-progress segment. Returns 1 if emitted."""
        if state.segment_start_ts is None or not state.segment_chunks:
            return 0
        n_samples = sum(len(c) for c in state.segment_chunks) // 2  # int16
        duration_s = n_samples / self.config.sample_rate
        logger.info(
            "VADWorker: flushing in-progress segment on channel=%s (%s, %.2fs)",
            channel_id,
            reason,
            duration_s,
        )
        self._finalize_segment(channel_id, state, end_ts=state.segment_start_ts + duration_s)
        state.triggered = False
        state.scores.clear()
        # VADIterator still believes it is mid-speech; without resetting it
        # would not emit another `start` when packets resume, silently
        # swallowing the next utterance.
        state.vad_iter.reset_states()
        return 1

    # ── Per-packet handling ─────────────────────────────────────

    def _handle_packet(self, packet: AudioPacket) -> None:
        state = self._channels.get(packet.channel_id)
        if state is None:
            state = self._new_channel_state()
            self._channels[packet.channel_id] = state
        state.last_packet_at = time.monotonic()

        float_chunk = self._bytes_to_float_tensor(packet.samples)

        if self.config.rms_gate_enabled and not state.triggered:
            rms = self._rms(float_chunk)
            if rms < self.config.silence_rms_floor:
                # Confidently silent: skip the Silero forward pass entirely.
                # Purely a compute optimization -- the chunk still lands in
                # preroll below, same as any other non-triggering chunk.
                self._push_preroll(state, packet)
                return

        result = state.vad_iter(float_chunk, return_seconds=True)

        if state.triggered:
            state.segment_chunks.append(packet.samples)

        if result and "start" in result:
            state.triggered = True
            state.segment_chunks = [b for _, b in state.preroll] + [packet.samples]
            state.segment_start_ts = state.preroll[0][0] if state.preroll else packet.timestamp
            state.preroll.clear()
            state.scores.clear()

        elif result and "end" in result:
            state.triggered = False
            self._finalize_segment(packet.channel_id, state, end_ts=packet.timestamp)
            state.scores.clear()

        elif state.triggered:
            if self.config.segment_limits_enabled:
                # No boundary event and still mid-speech: the only path where
                # a segment can grow without bound, so limits apply here.
                self._maybe_cut(packet.channel_id, state, packet)

        else:
            # Reached the model but didn't trigger -- still pre-speech, so it
            # belongs in preroll just like a gated-out chunk. Buffering here
            # (rather than beside the RMS gate) is what keeps preroll working
            # when the gate is off: Silero needs a chunk or two of signal
            # before it reports `start`, so without preroll the true onset of
            # speech is discarded regardless of the gate.
            self._push_preroll(state, packet)

    # ── Segment-length limits ───────────────────────────────────

    def _maybe_cut(self, channel_id: str, state: _ChannelState, packet: AudioPacket) -> None:
        """Cut an over-long segment, preferring a natural pause over a hard chop.

        Ported from scratch/silero_moonshine.py's do_cut(). Past soft_cut_s we
        scan the lookahead window for the deepest dip in VAD confidence and cut
        just after it, so the boundary lands in a pause rather than mid-word.
        If no dip qualifies before max_segment_s, cut anyway.
        """
        cfg = self.config
        samples_per_chunk = len(packet.samples) // 2  # int16
        if samples_per_chunk <= 0:
            return
        chunk_s = samples_per_chunk / cfg.sample_rate
        n_chunks = len(state.segment_chunks)
        seg_s = n_chunks * chunk_s

        if seg_s >= cfg.max_segment_s:
            self._cut_segment(channel_id, state, n_chunks, chunk_s, "hard cap")
            return

        # Only start recording scores once a cut is plausibly near, so normal
        # segments carry no bookkeeping at all.
        if seg_s < cfg.soft_cut_s - cfg.soft_cut_lookahead_s:
            return
        state.scores.append((n_chunks - 1, state.scorer.last_score))

        if seg_s < cfg.soft_cut_s:
            return

        lookahead_chunks = max(1, int(cfg.soft_cut_lookahead_s / chunk_s))
        recent = state.scores[-lookahead_chunks:]
        if not recent:
            return

        min_idx, min_score = min(recent, key=lambda pair: pair[1])
        if min_score < state.scorer.last_score - cfg.soft_cut_min_dip:
            self._cut_segment(channel_id, state, min_idx + 1, chunk_s, "soft cut")

    def _cut_segment(
        self, channel_id: str, state: _ChannelState, cut_idx: int, chunk_s: float, reason: str
    ) -> None:
        """Emit segment_chunks[:cut_idx]; carry the tail into a new segment.

        Speech is still in progress, so `triggered` stays set and the tail
        becomes the head of the next segment (the scratch's tail replay).
        """
        if cut_idx <= 0 or cut_idx > len(state.segment_chunks) or state.segment_start_ts is None:
            return

        tail = state.segment_chunks[cut_idx:]
        cut_ts = state.segment_start_ts + cut_idx * chunk_s
        state.segment_chunks = state.segment_chunks[:cut_idx]

        logger.debug(
            "VADWorker: %s on channel=%s at %.2fs (%d chunks emitted, %d carried)",
            reason,
            channel_id,
            cut_ts,
            cut_idx,
            len(tail),
        )
        self._finalize_segment(channel_id, state, end_ts=cut_ts)

        # _finalize_segment cleared these; re-seed from the tail.
        state.segment_chunks = tail
        state.segment_start_ts = cut_ts
        state.scores = [(i - cut_idx, s) for i, s in state.scores if i >= cut_idx]

    def _finalize_segment(self, channel_id: str, state: _ChannelState, end_ts: float) -> None:
        if state.segment_start_ts is None or not state.segment_chunks:
            return
        state.seg_counter += 1
        segment = SpeechSegment(
            channel_id=channel_id,
            start=state.segment_start_ts,
            end=end_ts,
            audio=b"".join(state.segment_chunks),
            segment_id=f"{channel_id}-{state.segment_start_ts:.3f}-{state.seg_counter}",
        )
        fanout_put(segment, self.segment_queue, self.dump_queue)
        state.segment_chunks = []
        state.segment_start_ts = None

    # ── Helpers ──────────────────────────────────────────────────

    def _new_channel_state(self) -> _ChannelState:
        """Per-channel Silero model, VADIterator, and score wrapper.

        Each channel needs its OWN model, not just its own VADIterator:
        VADIterator holds only the state machine (triggered/temp_end), while
        the LSTM hidden state lives inside the model. Sharing one model
        across interleaved channels lets rx and tx corrupt each other's
        state -- measured as doubled segment counts (rx 4->8, tx 4->8) on
        the recorded call fixtures. A second model costs ~0.06s and ~4MB.
        """
        model = self.model if self.model is not None else self._load_model()
        scorer = _ScoreCapturingModel(model)
        return _ChannelState(vad_iter=self._new_vad_iterator(scorer), scorer=scorer)

    @staticmethod
    def _load_model() -> Any:
        model, _ = torch.hub.load(
            repo_or_dir="snakers4/silero-vad",
            model="silero_vad",
            force_reload=False,
            trust_repo=True,
        )
        return model

    def _new_vad_iterator(self, model: Any):
        from silero_vad.utils_vad import VADIterator  # adjust import to your install

        return VADIterator(
            model,
            threshold=self.config.threshold,
            sampling_rate=self.config.sample_rate,
            min_silence_duration_ms=self.config.min_silence_duration_ms,
            speech_pad_ms=self.config.speech_pad_ms,
        )

    def _bytes_to_float_tensor(self, pcm_bytes: bytes) -> torch.Tensor:
        # int16 mono PCM -> float32 normalized to [-1, 1]
        arr = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / 32768.0
        return torch.from_numpy(arr)

    def _rms(self, float_tensor: torch.Tensor) -> float:
        return float(torch.sqrt(torch.mean(float_tensor**2)))

    def _push_preroll(self, state: _ChannelState, packet: AudioPacket) -> None:
        state.preroll.append((packet.timestamp, packet.samples))
        if len(state.preroll) > self.config.preroll_chunks:
            state.preroll.pop(0)

    def reset_channel(self, channel_id: str) -> None:
        """Call on stream discontinuity (MQTT reconnect gap, etc.)

        UNRESOLVED -- this DISCARDS any in-progress segment. That is right for
        a reconnect gap (the buffered audio is stale and has a hole in it), but
        wrong for a call ending, which is the same drop-the-last-utterance bug
        flush() was added to fix: tx_recorded_1.wav ends mid-utterance and lost
        2.62s of real speech that way. Nothing calls this yet, so nothing is
        losing data today -- but before wiring it to a call-end signal, decide
        per caller and add flush-then-reset for the end-of-call case.
        """
        if channel_id in self._channels:
            state = self._channels[channel_id]
            state.vad_iter.reset_states()
            state.triggered = False
            state.preroll.clear()
            state.segment_chunks = []
            state.segment_start_ts = None
            state.scores.clear()
