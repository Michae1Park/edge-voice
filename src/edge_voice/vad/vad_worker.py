from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import torch

from edge_voice.observability.logging import get_stage_logger
from edge_voice.pipeline.fanout import fanout_put
from edge_voice.pipeline.models import AudioPacket, SpeechSegment

logger = get_stage_logger(__name__, stage="vad")

INT16_SAMPLE_BYTES = 2
INT16_FULL_SCALE = 32768.0


@dataclass
class VADWorkerConfig:
    threshold: float = 0.5
    sample_rate: int = 16000
    rms_gate_enabled: bool = True
    # CALIBRATE: normalized float32 RMS, not raw int16.
    silence_rms_floor: float = 0.01
    preroll_chunks: int = 3
    min_silence_duration_ms: int = 100
    speech_pad_ms: int = 30

    # Every other boundary rule needs packets to keep arriving, so a sender that just stops
    # would leave its segment buffered until shutdown. 0 disables.
    idle_flush_s: float = 2.0

    # For speech with no pause long enough for Silero to fire `end`; bounds emission latency and segment size.
    segment_limits_enabled: bool = False
    max_segment_s: float = 7.0
    # A soft cut backdates `.end` to the chosen pause, which inflates latency measured against `.end`
    # (~0.7-1.75s per piece on RPi5, see docs/BENCHMARK.md). The hard cap is what bounds backlog.
    soft_cut_enabled: bool = True
    soft_cut_s: float = 5.0
    soft_cut_lookahead_s: float = 1.0
    soft_cut_min_dip: float = 0.10

    # Partials never reach dump_queue and are dropped rather than queued when STT is behind.
    partial_interval_s: float = 0.0
    partial_min_segment_s: float = 1.5


# Captures the exact score VADIterator used, without a second forward pass that would
# also feed Silero's stateful RNN each window twice.
class _ScoreCapturingModel:
    def __init__(self, model: Any) -> None:
        self._model = model
        self.last_score = 0.0

    def __call__(self, x: Any, sr: int) -> Any:
        out = self._model(x, sr)
        self.last_score = float(out.item()) if hasattr(out, "item") else float(out)
        return out

    def __getattr__(self, name: str) -> Any:
        return getattr(self._model, name)


@dataclass
class _ChannelState:
    vad_iter: Any
    scorer: Any = None
    triggered: bool = False
    preroll: list = field(default_factory=list)
    segment_chunks: list = field(default_factory=list)
    segment_start_ts: float | None = None
    segment_id: str | None = None
    seg_counter: int = 0
    last_packet_at: float = 0.0
    scores: list = field(default_factory=list)
    last_partial_s: float = 0.0


class VADWorker(threading.Thread):
    def __init__(
        self,
        routed_queue: "queue.Queue[AudioPacket]",
        segment_queues: "dict[str, queue.Queue[SpeechSegment]]",
        channel_ids: list[str],
        config: VADWorkerConfig | None = None,
        model=None,
        name: str = "VADWorker",
        dump_queue: "queue.Queue[SpeechSegment] | None" = None,
    ) -> None:
        super().__init__(name=name, daemon=True)
        self.routed_queue = routed_queue
        self.segment_queues = segment_queues
        self.dump_queue = dump_queue
        self.config = config or VADWorkerConfig()

        self.model = model

        self._channels: dict[str, _ChannelState] = {
            channel_id: self._new_channel_state() for channel_id in channel_ids
        }
        self._stop_event = threading.Event()
        self._last_activity = time.monotonic()

        # A real lock: a new key can resize the dict while MetricsCollector copies it from another thread.
        self._latency_lock = threading.Lock()
        self._silero_latency_s: dict[str, float] = {}
        self._rms_gate_latency_s: dict[str, float] = {}

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    @property
    def last_activity(self) -> float:
        return self._last_activity

    # Lets the supervisor log a crash that ate a live utterance before replacing the worker.
    def pending_loss(self) -> str | None:
        parts = []
        for channel_id, state in self._channels.items():
            if state.segment_start_ts is not None and state.segment_chunks:
                n_samples = sum(len(c) for c in state.segment_chunks) // INT16_SAMPLE_BYTES
                parts.append(f"{channel_id}={n_samples / self.config.sample_rate:.2f}s")
        return ", ".join(parts) if parts else None

    def silero_latencies_s(self) -> dict[str, float]:
        with self._latency_lock:
            return dict(self._silero_latency_s)

    def rms_gate_latencies_s(self) -> dict[str, float]:
        with self._latency_lock:
            return dict(self._rms_gate_latency_s)

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                packet = self.routed_queue.get(timeout=0.5)
            except queue.Empty:
                self._flush_idle()
                continue

            if packet is None:
                break

            self._last_activity = time.monotonic()

            try:
                self._handle_packet(packet)
            except Exception:
                logger.exception(
                    "VADWorker failed on packet from channel=%s",
                    packet.channel_id,
                    extra={"channel_id": packet.channel_id},
                )

            # Checked after packets too: the other channel can keep the queue busy so `Empty` never arrives.
            self._flush_idle()

        self.flush("shutdown")

    def flush(self, reason: str = "flush") -> int:
        return sum(
            self._flush_channel(channel_id, state, reason)
            for channel_id, state in self._channels.items()
        )

    def _flush_idle(self) -> int:
        if self.config.idle_flush_s <= 0:
            return 0
        now = time.monotonic()
        return sum(
            self._flush_channel(channel_id, state, "idle")
            for channel_id, state in self._channels.items()
            if now - state.last_packet_at >= self.config.idle_flush_s
        )

    def _flush_channel(self, channel_id: str, state: _ChannelState, reason: str) -> int:
        if state.segment_start_ts is None or not state.segment_chunks:
            return 0
        n_samples = sum(len(c) for c in state.segment_chunks) // INT16_SAMPLE_BYTES
        duration_s = n_samples / self.config.sample_rate
        logger.info(
            "VADWorker: flushing in-progress segment on channel=%s (%s, %.2fs)",
            channel_id,
            reason,
            duration_s,
            extra={"channel_id": channel_id, "segment_id": state.segment_id},
        )
        self._finalize_segment(channel_id, state, end_ts=state.segment_start_ts + duration_s)
        state.triggered = False
        state.scores.clear()
        # Otherwise VADIterator still thinks it's mid-speech and swallows the next `start`.
        state.vad_iter.reset_states()
        return 1


    def _handle_packet(self, packet: AudioPacket) -> None:
        state = self._channels[packet.channel_id]
        state.last_packet_at = time.monotonic()

        float_chunk = self._bytes_to_float_tensor(packet.samples)

        if self.config.rms_gate_enabled and not state.triggered:
            t0 = time.monotonic()
            rms = self._rms(float_chunk)
            if rms < self.config.silence_rms_floor:
                with self._latency_lock:
                    self._rms_gate_latency_s[packet.channel_id] = time.monotonic() - t0
                self._push_preroll(state, packet)
                return

        t0 = time.monotonic()
        result = state.vad_iter(float_chunk, return_seconds=True)
        with self._latency_lock:
            self._silero_latency_s[packet.channel_id] = time.monotonic() - t0

        if state.triggered:
            state.segment_chunks.append(packet.samples)

        started = bool(result and "start" in result)
        ended = bool(result and "end" in result)

        if started:
            self._start_segment(packet, state)
        elif ended:
            self._end_segment(packet, state)
        elif state.triggered:
            self._continue_segment(packet, state)
        else:
            # Silero needs a chunk or two of signal before `start`, so preroll keeps the true onset even with the gate off.
            self._push_preroll(state, packet)

    def _start_segment(self, packet: AudioPacket, state: _ChannelState) -> None:
        state.triggered = True
        state.segment_chunks = [b for _, b in state.preroll] + [packet.samples]
        state.segment_start_ts = state.preroll[0][0] if state.preroll else packet.timestamp
        state.preroll.clear()
        state.scores.clear()
        state.last_partial_s = 0.0
        self._new_segment_id(packet.channel_id, state)

    def _end_segment(self, packet: AudioPacket, state: _ChannelState) -> None:
        state.triggered = False
        self._finalize_segment(packet.channel_id, state, end_ts=packet.timestamp)
        state.scores.clear()

    def _continue_segment(self, packet: AudioPacket, state: _ChannelState) -> None:
        if self.config.segment_limits_enabled:
            self._maybe_cut(packet.channel_id, state, packet)
        # After the cut check, or the partial would describe audio its own final is about to supersede.
        if self.config.partial_interval_s > 0:
            self._maybe_emit_partial(packet.channel_id, state, packet)


    def _maybe_emit_partial(
        self, channel_id: str, state: _ChannelState, packet: AudioPacket
    ) -> None:
        cfg = self.config
        # Paced by accumulated speech, not wall clock, so live and replayed audio behave identically.
        samples_per_chunk = len(packet.samples) // INT16_SAMPLE_BYTES
        start_ts = state.segment_start_ts
        if samples_per_chunk <= 0 or start_ts is None or not state.segment_chunks:
            return

        seg_s = len(state.segment_chunks) * (samples_per_chunk / cfg.sample_rate)
        if seg_s < cfg.partial_min_segment_s:
            return
        if seg_s - state.last_partial_s < cfg.partial_interval_s:
            return

        state.last_partial_s = seg_s
        self._emit_partial(channel_id, state, start_ts, seg_s)

    def _emit_partial(
        self, channel_id: str, state: _ChannelState, start_ts: float, seg_s: float
    ) -> None:
        # put_nowait, not fanout_put: blocking this real-time thread for throwaway work STT would drop is worse.
        segment = SpeechSegment(
            channel_id=channel_id,
            start=start_ts,
            end=start_ts + seg_s,
            audio=b"".join(state.segment_chunks),
            segment_id=state.segment_id or self._new_segment_id(channel_id, state),
            is_partial=True,
        )
        try:
            self.segment_queues[channel_id].put_nowait(segment)
        except queue.Full:
            logger.debug(
                "VADWorker: segment queue full -- dropping partial on channel=%s",
                channel_id,
                extra={"channel_id": channel_id, "segment_id": segment.segment_id},
            )


    # Past soft_cut_s, cut just after the deepest VAD-confidence dip in the lookahead window;
    # at max_segment_s cut unconditionally.
    def _maybe_cut(self, channel_id: str, state: _ChannelState, packet: AudioPacket) -> None:
        cfg = self.config
        samples_per_chunk = len(packet.samples) // INT16_SAMPLE_BYTES
        if samples_per_chunk <= 0:
            return
        chunk_s = samples_per_chunk / cfg.sample_rate
        n_chunks = len(state.segment_chunks)
        seg_s = n_chunks * chunk_s

        if seg_s >= cfg.max_segment_s:
            self._cut_segment(channel_id, state, n_chunks, chunk_s, "hard cap")
            return

        if not cfg.soft_cut_enabled:
            return

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
            extra={"channel_id": channel_id, "segment_id": state.segment_id},
        )
        self._finalize_segment(channel_id, state, end_ts=cut_ts)

        state.segment_chunks = tail
        state.segment_start_ts = cut_ts
        state.scores = [(i - cut_idx, s) for i, s in state.scores if i >= cut_idx]
        self._new_segment_id(channel_id, state)

    def _new_segment_id(self, channel_id: str, state: _ChannelState) -> str:
        state.seg_counter += 1
        state.segment_id = f"{channel_id}-{state.segment_start_ts:.3f}-{state.seg_counter}"
        return state.segment_id

    def _finalize_segment(self, channel_id: str, state: _ChannelState, end_ts: float) -> None:
        if state.segment_start_ts is None or not state.segment_chunks:
            return
        segment_id = state.segment_id or self._new_segment_id(channel_id, state)
        segment = SpeechSegment(
            channel_id=channel_id,
            start=state.segment_start_ts,
            end=end_ts,
            audio=b"".join(state.segment_chunks),
            segment_id=segment_id,
        )
        fanout_put(segment, self.segment_queues[channel_id], self.dump_queue)
        state.segment_chunks = []
        state.segment_start_ts = None
        state.segment_id = None
        state.last_partial_s = 0.0


    # Each channel needs its own model: the LSTM state lives in the model, and sharing it
    # doubled segment counts on recorded calls. A second model costs ~0.06s and ~4MB.
    def _new_channel_state(self) -> _ChannelState:
        model = self.model if self.model is not None else self._load_model()
        scorer = _ScoreCapturingModel(model)
        return _ChannelState(vad_iter=self._new_vad_iterator(scorer), scorer=scorer)

    @staticmethod
    def _load_model() -> Any:
        from silero_vad import load_silero_vad

        return load_silero_vad(onnx=True)

    def _new_vad_iterator(self, model: Any):
        from silero_vad.utils_vad import VADIterator

        return VADIterator(
            model,
            threshold=self.config.threshold,
            sampling_rate=self.config.sample_rate,
            min_silence_duration_ms=self.config.min_silence_duration_ms,
            speech_pad_ms=self.config.speech_pad_ms,
        )

    def _bytes_to_float_tensor(self, pcm_bytes: bytes) -> torch.Tensor:
        arr = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32) / INT16_FULL_SCALE
        return torch.from_numpy(arr)

    def _rms(self, float_tensor: torch.Tensor) -> float:
        return float(torch.sqrt(torch.mean(float_tensor**2)))

    def _push_preroll(self, state: _ChannelState, packet: AudioPacket) -> None:
        state.preroll.append((packet.timestamp, packet.samples))
        if len(state.preroll) > self.config.preroll_chunks:
            state.preroll.pop(0)

    # UNRESOLVED: discards the in-progress segment. Right for a reconnect gap, wrong for call end
    # (use flush-then-reset there). Currently unwired.
    def reset_channel(self, channel_id: str) -> None:
        if channel_id in self._channels:
            state = self._channels[channel_id]
            state.vad_iter.reset_states()
            state.triggered = False
            state.preroll.clear()
            state.segment_chunks = []
            state.segment_start_ts = None
            state.segment_id = None
            state.scores.clear()
