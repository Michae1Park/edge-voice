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


@dataclass
class _ChannelState:
    vad_iter: Any  # VADIterator instance, own state
    triggered: bool = False
    preroll: list = field(default_factory=list)  # list[(timestamp, bytes)]
    segment_chunks: list = field(default_factory=list)  # list[bytes], during active speech
    segment_start_ts: float | None = None
    seg_counter: int = 0


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

        if model is None:
            model, _ = torch.hub.load(
                repo_or_dir="snakers4/silero-vad", model="silero_vad", force_reload=False
            )
        self.model = model

        self._channels: dict[str, _ChannelState] = {}
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                packet = self.routed_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if packet is None:  # shutdown sentinel
                break

            try:
                self._handle_packet(packet)
            except Exception:
                logger.exception("VADWorker failed on packet from channel=%s", packet.channel_id)

    # ── Per-packet handling ─────────────────────────────────────

    def _handle_packet(self, packet: AudioPacket) -> None:
        state = self._channels.setdefault(
            packet.channel_id,
            _ChannelState(vad_iter=self._new_vad_iterator()),
        )

        float_chunk = self._bytes_to_float_tensor(packet.samples)

        if self.config.rms_gate_enabled and not state.triggered:
            rms = self._rms(float_chunk)
            if rms < self.config.silence_rms_floor:
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

        elif result and "end" in result:
            state.triggered = False
            self._finalize_segment(packet.channel_id, state, end_ts=packet.timestamp)

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

    def _new_vad_iterator(self):
        from silero_vad.utils_vad import VADIterator  # adjust import to your install

        return VADIterator(
            self.model,
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
        """Call on stream discontinuity (call end, MQTT reconnect gap, etc.)"""
        if channel_id in self._channels:
            state = self._channels[channel_id]
            state.vad_iter.reset_states()
            state.triggered = False
            state.preroll.clear()
            state.segment_chunks = []
            state.segment_start_ts = None
