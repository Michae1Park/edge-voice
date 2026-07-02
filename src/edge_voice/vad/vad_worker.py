"""Real Silero VAD segmentation worker.

Shared VAD model + per-channel VADIterator with thread-safe processing.
One shared Silero VAD model (stateless feedforward neural net) with a
global lock that serialises both the explicit score call AND the
VADIterator's internal model call so channels never touch the model
concurrently. Each channel gets its own VADIterator + per-channel state.

Consumes AudioPacket from routed_queue (from PacketCopier).
Emits SpeechSegment to segment_queue with the captured audio.

Soft-cut: when a segment exceeds SOFT_CUT_S, scan the recent VAD scores
for a local-minimum confidence dip; use that as the segment cut point
instead of a hard boundary. Segments longer than MAX_SEGMENT_S always
get hard-capped.
"""

from __future__ import annotations

import itertools
import logging
import queue
import threading
import time
from typing import Any, Dict, List, Optional

import numpy as np
import torch

from edge_voice.pipeline.models import AudioPacket, SpeechSegment

logger = logging.getLogger(__name__)

VAD_SR = 16000  # Silero expects 16 kHz
VAD_WINDOW_SAMPLES = 512  # 32 ms @ 16 kHz
QUEUE_GET_TIMEOUT_S = 0.2

_seg_ctr = itertools.count(1)

# ── Shared VAD instance (one-time global init) ──────────────────────────

_vad_model: Optional[Any] = None
_vad_lock = threading.Lock()


def _get_vad_model() -> Any:
    """Lazily load the Silero VAD model (singleton across all worker instances)."""
    global _vad_model
    if _vad_model is None:
        from silero_vad import load_silero_vad

        logger.info("[VAD] Loading shared Silero VAD model …")
        _vad_model = load_silero_vad()
        logger.info("[VAD] Model loaded.")
    return _vad_model


# ── Per-channel mutable state ─────────────────────────────────────────────
#
# Each instance lives entirely within one VadWorker thread.  Access to
# vad_iter is guarded by the global _vad_lock AND the instance's own lock.


class _Ch:
    """Per-channel VAD state."""

    __slots__ = (
        "channel_id",
        "vad_iter",
        "in_speech",
        "seg_index",
        "seg_start_s",
        "win_buf",  # List[torch.Tensor] — VAD window tensors
        "aud_buf",  # List[np.ndarray]   — raw float32 samples per window
        "scores",  # List[float]        — one confidence score per window
        "lock",
    )

    def __init__(self, channel_id: str, vad_iter: Any) -> None:
        self.channel_id = channel_id
        self.vad_iter = vad_iter
        self.in_speech = False
        self.seg_index = 0
        self.seg_start_s = 0.0
        self.win_buf: List[torch.Tensor] = []
        self.aud_buf: List[np.ndarray] = []
        self.scores: List[float] = []
        self.lock = threading.Lock()

    def reset(self) -> None:
        self.in_speech = False
        self.seg_start_s = 0.0
        self.win_buf.clear()
        self.aud_buf.clear()
        self.scores.clear()

    @property
    def n_windows(self) -> int:
        return len(self.win_buf)


# ── VadWorker ─────────────────────────────────────────────────────────────


class VadWorker(threading.Thread):
    """Multichannel VAD segmentation with soft-cut support.

    A single thread pumps all packets from the routed_queue. Each packet
    is split into VAD_WINDOW_SAMPLES (512)-sample windows. For every
    window:

    1. Confidence score under ``_vad_lock``.
    2. Feed per-channel VADIterator (same lock) for state transitions.
    3. Record every score so soft-cut can find the best cut-point.
    """

    def __init__(
        self,
        settings: Any,
        routed_queue: queue.Queue[AudioPacket],
        segment_queue: queue.Queue[SpeechSegment],
    ) -> None:
        super().__init__(name="VadWorker", daemon=False)
        self._settings = settings
        self._routed_queue = routed_queue
        self._segment_queue = segment_queue
        self._stop_event = threading.Event()

        self._vad_model: Optional[Any] = None

        # Per-channel state
        self._ch_map: Dict[str, _Ch] = {}
        self._ch_lock = threading.Lock()

        # Config parameters
        self._threshold = settings.vad.threshold
        self._max_seg_s = settings.vad.max_segment_s
        self._soft_cut_s = settings.vad.soft_cut_s
        self._soft_cut_la_s = settings.vad.soft_cut_lookahead_s
        self._soft_cut_dip = settings.vad.soft_cut_min_dip
        self._min_sil_ms = settings.vad.min_silence_ms

        # Derived constants
        self._max_seg_n = int(self._max_seg_s * VAD_SR / VAD_WINDOW_SAMPLES)
        self._soft_cut_n = int(self._soft_cut_s * VAD_SR / VAD_WINDOW_SAMPLES)
        self._soft_cut_la_n = int(self._soft_cut_la_s * VAD_SR / VAD_WINDOW_SAMPLES)

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def run(self) -> None:
        logger.info(
            "[VAD] started (sr=%d, win=%d, thresh=%.2f, max=%.1fs, sc=%.1fs)",
            VAD_SR,
            VAD_WINDOW_SAMPLES,
            self._threshold,
            self._max_seg_s,
            self._soft_cut_s,
        )
        self._vad_model = _get_vad_model()

        while not self._stop_event.is_set():
            try:
                pkt = self._routed_queue.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue
            if self._stop_event.is_set():
                break
            self._process_pkt(pkt)

        logger.info("[VAD] stopped")

    def stop(self) -> None:
        self._stop_event.set()

    # ── Channel state factory ───────────────────────────────────────────────

    def _get_ch(self, ch_id: str) -> _Ch:
        """Get or create per-channel VAD state."""
        with self._ch_lock:
            if ch_id not in self._ch_map:
                from silero_vad import VADIterator

                it = VADIterator(
                    self._vad_model,
                    threshold=self._threshold,
                    sampling_rate=VAD_SR,
                    min_silence_duration_ms=self._min_sil_ms,
                )
                self._ch_map[ch_id] = _Ch(ch_id, it)
                logger.info("[VAD] VADIterator ch=%s", ch_id)
            return self._ch_map[ch_id]

    # ── Per-packet processing ───────────────────────────────────────────────

    def _process_pkt(self, pkt: AudioPacket) -> None:
        ch = self._get_ch(pkt.channel_id)

        # Decode raw PCM bytes → float32 [-1, 1]
        samples = self._pcm_to_float(pkt.samples)
        n = len(samples)
        if n == 0:
            return

        n_wins = n // VAD_WINDOW_SAMPLES
        base_t = pkt.timestamp

        for wi in range(n_wins):
            lo = wi * VAD_WINDOW_SAMPLES
            hi = lo + VAD_WINDOW_SAMPLES
            raw_win = samples[lo:hi]
            win = torch.from_numpy(raw_win).float()
            win_t = base_t + wi * VAD_WINDOW_SAMPLES / VAD_SR

            if not ch.in_speech:
                # Not yet in speech: score + check for "start" event
                score, ev = self._score_and_iter(ch, win, win_t)
                if ev is not None and "start" in ev:
                    self._beg_seg(ch, win, raw_win, score, win_t)
                continue

            # Inside speech: score the window AND feed iterator
            score = self._score_win(ch, win)
            with ch.lock:
                ev = ch.vad_iter(win)
            if ev is not None:
                if "end" in ev:
                    # VAD says speech ended: record this window then emit
                    with ch.lock:
                        self._record(ch, win, raw_win, score)
                        cut = self._find_good_cut(ch)
                        if cut <= 0:
                            cut = ch.n_windows
                        self._emit(ch, cut)
                        ch.win_buf = ch.win_buf[cut:]
                        ch.aud_buf = ch.aud_buf[cut:]
                        ch.scores = ch.scores[cut:]
                        if ch.win_buf:
                            ch.seg_start_s = cut * VAD_WINDOW_SAMPLES / VAD_SR
                            ch.seg_index += 1
                        else:
                            ch.reset()
                        if not ch.in_speech and ch.win_buf:
                            # Re-arm for next segment after VAD "end"
                            ch.in_speech = True
                    continue

            # Normal window (silence or no event) inside speech segment
            with ch.lock:
                self._record(ch, win, raw_win, score)
            self._try_cut(ch, win_t)

        # Leftover samples (partial window at end of packet)
        rem = n - n_wins * VAD_WINDOW_SAMPLES
        if rem > 0:
            r = torch.from_numpy(samples[n_wins * VAD_WINDOW_SAMPLES :]).float()
            padded = torch.nn.functional.pad(r, (0, VAD_WINDOW_SAMPLES - rem))
            if ch.in_speech:
                with ch.lock:
                    self._record(ch, padded, r.numpy(), 0.0)
                self._try_cut(ch, base_t + n / VAD_SR)

    # ── Score one window (under global lock) ────────────────────────────────

    def _score_and_iter(self, ch: _Ch, win: torch.Tensor, t: float):
        """Score + feed VADIterator in one critical section."""
        vad_model = self._vad_model
        if vad_model is None:
            raise RuntimeError("VAD model not loaded")
        t0 = time.perf_counter()
        with _vad_lock, torch.no_grad():
            score = vad_model(win, VAD_SR).item()
            ev = ch.vad_iter(win)
        ms = (time.perf_counter() - t0) * 1000
        if ms > 20:
            logger.warning("[VAD] slow window: %.1f ms", ms)
        return score, ev

    def _score_win(self, ch: _Ch, win: torch.Tensor) -> float:
        """Score only (no iterator advance)."""
        vad_model = self._vad_model
        if vad_model is None:
            raise RuntimeError("VAD model not loaded")
        with _vad_lock, torch.no_grad():
            return vad_model(win, VAD_SR).item()

    # ── Segment start ───────────────────────────────────────────────────────

    def _beg_seg(self, ch: _Ch, win: torch.Tensor, raw: np.ndarray, score: float, t: float) -> None:
        """Begin a new speech segment."""
        ch.seg_index += 1
        ch.seg_start_s = t
        ch.in_speech = True
        ch.win_buf = [win]
        ch.aud_buf = [raw]
        ch.scores = [score]
        logger.info(
            "[VAD] %s seg %d start @ %.2fs (score=%.3f)",
            ch.channel_id,
            ch.seg_index,
            t,
            score,
        )

    # ── Segment end: find good cut-point inside buffer ──────────────────────

    def _find_good_cut(self, ch: _Ch) -> int:
        """Find the best cut-point inside the current speech buffer.

        If a segment has exceeded SOFT_CUT_S, scan for a confidence dip.
        Otherwise, return the last window where confidence is still high
        (back from the end).
        """
        if ch.n_windows <= self._soft_cut_n:
            # Under soft-cut threshold: just use the whole buffer
            return ch.n_windows

        # Look in the last look-ahead windows for the minimum confidence
        lo = max(0, len(ch.scores) - self._soft_cut_la_n)
        recent = ch.scores[lo:]
        min_sc = min(recent)

        if min_sc >= self._soft_cut_dip:
            # No significant dip: use the whole buffer
            return ch.n_windows

        # Found a dip: cut at the minimum
        cut = lo + recent.index(min_sc) + 1
        logger.info(
            "[VAD] Good cut at idx %d (min=%.3f, dip<%s)",
            cut,
            min_sc,
            self._soft_cut_dip,
        )
        return cut

    # ── Emit one SpeechSegment ──────────────────────────────────────────────

    def _emit(self, ch: _Ch, cut: int) -> None:
        n = cut
        audios = ch.aud_buf[:n]
        if not audios:
            return
        arr = np.concatenate(audios)
        # Clamp to 16-bit and pack
        arr = np.clip(arr, -1.0, 1.0)
        pcm = (arr * 32767).astype(np.int16).tobytes()
        dur_s = n * VAD_WINDOW_SAMPLES / VAD_SR

        seg = SpeechSegment(
            channel_id=ch.channel_id,
            start=ch.seg_start_s,
            end=ch.seg_start_s + dur_s,
            audio=pcm,
            segment_id=f"vad-seg-{next(_seg_ctr)}",
        )
        try:
            self._segment_queue.put(seg, timeout=2.0)
        except queue.Full:
            logger.warning("[VAD] segment_queue full — dropping %s", seg.segment_id)
        logger.info(
            "[VAD] %s seg %d emitted (cut@%d) (%.3fs, %d B)",
            ch.channel_id,
            ch.seg_index,
            cut,
            dur_s,
            len(pcm),
        )

    # ── Soft-cut / hard-cut ─────────────────────────────────────────────────

    def _try_cut(self, ch: _Ch, t: float) -> None:
        """Check for soft-cut or hard-cut and emit if needed."""
        n = ch.n_windows

        # Hard cap at MAX_SEGMENT_S
        if n >= self._max_seg_n:
            logger.info(
                "[VAD] %s hard-cut seg %d (%.1fs >= %.1fs)",
                ch.channel_id,
                ch.seg_index,
                t - ch.seg_start_s,
                self._max_seg_s,
            )
            self._emit(ch, n)
            ch.reset()
            return

        # Soft cut: only if segment has exceeded SOFT_CUT_S
        if n < self._soft_cut_n:
            return

        # Need enough scores for a meaningful lookahead scan
        if len(ch.scores) < self._soft_cut_la_n + 1:
            return

        # Scan recent scores for local minimum
        lo = max(0, len(ch.scores) - self._soft_cut_la_n)
        recent = ch.scores[lo:]
        min_sc = min(recent)

        # Soft-cut if the minimum dips sufficiently
        if min_sc < self._soft_cut_dip:
            cut = lo + recent.index(min_sc)
            self._emit(ch, cut)
            # Tail becomes next segment
            ch.win_buf = list(ch.win_buf[cut:])
            ch.aud_buf = list(ch.aud_buf[cut:])
            ch.scores = list(ch.scores[cut:])
            ch.seg_start_s = cut * VAD_WINDOW_SAMPLES / VAD_SR
            ch.seg_index += 1
            logger.info(
                "[VAD] %s soft-cut seg %d @ idx %d (min=%.3f, seg=%.1fs)",
                ch.channel_id,
                ch.seg_index - 1,
                cut,
                min_sc,
                ch.seg_start_s - self._seg_prev(ch, cut),
            )

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _record(ch: _Ch, win: torch.Tensor, raw: np.ndarray, score: float) -> None:
        ch.win_buf.append(win)
        ch.aud_buf.append(raw)
        ch.scores.append(score)

    @staticmethod
    def _seg_prev(ch: _Ch, cut_idx: int) -> float:
        return cut_idx * VAD_WINDOW_SAMPLES / VAD_SR

    @staticmethod
    def _pcm_to_float(pcm: bytes) -> np.ndarray:
        """Convert raw PCM int16 bytes → float32 [-1, 1]."""
        return np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0
