#!/usr/bin/env python3
"""
Dual-channel Moonshine STT with fully streaming Silero VAD pre-filtering.

Two audio channels are processed in parallel — each in its own thread with
its own Transcriber instance and VADIterator.

Sharing strategy
────────────────
VAD model   — one shared instance.  Silero's network is stateless (pure
              feedforward); all mutable state lives in VADIterator, which is
              per-channel.  A vad_lock serialises both the explicit score call
              AND the VADIterator's internal model call in one critical section,
              so the two threads never touch the model concurrently.
              Critical section is ~1 ms per 32 ms window — contention is low.

Transcriber — two independent instances pointing at the same model_path.
              Moonshine is a streaming, stateful decoder (start/add_audio/stop),
              so sessions cannot be shared across channels.  The OS will
              typically memory-map the same weight file for both instances.

Channel assignment
──────────────────
Stereo WAV    →  channel 0 = left,  channel 1 = right
Two mono WAVs →  --ch0 <path>  --ch1 <path>
Single mono WAV  →  both channels run the same audio (benchmark / debug)

Architecture (per channel):
    WAV file (lazy packet reader, one per channel)
        └─► PACKET_DURATION_MS packets
                └─► VAD_WINDOW_SAMPLES sub-windows (512 samples = 32 ms)
                        └─► vad_lock: score + VADIterator decision
                                ├─► speech  → batch into feed_buf → Transcriber
                                └─► end     → flush → transcriber.stop/start

Soft-cut logic:
    Once a segment exceeds SOFT_CUT_S the VAD confidence score is tracked per
    window.  A local minimum within the lookahead is used as the cut point
    instead of a hard boundary.  The tail is replayed into the next session.

Usage:
    # Stereo WAV — left/right split automatically
    python moonshine_silero_vad_dual.py stereo_call.wav

    # Two separate mono WAVs
    python moonshine_silero_vad_dual.py --ch0 agent.wav --ch1 customer.wav

    # Single mono WAV on both channels (debug / benchmark)
    python moonshine_silero_vad_dual.py mono.wav

Dependencies:
    pip install silero-vad torch torchaudio soundfile moonshine-voice
"""

import argparse
import os
import sys
import threading
import time
from typing import Generator, List, Tuple

import torch
import torchaudio

from moonshine_voice import (
    Transcriber,
    TranscriptEventListener,
    get_model_for_language,
)

# ── General ────────────────────────────────────────────────────────────────────

DEFAULT_AUDIO = "/home/ai/workspace/ai-experiments/asr-coastguard/data/base/conversation_60s.wav"

# ── Silero VAD config ──────────────────────────────────────────────────────────
"""
Audio packet sizing reference:
Format: 16-bit PCM, mono (2 bytes/sample)

16 kHz sample rate:
    512 samples = 32 ms = 1024 bytes
    320 samples = 20 ms =  640 bytes
    256 samples = 16 ms =  512 bytes

8 kHz sample rate:
    256 samples = 32 ms = 512 bytes
    160 samples = 20 ms = 320 bytes
    128 samples = 16 ms = 256 bytes
"""

SILERO_SAMPLE_RATE = 16000

# Silero VAD input frame size
# 512 samples @ 16 kHz = 32 ms = 1024 bytes (16-bit mono PCM)
VAD_WINDOW_SAMPLES = 512
VAD_THRESHOLD = 0.5
# Amount of silence required before ending a speech segment
VAD_MIN_SILENCE_MS = 300

# ── Segment length / cut config ────────────────────────────────────────────────

MAX_SEGMENT_S = 7.0
SOFT_CUT_S = 5.0
SOFT_CUT_LOOKAHEAD_S = 1.0

MAX_SEGMENT_SAMPLES = int(MAX_SEGMENT_S * SILERO_SAMPLE_RATE)
SOFT_CUT_SAMPLES = int(SOFT_CUT_S * SILERO_SAMPLE_RATE)
SOFT_CUT_LOOKAHEAD = int(SOFT_CUT_LOOKAHEAD_S * SILERO_SAMPLE_RATE / VAD_WINDOW_SAMPLES)
SOFT_CUT_MIN_DIP = 0.10

# ── Streaming packet config ────────────────────────────────────────────────────

PACKET_DURATION_MS = 128

# ── Moonshine STT config ───────────────────────────────────────────────────────

STT_LANGUAGE = "ko"
STT_MODEL_ARCH = 0
STT_FEED_WINDOWS = 64

STT_OPTIONS = {
    "max_tokens_per_second": "13.0",
    "identify_speakers": "false",
    "log_api_calls": "false",
    "save_input_wav_path": "",
    "return_audio_data": "false",
}

# ── Derived constants ──────────────────────────────────────────────────────────

_PACKET_SAMPLES = int(SILERO_SAMPLE_RATE * PACKET_DURATION_MS / 1000)
_PACKET_SAMPLES = (_PACKET_SAMPLES // VAD_WINDOW_SAMPLES) * VAD_WINDOW_SAMPLES
_WINDOWS_PER_PACKET = _PACKET_SAMPLES // VAD_WINDOW_SAMPLES

# ── Thread-safe print ──────────────────────────────────────────────────────────

_print_lock = threading.Lock()


def tprint(*args, **kwargs):
    """Print with a global lock so lines from two threads don't interleave."""
    with _print_lock:
        print(*args, **kwargs)


# ── Silero VAD loader ──────────────────────────────────────────────────────────


def load_silero():
    """
    Load a single Silero VAD model instance shared across both channel threads.
    The model is stateless (pure feedforward); all mutable VAD state lives in
    VADIterator, which is instantiated per channel.  A vad_lock passed to each
    channel thread serialises the forward pass so threads never call it
    concurrently.
    """
    print("[VAD] Loading Silero VAD model …")
    from silero_vad import load_silero_vad

    model = load_silero_vad()
    print("[VAD] Model loaded.\n")
    return model


# ── Audio source helpers ───────────────────────────────────────────────────────


def packet_stream_mono(
    path: str,
    packet_samples: int = _PACKET_SAMPLES,
) -> Generator[Tuple[torch.Tensor, float], None, None]:
    """
    Yield (packet_tensor, offset_s) from a mono (or already-extracted) WAV.
    Final partial packet is zero-padded.
    """
    import soundfile as sf

    with sf.SoundFile(path) as f:
        src_sr = f.samplerate
        resampler = (
            torchaudio.transforms.Resample(src_sr, SILERO_SAMPLE_RATE)
            if src_sr != SILERO_SAMPLE_RATE
            else None
        )
        src_block = (
            int(packet_samples * src_sr / SILERO_SAMPLE_RATE)
            if src_sr != SILERO_SAMPLE_RATE
            else packet_samples
        )

        sample_offset = 0
        for block in f.blocks(blocksize=src_block, dtype="float32"):
            audio = torch.from_numpy(block)
            if audio.dim() == 2:
                audio = audio.mean(dim=1)  # fallback: mix to mono
            if resampler is not None:
                audio = resampler(audio.unsqueeze(0)).squeeze(0)
            if len(audio) < packet_samples:
                audio = torch.nn.functional.pad(audio, (0, packet_samples - len(audio)))
            offset_s = sample_offset / SILERO_SAMPLE_RATE
            yield audio, offset_s
            sample_offset += packet_samples


def packet_stream_stereo_channel(
    path: str,
    channel: int,
    packet_samples: int = _PACKET_SAMPLES,
) -> Generator[Tuple[torch.Tensor, float], None, None]:
    """
    Yield (packet_tensor, offset_s) from a single channel of a stereo WAV.
    channel=0 → left,  channel=1 → right.
    Falls back to mono mix if the file has only one channel.
    """
    import soundfile as sf

    with sf.SoundFile(path) as f:
        n_channels = f.channels
        src_sr = f.samplerate
        resampler = (
            torchaudio.transforms.Resample(src_sr, SILERO_SAMPLE_RATE)
            if src_sr != SILERO_SAMPLE_RATE
            else None
        )
        src_block = (
            int(packet_samples * src_sr / SILERO_SAMPLE_RATE)
            if src_sr != SILERO_SAMPLE_RATE
            else packet_samples
        )

        sample_offset = 0
        for block in f.blocks(blocksize=src_block, dtype="float32"):
            audio = torch.from_numpy(block)
            if audio.dim() == 2:
                if n_channels > channel:
                    audio = audio[:, channel]
                else:
                    audio = audio.mean(dim=1)
            if resampler is not None:
                audio = resampler(audio.unsqueeze(0)).squeeze(0)
            if len(audio) < packet_samples:
                audio = torch.nn.functional.pad(audio, (0, packet_samples - len(audio)))
            offset_s = sample_offset / SILERO_SAMPLE_RATE
            yield audio, offset_s
            sample_offset += packet_samples


# ── VAD segment export ─────────────────────────────────────────────────────────


def save_segment(
    audio_buf: List[torch.Tensor],
    seg_index: int,
    source_stem: str,
    out_dir: str,
    ch_label: str = "",
) -> str:
    os.makedirs(out_dir, exist_ok=True)
    audio = torch.cat(audio_buf)
    tag = f"_{ch_label}" if ch_label else ""
    out_path = os.path.join(out_dir, f"{source_stem}{tag}_seg_{seg_index:02d}.wav")
    torchaudio.save(out_path, audio.unsqueeze(0), SILERO_SAMPLE_RATE)
    dur = len(audio) / SILERO_SAMPLE_RATE
    tprint(f"  [VAD] saved seg {seg_index:02d}: {out_path}  ({dur:.3f}s)")
    return out_path


# ── Moonshine listener ─────────────────────────────────────────────────────────


class OffsetListener(TranscriptEventListener):
    def __init__(self, seg_index: int, offset_s: float, ch_label: str = ""):
        self.seg_index = seg_index
        self.offset_s = offset_s
        self.ch_label = ch_label
        self.best_partial = ""

    def _prefix(self):
        return f"[{self.ch_label}] " if self.ch_label else ""

    @staticmethod
    def _is_repetitive(text: str, threshold: float = 0.45) -> bool:
        tokens = text.split()
        if len(tokens) < 4:
            return False
        return (len(set(tokens)) / len(tokens)) < threshold

    def on_line_text_changed(self, event):
        text = event.line.text
        tprint(
            f"  {self._prefix()}[seg {self.seg_index:02d}] "
            f"{self._t(event.line.start_time):.2f}s  ~ changed  : {text}"
        )
        if not self._is_repetitive(text):
            self.best_partial = text

    def on_line_completed(self, event):
        text = event.line.text
        if self._is_repetitive(text):
            text = self.best_partial
            tprint(
                f"  {self._prefix()}[seg {self.seg_index:02d}] "
                f"⚠ final was repetitive, using best partial"
            )
        s = self._t(event.line.start_time)
        e = self._t(event.line.start_time + event.line.duration)
        tprint(
            f"  {self._prefix()}[seg {self.seg_index:02d}] "
            f"[{s:.2f}s – {e:.2f}s]  ✔ completed: {text}"
        )

    def _t(self, t):
        return t + self.offset_s


# ── Per-channel processing ─────────────────────────────────────────────────────


def process_channel(
    stream: Generator[Tuple[torch.Tensor, float], None, None],
    transcriber: Transcriber,
    vad_model,
    vad_lock: threading.Lock,
    ch_label: str = "",
    source_stem: str = "",
    vad_threshold: float = VAD_THRESHOLD,
    min_silence_ms: int = VAD_MIN_SILENCE_MS,
    save_vad_dir: str = "",
):
    """
    Full streaming VAD→STT pipeline for a single channel.
    Designed to run inside a dedicated thread.

    vad_lock must cover both the explicit vad_model() score call AND the
    VADIterator call (which makes its own internal model call).  Both must
    be inside the same critical section so the two channel threads never
    overlap on the shared model.
    """
    from silero_vad import VADIterator

    packet_samples = _PACKET_SAMPLES
    effective_ms = packet_samples / SILERO_SAMPLE_RATE * 1000
    tag = f"[{ch_label}] " if ch_label else ""

    tprint(
        f"{tag}Packet : {effective_ms:.0f} ms  ({packet_samples} samples, "
        f"{packet_samples // VAD_WINDOW_SAMPLES} VAD windows/packet)"
    )

    vad_iter = VADIterator(
        vad_model,
        threshold=vad_threshold,
        sampling_rate=SILERO_SAMPLE_RATE,
        min_silence_duration_ms=min_silence_ms,
    )

    seg_index = 0
    seg_start_s = 0.0
    in_speech = False

    speech_buf: List[torch.Tensor] = []
    vad_scores: List[float] = []
    feed_buf: List[torch.Tensor] = []

    # ── feed buffer helpers ────────────────────────────────────────────────────

    def feed_window(window: torch.Tensor):
        feed_buf.append(window)
        if len(feed_buf) >= STT_FEED_WINDOWS:
            transcriber.add_audio(torch.cat(feed_buf).tolist(), SILERO_SAMPLE_RATE)
            feed_buf.clear()

    def flush_feed_buf():
        if feed_buf:
            transcriber.add_audio(torch.cat(feed_buf).tolist(), SILERO_SAMPLE_RATE)
            feed_buf.clear()

    # ── segment lifecycle ──────────────────────────────────────────────────────

    def arm_segment(offset_s: float):
        transcriber.remove_all_listeners()
        transcriber.add_listener(OffsetListener(seg_index, offset_s, ch_label))
        tprint(f"\n{tag}[STT] Segment {seg_index:02d} started at {offset_s:.2f}s")

    def close_segment(offset_s: float, reason: str = ""):
        nonlocal in_speech
        in_speech = False
        flush_feed_buf()
        t0 = time.perf_counter()
        transcriber.stop()
        elapsed = time.perf_counter() - t0
        dur = offset_s - seg_start_s
        label = f"  ({reason})" if reason else ""
        tprint(
            f"  {tag}[STT] closed at {offset_s:.2f}s  "
            f"flush took {elapsed:.3f}s for ~{dur:.2f}s audio{label}"
        )
        if save_vad_dir:
            save_segment(speech_buf, seg_index, source_stem, save_vad_dir, ch_label)

    # ── soft/hard cut helper ───────────────────────────────────────────────────

    def do_cut(cut_win: int, offset_s: float, reason: str):
        nonlocal speech_buf, vad_scores, seg_index, seg_start_s, in_speech

        tail_buf = speech_buf[cut_win:]
        tail_scores = vad_scores[cut_win:]
        speech_buf = speech_buf[:cut_win]
        vad_scores = vad_scores[:cut_win]

        cut_offset_s = seg_start_s + cut_win * VAD_WINDOW_SAMPLES / SILERO_SAMPLE_RATE

        close_segment(cut_offset_s, reason)

        seg_index += 1
        seg_start_s = cut_offset_s
        in_speech = True
        speech_buf = list(tail_buf)
        vad_scores = list(tail_scores)

        transcriber.start()
        arm_segment(seg_start_s)

        for tw in tail_buf:
            feed_window(tw)

    # ── main loop ─────────────────────────────────────────────────────────────

    transcriber.start()
    offset_s = 0.0  # ensure defined even if stream is empty

    for packet, packet_offset_s in stream:
        for w in range(packet_samples // VAD_WINDOW_SAMPLES):
            window = packet[w * VAD_WINDOW_SAMPLES : (w + 1) * VAD_WINDOW_SAMPLES]
            offset_s = packet_offset_s + w * VAD_WINDOW_SAMPLES / SILERO_SAMPLE_RATE

            # Lock covers both calls: our explicit score call AND the
            # VADIterator's internal model call.  Both must be in the same
            # critical section or the two threads can collide on the shared
            # model mid-call.  Section is ~1 ms; contention is negligible.
            t_vad = time.perf_counter()
            with vad_lock, torch.no_grad():
                score = vad_model(window, SILERO_SAMPLE_RATE).item()
                event = vad_iter(window)
            vad_ms = (time.perf_counter() - t_vad) * 1000
            if vad_ms > 20:
                tprint(f"  {tag}[VAD] window took {vad_ms:.1f}ms")

            # ── VAD state machine ──────────────────────────────────────────────

            if event:
                if "start" in event:
                    if in_speech:
                        close_segment(offset_s, "forced by VAD start")
                        transcriber.start()

                    seg_index += 1
                    seg_start_s = offset_s
                    in_speech = True
                    speech_buf = [window]
                    vad_scores = [score]

                    arm_segment(seg_start_s)
                    feed_window(window)

                elif "end" in event and in_speech:
                    speech_buf.append(window)
                    vad_scores.append(score)
                    feed_window(window)
                    close_segment(offset_s, "VAD end")
                    speech_buf = []
                    vad_scores = []
                    transcriber.start()

            elif in_speech:
                speech_buf.append(window)
                vad_scores.append(score)
                feed_window(window)

                buf_samples = len(speech_buf) * VAD_WINDOW_SAMPLES

                if buf_samples >= MAX_SEGMENT_SAMPLES:
                    do_cut(len(speech_buf), offset_s, "hard cap")

                elif buf_samples >= SOFT_CUT_SAMPLES:
                    scan_start = max(0, len(vad_scores) - SOFT_CUT_LOOKAHEAD)
                    recent_scores = vad_scores[scan_start:]

                    if recent_scores:
                        local_min_pos = int(torch.tensor(recent_scores).argmin().item())
                        min_win = scan_start + local_min_pos
                        min_score = vad_scores[min_win]

                        if min_score < score - SOFT_CUT_MIN_DIP:
                            do_cut(min_win + 1, offset_s, "soft cut")

    # EOF
    if in_speech and speech_buf:
        close_segment(offset_s, "EOF")
    else:
        transcriber.stop()

    tprint(f"\n{tag}[Done] {seg_index} segment(s)\n")


# ── Entry point ────────────────────────────────────────────────────────────────


def main():
    global MAX_SEGMENT_S, MAX_SEGMENT_SAMPLES, SOFT_CUT_S, SOFT_CUT_SAMPLES
    global SOFT_CUT_LOOKAHEAD, STT_FEED_WINDOWS

    parser = argparse.ArgumentParser(
        description="Dual-channel Moonshine STT with streaming Silero VAD"
    )
    # Input modes (mutually exclusive group)
    input_group = parser.add_mutually_exclusive_group()
    input_group.add_argument(
        "stereo_wav",
        nargs="?",
        help="Stereo WAV: left→CH0, right→CH1.  Also accepts a mono WAV "
        "(both channels run the same audio — useful for benchmarking).",
    )
    input_group.add_argument(
        "--ch0", metavar="WAV", help="Explicit mono WAV for channel 0 (use together with --ch1)"
    )
    parser.add_argument(
        "--ch1", metavar="WAV", help="Explicit mono WAV for channel 1 (use together with --ch0)"
    )
    parser.add_argument("--language", default=STT_LANGUAGE)
    parser.add_argument("--model-arch", type=int, default=STT_MODEL_ARCH)
    parser.add_argument("--vad-threshold", type=float, default=VAD_THRESHOLD)
    parser.add_argument("--min-silence-ms", type=int, default=VAD_MIN_SILENCE_MS)
    parser.add_argument("--max-segment-s", type=float, default=MAX_SEGMENT_S)
    parser.add_argument("--soft-cut-s", type=float, default=SOFT_CUT_S)
    parser.add_argument("--soft-cut-lookahead", type=float, default=SOFT_CUT_LOOKAHEAD_S)
    parser.add_argument("--feed-windows", type=int, default=STT_FEED_WINDOWS)
    parser.add_argument("--save-vad-segments", metavar="DIR", default="")
    args = parser.parse_args()

    # Validate --ch0 / --ch1 usage
    if bool(args.ch0) != bool(args.ch1):
        parser.error("--ch0 and --ch1 must be used together")

    MAX_SEGMENT_S = args.max_segment_s
    MAX_SEGMENT_SAMPLES = int(MAX_SEGMENT_S * SILERO_SAMPLE_RATE)
    SOFT_CUT_S = args.soft_cut_s
    SOFT_CUT_SAMPLES = int(SOFT_CUT_S * SILERO_SAMPLE_RATE)
    SOFT_CUT_LOOKAHEAD = int(args.soft_cut_lookahead * SILERO_SAMPLE_RATE / VAD_WINDOW_SAMPLES)
    STT_FEED_WINDOWS = args.feed_windows

    # ── Resolve input streams ──────────────────────────────────────────────────

    if args.ch0 and args.ch1:
        # Two explicit mono files
        path_ch0, path_ch1 = args.ch0, args.ch1
        for p in (path_ch0, path_ch1):
            if not os.path.exists(p):
                print(f"[!] File not found: {p}", file=sys.stderr)
                sys.exit(1)
        stream_ch0 = packet_stream_mono(path_ch0)
        stream_ch1 = packet_stream_mono(path_ch1)
        stem_ch0 = os.path.splitext(os.path.basename(path_ch0))[0]
        stem_ch1 = os.path.splitext(os.path.basename(path_ch1))[0]
        source_label = f"{stem_ch0} / {stem_ch1}"
    else:
        # Single stereo (or mono) WAV
        wav_path = args.stereo_wav or DEFAULT_AUDIO
        if not os.path.exists(wav_path):
            print(f"[!] File not found: {wav_path}", file=sys.stderr)
            sys.exit(1)
        stream_ch0 = packet_stream_stereo_channel(wav_path, channel=0)
        stream_ch1 = packet_stream_stereo_channel(wav_path, channel=1)
        stem = os.path.splitext(os.path.basename(wav_path))[0]
        stem_ch0 = stem_ch1 = stem
        source_label = wav_path

    # ── Load models ────────────────────────────────────────────────────────────

    # One shared VAD model — stateless network, so safe to share.
    # vad_lock serialises the hot path: both the score call and VADIterator's
    # internal call are covered in a single critical section per window.
    vad_model = load_silero()
    vad_lock = threading.Lock()

    model_path, model_arch = get_model_for_language(args.language, args.model_arch)
    print(f"[STT] Model : {model_path}  arch={model_arch}")
    print(f"[STT] Source: {source_label}\n")
    print("=" * 72)

    # Two independent Transcriber instances — same weights, separate sessions.
    transcriber_ch0 = Transcriber(
        model_path=model_path,
        model_arch=model_arch,
        options=STT_OPTIONS,
    )
    transcriber_ch1 = Transcriber(
        model_path=model_path,
        model_arch=model_arch,
        options=STT_OPTIONS,
    )

    # ── Launch threads ─────────────────────────────────────────────────────────

    common_kwargs = dict(
        vad_model=vad_model,
        vad_lock=vad_lock,
        vad_threshold=args.vad_threshold,
        min_silence_ms=args.min_silence_ms,
        save_vad_dir=args.save_vad_segments,
    )

    t_ch0 = threading.Thread(
        target=process_channel,
        name="CH0",
        kwargs=dict(
            stream=stream_ch0,
            transcriber=transcriber_ch0,
            ch_label="CH0",
            source_stem=stem_ch0,
            **common_kwargs,
        ),
    )
    t_ch1 = threading.Thread(
        target=process_channel,
        name="CH1",
        kwargs=dict(
            stream=stream_ch1,
            transcriber=transcriber_ch1,
            ch_label="CH1",
            source_stem=stem_ch1,
            **common_kwargs,
        ),
    )

    t_ch0.start()
    t_ch1.start()
    t_ch0.join()
    t_ch1.join()

    print("=" * 72)
    print("[Done] Both channels finished.")


if __name__ == "__main__":
    main()
