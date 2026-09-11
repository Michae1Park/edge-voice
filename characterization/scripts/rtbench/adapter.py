"""ASR adapters: transcribe(audio_path) -> str.

The benchmark engine (engine.py) never assumes how the ASR is invoked -- it
only ever calls `adapter(audio_path)`. This module supplies:

  - MoonshineAdapter: wraps this project's real STT model (moonshine_voice),
    loaded ONCE at construction (matching STTWorker's "resolved eagerly, off
    the real-time path" design in edge_voice/stt/stt_worker.py) -- never
    reloaded per request.
  - DummyAdapter: a synthetic stand-in with a configurable service time, for
    exercising/validating the engine itself without a model or hardware.

Anything else that exposes .transcribe(path) -> str (or a plain function
with that signature) plugs into the same engine unchanged.
"""

from __future__ import annotations

import os
import random
import time
from pathlib import Path
from typing import Protocol

# Must be set before moonshine_voice/onnxruntime is imported -- see
# characterization/scripts/run_experiment.py's docstring for why: without
# it, onnxruntime spreads a single decode's intra-op work across every core,
# which is not how this is deployed (configs/default.yaml + cli.py always
# set this) and would misrepresent single-worker capacity.
os.environ.setdefault("MOONSHINE_ORT_SINGLE_THREAD", "1")

import numpy as np
import soundfile as sf


class AsrAdapter(Protocol):
    def transcribe(self, audio_path: str) -> str: ...


class DummyAdapter:
    """Synthetic ASR: sleeps for a configurable service time per request.

    For validating the producer/consumer engine's timing/queueing behavior
    in isolation from any real model -- no hardware or downloaded weights
    required. service_s can be a fixed float or (mean_s, jitter_s) for a
    uniform-jitter service time, so the engine's queueing math can be
    exercised under variable service times too.
    """

    def __init__(self, service_s: float = 1.0, jitter_s: float = 0.0, rtf: float | None = None) -> None:
        self.service_s = service_s
        self.jitter_s = jitter_s
        self.rtf = rtf  # if set, overrides service_s as rtf * audio_duration_s

    def transcribe(self, audio_path: str) -> str:
        if self.rtf is not None:
            duration_s = sf.info(audio_path).duration
            service_s = self.rtf * duration_s
        else:
            service_s = self.service_s
        if self.jitter_s:
            service_s += random.uniform(-self.jitter_s, self.jitter_s)
        time.sleep(max(0.0, service_s))
        return "[dummy transcript]"


class MoonshineAdapter:
    """Real moonshine_voice Transcriber, loaded once, called per audio file.

    Mirrors STTWorker._transcribe (edge_voice/stt/stt_worker.py) exactly --
    same start()/add_audio()/stop() bracketing (stop() resets decoder state
    between calls, verified there byte-for-byte against a fresh instance) and
    the same repetitive-output guard -- but reads a whole WAV file per call
    instead of a VAD-delivered in-memory segment, since this benchmark treats
    the ASR as "complete audio file in, transcription out" with no VAD/queue
    machinery of its own.

    language/model_arch/options default to configs/default.yaml's stt: block
    (via Settings.load()) so benchmark numbers reflect the actually-deployed
    model, not an arbitrary substitute.
    """

    # Below this unique/total token ratio, a completed line is treated as the
    # model looping on itself -- same default and logic as STTWorkerConfig
    # (edge_voice/stt/stt_worker.py); reimplemented locally so this adapter
    # has no dependency on that module's private helpers.
    REPETITIVE_RATIO = 0.45

    def __init__(
        self,
        language: str | None = None,
        model_arch: str | None = None,
        options: dict[str, str] | None = None,
        sample_rate: int = 16000,
    ) -> None:
        from moonshine_voice import Transcriber, get_model_for_language, string_to_model_arch

        if language is None or model_arch is None or options is None:
            from edge_voice.config.settings import Settings

            settings = Settings.load()
            language = language or settings.stt.language
            model_arch = model_arch or settings.stt.model_arch
            if options is None:
                options = {
                    "max_tokens_per_second": str(settings.stt.max_tokens_per_second),
                    "vad_threshold": str(settings.stt.vad_threshold),
                    "identify_speakers": str(settings.stt.identify_speakers).lower(),
                    "log_api_calls": str(settings.stt.log_api_calls).lower(),
                    "save_input_wav_path": settings.stt.save_input_wav_path,
                    "return_audio_data": str(settings.stt.return_audio_data).lower(),
                }

        self.language = language
        self.model_arch = model_arch
        self.sample_rate = sample_rate

        model_path, model_arch_enum = get_model_for_language(language, string_to_model_arch(model_arch))
        self._transcriber = Transcriber(model_path=model_path, model_arch=model_arch_enum, options=options)
        self._collector_cls = self._build_collector_cls()

    def _build_collector_cls(self):
        try:
            from moonshine_voice import TranscriptEventListener as _Base
        except ImportError:
            _Base = object

        ratio = self.REPETITIVE_RATIO

        class _Collector(_Base):  # type: ignore[misc, valid-type]
            def __init__(self) -> None:
                self.lines: list[str] = []
                self.best_partial = ""

            def on_line_text_changed(self, event) -> None:
                text = event.line.text
                if not _is_repetitive(text, ratio):
                    self.best_partial = text

            def on_line_completed(self, event) -> None:
                text = event.line.text
                if _is_repetitive(text, ratio):
                    text = self.best_partial
                if text:
                    self.lines.append(text)

            def text(self) -> str:
                return " ".join(self.lines).strip()

        return _Collector

    def transcribe(self, audio_path: str) -> str:
        audio, sr = sf.read(audio_path, dtype="float32")
        if audio.ndim != 1:
            audio = audio.mean(axis=1)
        samples = audio.tolist()

        collector = self._collector_cls()
        self._transcriber.remove_all_listeners()
        self._transcriber.add_listener(collector)

        self._transcriber.start()
        try:
            self._transcriber.add_audio(samples, sr)
        finally:
            self._transcriber.stop()

        return str(collector.text())


def _is_repetitive(text: str, threshold: float) -> bool:
    tokens = text.split()
    if len(tokens) < 4:
        return False
    return (len(set(tokens)) / len(tokens)) < threshold


def build_adapter(kind: str, **kwargs) -> AsrAdapter:
    if kind == "moonshine":
        return MoonshineAdapter(**kwargs)
    if kind == "dummy":
        return DummyAdapter(**kwargs)
    raise ValueError(f"Unknown adapter kind {kind!r} (expected 'moonshine' or 'dummy')")
