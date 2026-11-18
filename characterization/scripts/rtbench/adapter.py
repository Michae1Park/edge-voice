from __future__ import annotations

import os
import random
import time
from typing import Protocol

os.environ.setdefault("MOONSHINE_ORT_SINGLE_THREAD", "1")

import soundfile as sf


class AsrAdapter(Protocol):
    def transcribe(self, audio_path: str) -> str: ...


class DummyAdapter:

    def __init__(
        self, service_s: float = 1.0, jitter_s: float = 0.0, rtf: float | None = None
    ) -> None:
        self.service_s = service_s
        self.jitter_s = jitter_s
        self.rtf = rtf

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
            model_arch = model_arch or settings.stt.moonshine.model_arch
            if options is None:
                options = {
                    "max_tokens_per_second": str(settings.stt.moonshine.max_tokens_per_second),
                    "vad_threshold": str(settings.stt.moonshine.vad_threshold),
                    "identify_speakers": str(settings.stt.moonshine.identify_speakers).lower(),
                    "log_api_calls": str(settings.stt.moonshine.log_api_calls).lower(),
                    "save_input_wav_path": settings.stt.moonshine.save_input_wav_path,
                    "return_audio_data": str(settings.stt.moonshine.return_audio_data).lower(),
                }

        self.language = language
        self.model_arch = model_arch
        self.sample_rate = sample_rate

        model_path, model_arch_enum = get_model_for_language(
            language, string_to_model_arch(model_arch)
        )
        self._transcriber = Transcriber(
            model_path=model_path, model_arch=model_arch_enum, options=options
        )
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
