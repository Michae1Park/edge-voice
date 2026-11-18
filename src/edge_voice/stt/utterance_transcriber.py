from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import numpy as np


class UtteranceRecognizer(Protocol):
    def transcribe(self, samples: np.ndarray, sample_rate: int) -> str: ...


@dataclass
class _Line:
    text: str


@dataclass
class _LineEvent:
    line: _Line


@dataclass
class _Transcript:
    lines: list[_Line] = field(default_factory=list)


class UtteranceTranscriber:

    def __init__(self, recognizer: UtteranceRecognizer, window_s: float | None = None) -> None:
        self._recognizer = recognizer
        self._window_s = window_s
        self._listeners: list[Any] = []
        self._buffer: list[np.ndarray] = []
        self._sample_rate = 16000

    def remove_all_listeners(self) -> None:
        self._listeners.clear()

    def add_listener(self, listener: Any) -> None:
        self._listeners.append(listener)

    def start(self) -> None:
        self._buffer = []

    def add_audio(self, samples: Any, sample_rate: int) -> None:
        self._buffer.append(np.asarray(samples, dtype=np.float32))
        self._sample_rate = sample_rate

    def stop(self) -> None:
        audio = np.concatenate(self._buffer) if self._buffer else np.zeros(0, np.float32)
        self._buffer = []
        text = self._recognize(audio, self._sample_rate)
        if not text:
            return
        event = _LineEvent(_Line(text))
        for listener in list(self._listeners):
            listener.on_line_completed(event)

    def transcribe_without_streaming(self, samples: Any, sample_rate: int) -> _Transcript:
        text = self._recognize(np.asarray(samples, dtype=np.float32), sample_rate)
        return _Transcript([_Line(text)] if text else [])

    def _recognize(self, audio: np.ndarray, sample_rate: int) -> str:
        if audio.size == 0:
            return ""
        window = int(self._window_s * sample_rate) if self._window_s else audio.size
        texts = (
            self._recognizer.transcribe(audio[i : i + window], sample_rate).strip()
            for i in range(0, audio.size, max(1, window))
        )
        return " ".join(t for t in texts if t)
