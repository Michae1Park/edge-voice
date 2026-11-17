"""Load the NPU model once in a throwaway process and exit 0 on success.

sherpa-onnx calls exit() instead of raising when RKNN init fails (bad model,
runtime/driver mismatch, NPU memory exhausted), which would take the whole
STT process down with it. Running the first load here turns that into an
exit code the caller can fall back on.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

from edge_voice.stt.sensevoice.config import SenseVoiceConfig
from edge_voice.stt.sensevoice.recognizer import SenseVoiceRecognizer


def main(argv: list[str]) -> int:
    model_dir, config = Path(argv[1]), SenseVoiceConfig(**json.loads(argv[2]))
    recognizer = SenseVoiceRecognizer(model_dir, "npu", config)
    recognizer.transcribe(np.zeros(1600, dtype=np.float32), 16000)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
