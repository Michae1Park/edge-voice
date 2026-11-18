from __future__ import annotations

# sherpa-onnx calls exit() when RKNN init fails, so the first NPU load runs here to turn that into an exit code.
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
