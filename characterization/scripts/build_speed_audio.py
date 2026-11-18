#!/usr/bin/env python3
# Builds the five speaking-rate test files used by run_experiment.py.

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import soundfile as sf
import torch
import torchaudio

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_test_audio import (  # noqa: E402
    DEFAULT_OUT_DIR,
    _load_base_utterance,
    build_condition,
    build_warmup,
)

OCCUPANCY_TARGET = 0.60
SPEEDS = [0.5, 1.0, 1.5, 2.0, 3.0]

N_FFT = 512
HOP_LENGTH = N_FFT // 4


def time_scale_modify(audio: np.ndarray, sr: int, rate: float) -> np.ndarray:
    if rate == 1.0:
        return audio.copy()
    wav = torch.from_numpy(audio).unsqueeze(0)
    spec = torchaudio.transforms.Spectrogram(n_fft=N_FFT, hop_length=HOP_LENGTH, power=None)(wav)
    stretch = torchaudio.transforms.TimeStretch(hop_length=HOP_LENGTH, n_freq=N_FFT // 2 + 1)
    stretched = stretch(spec, rate)
    inv = torchaudio.transforms.InverseSpectrogram(n_fft=N_FFT, hop_length=HOP_LENGTH)(stretched)
    return inv.squeeze(0).numpy().astype(np.float32)


def _speed_label(speed: float) -> str:
    return f"{speed:g}x"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    base_audio, sr, base_text = _load_base_utterance()
    print(f'Base utterance: 3s.wav ({len(base_audio) / sr:.3f}s) -- "{base_text}"')

    warmup_path = args.out_dir / "warmup.wav"
    if not warmup_path.exists():
        warmup_path = build_warmup(base_audio, sr, args.out_dir)
        print(f"  warmup: built -> {warmup_path}")
    else:
        print(f"  warmup: already exists, reusing -> {warmup_path}")

    conditions = []
    for speed in SPEEDS:
        label = _speed_label(speed)
        stretched = time_scale_modify(base_audio, sr, speed)
        stretched_dur_s = len(stretched) / sr
        gap_s = stretched_dur_s * (1 - OCCUPANCY_TARGET) / OCCUPANCY_TARGET

        cond = build_condition(
            label,
            f"{label}_speed.wav",
            gap_s,
            OCCUPANCY_TARGET,
            stretched,
            sr,
            base_text,
            extra={
                "speed": speed,
                "original_utterance_duration_s": round(len(base_audio) / sr, 3),
            },
        )
        out_path = args.out_dir / cond["filename"]
        sf.write(out_path, cond.pop("_audio"), sr, subtype="PCM_16")
        conditions.append(cond)
        print(
            f"  {label:>5}: utterance {stretched_dur_s:.3f}s (from {len(base_audio) / sr:.3f}s)  "
            f"gap={gap_s:.3f}s  N={cond['repeats']}  total={cond['total_duration_s']:.2f}s  "
            f"occupancy actual={cond['occupancy_actual']:.1%}  -> {out_path}"
        )

    metadata_path = args.out_dir / "metadata_speed.json"
    metadata_path.write_text(json.dumps(conditions, ensure_ascii=False, indent=2))
    print(f"\nWrote {metadata_path}")


if __name__ == "__main__":
    main()
