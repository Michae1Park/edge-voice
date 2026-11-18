#!/usr/bin/env python3
# Builds the three speech-density test files used by run_experiment.py.

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import soundfile as sf

THIS_DIR = Path(__file__).resolve().parent
CHARACTERIZATION_DIR = THIS_DIR.parent
TESTDATA_DIR = CHARACTERIZATION_DIR / "testsegments"
DEFAULT_OUT_DIR = CHARACTERIZATION_DIR / "testaudio"

BASE_UTTERANCE_FILE = "3s.wav"
TARGET_TOTAL_S = 30.0

DENSITY_CONDITIONS = [
    ("extreme", 0.15, 0.95),
    ("high", 0.5, 0.85),
    ("medium", 2.0, 0.60),
    ("low", 7.0, 0.30),
]

WARMUP_TRAILING_SILENCE_S = 2.0


def _load_base_utterance(filename: str = BASE_UTTERANCE_FILE) -> tuple[np.ndarray, int, str]:
    audio, sr = sf.read(TESTDATA_DIR / filename, dtype="float32")
    if audio.ndim != 1:
        raise ValueError(f"{filename} must be mono, got shape {audio.shape}")
    metadata = json.loads((TESTDATA_DIR / "metadata.json").read_text())
    entry = next(m for m in metadata if m["filename"] == filename)
    return audio, sr, entry["transcription"]


def _repeats_for_target(speech_dur_s: float, gap_s: float, target_s: float) -> int:
    raw_n = target_s / (speech_dur_s + gap_s)
    return max(1, round(raw_n))


def _tile_with_symmetric_gaps(
    speech_audio: np.ndarray, gap_s: float, n: int, sr: int
) -> np.ndarray:
    gap_samples = int(round(gap_s * sr))
    silence = np.zeros(gap_samples, dtype=np.float32)
    pieces = [speech_audio]
    for _ in range(n - 1):
        pieces.append(silence)
        pieces.append(speech_audio)
    pieces.append(silence)
    return np.concatenate(pieces)


def build_warmup(base_audio: np.ndarray, sr: int, out_dir: Path) -> Path:
    warmup_silence = np.zeros(int(round(WARMUP_TRAILING_SILENCE_S * sr)), dtype=np.float32)
    warmup_audio = np.concatenate([base_audio, warmup_silence])
    warmup_path = out_dir / "warmup.wav"
    sf.write(warmup_path, warmup_audio, sr, subtype="PCM_16")
    return warmup_path


def build_condition(
    label: str,
    filename: str,
    gap_s: float,
    occupancy_target: float,
    speech_audio: np.ndarray,
    sr: int,
    base_text: str,
    extra: dict | None = None,
) -> dict:
    speech_dur_s = len(speech_audio) / sr
    n = _repeats_for_target(speech_dur_s, gap_s, TARGET_TOTAL_S)
    full_audio = _tile_with_symmetric_gaps(speech_audio, gap_s, n, sr)

    total_duration_s = len(full_audio) / sr
    speech_time_s = n * speech_dur_s
    occupancy_actual = speech_time_s / total_duration_s

    result = {
        "label": label,
        "filename": filename,
        "base_source": f"testsegments/{BASE_UTTERANCE_FILE}",
        "utterance_duration_s": round(speech_dur_s, 3),
        "gap_s": round(gap_s, 3),
        "repeats": n,
        "total_duration_s": round(total_duration_s, 3),
        "speech_time_s": round(speech_time_s, 3),
        "occupancy_target": occupancy_target,
        "occupancy_actual": round(occupancy_actual, 4),
        "reference_transcript_per_segment": [base_text] * n,
        "reference_transcript_concat": " ".join([base_text] * n),
        "sample_rate": sr,
        "_audio": full_audio,
    }
    if extra:
        result.update(extra)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    base_audio, sr, base_text = _load_base_utterance()
    print(f'Base utterance: {BASE_UTTERANCE_FILE} ({len(base_audio) / sr:.3f}s) -- "{base_text}"')

    warmup_path = build_warmup(base_audio, sr, args.out_dir)
    print(f"  warmup: utterance + {WARMUP_TRAILING_SILENCE_S}s trailing silence -> {warmup_path}")

    conditions = []
    for label, gap_s, occupancy_target in DENSITY_CONDITIONS:
        cond = build_condition(
            label, f"{label}_density.wav", gap_s, occupancy_target, base_audio, sr, base_text
        )
        out_path = args.out_dir / cond["filename"]
        sf.write(out_path, cond.pop("_audio"), sr, subtype="PCM_16")
        conditions.append(cond)
        print(
            f"  {label:>6}: gap={gap_s}s  N={cond['repeats']}  "
            f"total={cond['total_duration_s']:.2f}s  "
            f"occupancy target={occupancy_target:.0%} actual={cond['occupancy_actual']:.1%}  "
            f"-> {out_path}"
        )

    metadata_path = args.out_dir / "metadata.json"
    metadata_path.write_text(json.dumps(conditions, ensure_ascii=False, indent=2))
    print(f"\nWrote {metadata_path}")


if __name__ == "__main__":
    main()
