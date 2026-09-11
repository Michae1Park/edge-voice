#!/usr/bin/env python3
"""Same cell as ss_len10_max (97% occupancy, 2.0x rate, 10s crop) but with a
few DIFFERENT source recordings, to test how often the repetitive-decode
collapse (found in p2_b_max, absent in ss_len10_max) actually triggers --
i.e. is it specific to 10s.wav's content, or a property of the 97%/2.0x
operating point itself that most content will eventually hit.

Reuses the exact same crop/TSM/tile pipeline as
build_boundary_singlesource_audio.py, just parameterized by source file.

Output: characterization/testaudio/boundary_content_<name>_len10_max.wav
plus characterization/testaudio/metadata_content_check.json.

Usage:
    python characterization/scripts/build_content_check_audio.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import soundfile as sf

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_test_audio import DEFAULT_OUT_DIR, TARGET_TOTAL_S, _repeats_for_target, _tile_with_symmetric_gaps  # noqa: E402
from build_speed_audio import time_scale_modify  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent.parent

SOURCES = [
    ("conv004", ROOT / "characterization" / "data" / "conv_004.wav"),
    ("convo60s", ROOT / "wav" / "conversation_60s.wav"),
    ("ttstx", ROOT / "wav" / "tts_tx.wav"),
]
LENGTH_S = 10
DENSITY = 0.97
RATE = 2.00


def build_cell(audio: np.ndarray, sr: int) -> tuple[np.ndarray, dict]:
    n_samples = int(round(LENGTH_S * sr))
    clip = audio[:n_samples]
    stretched = time_scale_modify(clip, sr, RATE)
    atomic_dur_s = len(stretched) / sr
    gap_s = atomic_dur_s * (1 - DENSITY) / DENSITY
    n = _repeats_for_target(atomic_dur_s, gap_s, TARGET_TOTAL_S)
    full = _tile_with_symmetric_gaps(stretched, gap_s, n, sr)
    total_dur_s = len(full) / sr
    occupancy_actual = (n * atomic_dur_s) / total_dur_s
    meta = {
        "cropped_length_s": LENGTH_S,
        "rate": RATE,
        "atomic_segment_duration_s": round(atomic_dur_s, 3),
        "occupancy_target": DENSITY,
        "gap_s": round(gap_s, 3),
        "n_repeats": n,
        "total_duration_s": round(total_dur_s, 3),
        "occupancy_actual": round(occupancy_actual, 4),
    }
    return full, meta


def main() -> None:
    out_dir = DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    all_meta = {}
    for name, path in SOURCES:
        audio, sr = sf.read(path, dtype="float32")
        if audio.ndim != 1:
            audio = audio.mean(axis=1)
        label = f"content_{name}_len10_max"
        full, meta = build_cell(audio, sr)
        filename = f"boundary_{label}.wav"
        out_path = out_dir / filename
        sf.write(out_path, full, sr, subtype="PCM_16")
        meta["label"] = label
        meta["source_file"] = str(path.relative_to(ROOT))
        meta["filename"] = filename
        all_meta[label] = meta
        print(
            f"  {label:28s}: atomic={meta['atomic_segment_duration_s']}s gap={meta['gap_s']}s "
            f"N={meta['n_repeats']} total={meta['total_duration_s']}s "
            f"occupancy={meta['occupancy_actual']:.1%} -> {out_path}"
        )

    metadata_path = out_dir / "metadata_content_check.json"
    metadata_path.write_text(json.dumps(all_meta, ensure_ascii=False, indent=2))
    print(f"\nWrote {metadata_path}")


if __name__ == "__main__":
    main()
