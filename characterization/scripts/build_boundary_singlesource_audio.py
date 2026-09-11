#!/usr/bin/env python3
"""Length sweep using ONE base recording, to isolate length from content.

The two prior boundary experiments used a different real recording per
length value (6s/7s/9s/10s.wav) -- necessary because no zero-gap-splice
method survived contact with VAD (see build_boundary_audio.py's docstring),
but it meant "length" and "which sentence" were confounded. That confound
turned out to matter: 9s.wav failed at density/rate combos where 10s.wav
was clean, and a control run showed 9s.wav itself isn't intrinsically
VAD-hostile (clean at mild settings) -- so something about EACH recording's
own content interacts differently with stress, on top of (or instead of)
pure duration.

This script isolates length from content directly: testsegments/17s.wav
(one recording, confirmed via an offline Silero VAD pass to be a single
continuous utterance with no internal pause the model itself detects -- see
conversation) is truncated to a fixed set of shorter durations, all sharing
the SAME leading content. length=9 is the first 9.0s of the 17.152s
recording, length=15 is the first 15.0s, etc. -- every length variant is a
strict prefix of every longer one.

Caveat, stated rather than hidden: a fixed-time cut lands mid-word/phoneme
for most target lengths (only ones landing near one of the recording's own
natural micro-pauses, e.g. ~9.2s or ~13.1s, are close to a clean edge). That
trades the (larger, already-demonstrated) different-recording confound for
a smaller, CONSISTENT one -- an abrupt clipped ending -- present at every
length rather than varying unpredictably by which sentence was used.

Two density/rate combos per length, matching the two most informative
points from the fine 10s.wav grid (build_boundary_fine_audio.py): the exact
corner that failed there (97%/2.0x) and its nearest clean neighbor
(95%/1.75x). Gap safety checked below at build time.

Output: characterization/testaudio/boundary_ss_<label>.wav plus
characterization/testaudio/metadata_boundary_singlesource.json.

Usage:
    python characterization/scripts/build_boundary_singlesource_audio.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import soundfile as sf

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_test_audio import (  # noqa: E402
    DEFAULT_OUT_DIR,
    TARGET_TOTAL_S,
    TESTDATA_DIR,
    _repeats_for_target,
    _tile_with_symmetric_gaps,
)
from build_speed_audio import time_scale_modify  # noqa: E402

SOURCE_FILE = "17s.wav"
LENGTHS_S = [9, 10, 11, 12, 13, 14, 15]
# (label_suffix, density, rate)
COMBOS = [
    ("max", 0.97, 2.00),
    ("near", 0.95, 1.75),
]


def _load_source() -> tuple[np.ndarray, int]:
    audio, sr = sf.read(TESTDATA_DIR / SOURCE_FILE, dtype="float32")
    if audio.ndim != 1:
        raise ValueError(f"{SOURCE_FILE} must be mono, got shape {audio.shape}")
    return audio, sr


def build_cell(audio: np.ndarray, sr: int, length_s: int, rate: float, occupancy: float) -> tuple[np.ndarray, dict]:
    n_samples = int(round(length_s * sr))
    if n_samples > len(audio):
        raise ValueError(f"requested length {length_s}s exceeds source duration {len(audio)/sr:.3f}s")
    clip = audio[:n_samples]

    stretched = time_scale_modify(clip, sr, rate)
    atomic_dur_s = len(stretched) / sr

    gap_s = atomic_dur_s * (1 - occupancy) / occupancy
    n = _repeats_for_target(atomic_dur_s, gap_s, TARGET_TOTAL_S)
    full = _tile_with_symmetric_gaps(stretched, gap_s, n, sr)
    total_dur_s = len(full) / sr
    occupancy_actual = (n * atomic_dur_s) / total_dur_s

    meta = {
        "source_file": SOURCE_FILE,
        "cropped_length_s": length_s,
        "rate": rate,
        "atomic_segment_duration_s": round(atomic_dur_s, 3),
        "occupancy_target": occupancy,
        "gap_s": round(gap_s, 3),
        "n_repeats": n,
        "total_duration_s": round(total_dur_s, 3),
        "occupancy_actual": round(occupancy_actual, 4),
    }
    return full, meta


def main() -> None:
    out_dir = DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    audio, sr = _load_source()
    print(f"Source: {SOURCE_FILE} ({len(audio)/sr:.3f}s)")

    all_meta = {}
    for length_s in LENGTHS_S:
        for suffix, density, rate in COMBOS:
            label = f"ss_len{length_s}_{suffix}"
            full, meta = build_cell(audio, sr, length_s, rate, density)
            filename = f"boundary_{label}.wav"
            out_path = out_dir / filename
            sf.write(out_path, full, sr, subtype="PCM_16")
            meta["label"] = label
            meta["filename"] = filename
            all_meta[label] = meta
            print(
                f"  {label:16s}: crop={length_s}s @ {rate}x -> atomic={meta['atomic_segment_duration_s']}s  "
                f"gap={meta['gap_s']}s  N={meta['n_repeats']}  total={meta['total_duration_s']}s  "
                f"occupancy target={density:.0%} actual={meta['occupancy_actual']:.1%}  -> {out_path}"
            )

    metadata_path = out_dir / "metadata_boundary_singlesource.json"
    metadata_path.write_text(json.dumps(all_meta, ensure_ascii=False, indent=2))
    print(f"\nWrote {metadata_path}")


if __name__ == "__main__":
    main()
