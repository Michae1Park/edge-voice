#!/usr/bin/env python3
"""Builds test files for the systematic boundary-search experiment.

Three independent variables:

  - Speaking rate: pitch-preserving TSM on the base utterance (same method
    as build_speed_audio.py). Capped at 2x in this experiment -- TSM's own
    audio quality degrades past that (confirmed in the speaking-rate
    experiment), which would confound "system can't keep up" with "the
    audio itself became unintelligible."

  - Segment length: real recordings at each target length
    (testsegments/{6s,7s,10s}.wav), TSM-adjusted for the cell's rate. An
    earlier version of this script tried to control length synthetically
    -- repeating a short clip M times with ZERO gap so VAD would perceive
    one continuous segment -- but that failed empirically: VAD split the
    zero-gap splice right back into separate segments every time (found
    via captured segment counts being exactly 2x expected, then confirmed
    by checking captured duration_s ~= the original short clip's length,
    not the intended longer one). It also produced 100% repetitive/garbage
    decodes at 1.5x rate, likely the splice landing somewhere acoustically
    incoherent once time-stretched. Reverted to real recordings, which are
    already proven to work (used throughout the density/rate/length and
    combo experiments) -- this reintroduces a content confound (a 10s
    recording is a different sentence than the 6s one, not the same
    sentence stretched), which should be stated explicitly as a limitation
    rather than papered over with a synthetic method that turned out to be
    broken.

  - Density (occupancy): the segment is tiled with silence gaps (same
    symmetric N+trailing-gap pattern used throughout this project) to hit
    a target occupancy. Gaps are kept at >=4x vad.min_silence_duration_ms=
    50ms (the fix validated in the sustained-load investigation, itself
    confirmed safe at ~3.9x margin) -- a failure at a gap close to that
    floor would just be re-discovering that already-fixed VAD-threshold
    bug, not a new finding. 99% occupancy was dropped to 97% for this
    reason: at 99%, the shorter segments produce gaps as tight as 1.4x the
    floor (checked before running, not after) -- 97% clears >=4.2x
    everywhere this design uses it.

Output is WAV files only (no metadata.json consumed downstream -- these
feed scratch/bench_pipeline_load.py's sustained/looped playback directly,
not run_experiment.py's one-shot CER-oriented harness). A companion
metadata_boundary.json records exact achieved parameters per cell for
reference/reporting.

Usage:
    python characterization/scripts/build_boundary_audio.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import soundfile as sf

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_test_audio import (  # noqa: E402
    DEFAULT_OUT_DIR,
    TARGET_TOTAL_S,
    _load_base_utterance,
    _repeats_for_target,
    _tile_with_symmetric_gaps,
    build_warmup,
)
from build_speed_audio import time_scale_modify  # noqa: E402

# (label, density, rate, source_file) -- see conversation for the
# phase 1 (single-axis escalation from baseline) / phase 2 (combined
# extremes) design. Length varies via real recordings (6s/7s/10s.wav),
# not synthetically -- see module docstring for why.
CELLS = [
    ("p1_baseline", 0.85, 1.0, "6s.wav"),
    ("p1_density_95", 0.95, 1.0, "6s.wav"),
    ("p1_density_97", 0.97, 1.0, "6s.wav"),
    ("p1_rate_1.5", 0.85, 1.5, "6s.wav"),
    ("p1_rate_2.0", 0.85, 2.0, "6s.wav"),
    ("p1_length_7", 0.85, 1.0, "7s.wav"),
    ("p1_length_10", 0.85, 1.0, "10s.wav"),
    ("p2_a", 0.95, 2.0, "7s.wav"),
    ("p2_b_max", 0.97, 2.0, "10s.wav"),
    ("p2_c", 0.97, 1.0, "10s.wav"),
    ("p2_d", 0.85, 2.0, "10s.wav"),
]


def build_cell(sr: int, source_file: str, rate: float, occupancy: float) -> tuple[np.ndarray, dict]:
    audio, file_sr, text = _load_base_utterance(source_file)
    assert file_sr == sr
    stretched = time_scale_modify(audio, sr, rate)
    atomic_dur_s = len(stretched) / sr

    gap_s = atomic_dur_s * (1 - occupancy) / occupancy
    n = _repeats_for_target(atomic_dur_s, gap_s, TARGET_TOTAL_S)
    full = _tile_with_symmetric_gaps(stretched, gap_s, n, sr)
    total_dur_s = len(full) / sr
    occupancy_actual = (n * atomic_dur_s) / total_dur_s

    meta = {
        "source_file": source_file,
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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    warmup_base_audio, sr, _ = _load_base_utterance("3s.wav")
    warmup_path = args.out_dir / "warmup.wav"
    if not warmup_path.exists():
        warmup_path = build_warmup(warmup_base_audio, sr, args.out_dir)
        print(f"  warmup: built -> {warmup_path}")
    else:
        print(f"  warmup: already exists, reusing -> {warmup_path}")

    all_meta = {}
    for label, density, rate, source_file in CELLS:
        full, meta = build_cell(sr, source_file, rate, density)
        filename = f"boundary_{label}.wav"
        out_path = args.out_dir / filename
        sf.write(out_path, full, sr, subtype="PCM_16")
        meta["label"] = label
        meta["filename"] = filename
        all_meta[label] = meta
        print(
            f"  {label:15s}: {source_file} @ {rate}x -> atomic={meta['atomic_segment_duration_s']}s  "
            f"gap={meta['gap_s']}s  N={meta['n_repeats']}  "
            f"total={meta['total_duration_s']}s  occupancy target={density:.0%} actual={meta['occupancy_actual']:.1%}  "
            f"-> {out_path}"
        )

    metadata_path = args.out_dir / "metadata_boundary.json"
    metadata_path.write_text(json.dumps(all_meta, ensure_ascii=False, indent=2))
    print(f"\nWrote {metadata_path}")


if __name__ == "__main__":
    main()
