#!/usr/bin/env python3
"""Dense follow-up grid around the boundary found in build_boundary_audio.py.

That first search found the failure is a genuine three-way interaction, not
a single-axis threshold: every pairwise extreme (97% density + 2x rate at
7s; 97% density + 1x rate at 10s; 85% density + 2x rate at 10s) passed
cleanly, and only the full triple (97% density + 2x rate + 10s length)
failed. So length is fixed at 10s.wav here -- the other two lengths never
showed any failure potential even paired with the other two extremes -- and
this grid instead resolves the density x rate surface at fixed length,
sweeping both axes near the known PASS/FAIL corner:

  - Fixed density=97% (max), rate swept 1.25/1.5/1.75 between the known
    PASS (1.0x, p2_c) and known FAIL (2.0x, p2_b_max) anchors.
  - Fixed rate=2.0x (max), density swept 90/93/95/96 between the known
    PASS (85%, p2_d) and known FAIL (97%, p2_b_max) anchors.
  - Three interior points (95%/1.5x, 95%/1.75x, 93%/1.75x) to check the
    failure region is a smooth corner and not an off-diagonal cliff the
    two pure sweeps above would miss.

Gap safety: even the tightest cell here (97% density, rate=2.0, already
built by build_boundary_audio.py) has gap_s=0.156s, 3.1x the
vad.min_silence_duration_ms=50ms floor -- every cell in this file is looser
than that on at least one axis, so all clear the same margin.

Output: characterization/testaudio/boundary_fine_<label>.wav plus
characterization/testaudio/metadata_boundary_fine.json (same schema as
metadata_boundary.json, kept separate so the original 11-cell run's
metadata is untouched).

Usage:
    python characterization/scripts/build_boundary_fine_audio.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_boundary_audio import build_cell  # noqa: E402
from build_test_audio import DEFAULT_OUT_DIR, _load_base_utterance  # noqa: E402
import soundfile as sf  # noqa: E402

# (label, density, rate) -- source_file fixed at 10s.wav throughout, see
# module docstring on why.
FINE_CELLS = [
    ("fine_r125", 0.97, 1.25, "10s.wav"),
    ("fine_r150", 0.97, 1.50, "10s.wav"),
    ("fine_r175", 0.97, 1.75, "10s.wav"),
    ("fine_d90", 0.90, 2.00, "10s.wav"),
    ("fine_d93", 0.93, 2.00, "10s.wav"),
    ("fine_d95", 0.95, 2.00, "10s.wav"),
    ("fine_d96", 0.96, 2.00, "10s.wav"),
    ("fine_int_d95_r150", 0.95, 1.50, "10s.wav"),
    ("fine_int_d95_r175", 0.95, 1.75, "10s.wav"),
    ("fine_int_d93_r175", 0.93, 1.75, "10s.wav"),
    # 9s.wav fills the length gap between the known-safe 6s/7s (passed even
    # at 95-97% density + 2x rate) and the known-failing 10s (p2_b_max) --
    # tells us whether the length threshold is sharp between 9 and 10, or
    # whether 9s already shows the same failure.
    ("fine_len9_max", 0.97, 2.00, "9s.wav"),
    ("fine_len9_r175", 0.97, 1.75, "9s.wav"),
    ("fine_len9_d95", 0.95, 2.00, "9s.wav"),
]


def main() -> None:
    out_dir = DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    all_meta = {}
    for label, density, rate, source_file in FINE_CELLS:
        _, sr, _ = _load_base_utterance(source_file)
        full, meta = build_cell(sr, source_file, rate, density)
        filename = f"boundary_{label}.wav"
        out_path = out_dir / filename
        sf.write(out_path, full, sr, subtype="PCM_16")
        meta["label"] = label
        meta["filename"] = filename
        all_meta[label] = meta
        print(
            f"  {label:20s}: {source_file} @ {rate}x -> atomic={meta['atomic_segment_duration_s']}s  "
            f"gap={meta['gap_s']}s  N={meta['n_repeats']}  "
            f"total={meta['total_duration_s']}s  occupancy target={density:.0%} actual={meta['occupancy_actual']:.1%}  "
            f"-> {out_path}"
        )

    metadata_path = out_dir / "metadata_boundary_fine.json"
    metadata_path.write_text(json.dumps(all_meta, ensure_ascii=False, indent=2))
    print(f"\nWrote {metadata_path}")


if __name__ == "__main__":
    main()
