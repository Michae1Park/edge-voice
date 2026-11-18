#!/usr/bin/env python3
# Dense follow-up grid around the boundary found in build_boundary_audio.py.
# Usage: python characterization/scripts/build_boundary_fine_audio.py

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_boundary_audio import build_cell  # noqa: E402
from build_test_audio import DEFAULT_OUT_DIR, _load_base_utterance  # noqa: E402
import soundfile as sf  # noqa: E402

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
