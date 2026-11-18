#!/usr/bin/env python3
# Builds the five "limit testing" (한계치 측정) combo files -- the most demanding value from each of the
# three prior experiments (density, speaking rate, segment length), combined into single test ...

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import soundfile as sf

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_test_audio import (  # noqa: E402
    DEFAULT_OUT_DIR,
    _load_base_utterance,
    build_condition,
    build_warmup,
)
from build_speed_audio import time_scale_modify  # noqa: E402

COMBOS = [
    ("combo1", "6s.wav", 1.0, 0.85),
    ("combo2", "7s.wav", 1.0, 0.85),
    ("combo3", "6s.wav", 1.0, 0.95),
    ("combo4", "7s.wav", 1.0, 0.95),
    ("combo5", "7s.wav", 2.0, 0.95),
    ("combo6", "10s.wav", 1.0, 0.85),
    ("combo7", "10s.wav", 2.0, 0.85),
    ("combo8", "10s.wav", 1.0, 0.95),
    ("combo9", "10s.wav", 2.0, 0.95),
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    warmup_path = args.out_dir / "warmup.wav"
    if not warmup_path.exists():
        w_audio, w_sr, _ = _load_base_utterance("3s.wav")
        warmup_path = build_warmup(w_audio, w_sr, args.out_dir)
        print(f"  warmup: built -> {warmup_path}")
    else:
        print(f"  warmup: already exists, reusing -> {warmup_path}")

    conditions = []
    for label, source_file, speed, occupancy in COMBOS:
        audio, sr, text = _load_base_utterance(source_file)
        stretched = time_scale_modify(audio, sr, speed)
        stretched_dur_s = len(stretched) / sr
        gap_s = stretched_dur_s * (1 - occupancy) / occupancy

        cond = build_condition(
            label,
            f"{label}.wav",
            gap_s,
            occupancy,
            stretched,
            sr,
            text,
            extra={
                "source_file": source_file,
                "speed": speed,
                "original_utterance_duration_s": round(len(audio) / sr, 3),
            },
        )
        out_path = args.out_dir / cond["filename"]
        sf.write(out_path, cond.pop("_audio"), sr, subtype="PCM_16")
        conditions.append(cond)
        print(
            f"  {label}: {source_file} @ {speed}x -> {stretched_dur_s:.3f}s utterance  "
            f"gap={gap_s:.3f}s  N={cond['repeats']}  total={cond['total_duration_s']:.2f}s  "
            f"occupancy actual={cond['occupancy_actual']:.1%}  -> {out_path}"
        )

    metadata_path = args.out_dir / "metadata_combo.json"
    metadata_path.write_text(json.dumps(conditions, ensure_ascii=False, indent=2))
    print(f"\nWrote {metadata_path}")


if __name__ == "__main__":
    main()
