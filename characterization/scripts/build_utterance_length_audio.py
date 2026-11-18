#!/usr/bin/env python3
# Builds test files for the utterance-LENGTH question, holding occupancy fixed -- isolates "does the
# base speech unit's own duration matter" from density (already tested with a 3s unit) and speaking
# ...
# Usage: python characterization/scripts/build_utterance_length_audio.py --file 7s.wav

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

DEFAULT_OCCUPANCY = 0.60


def _label(filename: str) -> str:
    return Path(filename).stem


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--file",
        nargs="+",
        default=["7s.wav"],
        help="testsegments/*.wav file(s) to use as the base utterance (default: 7s.wav)",
    )
    parser.add_argument(
        "--occupancy",
        type=float,
        default=DEFAULT_OCCUPANCY,
        help="Target speech occupancy (default 0.60)",
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    warmup_path = args.out_dir / "warmup.wav"
    if not warmup_path.exists():
        w_audio, w_sr, _ = _load_base_utterance(args.file[0])
        warmup_path = build_warmup(w_audio, w_sr, args.out_dir)
        print(f"  warmup: built -> {warmup_path}")
    else:
        print(f"  warmup: already exists, reusing -> {warmup_path}")

    conditions = []
    for filename in args.file:
        audio, sr, text = _load_base_utterance(filename)
        speech_dur_s = len(audio) / sr
        gap_s = speech_dur_s * (1 - args.occupancy) / args.occupancy
        label = _label(filename)

        cond = build_condition(
            label,
            f"{label}_len.wav",
            gap_s,
            args.occupancy,
            audio,
            sr,
            text,
            extra={"source_file": filename},
        )
        out_path = args.out_dir / cond["filename"]
        sf.write(out_path, cond.pop("_audio"), sr, subtype="PCM_16")
        conditions.append(cond)
        print(
            f'  {label:>4}: utterance {speech_dur_s:.3f}s -- "{text}"\n'
            f"        gap={gap_s:.3f}s  N={cond['repeats']}  total={cond['total_duration_s']:.2f}s  "
            f"occupancy actual={cond['occupancy_actual']:.1%}  -> {out_path}"
        )

    metadata_path = args.out_dir / "metadata_length.json"
    existing = []
    if metadata_path.exists():
        existing = [
            c
            for c in json.loads(metadata_path.read_text())
            if c["label"] not in {c2["label"] for c2 in conditions}
        ]
    metadata_path.write_text(json.dumps(existing + conditions, ensure_ascii=False, indent=2))
    print(f"\nWrote {metadata_path}")


if __name__ == "__main__":
    main()
