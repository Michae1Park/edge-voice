#!/usr/bin/env python3
"""Builds the five "limit testing" (한계치 측정) combo files -- the most
demanding value from each of the three prior experiments (density, speaking
rate, segment length), combined into single test conditions:

    combo   density(occupancy)   rate    segment
    1       85%                  1.5x    6s
    2       85%                  1.5x    7s
    3       95%                  1.5x    6s
    4       95%                  1.5x    7s
    5       95%                  2x      7s

Each combo: take the named testsegments/{N}s.wav utterance, time-scale-modify
it to the given rate (pitch-preserving phase vocoder, same as
build_speed_audio.py), then tile the RESULT with silence gaps computed from
its actual post-TSM duration to hit the exact occupancy target -- same
symmetric N-speech+N-gap pattern (trailing gap included, avoids the
vad.idle_flush_s last-segment artifact) as every other generator here.

Each combo carries a "speed" key in its metadata entry, so
run_experiment.py's existing per-condition max_tokens_per_second scaling
(18.0 * speed) applies automatically -- same reasoning as the speaking-rate
experiment: without it, TSM-compressed audio starves the decoder's token
budget regardless of how hard the content itself is to recognize.

combo5's gap (~0.2s at 7s/2x/95%) is only ~2x vad.min_silence_duration_ms
(100ms) -- same tight-margin risk already flagged for "extreme" density;
run_experiment.py already tolerates n_segments != n_expected without
failing, so this is a risk to watch in results, not a crash risk.

Output:
    characterization/testaudio/combo{1..5}.wav
    characterization/testaudio/warmup.wav  -- built if not already present
    characterization/testaudio/metadata_combo.json
"""

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

# (label, source_file, speed, occupancy_target) -- see module docstring table.
COMBOS = [
    # Round 2 (all of round 1's combos superseded by this re-run): every
    # combo now uses only speed in {1.0x, 2.0x} for a consistent grid, run
    # under the sustained/looped methodology (bench_pipeline_load.py,
    # --disable-reliability --min-silence-duration-ms 50) that replaced the
    # original one-shot run_experiment.py pass -- searching for a genuine
    # capacity limit now that the min_silence_duration_ms=50 fix removes
    # the VAD-threshold confound that explained the original combo5's
    # "clogging" (see run_experiment.py's sustained-load investigation).
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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
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
            label, f"{label}.wav", gap_s, occupancy, stretched, sr, text,
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
