#!/usr/bin/env python3
"""Builds the three speech-density test files used by run_experiment.py.

Speech density = how much of the audio's total duration is actual speech
(vs silence), controlled by the gap between repeated speech bursts:

    Extreme density (gap=0.15s): SPEECH-SPEECH-SPEECH-   (~95% occupancy)
    High density    (gap=0.5s):  SPEECH--SPEECH--SPEECH--   (~85% occupancy)
    Medium density  (gap=2.0s):  SPEECH----SPEECH----SPEECH----   (~60% occupancy)
    Low density     (gap=7.0s):  SPEECH--------SPEECH--------SPEECH--------   (~30% occupancy)

Each file tiles ONE real speech utterance -- characterization/testsegments/3s.wav
(3.392s, transcription in testsegments/metadata.json) -- N times, with a
silence gap after EVERY copy including the last (N speech + N gaps, a fully
symmetric repeating unit), where N is chosen so the total length lands as
close as possible to ~30s. Occupancy is therefore an *outcome* of (utterance
length, gap length, N), not something forced exactly -- the actual achieved
value is computed and recorded per file rather than assumed to hit the
target precisely.

The trailing gap after the LAST utterance matters beyond symmetry: without
it (an earlier version ended right on the final speech sample, no silence
after), VAD has no in-content pause to detect the end of that last
utterance and falls back to vad.idle_flush_s (2.0s of no packets at all) to
force-finalize it -- adding a ~2000ms artifact to that one segment's
latency that has nothing to do with density, confirmed via
run_experiment.py measurements where it appeared on exactly the last
segment of a run. The trailing gap gives VAD real silence content to detect
a natural pause on, same as every other utterance boundary in the file.

Ground truth for CER: the source utterance's known transcription (from
testdata/metadata.json), repeated N times -- since every speech burst in the
generated file is an identical copy of the same utterance.

Also builds testaudio/warmup.wav (the base utterance + 2.0s trailing
silence, baked into one continuous file) for run_experiment.py's per-run
warm-up. A run_experiment.py version that published the raw
testsegments/3s.wav directly (no trailing silence) as a separate
wav_source.main() call, then slept, then published the real condition file
as a THIRD, still-separate call hit a bug: the raw recording's own natural
trailing content caused VAD to close the warm-up segment and immediately
reopen a new one on some trailing noise, which then absorbed the entire
silent gap and the real file's first utterance into one 8+ second merged
segment -- because nothing gave VAD a clean, unambiguous silence to close
on. Baking 2.0s of real silence into one continuous warm-up file (same
fix already proven for the analogous last-segment-of-run bug) sidesteps
needing to fully understand that failure mode.

Output:
    characterization/testaudio/{extreme,high,medium,low}_density.wav
    characterization/testaudio/warmup.wav
    characterization/testaudio/metadata.json  -- one entry per density file
    with the exact composition (N, gap_s, durations, occupancy) and
    reference transcript, so run_experiment.py doesn't need to re-derive
    any of this.
"""

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

# (label, gap_s, occupancy_target) -- occupancy_target is documentation only,
# see module docstring on why the achieved value is computed, not forced.
# extreme's 0.15s gap is only 1.5x vad.min_silence_duration_ms (100ms) --
# close enough to the VAD's own silence-detection floor that a merged
# segment (two bursts read as one) is possible; run_experiment.py already
# handles n_segments != n_expected without failing, so this is a risk to
# watch for in results, not a crash risk.
DENSITY_CONDITIONS = [
    ("extreme", 0.15, 0.95),
    ("high", 0.5, 0.85),
    ("medium", 2.0, 0.60),
    ("low", 7.0, 0.30),
]

# Trailing silence baked into the warm-up file -- see module docstring.
WARMUP_TRAILING_SILENCE_S = 2.0


def _load_base_utterance(filename: str = BASE_UTTERANCE_FILE) -> tuple[np.ndarray, int, str]:
    """(audio, sample_rate, transcription) for a testsegments/*.wav file.

    Duration is always measured directly from the audio (len(audio)/sr),
    never trusted from metadata.json's own "duration_seconds" field -- that
    field and the file's actual content have been observed to disagree
    (e.g. 7s.wav: metadata says 7.152s, the file itself measures 7.411s;
    its start/end/original_filename timestamp fields don't agree with
    EACH OTHER either, let alone the file -- stale/inconsistent bookkeeping
    from however testsegments/ was assembled, not something to silently
    trust for an occupancy calculation).
    """
    audio, sr = sf.read(TESTDATA_DIR / filename, dtype="float32")
    if audio.ndim != 1:
        raise ValueError(f"{filename} must be mono, got shape {audio.shape}")
    metadata = json.loads((TESTDATA_DIR / "metadata.json").read_text())
    entry = next(m for m in metadata if m["filename"] == filename)
    return audio, sr, entry["transcription"]


def _repeats_for_target(speech_dur_s: float, gap_s: float, target_s: float) -> int:
    """N speech copies + N gaps (symmetric, trailing gap included), chosen
    to land total closest to target_s."""
    # total(N) = N * (speech_dur + gap)
    raw_n = target_s / (speech_dur_s + gap_s)
    return max(1, round(raw_n))


def _tile_with_symmetric_gaps(speech_audio: np.ndarray, gap_s: float, n: int, sr: int) -> np.ndarray:
    """N copies of speech_audio, each followed by a gap_s silence gap
    (including after the last copy) -- see module docstring on why the
    trailing gap is required, not just symmetry for its own sake."""
    gap_samples = int(round(gap_s * sr))
    silence = np.zeros(gap_samples, dtype=np.float32)
    pieces = [speech_audio]
    for _ in range(n - 1):
        pieces.append(silence)
        pieces.append(speech_audio)
    pieces.append(silence)
    return np.concatenate(pieces)


def build_warmup(base_audio: np.ndarray, sr: int, out_dir: Path) -> Path:
    """utterance + WARMUP_TRAILING_SILENCE_S of silence, baked into one
    continuous file -- see module docstring. Shared across every
    characterization experiment's run_experiment.py invocation (the
    warm-up's job -- avoiding STT/ORT cold-start -- has nothing to do with
    which experiment is running)."""
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
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    base_audio, sr, base_text = _load_base_utterance()
    print(f"Base utterance: {BASE_UTTERANCE_FILE} ({len(base_audio) / sr:.3f}s) -- \"{base_text}\"")

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
