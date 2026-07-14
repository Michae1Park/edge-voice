#!/usr/bin/env python3
"""Remove all .wav files from the dumped_audio and dumped_vad_segments directories."""

from pathlib import Path


for name in ("dumped_audio", "dumped_vad_segments"):
    dumped = Path(name)
    files = list(dumped.glob("*.wav"))
    if not files:
        print(f"No .wav files found in {name}/")
        continue
    for f in files:
        f.unlink()
        print(f"Removed: {f}")
    print(f"\nDeleted {len(files)} file(s) from {name}/")
