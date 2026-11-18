#!/usr/bin/env python3

from pathlib import Path

TARGETS = (
    ("dumped_audio", "*.wav"),
    ("dumped_vad_segments", "*.wav"),
    ("logs", "*.log*"),
)

for name, pattern in TARGETS:
    dumped = Path(name)
    files = list(dumped.glob(pattern))
    if not files:
        print(f"No {pattern} files found in {name}/")
        continue
    for f in files:
        f.unlink()
        print(f"Removed: {f}")
    print(f"\nDeleted {len(files)} file(s) from {name}/")
