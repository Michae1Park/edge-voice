#!/usr/bin/env python3
"""Remove all .wav files from the dumped_audio directory."""

from pathlib import Path

import sys

dumped = Path("dumped_audio")

files = list(dumped.glob("*.wav"))

if not files:
    print("No .wav files found in dumped_audio/")
    sys.exit(0)

for f in files:
    f.unlink()
    print(f"Removed: {f}")

print(f"\nDeleted {len(files)} file(s)")
