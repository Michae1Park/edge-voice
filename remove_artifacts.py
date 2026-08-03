#!/usr/bin/env python3
"""Remove generated artifacts: dumped audio (.wav) and log files (.log, incl. rotated backups)."""

from pathlib import Path

TARGETS = (
    ("dumped_audio", "*.wav"),
    ("dumped_vad_segments", "*.wav"),
    ("logs", "*.log*"),  # *.log* also catches rotated backups like edge-voice-....log.1
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
