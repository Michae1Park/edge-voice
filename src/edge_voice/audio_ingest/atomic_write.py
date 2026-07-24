"""Crash-safe file writes for a power-loss-prone edge device.

Milestone 6, layer 3 (docs/BUILDPLAN.md): the deployment target can lose power
at any instant (someone trips the cord, flips a breaker). Writing audio
straight to its final path means a cut mid-write leaves a truncated, corrupt
file. The fix is the standard atomic-replace dance: write the whole file to a
temp path in the *same directory*, then os.replace() it onto the destination.
os.replace is atomic on POSIX, so a reader (or the next boot) sees either the
old file or the complete new one, never a torn one.

Deliberately NOT a database/WAL layer -- there is no database anywhere in this
app, and losing one in-flight dump file is already contained to that one file,
not a shared store. This just removes the torn-file failure mode from the two
debug dump workers, which are the only continuous local writers that exist.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import numpy as np
import soundfile as sf


def atomic_sf_write(
    path: str | os.PathLike[str],
    data: np.ndarray,
    samplerate: int,
    subtype: str,
    **kwargs: Any,
) -> None:
    """soundfile.write to `path` atomically via a same-directory temp file.

    The temp file shares the destination's directory so os.replace stays within
    one filesystem (a cross-device replace would raise). It is pid-tagged to
    avoid collisions between concurrent writers, and cleaned up on any failure
    so a crashed write never leaves a stray .tmp behind.
    """
    dest = Path(path)
    tmp = dest.with_name(f".{dest.name}.tmp-{os.getpid()}")
    # soundfile infers the container format from the file extension, but the
    # temp name doesn't end in .wav -- so pass it explicitly from the real
    # destination's suffix (unless the caller already specified one).
    kwargs.setdefault("format", dest.suffix.lstrip(".").upper() or None)
    try:
        sf.write(str(tmp), data, samplerate, subtype=subtype, **kwargs)
        os.replace(tmp, dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
