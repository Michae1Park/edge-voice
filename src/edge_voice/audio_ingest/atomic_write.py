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
    dest = Path(path)
    tmp = dest.with_name(f".{dest.name}.tmp-{os.getpid()}")
    kwargs.setdefault("format", dest.suffix.lstrip(".").upper() or None)
    try:
        sf.write(str(tmp), data, samplerate, subtype=subtype, **kwargs)
        os.replace(tmp, dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
