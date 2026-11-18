from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import numpy as np
import soundfile as sf


# Write to a same-directory temp file then os.replace, so a power cut never leaves a torn file.
def atomic_sf_write(
    path: str | os.PathLike[str],
    data: np.ndarray,
    samplerate: int,
    subtype: str,
    **kwargs: Any,
) -> None:
    dest = Path(path)
    tmp = dest.with_name(f".{dest.name}.tmp-{os.getpid()}")
    # The temp name has no .wav suffix, so soundfile can't infer the format from it.
    kwargs.setdefault("format", dest.suffix.lstrip(".").upper() or None)
    try:
        sf.write(str(tmp), data, samplerate, subtype=subtype, **kwargs)
        os.replace(tmp, dest)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
