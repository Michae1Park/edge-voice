"""Tests for edge_voice.audio_ingest.atomic_write."""

import numpy as np
import soundfile as sf

from edge_voice.audio_ingest.atomic_write import atomic_sf_write


def test_writes_readable_wav(tmp_path):
    dest = tmp_path / "seg.wav"
    data = (np.random.randn(1600) * 1000).astype(np.int16)
    atomic_sf_write(str(dest), data, 16000, subtype="PCM_16")

    assert dest.exists()
    read, sr = sf.read(str(dest), dtype="int16")
    assert sr == 16000
    np.testing.assert_array_equal(read, data)


def test_leaves_no_temp_file_behind(tmp_path):
    dest = tmp_path / "seg.wav"
    atomic_sf_write(str(dest), np.zeros(320, dtype=np.int16), 16000, subtype="PCM_16")
    # Only the destination should exist -- no stray .tmp-<pid> sibling.
    assert [p.name for p in tmp_path.iterdir()] == ["seg.wav"]


def test_replaces_existing_file_atomically(tmp_path):
    dest = tmp_path / "seg.wav"
    atomic_sf_write(str(dest), np.zeros(320, dtype=np.int16), 16000, subtype="PCM_16")

    new = np.ones(640, dtype=np.int16) * 5
    atomic_sf_write(str(dest), new, 16000, subtype="PCM_16")

    read, _ = sf.read(str(dest), dtype="int16")
    np.testing.assert_array_equal(read, new)
    assert [p.name for p in tmp_path.iterdir()] == ["seg.wav"]


def test_failed_write_cleans_up_temp(tmp_path):
    dest = tmp_path / "seg.wav"
    # An unwritable subtype makes soundfile raise mid-write; the temp file it
    # opened must not be left behind.
    try:
        atomic_sf_write(str(dest), np.zeros(320, dtype=np.int16), 16000, subtype="NOT_A_SUBTYPE")
    except Exception:
        pass
    assert list(tmp_path.iterdir()) == []
