from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[1]
RX_WAV = REPO_ROOT / "wav" / "rx_recorded_1.wav"
TX_WAV = REPO_ROOT / "wav" / "tx_recorded_1.wav"

STARTUP_BUFFER_S = 8
AUDIO_DURATION_S = 30
SETTLE_BUFFER_S = 7
CLI_RUN_SECS = STARTUP_BUFFER_S + AUDIO_DURATION_S + SETTLE_BUFFER_S

EXPECTED_SEGMENTS = {"rx": 4, "tx": 5}


def _broker_reachable(host: str = "localhost", port: int = 1883, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.mark.skipif(not _broker_reachable(), reason="no MQTT broker reachable on localhost:1883")
def test_pipeline_produces_expected_segment_counts(tmp_path):
    assert RX_WAV.exists() and TX_WAV.exists(), "test fixtures missing from wav/"

    env = os.environ.copy()
    env["EDGE_VOICE_SEGMENT_DUMP__ENABLED"] = "true"
    env["EDGE_VOICE_SEGMENT_DUMP__OUTPUT_DIR"] = str(tmp_path)
    env["EDGE_VOICE_DUMP__ENABLED"] = "false"

    cli_proc = subprocess.Popen(
        [sys.executable, "-m", "edge_voice.cli", "--run-secs", str(CLI_RUN_SECS)],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        time.sleep(STARTUP_BUFFER_S)

        source = subprocess.run(
            [
                sys.executable,
                "-m",
                "edge_voice.utils.audio_generation.wav_source",
                "--wav",
                str(RX_WAV),
                str(TX_WAV),
                "--channels",
                "rx",
                "tx",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=AUDIO_DURATION_S + 15,
        )
        assert source.returncode == 0, (
            f"wav_source failed:\nstdout:\n{source.stdout}\nstderr:\n{source.stderr}"
        )

        time.sleep(SETTLE_BUFFER_S)

        cli_stdout, _ = cli_proc.communicate(timeout=30)
    finally:
        if cli_proc.poll() is None:
            cli_proc.terminate()
            try:
                cli_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                cli_proc.kill()
                cli_proc.wait(timeout=10)

    actual = {ch: len(list(tmp_path.glob(f"{ch}_*.wav"))) for ch in EXPECTED_SEGMENTS}
    assert actual == EXPECTED_SEGMENTS, (
        f"segment counts drifted: got {actual}, expected {EXPECTED_SEGMENTS}\n"
        f"--- CLI output (tail) ---\n{(cli_stdout or '')[-4000:]}"
    )
