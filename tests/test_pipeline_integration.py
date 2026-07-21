"""Semi-integration regression test: run the real CLI pipeline end-to-end
against known WAV fixtures over a live MQTT broker, and check the number of
VAD segments dumped roughly matches what's been manually verified.

Requires a local MQTT broker on localhost:1883 (matches configs/default.yaml
mqtt.broker_host/broker_port) -- skipped automatically if unreachable.

Marked `integration` and excluded from the default `pytest` run (see
addopts in pyproject.toml) since it's slow (~45s) and needs a live broker.
Run explicitly with:

    pytest -m integration tests/test_pipeline_integration.py -v
"""

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

# Both fixture files are exactly 30s. The CLI needs time to connect to MQTT
# and load the Silero model before wav_source_raw starts publishing (or
# early chunks are lost), plus a settle buffer at the end so the last
# segment's min_silence_duration_ms has time to flush through VADWorker
# before we stop the CLI and count files.
STARTUP_BUFFER_S = 8
AUDIO_DURATION_S = 30
SETTLE_BUFFER_S = 7
CLI_RUN_SECS = STARTUP_BUFFER_S + AUDIO_DURATION_S + SETTLE_BUFFER_S

# Verified against these exact fixture files. A drift here means something in
# the VAD/repacketizer/gate chain changed behavior, even if the unit tests
# still pass -- re-verify by listening to the dumped segments before updating
# these numbers, don't just bump them to pass.
#
# Was {"rx": 4, "tx": 6} until three VAD bugs were fixed:
#   1. VADIterator was rebuilt per packet (dict.setdefault evaluates its
#      default eagerly), and its __init__ calls model.reset_states() -- so
#      Silero's LSTM state was wiped every 32ms.
#   2. All channels shared one model instance, so interleaved rx/tx corrupted
#      each other's LSTM state (counts doubled to 8/8 once #1 was fixed).
#   3. Speech still active when the stream ended was never emitted, since
#      segments only finalize on an `end` event. tx ends mid-utterance, so its
#      last 2.62s ("신고자분은 현재 안전한 곳에 계십니까?") was silently
#      dropped; VADWorker.flush() now emits it on shutdown.
# The two sub-second tx fragments that vanished with #1/#2 were artifacts --
# they transcribed as garbage ("그까"). tx legitimately has 5 utterances.
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
        time.sleep(STARTUP_BUFFER_S)  # let MQTT connect + Silero model load before publishing

        source = subprocess.run(
            [
                sys.executable,
                "-m",
                "edge_voice.utils.audio_generation.wav_source_raw",
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
            f"wav_source_raw failed:\nstdout:\n{source.stdout}\nstderr:\n{source.stderr}"
        )

        time.sleep(SETTLE_BUFFER_S)  # let the trailing segment flush through VADWorker

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
