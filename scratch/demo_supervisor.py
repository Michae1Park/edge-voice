#!/usr/bin/env python3
"""Manual demo of Milestone 6 reliability: watch the real supervisor recover
from a crash, a stall, and finally give up after too many restarts.

No MQTT broker, WAV file, or GPU needed -- this builds and starts the real
PipelineOrchestrator (MqttAudioIngest just logs a connect-timeout warning and
carries on, exactly like it would with a broker down), then reaches in to
simulate the two failure modes supervisor.py detects.

Usage:
    python scratch/demo_supervisor.py
"""

import logging
import os
import sys
import time

from edge_voice.config.settings import Settings
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")


class _BoomQueue:
    """A queue stand-in whose .get() raises something NOT caught by the
    worker's `except queue.Empty` clause -- i.e. a real, unhandled crash,
    the same as an actual bug escaping the inner per-item try/except."""

    def get(self, *a, **kw):
        raise RuntimeError("boom (simulated crash for Milestone 6 demo)")


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def main() -> None:
    # Fast timers so this demo finishes in under a minute instead of matching
    # configs/default.yaml's production-tuned values.
    settings = Settings.load()
    settings.reliability.tick_interval_s = 1.0
    settings.reliability.stall_timeout_s = 2.0
    settings.reliability.max_restarts = 2
    settings.reliability.restart_window_s = 30.0
    settings.reliability.watchdog_enabled = False  # no systemd here

    orch = PipelineOrchestrator(settings)
    orch.build()
    orch.start()
    time.sleep(0.5)

    section("1. Healthy baseline")
    print(orch.get_status())

    section("2. Simulating a VADWorker crash (routed_queue.get() raises)")
    # Reaching into worker internals on purpose to simulate a crash -- mypy
    # only sees `threading.Thread | None` here, same as orchestrator.py's own
    # _w() accessor works around; a demo has no equivalent seam, so silence it.
    dead_vad = orch._vad
    dead_vad.routed_queue = _BoomQueue()  # type: ignore[union-attr]
    print("...waiting for the supervisor to notice and restart (watch the log lines above)")
    time.sleep(3)
    print(orch.get_status())
    print(f"Same instance? {orch._vad is dead_vad}  (should be False -- it was replaced)")

    section("3. Simulating an STTWorker stall (handler hangs, input backs up)")
    orch._stt._handle_segment = lambda seg: time.sleep(999)  # type: ignore[union-attr]  # never returns
    from edge_voice.pipeline.models import SpeechSegment

    for i in range(3):
        orch._segment_queue.put(  # type: ignore[union-attr]
            SpeechSegment(
                channel_id="rx", start=0.0, end=1.0, audio=b"\x00" * 3200, segment_id=f"demo-{i}"
            )
        )
    print("...fed 3 segments; first one hangs forever, the other 2 pile up unread")
    print("...waiting past stall_timeout_s=2.0s for the supervisor to flag + restart it")
    time.sleep(4)
    print(orch.get_status())

    section("4. Crashing VADWorker repeatedly to exceed the restart budget (max_restarts=2)")
    for attempt in range(3):
        orch._vad.routed_queue = _BoomQueue()  # type: ignore[union-attr]
        print(f"...crash #{attempt + 1}, waiting for a restart attempt")
        time.sleep(1.5)
    print("...one more tick to let the 3rd failure land as DEGRADED")
    time.sleep(1.5)
    status = orch.get_status()
    print(status)
    print(f"\ndegraded={status['degraded']}  (should be True now)")
    print(
        "In a real deployment, VADWorker staying degraded is exactly what hands "
        "off to the OS watchdog -- see deploy/edge-voice.service."
    )

    section("Shutting down")
    orch.stop()
    orch.wait()
    print(
        "stop()/wait() returned -- but step 3's original STTWorker is still "
        "wedged in time.sleep(999) forever (that lambda can't be un-stuck; a "
        "restart could only replace it, not kill it). Since STTWorker is a "
        "non-daemon thread, THIS is the process-level hang the OS watchdog "
        "layer exists for -- only a full process restart clears it, which is "
        "exactly why this script force-exits below instead of waiting on it."
    )
    # os._exit() skips normal interpreter shutdown, including stdio flushing --
    # without this, all the print() narration above is silently lost whenever
    # stdout isn't a live TTY (e.g. piped to a file, or captured by a test).
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)  # noqa: SLF001 -- deliberate: see comment above, not a bug


if __name__ == "__main__":
    main()
