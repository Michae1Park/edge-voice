#!/usr/bin/env python3

import logging
import os
import sys
import time

from edge_voice.config.settings import Settings
from edge_voice.pipeline.orchestrator import PipelineOrchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")


class _BoomQueue:

    def get(self, *a, **kw):
        raise RuntimeError("boom (simulated crash for Milestone 6 demo)")


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def main() -> None:
    settings = Settings.load()
    settings.reliability.tick_interval_s = 1.0
    settings.reliability.stall_timeout_s = 2.0
    settings.reliability.max_restarts = 2
    settings.reliability.restart_window_s = 30.0
    settings.reliability.watchdog_enabled = False

    orch = PipelineOrchestrator(settings)
    orch.build()
    orch.start()
    time.sleep(0.5)

    section("1. Healthy baseline")
    print(orch.get_status())

    section("2. Simulating a VADWorker crash (routed_queue.get() raises)")
    dead_vad = orch._vad
    dead_vad.routed_queue = _BoomQueue()  # type: ignore[union-attr]
    print("...waiting for the supervisor to notice and restart (watch the log lines above)")
    time.sleep(3)
    print(orch.get_status())
    print(f"Same instance? {orch._vad is dead_vad}  (should be False -- it was replaced)")

    section("3. Simulating an STTWorker stall (handler hangs, input backs up)")
    orch._stt["rx"]._handle_segment = lambda seg: time.sleep(999)  # type: ignore[attr-defined]  # never returns
    from edge_voice.pipeline.models import SpeechSegment

    for i in range(3):
        orch._segment_queues["rx"].put(  # type: ignore[union-attr, index]
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
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)  # noqa: SLF001 -- deliberate: see comment above, not a bug


if __name__ == "__main__":
    main()
