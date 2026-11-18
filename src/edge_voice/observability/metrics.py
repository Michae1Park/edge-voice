from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class MetricsSnapshot:

    queue_depths: dict[str, int]
    stt_last_latency_s: float | None
    mqtt_connected: bool | None
    restart_status: dict[str, dict[str, object]] | None
    max_restarts: int | None
    restart_window_s: float | None
    vad_silero_latencies_s: dict[str, float]
    vad_rms_gate_latencies_s: dict[str, float]
    router_repacketize_latencies_s: dict[str, float]
    stt_channel_latencies_s: dict[str, float]
    taken_at: float = field(default_factory=time.monotonic)


def _rounded(latencies: dict[str, float], decimals: int) -> dict[str, float]:
    return {channel: round(value, decimals) for channel, value in latencies.items()}


class MetricsCollector(threading.Thread):

    def __init__(
        self,
        queue_depths: Callable[[], dict[str, int]],
        stt_latency_s: Callable[[], float | None],
        vad_silero_latencies_s: Callable[[], dict[str, float]],
        vad_rms_gate_latencies_s: Callable[[], dict[str, float]],
        router_repacketize_latencies_s: Callable[[], dict[str, float]],
        stt_channel_latencies_s: Callable[[], dict[str, float]],
        supervisor: Callable[[], Any | None] = lambda: None,
        mqtt_connected: Callable[[], bool | None] = lambda: None,
        emit_interval_s: float = 10.0,
        latency_log_decimals: int = 3,
        name: str = "MetricsCollector",
    ) -> None:
        super().__init__(name=name, daemon=True)
        self._queue_depths = queue_depths
        self._stt_latency_s = stt_latency_s
        self._vad_silero_latencies_s = vad_silero_latencies_s
        self._vad_rms_gate_latencies_s = vad_rms_gate_latencies_s
        self._router_repacketize_latencies_s = router_repacketize_latencies_s
        self._stt_channel_latencies_s = stt_channel_latencies_s
        self._supervisor = supervisor
        self._mqtt_connected = mqtt_connected
        self._emit_interval_s = emit_interval_s
        self._latency_log_decimals = latency_log_decimals
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._snapshot: MetricsSnapshot | None = None

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def run(self) -> None:
        logger.info("MetricsCollector started (emit_interval_s=%.1f)", self._emit_interval_s)
        while not self._stop_event.wait(self._emit_interval_s):
            self._tick()
        logger.info("MetricsCollector stopped")

    def _tick(self) -> None:
        sup = self._supervisor()
        snapshot = MetricsSnapshot(
            queue_depths=self._queue_depths(),
            stt_last_latency_s=self._stt_latency_s(),
            mqtt_connected=self._mqtt_connected(),
            restart_status=sup.status() if sup is not None else None,
            max_restarts=sup.max_restarts if sup is not None else None,
            restart_window_s=sup.restart_window_s if sup is not None else None,
            vad_silero_latencies_s=self._vad_silero_latencies_s(),
            vad_rms_gate_latencies_s=self._vad_rms_gate_latencies_s(),
            router_repacketize_latencies_s=self._router_repacketize_latencies_s(),
            stt_channel_latencies_s=self._stt_channel_latencies_s(),
        )
        with self._lock:
            self._snapshot = snapshot
        logger.info(
            "metrics snapshot: queues=%s stt_latency_s=%s mqtt_connected=%s restarts=%s "
            "vad_silero=%s vad_rms_gate=%s router_repacketize=%s stt_channel=%s",
            snapshot.queue_depths,
            f"{snapshot.stt_last_latency_s:.{self._latency_log_decimals}f}"
            if snapshot.stt_last_latency_s is not None
            else None,
            snapshot.mqtt_connected,
            snapshot.restart_status,
            _rounded(snapshot.vad_silero_latencies_s, self._latency_log_decimals),
            _rounded(snapshot.vad_rms_gate_latencies_s, self._latency_log_decimals),
            _rounded(snapshot.router_repacketize_latencies_s, self._latency_log_decimals),
            _rounded(snapshot.stt_channel_latencies_s, self._latency_log_decimals),
        )

    def snapshot(self) -> MetricsSnapshot | None:
        with self._lock:
            return self._snapshot
