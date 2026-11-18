from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass

from edge_voice.observability.logging import get_stage_logger
from edge_voice.pipeline.fanout import fanout_put
from edge_voice.pipeline.models import AudioPacket

logger = get_stage_logger(__name__, stage="channel")

QUEUE_GET_TIMEOUT_S = 0.2
QUEUE_PUT_TIMEOUT_S = 0.2


@dataclass(frozen=True, slots=True)
class RepacketizerConfig:

    incoming_ms: float = 20.0
    outgoing_ms: float = 32.0
    sample_rate: int = 16000
    bytes_per_sample: int = 2


def _ms_to_bytes(ms: float, sample_rate: int, bytes_per_sample: int) -> int:
    samples = ms * sample_rate / 1000.0
    rounded = round(samples)
    if abs(samples - rounded) > 1e-6:
        raise ValueError(
            f"{ms}ms at {sample_rate}Hz is not a whole number of samples "
            f"({samples}) -- pick an ms value that divides evenly"
        )
    return rounded * bytes_per_sample


class Repacketizer:

    def __init__(self, config: RepacketizerConfig) -> None:
        self._config = config
        self._incoming_bytes = _ms_to_bytes(
            config.incoming_ms, config.sample_rate, config.bytes_per_sample
        )
        self._outgoing_bytes = _ms_to_bytes(
            config.outgoing_ms, config.sample_rate, config.bytes_per_sample
        )
        self._outgoing_s = config.outgoing_ms / 1000.0

        self._buffers: dict[str, bytearray] = {}
        self._buffer_start_ts: dict[str, float] = {}

    def process(self, packet: AudioPacket) -> list[AudioPacket]:
        if len(packet.samples) != self._incoming_bytes:
            raise ValueError(
                f"expected {self._incoming_bytes}-byte "
                f"({self._config.incoming_ms}ms) packets, got "
                f"{len(packet.samples)} bytes on channel {packet.channel_id}"
            )

        buf = self._buffers.setdefault(packet.channel_id, bytearray())

        if not buf:
            self._buffer_start_ts[packet.channel_id] = packet.timestamp

        buf.extend(packet.samples)

        outgoing: list[AudioPacket] = []
        out_bytes = self._outgoing_bytes
        start_ts = self._buffer_start_ts[packet.channel_id]

        while len(buf) >= out_bytes:
            chunk = bytes(buf[:out_bytes])
            del buf[:out_bytes]
            outgoing.append(
                AudioPacket(
                    channel_id=packet.channel_id,
                    timestamp=start_ts,
                    samples=chunk,
                )
            )
            start_ts += self._outgoing_s

        self._buffer_start_ts[packet.channel_id] = start_ts

        if outgoing:
            logger.debug(
                "repacketizer out channel=%s emitted=%d bytes_each=%d carry=%dB",
                packet.channel_id,
                len(outgoing),
                out_bytes,
                len(buf),
                extra={"channel_id": packet.channel_id},
            )
        else:
            logger.debug(
                "repacketizer out channel=%s emitted=0 carry=%dB",
                packet.channel_id,
                len(buf),
                extra={"channel_id": packet.channel_id},
            )

        return outgoing

    def reset_channel(self, channel_id: str) -> None:
        self._buffers.pop(channel_id, None)
        self._buffer_start_ts.pop(channel_id, None)


class ChannelRouter(threading.Thread):

    def __init__(
        self,
        ingest_queue: queue.Queue[AudioPacket],
        routed_queue: queue.Queue[AudioPacket],
        channel_ids: list[str],
        dump_queue: queue.Queue[AudioPacket] | None = None,
        repacketizer_config: RepacketizerConfig | None = None,
    ) -> None:
        super().__init__(name="ChannelRouter", daemon=False)
        self._ingest_queue = ingest_queue
        self._routed_queue = routed_queue
        self._dump_queue = dump_queue
        self._channel_ids = set(channel_ids)
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._channel_last_seen: dict[str, float] = {}
        self._repacketize_latency_s: dict[str, float] = {}
        self._repacketizer = Repacketizer(repacketizer_config or RepacketizerConfig())
        self._last_activity = time.monotonic()

    def run(self) -> None:
        logger.info("ChannelRouter started for channels: %s", sorted(self._channel_ids))
        while not self._stop_event.is_set():
            try:
                packet = self._ingest_queue.get(timeout=QUEUE_GET_TIMEOUT_S)
            except queue.Empty:
                continue

            self._last_activity = time.monotonic()

            if packet.channel_id not in self._channel_ids:
                logger.warning(
                    "Unknown channel_id %s -- dropping packet",
                    packet.channel_id,
                    extra={"channel_id": packet.channel_id},
                )
                continue

            with self._lock:
                self._channel_last_seen[packet.channel_id] = time.time()

            t0 = time.monotonic()
            try:
                out_packets = self._repacketizer.process(packet)
            except ValueError:
                logger.exception(
                    "Repacketizer rejected packet on channel %s -- dropping",
                    packet.channel_id,
                    extra={"channel_id": packet.channel_id},
                )
                continue
            with self._lock:
                self._repacketize_latency_s[packet.channel_id] = time.monotonic() - t0

            for out_packet in out_packets:
                fanout_put(
                    out_packet,
                    self._routed_queue,
                    self._dump_queue,
                    put_timeout=QUEUE_PUT_TIMEOUT_S,
                )

        logger.info("ChannelRouter stopped")

    def stop(self) -> None:
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    @property
    def last_activity(self) -> float:
        return self._last_activity

    def get_freshness(self, channel_id: str) -> float | None:
        with self._lock:
            last = self._channel_last_seen.get(channel_id)
        if last is None:
            return None
        return time.time() - last

    def repacketize_latencies_s(self) -> dict[str, float]:
        with self._lock:
            return dict(self._repacketize_latency_s)

    def get_channel_ids(self) -> list[str]:
        return sorted(self._channel_ids)
