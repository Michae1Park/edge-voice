"""
Channel-aware audio router.

Consumes AudioPackets from the ingest queue, validates and tags each with its
channel_id, maintains per-channel bookkeeping (last-seen timestamp for
freshness checks), re-packetizes audio to a fixed outgoing frame size, and
forwards packets to the routed queue for downstream VAD/STT consumption.
Optionally copies packets to a dump queue for debugging.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass

from edge_voice.pipeline.fanout import fanout_put
from edge_voice.pipeline.models import AudioPacket

logger = logging.getLogger(__name__)

QUEUE_GET_TIMEOUT_S = 0.2
QUEUE_PUT_TIMEOUT_S = 0.2


# ── Repacketizer config ─────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class RepacketizerConfig:
    """
    incoming_ms  — frame size of packets arriving on the ingest queue
    outgoing_ms  — frame size to emit downstream
    sample_rate  — Hz
    bytes_per_sample — 2 for 16-bit PCM mono (this codebase's format)
    """

    incoming_ms: float = 20.0
    outgoing_ms: float = 32.0
    sample_rate: int = 16000
    bytes_per_sample: int = 2


def _ms_to_bytes(ms: float, sample_rate: int, bytes_per_sample: int) -> int:
    """Convert a millisecond duration to an exact byte count, or raise."""
    samples = ms * sample_rate / 1000.0
    rounded = round(samples)
    if abs(samples - rounded) > 1e-6:
        raise ValueError(
            f"{ms}ms at {sample_rate}Hz is not a whole number of samples "
            f"({samples}) -- pick an ms value that divides evenly"
        )
    return rounded * bytes_per_sample


class Repacketizer:
    """
    Buffers incoming AudioPackets per channel and re-emits fixed-size
    outgoing packets (e.g. 20ms in -> 32ms out).

    Each call to process() may return 0, 1, or more outgoing packets,
    since incoming/outgoing sizes don't generally divide evenly (e.g.
    20ms in / 32ms out yields a new packet every 1.6 incoming packets
    on average).

    Assumes packets for a given channel_id arrive contiguously (no gaps
    or drops) -- outgoing packet timestamps are derived by advancing a
    per-channel clock, not by trusting every incoming packet's own
    timestamp. If your ingest can drop packets or reconnect mid-stream,
    call reset_channel() on that event or timestamps will drift silently.
    """

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
        """Feed one incoming packet, return zero or more outgoing packets."""
        if len(packet.samples) != self._incoming_bytes:
            raise ValueError(
                f"expected {self._incoming_bytes}-byte "
                f"({self._config.incoming_ms}ms) packets, got "
                f"{len(packet.samples)} bytes on channel {packet.channel_id}"
            )

        buf = self._buffers.setdefault(packet.channel_id, bytearray())

        # Only re-anchor the clock when the buffer was empty *before* this
        # packet -- if there's carry-over from a prior packet, its start
        # timestamp must stay, or the outgoing timestamps will jump.
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
            )
        else:
            logger.debug(
                "repacketizer out channel=%s emitted=0 carry=%dB",
                packet.channel_id,
                len(buf),
            )

        return outgoing

    def reset_channel(self, channel_id: str) -> None:
        """Discard buffered partial audio for a channel (e.g. on reconnect)."""
        self._buffers.pop(channel_id, None)
        self._buffer_start_ts.pop(channel_id, None)


# ── Router ───────────────────────────────────────────────────────────────────


class ChannelRouter(threading.Thread):
    """Validates, re-packetizes, and routes AudioPackets to the routed queue."""

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
        self._repacketizer = Repacketizer(repacketizer_config or RepacketizerConfig())
        # Monotonic timestamp of the last packet handled, read by the
        # supervisor's stall check (docs/BUILDPLAN.md Milestone 6). A plain
        # float write/read is atomic under the GIL, so no lock is needed.
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
                logger.warning("Unknown channel_id %s -- dropping packet", packet.channel_id)
                continue

            with self._lock:
                self._channel_last_seen[packet.channel_id] = time.time()

            try:
                out_packets = self._repacketizer.process(packet)
            except ValueError:
                logger.exception(
                    "Repacketizer rejected packet on channel %s -- dropping",
                    packet.channel_id,
                )
                continue

            for out_packet in out_packets:
                fanout_put(
                    out_packet,
                    self._routed_queue,
                    self._dump_queue,
                    put_timeout=QUEUE_PUT_TIMEOUT_S,
                )

        logger.info("ChannelRouter stopped")

    def stop(self) -> None:
        """Signal the router to stop."""
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    @property
    def last_activity(self) -> float:
        """Monotonic time of the last packet handled (for supervisor stall check)."""
        return self._last_activity

    def get_freshness(self, channel_id: str) -> float | None:
        """Return seconds since last seen packet for a channel, or None."""
        with self._lock:
            last = self._channel_last_seen.get(channel_id)
        if last is None:
            return None
        return time.time() - last

    def get_channel_ids(self) -> list[str]:
        """Return the list of known valid channel IDs."""
        return sorted(self._channel_ids)
