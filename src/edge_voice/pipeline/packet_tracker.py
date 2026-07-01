"""Per-channel packet tracker for pipeline-wide state.

Maintains a single source of truth for all per-channel metrics:
- Last-seen timestamp
- Packet count
- Total bytes
- Current segment bytes (resets on segment boundary)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from edge_voice.pipeline.models import AudioPacket

logger = logging.getLogger(__name__)


@dataclass
class ChannelState:
    """Per-channel tracking state."""

    channel_id: str
    last_seen: float = field(default_factory=time.time)
    packet_count: int = 0
    total_bytes: int = 0
    current_segment_bytes: int = 0
    current_segment_packets: int = 0
    current_segment_start: float | None = None
    current_segment_end: float | None = None
    current_segment_id: int | None = None
    _segment_buffer: bytes = field(default=b"", repr=False)

    def on_packet(self, packet: AudioPacket) -> dict[str, Any]:
        """Record a packet. Returns snapshot of current state."""
        self.last_seen = time.time()
        self.packet_count += 1
        len(packet.samples) // 2  # 16-bit PCM = 2 bytes/sample
        self.total_bytes += len(packet.samples)
        self.current_segment_bytes += len(packet.samples)
        self.current_segment_packets += 1
        if self.current_segment_start is None:
            self.current_segment_start = packet.timestamp
        self.current_segment_end = packet.timestamp
        self._segment_buffer += packet.samples
        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        """Return a dict of current per-channel state."""
        return {
            "channel_id": self.channel_id,
            "last_seen": self.last_seen,
            "last_seen_age": time.time() - self.last_seen,
            "packet_count": self.packet_count,
            "total_bytes": self.total_bytes,
            "current_segment_bytes": self.current_segment_bytes,
            "current_segment_packets": self.current_segment_packets,
            "current_segment_start": self.current_segment_start,
            "current_segment_end": self.current_segment_end,
            "duration_s": (self.current_segment_end or 0) - (self.current_segment_start or 0),
        }


class AudioPacketTracker:
    """Central per-channel state tracker.

    Injected into PacketCopier so all pipeline stages get a single shared
    view of packet counts, timestamps, and segment tracking per channel.
    """

    def __init__(
        self,
        channel_ids: list[str] | None = None,
        on_segment_start: Any | None = None,
        on_segment_end: Any | None = None,
    ) -> None:
        self._channels: dict[str, ChannelState] = {}
        self._expected_channels: set[str] = set(channel_ids) if channel_ids else set()
        self._segment_counter: int = 0
        self._on_segment_start = on_segment_start
        self._on_segment_end = on_segment_end
        self._lock = __import__("threading").Lock()

    @property
    def channel_ids(self) -> set[str]:
        """Public accessor for expected channel IDs."""
        return self._expected_channels

    def track(self, packet: AudioPacket) -> dict[str, Any]:
        """Track a packet and return the channel snapshot."""
        ch = self._channels.setdefault(
            packet.channel_id,
            ChannelState(channel_id=packet.channel_id),
        )
        return ch.on_packet(packet)

    def on_segment_start(self, channel_id: str) -> ChannelState | None:
        """Called when a fresh segment begins."""
        with self._lock:
            ch = self._channels.get(channel_id)
            if ch is None:
                return None
            ch.current_segment_packets = 0
            ch.current_segment_bytes = 0
            ch.current_segment_start = None
            ch.current_segment_end = None
            ch._segment_buffer = b""
            self._segment_counter += 1
            ch.current_segment_id = self._segment_counter
        return ch

    def on_segment_end(self, channel_id: str) -> ChannelState | None:
        """Called when a segment ends."""
        with self._lock:
            ch = self._channels.get(channel_id)
            if ch is None:
                return None
        return ch

    def dump_channels(self) -> list[dict[str, Any]]:
        """Dump snapshots for all tracked channels."""
        with self._lock:
            return [ch.snapshot() for ch in self._channels.values()]
