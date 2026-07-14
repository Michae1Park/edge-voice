"""Backward-compatible re-export: PacketCopier IS QueueCopier."""

from edge_voice.pipeline.queue_copier import QueueCopier as PacketCopier

__all__ = ["PacketCopier"]
