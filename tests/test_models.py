"""Milestone 0 throwaway tests.

Just enough to confirm the shared dataclasses construct correctly. Real
unit tests (channel routing, VAD segmentation, config validation) arrive in
Milestone 8.
"""
# from src.edge_voice.pipeline.models import AudioPacket, SpeechSegment, TranscriptEvent
from edge_voice.pipeline.models import (
    AudioPacket,
    SpeechSegment,
    TranscriptEvent,
)

def test_audio_packet_construction():
    packet = AudioPacket(channel_id="channel-1", timestamp=1.0, samples=b"\x00\x01")
    assert packet.channel_id == "channel-1"
    assert packet.samples == b"\x00\x01"


def test_speech_segment_construction():
    segment = SpeechSegment(
        channel_id="channel-1",
        start=0.0,
        end=1.5,
        audio=b"\x00" * 10,
        segment_id="seg-1",
    )
    assert segment.end > segment.start


def test_transcript_event_construction():
    event = TranscriptEvent(
        channel_id="channel-1",
        segment_id="seg-1",
        text="[fake transcript]",
        start=0.0,
        end=1.5,
    )
    assert event.text == "[fake transcript]"
    assert event.created_at > 0
