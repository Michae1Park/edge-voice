"""Tests for edge_voice.audio_ingest.mqtt_client."""

# import json
# from base64 import b64encode
import queue
import time

from edge_voice.audio_ingest.mqtt_client import MqttAudioIngest
from edge_voice.config.settings import MQTTSettings, MQTTChannels


# def _make_payload(samples: bytes) -> bytes:
#     return json.dumps(
#         {
#             "samples_b64": b64encode(samples).decode(),
#             "timestamp": 1000.0,
#         }
#     ).encode("utf-8")


def test_resolve_channel_matches_topic():
    """Verify _resolve_channel uses configured topics."""
    settings = MQTTSettings(
        channels=[
            MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
            MQTTChannels(topic="stt/audio_tx", channel_id="tx"),
        ]
    )
    ingest_q = queue.Queue()
    client = MqttAudioIngest(settings, ingest_q)
    # _resolve_channel is called during message handling
    assert client._resolve_channel("stt/audio_rx") == "rx"
    assert client._resolve_channel("stt/audio_tx") == "tx"


def test_resolve_channel_fallback():
    settings = MQTTSettings(
        channels=[
            MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
        ]
    )
    ingest_q = queue.Queue()
    client = MqttAudioIngest(settings, ingest_q)
    assert client._resolve_channel("unknown/topic") == "topic"


# def test_parse_payload_valid():
#     settings = MQTTSettings(
#         channels=[
#             MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
#         ]
#     )
#     ingest_q = queue.Queue()
#     client = MqttAudioIngest(settings, ingest_q)
#     raw = b"\x00\x01\x02\x03"
#     payload = _make_payload(raw)
#     packet = client._parse_payload(payload, "rx")
#     assert packet is not None
#     assert packet.channel_id == "rx"
#     assert packet.samples == raw
#     assert packet.timestamp == 1000.0


# def test_parse_payload_invalid_json():
#     settings = MQTTSettings(
#         channels=[
#             MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
#         ]
#     )
#     ingest_q = queue.Queue()
#     client = MqttAudioIngest(settings, ingest_q)
#     packet = client._parse_payload(b"not json", "rx")
#     assert packet is None


# def test_parse_payload_missing_samples_b64():
#     settings = MQTTSettings(
#         channels=[
#             MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
#         ]
#     )
#     ingest_q = queue.Queue()
#     client = MqttAudioIngest(settings, ingest_q)
#     payload = json.dumps({"timestamp": 1.0}).encode("utf-8")
#     packet = client._parse_payload(payload, "rx")
#     assert packet is None


# def test_parse_payload_default_timestamp():
#     settings = MQTTSettings(
#         channels=[
#             MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
#         ]
#     )
#     ingest_q = queue.Queue()
#     client = MqttAudioIngest(settings, ingest_q)
#     payload = _make_payload(b"\x00\x01")
#     payload_dict = json.loads(payload.decode())
#     del payload_dict["timestamp"]
#     payload = json.dumps(payload_dict).encode("utf-8")
#     packet = client._parse_payload(payload, "rx")
#     assert packet is not None
#     assert packet.timestamp > 0


def test_stop_sets_event():
    settings = MQTTSettings(
        channels=[
            MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
        ]
    )
    ingest_q = queue.Queue()
    client = MqttAudioIngest(settings, ingest_q)
    client.start()
    time.sleep(0.3)
    client.stop()
    client.join(timeout=3)
    assert not client.is_alive()
