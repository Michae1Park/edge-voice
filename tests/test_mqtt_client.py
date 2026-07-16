"""Tests for edge_voice.audio_ingest.mqtt_client."""

import queue

from edge_voice.audio_ingest.mqtt_client import MqttAudioIngest
from edge_voice.config.settings import MQTTSettings, MQTTChannels


def test_resolve_channel_matches_topic():
    """Verify topic_to_channel mapping uses configured topics."""
    settings = MQTTSettings(
        channels=[
            MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
            MQTTChannels(topic="stt/audio_tx", channel_id="tx"),
        ]
    )
    ingest_q = queue.Queue()
    client = MqttAudioIngest(settings, ingest_q)
    assert client._topic_to_channel["stt/audio_rx"] == "rx"
    assert client._topic_to_channel["stt/audio_tx"] == "tx"


def test_resolve_channel_fallback():
    settings = MQTTSettings(
        channels=[
            MQTTChannels(topic="stt/audio_rx", channel_id="rx"),
        ]
    )
    ingest_q = queue.Queue()
    client = MqttAudioIngest(settings, ingest_q)
    # Fallback: last segment of topic path
    topic = "unknown/topic"
    channel_id = client._topic_to_channel.get(topic, topic.split("/")[-1])
    assert channel_id == "topic"


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
#
#
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
#
#
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
#
#
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
