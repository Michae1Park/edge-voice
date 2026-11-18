import queue

from edge_voice.audio_ingest.mqtt_client import MqttAudioIngest
from edge_voice.config.settings import MQTTSettings, MQTTChannels


def test_resolve_channel_matches_topic():
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
    topic = "unknown/topic"
    channel_id = client._topic_to_channel.get(topic, topic.split("/")[-1])
    assert channel_id == "topic"
