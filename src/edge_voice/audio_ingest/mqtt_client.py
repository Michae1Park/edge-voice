"""
MQTT subscriber worker for audio ingestion.

Subscribes to per-channel MQTT topics and pushes raw AudioPacket objects
onto the shared ingest queue. Handles reconnection with exponential backoff
internally -- invisible to pipeline supervision.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt  # type: ignore[import-untyped]

from edge_voice.config.settings import MQTTSettings
from edge_voice.pipeline.models import AudioPacket

logger = logging.getLogger(__name__)

QUEUE_PUT_TIMEOUT_S = 0.2
CONNECT_TIMEOUT_S = 5.0


class MqttAudioIngest(threading.Thread):
    """Subscribes to per-channel MQTT topics and pushes AudioPackets to ingest_queue.

    Each MQTT topic yields a separate channel_id (derived from subscription
    configuration, not the message payload). The raw MQTT payload bytes are
    used directly as PCM samples on the resulting AudioPacket.
    """

    def __init__(
        self,
        settings: MQTTSettings,
        ingest_queue: queue.Queue[AudioPacket],
    ) -> None:
        super().__init__(name="MqttAudioIngest", daemon=False)
        self._settings = settings
        self._ingest_queue = ingest_queue
        self._channels = list(settings.channels)
        self._topic_to_channel = {ch.topic: ch.channel_id for ch in self._channels}
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        # Monotonic timestamp of the last message received. Exposed for a
        # uniform worker interface (docs/BUILDPLAN.md Milestone 6); the
        # supervisor does NOT stall-check this worker -- run() only blocks on
        # the stop event, and paho owns reconnect internally.
        self._last_activity = time.monotonic()
        self._client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self._on_connected: list[Callable[[], None]] = []
        # paho-mqtt 2.x built-in reconnect with exponential backoff
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)
        self._client.reconnect_on_disconnect = True  # type: ignore[attr-defined]

    def on_connected(self, callback: Callable[[], None]) -> None:
        """Register a callback to invoke once the broker connection is active."""
        self._on_connected.append(callback)

    def run(self) -> None:
        """Connect, subscribe and loop until stop() is called."""
        logger.info(
            "MqttAudioIngest starting (host=%s:%d)",
            self._settings.broker_host,
            self._settings.broker_port,
        )

        self._client.on_connect = self._on_connect  # type: ignore[assignment]
        self._client.on_disconnect = self._on_disconnect  # type: ignore[assignment]
        self._client.on_message = self._on_message

        self._client.connect_async(self._settings.broker_host, self._settings.broker_port)
        self._client.loop_start()

        if not self._connected_event.wait(timeout=CONNECT_TIMEOUT_S):
            logger.warning("Timed out waiting for MQTT connection")

        self._stop_event.wait()  # main loop: block until stop() is called

        self._client.loop_stop()
        self._client.disconnect()
        logger.info("MqttAudioIngest stopped")

    def stop(self) -> None:
        """Signal the subscriber thread to stop."""
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    @property
    def last_activity(self) -> float:
        """Monotonic time of the last MQTT message received."""
        return self._last_activity

    def _on_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        _flags: Any,
        rc: int,
        _properties: Any,
    ) -> None:
        for ch in self._channels:
            client.subscribe(ch.topic)
            logger.info("Subscribed to %s for channel %s", ch.topic, ch.channel_id)
        for cb in self._on_connected:
            cb()
        self._connected_event.set()
        logger.info("Connected and subscribed to %d channel(s)", len(self._channels))

    def _on_disconnect(
        self, _client: mqtt.Client, _userdata: Any, rc: Any, *_args: Any, **_kwargs: Any
    ) -> None:
        logger.warning("Disconnected from MQTT broker (rc=%s)", rc)
        self._connected_event.clear()

    def _on_message(self, _client: mqtt.Client, _userdata: Any, msg: mqtt.MQTTMessage) -> None:  # type: ignore[override]
        self._last_activity = time.monotonic()

        if not msg.payload:
            logger.warning("Empty MQTT audio payload on topic %s", msg.topic)
            return

        channel_id = self._topic_to_channel.get(msg.topic, msg.topic.split("/")[-1])
        packet = AudioPacket(channel_id=channel_id, timestamp=time.time(), samples=msg.payload)

        try:
            self._ingest_queue.put(packet, timeout=QUEUE_PUT_TIMEOUT_S)
        except queue.Full:
            logger.warning("Ingest queue full -- dropping packet from channel %s", channel_id)
