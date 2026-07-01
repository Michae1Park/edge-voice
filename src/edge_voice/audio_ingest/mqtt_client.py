"""
MQTT subscriber worker for audio ingestion.

Subscribes to per-channel MQTT topics and pushes raw AudioPacket objects
onto the shared ingest queue. Handles reconnection with exponential backoff
internally -- invisible to pipeline supervision.
"""

from __future__ import annotations

from base64 import b64decode
import binascii
import json
import logging
import queue
import time
import threading
from collections.abc import Callable
from typing import Any

import paho.mqtt.client as mqtt  # type: ignore[import-untyped]

from edge_voice.config.settings import MQTTSettings
from edge_voice.pipeline.models import AudioPacket

logger = logging.getLogger(__name__)

QUEUE_PUT_TIMEOUT_S = 1.0
CONNECT_TIMEOUT_S = 5.0
RECONNECT_WAIT_S = 2.0


class MqttAudioIngest(threading.Thread):
    """Subscribes to per-channel MQTT topics and pushes AudioPackets to ingest_queue.

    Each MQTT topic yields a separate channel_id (derived from subscription
    configuration rather than the message payload). Incoming messages are
    expected to be JSON envelopes:

        {"samples_b64": "<base64-encoded PCM bytes>", "timestamp": 0.0}

    The channel_id on the resulting AudioPacket is determined by the subscriber
    subscription configuration, not the message payload."""

    def __init__(
        self,
        settings: MQTTSettings,
        ingest_queue: queue.Queue[AudioPacket],
    ) -> None:
        super().__init__(name="MqttAudioIngest", daemon=False)
        self._settings = settings
        self._ingest_queue = ingest_queue
        self._channels = list(settings.channels)
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
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

        # Main loop: just wait for stop signal
        while not self._stop_event.wait(timeout=1.0):
            pass

        self._client.loop_stop()
        self._client.disconnect()
        logger.info("MqttAudioIngest stopped")

    def stop(self) -> None:
        """Signal the subscriber thread to stop."""
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def is_alive(self) -> bool:
        return not self._stop_event.is_set()

    def _on_connect(
        self,
        client: mqtt.Client,
        _userdata: Any,
        flags: Any,
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
        self, client: mqtt.Client, userdata: Any, rc: int, *args: Any, **kwargs: Any
    ) -> None:
        logger.warning("Disconnected from MQTT broker (rc=%d)", rc)
        self._connected_event.clear()

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:  # type: ignore[override]
        channel_id = self._resolve_channel(msg.topic)
        raw = msg.payload

        packet = self._parse_payload(raw, channel_id)
        if packet is None:
            return

        try:
            self._ingest_queue.put(packet, timeout=QUEUE_PUT_TIMEOUT_S)
        except queue.Full:
            logger.warning("Ingest queue full -- dropping packet from channel %s", channel_id)

    def _resolve_channel(self, topic: str) -> str:
        """Return the channel_id corresponding to the given MQTT topic."""
        for ch in self._channels:
            if topic == ch.topic:
                return ch.channel_id
        return topic.split("/")[-1]

    def _parse_payload(self, payload: bytes, channel_id: str) -> AudioPacket | None:
        """Parse a JSON message envelope into an AudioPacket."""
        try:
            body = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Invalid MQTT message JSON from %s: %s", channel_id, exc)
            return None

        try:
            raw_b64 = body["samples_b64"]
            samples = b64decode(raw_b64)
        except (KeyError, binascii.Error, TypeError) as exc:
            logger.warning("Invalid samples_b64 field in message from %s: %s", channel_id, exc)
            return None

        ts = body.get("timestamp")
        if ts is None:
            ts = time.time()

        return AudioPacket(
            channel_id=channel_id,
            timestamp=float(ts),
            samples=samples,
        )
