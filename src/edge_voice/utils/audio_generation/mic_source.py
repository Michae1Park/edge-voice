"""Live microphone audio source that publishes PCM frames to MQTT for the
pipeline.

Captures from a system microphone, splits into 20ms chunks, and publishes
each frame as raw PCM bytes (no envelope) to the configured MQTT topics --
same wire format as wav_source_raw.py, which MqttAudioIngest consumes
directly as PCM samples (audio_ingest/mqtt_client.py:_on_message). This is
what lets edge-voice (run via cli.py, listening on stt/audio_chunks_*)
simply receive and transcribe whatever this script captures.

Run as a separate process, alongside the real edge-voice pipeline:
    python -m edge_voice.utils.audio_generation.mic_source --channel rx
    python -m edge_voice.utils.audio_generation.mic_source --list-devices

Not imported by cli.py/orchestrator.py -- a live mic is a single ongoing
capture, not something the pipeline itself owns; it's a separate process
publishing over MQTT, just like a real call leg (or wav_source_raw.py) would.
"""

from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Sequence

import paho.mqtt.client as mqtt  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# Matches configs/default.yaml audio.sample_rate / the 20ms chunk every
# other audio_generation source uses.
SAMPLE_RATE = 16000
CHUNK_SAMPLES = 320
MQTT_QOS = 1


def _list_devices() -> None:
    import sounddevice as sd

    for i, info in enumerate(sd.query_devices()):
        print(
            f"{i}: {info['name']} ({info['max_input_channels']} in, {info['max_output_channels']} out)"
        )


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Publish live microphone audio to MQTT for the edge-voice pipeline"
    )
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument(
        "--channels",
        nargs="+",
        default=["rx"],
        help=(
            "Channel(s) to publish to (e.g. rx, or rx tx to duplicate this "
            "one mic to both -- a live mic is one feed, so 'rx' alone is "
            "the common case)"
        ),
    )
    parser.add_argument("--sr", type=int, default=SAMPLE_RATE, help="Capture sample rate")
    parser.add_argument("--chunk", type=int, default=CHUNK_SAMPLES, help="Chunk size in samples")
    parser.add_argument(
        "-d", "--device", type=int, default=None, help="sounddevice input device index"
    )
    parser.add_argument(
        "--list-devices", action="store_true", help="List available audio devices and exit"
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO)

    if args.list_devices:
        _list_devices()
        return

    import sounddevice as sd

    topic_map = {ch: f"stt/audio_chunks_{ch}" for ch in args.channels}

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    connected = False

    def on_connect(_client, _userdata, flags, rc, props):
        nonlocal connected
        connected = True

    client.on_connect = on_connect  # type: ignore[assignment]
    client.connect(args.broker, args.port)
    client.loop_start()

    while not connected:
        time.sleep(0.05)

    logger.info(
        "MicSource: capturing from device=%s at %d Hz -> %s",
        args.device if args.device is not None else "default",
        args.sr,
        {ch: topic_map[ch] for ch in args.channels},
    )

    stream = sd.RawInputStream(
        samplerate=args.sr,
        channels=1,
        dtype="int16",
        blocksize=args.chunk,
        device=args.device,
    )
    stream.start()

    frame_num = 0
    start = time.time()
    try:
        while True:
            # A blocking read paces this loop at real time on its own --
            # unlike wav_source_raw.py, which reads a file near-instantly
            # and has to throttle itself to match.
            raw, _overflowed = stream.read(args.chunk)
            payload = bytes(raw)
            for ch in args.channels:
                client.publish(topic_map[ch], payload, qos=MQTT_QOS)
            frame_num += 1
    except KeyboardInterrupt:
        pass
    finally:
        stream.stop()
        stream.close()
        client.loop_stop()
        client.disconnect()
        elapsed = time.time() - start
        logger.info("MicSource: stopped after %d frames (%.1fs)", frame_num, elapsed)


if __name__ == "__main__":
    main()
