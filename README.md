# edge-voice

Real-time, dual-channel (Rx/Tx) phone-call transcription for edge devices (Raspberry Pi 5, Jetson), using Silero VAD for speech segmentation and Moonshine for streaming STT. Audio for each call leg arrives as a separate MQTT stream and is transcribed independently, in order, with channel attribution preserved throughout.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full design doc and [`docs/BUILDPLAN.md`](docs/BUILDPLAN.md) for current status and what's next.

## Requirements

- Python 3.12+
- An MQTT broker reachable by the pipeline (e.g. [Mosquitto](https://mosquitto.org/)) — audio ingestion is MQTT-only, there is no direct-mic-to-pipeline path in production use
- Linux/macOS with a working audio backend if using `mic_source.py` for local testing ([`sounddevice`](https://python-sounddevice.readthedocs.io/), no compiler toolchain required)

## Quick start

```bash
git clone https://github.com/Michae1Park/edge-voice.git
cd edge-voice

python3.12 -m venv venv
source venv/bin/activate

make install   # pip install -e ".[dev]"
make test      # pytest
```

## Running the pipeline

The pipeline consumes audio over MQTT, so it expects a broker running (`localhost:1883` by default — see [Configuration](#configuration)) and a source publishing per-channel audio to it. In production the source is a real call leg; for local development, `wav_source.py` replays a `.wav` file over MQTT the same way a real leg would.

**Terminal 1 — start the pipeline:**

```bash
edge-voice
# or: python -m edge_voice.cli
```

**Terminal 2 — publish audio to it:**

```bash
# Synthetic two-channel conversation
python -m edge_voice.utils.audio_generation.wav_source \
    --wav wav/conversation_60s.wav --channels rx tx

# Real recorded call legs, one file per channel
python -m edge_voice.utils.audio_generation.wav_source_raw \
    --wav wav/rx_recorded_1.wav wav/tx_recorded_1.wav --channels rx tx
```

Useful `edge-voice` flags:

| Flag | Default | Description |
|---|---|---|
| `--run-secs N` | `0` | Exit automatically after `N` seconds (`0` = run until Ctrl-C) |
| `--debug` | off | Verbose (`DEBUG`-level) logging |

## Configuration

Settings are layered, lowest to highest precedence:

1. Code defaults (`config/settings.py`)
2. [`configs/default.yaml`](configs/default.yaml)
3. `configs/local.yaml` (gitignored, optional per-deployment overrides)
4. Environment variables: `EDGE_VOICE__<SECTION>__<FIELD>`, e.g. `EDGE_VOICE__VAD__THRESHOLD=0.5`

`configs/default.yaml` documents every tunable inline — MQTT broker/topics, audio format, VAD thresholds and segment-cut limits, STT model/language selection, and queue sizes.

## Architecture

Two logically separate pieces talk only over MQTT; everything after ingestion runs in-process on worker threads connected by bounded queues:

```
 Audio source              MQTT broker              Pipeline (in-process)
 (real call leg, or        ┌──────────┐    ┌──────────────────────────────────┐
  wav_source.py for dev)   │          │    │  MqttAudioIngest                 │
        │  publish PCM     │          │    │        │                        │
        │  per channel  ─▶ │          │ ─▶ │  ChannelRouter                   │
        └──────────────────┘          │    │        │                        │
                            └──────────┘    │  VADWorker  (Silero, per-channel)│
                                             │        │                        │
                                             │  STTWorker  (Moonshine)          │
                                             │        │                        │
                                             │  TranscriptEvent → logs / UI    │
                                             └──────────────────────────────────┘
```

`MqttAudioIngest → ChannelRouter → VADWorker → STTWorker` communicate via in-memory `queue.Queue`s — no MQTT between pipeline stages, only at the boundary.

## Development

```bash
make lint        # ruff check .
make format      # ruff format .
make typecheck   # mypy --package edge_voice
make test        # pytest
make ci          # all of the above, what CI runs
```

CI (`.github/workflows/ci.yml`) runs `make ci` equivalent checks on every push and pull request against `main`.

## Project status

Core pipeline (MQTT ingest → routing → VAD → STT) is real end-to-end. Reliability (worker supervision/restart), observability, health reporting, and the web UI are planned but not yet built — see [`docs/BUILDPLAN.md`](docs/BUILDPLAN.md) for the milestone-by-milestone breakdown.

## License

MIT — see `pyproject.toml`.
