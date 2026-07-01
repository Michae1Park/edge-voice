# edge-voice

Real-time dual-channel (Rx/Tx) phone-call transcription for edge devices (Raspberry Pi 5, Jetson) using Silero VAD and Moonshine STT, with audio streamed over MQTT.

See `docs/architecture.md` for the full design doc and architecture rationale.

## Quick start

```bash
git clone https://github.com/Michae1Park/edge-voice.git
cd edge-voice
scripts/setup_venv.sh --compile   # first time: resolves + installs deps
source .venv/bin/activate
pytest                            # run unit tests
```

### Dev setup
```bash
cd edge-voice

source venv/bin/activate

pip install -U pip
pip install -e ".[dev]"

pytest
ruff check .
mypy src
```

### How to run

The pipeline reads audio from MQTT. WAV source and the pipeline run as separate processes:

#### Terminal 1 — start the pipeline:

```bash
python -m edge_voice.cli 
```

#### Terminal 2 — publish WAV audio to the pipeline:

```bash
python -m edge_voice.utils.audio_generation.wav_source --wav wav/conversation_60s.wav --channels rx tx
```

### Architecture

Two processes communicate via MQTT, everything else is in-process threading:

```
WAV source          MQTT broker        Pipeline (in-process)
(separate process)   (optional)        ╔═══════════════════════╗
  │                     │                ║ MqttAudioIngest       ║
  │  publishes PCM      ║  subscribes    ║  ┌─────────────────┐ ║
  │  frame frames to    ║                 ║  │ channel router  │ ║
  │  MQTT topics        ║                 ║  │                 │ ║
  │  ──────────────>    ║  <──────────    ║  │ FakeVAD         │ ║
  │                     ║                 ║  │                 │ ║
  │                     ║                 ║  │ FakeSTT         │ ║
  ╚═════════════════════╩══════════════════╩═╩═══════════════════╝

All stages inside the pipeline (MqttAudioIngest → router → VAD → STT)
communicate via in-memory queue.Queue — no MQTT between them.
```

### Running in-process (test only)

For local testing without a separate WAV source process, the pipeline also supports
internal audio generation:

```bash
python -m edge_voice.cli --wav-file /path/to/audio.wav 
```

This uses `WavSource` internally (bypasses MQTT) for quick integration tests.