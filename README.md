# edge-voice

Real-time dual-channel (Rx/Tx) phone-call transcription for edge devices (Raspberry Pi 5, Jetson) using Silero VAD and Moonshine STT, with audio streamed over MQTT.

See `docs/design.md` for the full design doc and architecture rationale.

## Quick start

```bash
git clone https://github.com/Michae1Park/edge-voice.git
cd edge-voice
scripts/setup_venv.sh --compile   # first time: resolves + installs deps
source .venv/bin/activate
pytest                            # run unit tests
```

## Repo layout

See `docs/design.md` → "Repository structure" for what lives where.
