.PHONY: req install install-service test lint format typecheck ci

req:
	pip-compile \
		--extra=dev \
		--extra-index-url https://download.pytorch.org/whl/cpu \
		-o requirements-dev.txt \
		pyproject.toml

# System packages (Debian/Raspberry Pi OS, not pip):
#   libportaudio2      -- native lib behind sounddevice, used by mic_source.py
#   mosquitto          -- the MQTT broker the pipeline ingests audio from;
#                         apt enables it on localhost:1883, matching the
#                         mqtt.broker_host/broker_port defaults
#   mosquitto-clients  -- mosquitto_sub/pub, for inspecting topics by hand
install:
	sudo apt-get update
	sudo apt-get install -y libportaudio2 mosquitto mosquitto-clients
	pip install --extra-index-url https://download.pytorch.org/whl/cpu -e ".[dev]"

# Deploys/restarts the systemd unit -- separate from `install` since this
# needs sudo and restarts the live service, unlike a routine dependency
# refresh. Re-run after editing deploy/edge-voice.service.
install-service:
	sudo cp deploy/edge-voice.service /etc/systemd/system/
	sudo systemctl daemon-reload
	sudo systemctl enable --now edge-voice

test:
	pytest

lint:
	ruff check .

format:
	ruff format .

typecheck:
	mypy --package edge_voice

ci:
	ruff check .
	ruff format --check .
	mypy --package edge_voice
	pytest