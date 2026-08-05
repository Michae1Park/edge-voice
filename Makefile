.PHONY: req install install-service test lint format typecheck ci

req:
	pip-compile \
		--extra=dev \
		--extra-index-url https://download.pytorch.org/whl/cpu \
		-o requirements-dev.txt \
		pyproject.toml

# Native PortAudio lib (Debian/Raspberry Pi OS package, not pip) is needed
# for sounddevice, used by mic_source.py to capture from a live mic.
install:
	sudo apt-get update
	sudo apt-get install -y libportaudio2
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