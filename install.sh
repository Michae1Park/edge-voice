#!/usr/bin/env bash
set -euo pipefail

# Full bootstrap for deploying edge-voice as an unattended systemd service on
# a Debian/Raspberry Pi OS edge device: system packages, a Python venv, the
# package itself, then the systemd unit templated to this install and
# enabled.
#
# For local development, use `make install` + `source venv/bin/activate`
# instead -- this script targets the standing-deployment case, not the
# manual dev loop, so it deliberately:
#   - skips the `[dev]` extras (pytest/ruff/mypy have no business running
#     unattended on the device)
#   - installs + enables the systemd unit, which `make install` does not
#
# Usage: ./install.sh   (run as the user the service should run as; uses
# sudo itself for the apt/systemd steps that need it, same pattern as the
# Makefile -- do not run the whole script with sudo, or the service will be
# templated to run as root)

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

if ! command -v python3.12 >/dev/null 2>&1; then
    echo "error: python3.12 not found. Install it first (see README.md#requirements)." >&2
    exit 1
fi

echo "==> Installing system packages (apt)"
sudo apt-get update
sudo apt-get install -y libportaudio2 mosquitto mosquitto-clients

echo "==> Creating virtualenv (venv/)"
test -d venv || python3.12 -m venv venv

echo "==> Installing edge-voice"
venv/bin/pip install --extra-index-url https://download.pytorch.org/whl/cpu -e .

echo "==> Installing systemd service"
# $SUDO_USER, not $(whoami): if this script itself were run under sudo,
# whoami would report root, and the service would end up running as root
# instead of the intended device user. Falls back to whoami for the
# expected case of running this unprivileged (it invokes sudo itself, only
# for the apt/systemctl lines above/below).
SERVICE_USER="${SUDO_USER:-$(whoami)}"
sed \
    -e "s|^User=.*|User=${SERVICE_USER}|" \
    -e "s|^WorkingDirectory=.*|WorkingDirectory=${REPO_DIR}|" \
    -e "s|^ExecStart=.*|ExecStart=${REPO_DIR}/venv/bin/edge-voice|" \
    -e "s|^ReadWritePaths=.*|ReadWritePaths=${REPO_DIR}|" \
    deploy/edge-voice.service | sudo tee /etc/systemd/system/edge-voice.service > /dev/null

sudo systemctl daemon-reload
sudo systemctl enable --now edge-voice

echo "==> Done. edge-voice is running under systemd as '${SERVICE_USER}'."
echo "    systemctl status edge-voice"
echo "    journalctl -u edge-voice -f"
