import os
import socket

from edge_voice.pipeline import systemd_watchdog


def test_notify_is_noop_without_socket(monkeypatch):
    monkeypatch.delenv("NOTIFY_SOCKET", raising=False)
    assert systemd_watchdog.available() is False
    assert systemd_watchdog.notify("WATCHDOG=1") is False


def test_notify_sends_datagram_to_socket(monkeypatch, tmp_path):
    sock_path = str(tmp_path / "notify.sock")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    server.bind(sock_path)
    server.settimeout(2.0)
    try:
        monkeypatch.setenv("NOTIFY_SOCKET", sock_path)
        assert systemd_watchdog.available() is True

        assert systemd_watchdog.notify("WATCHDOG=1") is True
        assert server.recv(64) == b"WATCHDOG=1"

        assert systemd_watchdog.notify("READY=1") is True
        assert server.recv(64) == b"READY=1"
    finally:
        server.close()
        os.unlink(sock_path)


def test_notify_does_not_raise_on_dead_socket(monkeypatch, tmp_path):
    monkeypatch.setenv("NOTIFY_SOCKET", str(tmp_path / "nonexistent.sock"))
    assert systemd_watchdog.notify("WATCHDOG=1") is False
