"""Minimal sd_notify client for the systemd hardware/software watchdog.

Milestone 6, layer 3 (docs/BUILDPLAN.md): the app periodically tells systemd
"I'm alive" (`WATCHDOG=1`). If those pings stop -- because the process hung,
deadlocked, or was OOM-killed, none of which in-process supervision can catch
from inside the same wedged process -- systemd restarts the whole unit.

This is deliberately dependency-free (no `systemd` python package): the
protocol is a single datagram written to the `$NOTIFY_SOCKET` UNIX socket, so
the stdlib `socket` module is all it takes, and one fewer native dependency to
cross-compile for the target board.

Everything here is a **no-op when `$NOTIFY_SOCKET` is unset** -- i.e. whenever
the process was not launched by a systemd unit with `NotifyAccess=`. That is
what keeps it safe to leave enabled in dev, CI, and any off-device run:
`notify()` simply returns False and changes nothing. It only does real work
under an actual systemd unit (see deploy/edge-voice.service).
"""

from __future__ import annotations

import logging
import os
import socket

logger = logging.getLogger(__name__)


def notify(state: str) -> bool:
    """Send one sd_notify datagram (e.g. "WATCHDOG=1", "READY=1").

    Returns True if a datagram was sent, False if there was no systemd socket
    to send to (the normal case off-device) or the send failed. Never raises --
    a watchdog helper must not be able to crash the thread it pings from.
    """
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return False

    # systemd abstract-namespace sockets start with '@', encoded as a leading
    # NUL byte in the sockaddr. A leading '/' is a normal filesystem path.
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    elif not addr.startswith("/"):
        logger.debug("NOTIFY_SOCKET=%r is neither abstract nor absolute; ignoring", addr)
        return False

    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM | socket.SOCK_CLOEXEC) as sock:
            sock.connect(addr)
            sock.sendall(state.encode("utf-8"))
        return True
    except OSError:
        # Broker/socket gone is not fatal to us -- if pings genuinely stop
        # arriving, that IS the signal the watchdog exists to act on.
        logger.debug("sd_notify(%r) failed", state, exc_info=True)
        return False


def available() -> bool:
    """True if a systemd notify socket is present (i.e. notify() will act)."""
    return bool(os.environ.get("NOTIFY_SOCKET"))
