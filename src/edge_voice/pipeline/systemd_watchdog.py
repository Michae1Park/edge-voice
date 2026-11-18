from __future__ import annotations

import logging
import os
import socket

logger = logging.getLogger(__name__)


def notify(state: str) -> bool:
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return False

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
        logger.debug("sd_notify(%r) failed", state, exc_info=True)
        return False


def available() -> bool:
    return bool(os.environ.get("NOTIFY_SOCKET"))
