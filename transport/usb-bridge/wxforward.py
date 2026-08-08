"""USB transport (fallback): expose the phone's WeKit port to another host.

⚠️ **This is the fallback, not the recommended transport.** Prefer reaching the
phone over WiFi (see ../README.md). On the reference deployment this USB path
was measured at roughly 170 long-poll failures per hour — about one break every
21 seconds — because the host's adb server crashed and respawned on its own
every 10-30 seconds. `adb forward` rules live in the adb server's memory, so
each crash silently vaporised the forward and killed the in-flight long poll.
The phone's USB connection was never the problem; `adb devices` reported
`device` throughout.

Use this only when the agent host genuinely cannot reach the phone over the
network, and expect to babysit it.

It does two things:

1. keeps `adb forward tcp:<port> tcp:<port>` alive, rebuilding it **only when it
   is actually missing** — re-running `adb forward` unconditionally replaces the
   rule and severs every active connection, which by itself is enough to break
   every long poll;
2. accepts TCP on LISTEN_HOST:LISTEN_PORT and proxies to the forwarded port,
   so an agent in a VM/WSL (which cannot see the host's adb forward on
   127.0.0.1) can still reach it.

Configuration by environment variable:

    WEKIT_ADB_PATH      path to adb            (default: "adb")
    WEKIT_ADB_SERIAL    device serial          (default: the only attached device)
    WEKIT_PHONE_PORT    WeKit port on phone    (default: 3001)
    WEKIT_BRIDGE_HOST   listen address         (default: 127.0.0.1)
    WEKIT_BRIDGE_PORT   listen port            (default: 13001)

⚠️ WEKIT_BRIDGE_HOST defaults to 127.0.0.1 on purpose. Binding 0.0.0.0 exposes
full control of the WeChat account to everything on the network, guarded only by
a bearer token sent in cleartext. Widen it only deliberately.
"""

import contextlib
import os
import socket
import subprocess
import threading
import time

ADB = os.getenv("WEKIT_ADB_PATH", "adb")
SERIAL = os.getenv("WEKIT_ADB_SERIAL") or ""
PHONE_PORT = int(os.getenv("WEKIT_PHONE_PORT") or 3001)
LISTEN = (os.getenv("WEKIT_BRIDGE_HOST", "127.0.0.1"),
          int(os.getenv("WEKIT_BRIDGE_PORT") or 13001))
TARGET = ("127.0.0.1", PHONE_PORT)


def _adb(*args: str, timeout: int = 15):
    cmd = [ADB]
    if SERIAL:
        cmd += ["-s", SERIAL]
    return subprocess.run(cmd + list(args), timeout=timeout,
                          capture_output=True, text=True)


def ensure_forward() -> None:
    """Rebuild the forward only when it is missing.

    Never re-run `adb forward` unconditionally: it replaces the rule and cuts
    every live connection, which would break the long poll on every pass.
    """
    spec = f"tcp:{PHONE_PORT}"
    while True:
        try:
            r = _adb("forward", "--list")
            alive = r.returncode == 0 and spec in (r.stdout or "")
            if not alive:
                _adb("forward", spec, spec)
        except Exception:
            pass
        time.sleep(20)


def pipe(a: socket.socket, b: socket.socket) -> None:
    try:
        while True:
            data = a.recv(65536)
            if not data:
                break
            b.sendall(data)
    except OSError:
        pass
    finally:
        for s in (a, b):
            with contextlib.suppress(OSError):
                s.close()


def main() -> None:
    threading.Thread(target=ensure_forward, daemon=True).start()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(LISTEN)
    srv.listen(64)
    print(f"bridge listening on {LISTEN[0]}:{LISTEN[1]} -> {TARGET[0]}:{TARGET[1]}",
          flush=True)
    while True:
        try:
            client, _ = srv.accept()
        except OSError:
            continue
        try:
            upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upstream.settimeout(10)
            upstream.connect(TARGET)
            upstream.settimeout(None)
        except OSError:
            with contextlib.suppress(OSError):
                client.close()
            continue
        threading.Thread(target=pipe, args=(client, upstream), daemon=True).start()
        threading.Thread(target=pipe, args=(upstream, client), daemon=True).start()


if __name__ == "__main__":
    main()
