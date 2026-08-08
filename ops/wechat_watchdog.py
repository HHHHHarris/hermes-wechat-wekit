"""Keep WeChat (and therefore the WeKit API server) alive on the phone.

Health is checked over **HTTP** against the same WeKit endpoint the agent uses,
so a healthy system costs zero adb calls. adb is touched only after the HTTP
probe has failed repeatedly, and then only to see whether WeChat is running and
relaunch it if it is not.

Two hard-won rules encoded here:

* **Never force-stop WeChat.** It tears down the Xposed/WeKit injection, and
  recovering from that takes a phone reboot. Relaunch with `monkey` instead.
* **Do not poll over adb.** An earlier version ran `adb shell pidof` every 60s;
  on the reference host that was enough to disturb the adb server, which is
  itself unstable. HTTP probing avoids adb entirely on the happy path.

Configuration is entirely by environment variable:

    WEKIT_BASE_URL     required, e.g. http://192.168.1.50:3001
    WEKIT_TOKEN        required, the WeKit API bearer token
    WEKIT_ADB_SERIAL   optional, adb device serial; if unset, adb picks the
                       only attached device
    WEKIT_ADB_PATH     optional, path to the adb binary (default: "adb")
    WEKIT_LOG_PATH     optional, log file (default: ./wechat_watchdog.log)
    WEKIT_PROBE_INTERVAL_S     optional, default 120
    WEKIT_FAILS_BEFORE_ADB     optional, default 3
    WEKIT_BOOT_GRACE_S         optional, default 60

Runs anywhere Python 3 and adb are available — Windows (Scheduled Task),
Linux/macOS (systemd unit, launchd, cron @reboot).
"""

import datetime
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE_URL = (os.getenv("WEKIT_BASE_URL") or "").rstrip("/")
TOKEN = os.getenv("WEKIT_TOKEN") or ""
ADB = os.getenv("WEKIT_ADB_PATH", "adb")
SERIAL = os.getenv("WEKIT_ADB_SERIAL") or ""
LOG = os.getenv("WEKIT_LOG_PATH", "wechat_watchdog.log")

WECHAT_PKG = "com.tencent.mm"


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except ValueError:
        return default


INTERVAL = _int_env("WEKIT_PROBE_INTERVAL_S", 120)
FAILS_BEFORE_ADB = _int_env("WEKIT_FAILS_BEFORE_ADB", 3)
BOOT_GRACE = _int_env("WEKIT_BOOT_GRACE_S", 60)


def log(msg: str) -> None:
    line = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


def api_ok() -> bool:
    """HTTP health probe. Deliberately does not touch adb."""
    req = urllib.request.Request(
        f"{BASE_URL}/api/self/info", headers={"Authorization": f"Bearer {TOKEN}"}
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except urllib.error.HTTPError as e:
        # A 401 means we reached the server but the token is wrong — relaunching
        # WeChat will never fix that, so say so plainly instead of looping.
        if e.code == 401:
            log("API returned 401 — WEKIT_TOKEN does not match the token "
                "configured in WeKit. Fix the token; not a WeChat problem.")
        return False
    except Exception:
        return False


def adb_cmd(*args: str, timeout: int = 25):
    cmd = [ADB]
    if SERIAL:
        cmd += ["-s", SERIAL]
    cmd += list(args)
    try:
        return subprocess.run(cmd, timeout=timeout, capture_output=True, text=True)
    except Exception as e:  # noqa: BLE001 - adb failures must never kill the loop
        log(f"adb error {e!r} running {args}")

        class _Failed:
            returncode = -1
            stdout = ""
            stderr = str(e)

        return _Failed()


def revive() -> None:
    """Called only after repeated HTTP failures."""
    pid = (adb_cmd("shell", "pidof", WECHAT_PKG).stdout or "").strip()
    if pid:
        log(f"API down but WeChat is running (pid={pid}) — leaving it alone. "
            "Check the network path, or whether WeKit's API server is enabled.")
        return
    # monkey launches via the normal launcher intent. Never force-stop first:
    # that would kill the Xposed injection and require a reboot to restore.
    r = adb_cmd("shell", "monkey", "-p", WECHAT_PKG,
                "-c", "android.intent.category.LAUNCHER", "1")
    log(f"API down and WeChat not running -> relaunched (rc={r.returncode}). "
        "The WeKit server needs a DexKit scan on first start; allow a few minutes.")


def main() -> int:
    if not BASE_URL or not TOKEN:
        print("WEKIT_BASE_URL and WEKIT_TOKEN must be set", file=sys.stderr)
        return 2

    log(f"watchdog started (probe {BASE_URL} every {INTERVAL}s)")
    time.sleep(BOOT_GRACE)
    fails = 0
    while True:
        try:
            if api_ok():
                if fails:
                    log(f"API recovered after {fails} consecutive failures")
                fails = 0
            else:
                fails += 1
                log(f"API probe failed ({fails}/{FAILS_BEFORE_ADB})")
                if fails >= FAILS_BEFORE_ADB:
                    revive()
                    fails = 0
        except Exception as e:  # noqa: BLE001 - the watchdog must never die
            log(f"loop error {e!r}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    raise SystemExit(main())
