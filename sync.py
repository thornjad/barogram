import json
import time
import tomllib
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SyncConfig:
    api_key: str
    folder_id: str
    url: str = "http://localhost:8384"
    timeout: int = 30


def load_env(path: Path) -> SyncConfig | None:
    """Load sync config from a local env file. Returns None if not configured."""
    if not path.exists():
        return None
    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except tomllib.TOMLDecodeError:
        return None
    section = data.get("syncthing")
    if not section:
        return None
    api_key = section.get("api_key", "").strip()
    folder_id = section.get("folder_id", "").strip()
    if not api_key or not folder_id:
        return None
    return SyncConfig(
        api_key=api_key,
        folder_id=folder_id,
        url=section.get("url", "http://localhost:8384"),
        timeout=int(section.get("timeout", 30)),
    )


def wait_for_idle(conf: SyncConfig) -> bool | None:
    """
    Wait until the Syncthing folder is idle (no active sync in progress).

    Returns True if idle within the timeout.
    Returns False if Syncthing is running but the folder did not go idle within
    the timeout — the caller should abort to avoid writing during an active sync.
    Returns None if Syncthing is unreachable (daemon not running or not
    configured on this machine) — the caller may proceed safely.

    Offline remote devices do not affect this check: we only wait for the local
    sync engine to finish processing any in-progress transfer.
    """
    url = f"{conf.url}/rest/db/status?folder={conf.folder_id}"
    headers = {"X-API-Key": conf.api_key}
    deadline = time.monotonic() + conf.timeout

    while time.monotonic() < deadline:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            state = data.get("state", "")
            if state == "idle":
                return True
            print(f"  syncthing: folder is {state!r}, waiting...")
            time.sleep(2)
        except (urllib.error.URLError, OSError, TimeoutError):
            return None
        except Exception:
            return None

    return False
