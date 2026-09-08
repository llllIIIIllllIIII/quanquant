"""本機非秘密 profile registry（spec §5.3）。

- `profiles.json` 只存非秘密 metadata（profile_id/username/buffer_path/created_at）；
  秘密（token/永豐憑證）一律走 `keyring_store.py`，不進這個檔。
- 讀寫全程跨程序鎖（`profiles.lock`，flock/msvcrt）保護「reload→modify→fsync→
  os.replace」整段，避免多個 agent 程序同時寫壞 registry。
- `(site_origin, profile_id)` 唯一鍵 upsert。
- `buffer_path_for()`：origin_dir/profile_dir 各自 sha256 前 16 hex，
  filesystem-safe，且同 host 異 port 不共用（避免多站台/多埠 outbox 互蓋）。
- `InstanceLock`：綁定解析後的 buffer 路徑，per-profile 單實例 process lock，
  非阻塞 acquire，拿不到就 raise AgentAlreadyRunningError。
"""

import hashlib
import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REGISTRY_PATH = Path.home() / ".quanquant-agent" / "profiles.json"
LOCK_PATH = Path.home() / ".quanquant-agent" / "profiles.lock"


@dataclass(frozen=True)
class ProfileEntry:
    profile_id: str
    username: str
    buffer_path: str
    created_at: str


class AgentAlreadyRunningError(RuntimeError):
    """同一 profile 已有另一個 agent 程序持有 instance lock（spec §5.3 單實例）。"""


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_write(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)  # 同檔系統內 rename，POSIX/NTFS 皆原子


@contextmanager
def _registry_lock(*, timeout: float = 10.0):
    """全域跨程序鎖：flock（POSIX）/ msvcrt.locking（Windows），保護
    「重新載入→修改→fsync→os.replace」整段。"""
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("無法取得 profile registry 鎖（逾時，另一個 agent 程序可能卡住）")
                time.sleep(0.05)
        yield
    finally:
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def upsert_profile(*, site_origin: str, profile_id: str, username: str, buffer_path: str) -> None:
    """(site_origin, profile_id) 唯一鍵 upsert（spec §5.3）。"""
    with _registry_lock():
        data = _load(REGISTRY_PATH)
        entries = data.setdefault(site_origin, [])
        for entry in entries:
            if entry["profile_id"] == profile_id:
                entry["username"] = username
                entry["buffer_path"] = buffer_path
                break
        else:
            entries.append({"profile_id": profile_id, "username": username,
                             "buffer_path": buffer_path, "created_at": _utcnow_iso()})
        _atomic_write(REGISTRY_PATH, data)


def remove_profile(*, site_origin: str, profile_id: str) -> None:
    with _registry_lock():
        data = _load(REGISTRY_PATH)
        remaining = [e for e in data.get(site_origin, []) if e["profile_id"] != profile_id]
        if remaining:
            data[site_origin] = remaining
        else:
            data.pop(site_origin, None)
        _atomic_write(REGISTRY_PATH, data)


def list_profiles(*, site_origin: str) -> list[ProfileEntry]:
    with _registry_lock():
        data = _load(REGISTRY_PATH)
    return [ProfileEntry(**e) for e in data.get(site_origin, [])]


def find_profile(*, site_origin: str, profile_id: str) -> ProfileEntry | None:
    return next((e for e in list_profiles(site_origin=site_origin) if e.profile_id == profile_id), None)


def buffer_path_for(*, site_origin: str, profile_id: str) -> Path:
    """origin_dir = sha256(site_origin) 前 16 hex；profile_dir = sha256(profile_id) 前
    16 hex——固定長度 hex，filesystem-safe（spec §5.3）。"""
    origin_dir = hashlib.sha256(site_origin.encode("utf-8")).hexdigest()[:16]
    profile_dir = hashlib.sha256(profile_id.encode("utf-8")).hexdigest()[:16]
    return Path.home() / ".quanquant-agent" / origin_dir / profile_dir / "outbox.db"


class InstanceLock:
    """綁定 buffer 路徑的單實例 process lock；acquire() 非阻塞，拿不到就
    raise AgentAlreadyRunningError。"""

    def __init__(self, buffer_path: Path) -> None:
        self._lock_path = Path(str(buffer_path) + ".instance.lock")
        self._fd: int | None = None

    def acquire(self) -> None:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise AgentAlreadyRunningError(
                f"同一 profile 已有另一個 agent 程序在跑（lock={self._lock_path}）"
            )
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None
