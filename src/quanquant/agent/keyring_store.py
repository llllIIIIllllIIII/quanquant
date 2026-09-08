"""OS keychain 憑證儲存層（spec §5.2）。

- `check_secure_backend()`：啟動時能力檢查，fail-closed——明確拒絕 fail/plaintext/
  未鎖定/檔案型 backend；呼叫端在此回傳 False 時不得寫入，絕不退回明文檔案。
- token 筆（`save_token`/`rotate_token_secret`/`load_token`）與永豐憑證筆
  （`save_broker_credentials`/`load_broker_credentials`）各自逐筆自治：一筆的寫入
  失敗只影響、只復原這一筆自己的快照，兩筆互不回滾。
- `rotate_token_secret` 先刪後寫：刪除失敗即 fail closed，不繼續寫入新值。
- `clear_secret`/`clear_profile`：只刪 keyring 這一筆／這個 profile 的兩筆，
  registry entry 的清除由呼叫端另外處理。
"""

import json
from dataclasses import dataclass

import keyring
import keyring.errors

_SERVICE_NAME = "quanquant-agent"
_UNSAFE_BACKEND_MODULE_PREFIXES = (
    "keyring.backends.fail", "keyring.backends.null", "keyring.backends.chainer",
    "keyrings.alt",
)


@dataclass(frozen=True)
class KeyringResult:
    ok: bool
    error: str | None = None


def check_secure_backend() -> bool:
    """啟動時能力檢查（spec §5.2）：明確拒絕 fail/plaintext/未鎖定/檔案型 backend。"""
    backend = keyring.get_keyring()
    module = type(backend).__module__
    return not any(module.startswith(prefix) for prefix in _UNSAFE_BACKEND_MODULE_PREFIXES)


def _token_key(site_origin: str, profile_id: str) -> str:
    return f"token:{site_origin}:{profile_id}"


def _broker_key(site_origin: str, profile_id: str) -> str:
    return f"broker:{site_origin}:{profile_id}"


def save_token(
    *, site_origin: str, profile_id: str, token: str, expires_at: str, username: str,
) -> KeyringResult:
    payload = json.dumps({"token": token, "expires_at": expires_at, "username": username})
    try:
        keyring.set_password(_SERVICE_NAME, _token_key(site_origin, profile_id), payload)
    except keyring.errors.KeyringError as exc:
        return KeyringResult(ok=False, error=f"儲存失敗：{exc}")
    return KeyringResult(ok=True)


def rotate_token_secret(
    *, site_origin: str, profile_id: str, new_token: str, expires_at: str, username: str,
) -> KeyringResult:
    """先刪後寫：舊枚已被 server 撤銷，順序固定──①先刪 ②再寫。刪除失敗 → fail closed，
    不繼續寫入。各 backend 對『刪除不存在的 key』與『真正刪除失敗』的例外語意不一致，
    先用 get_password 探測是否存在：不存在則跳過刪除（視為成功）；存在則呼叫
    delete_password，任何例外都視為刪除失敗（寧可誤判也不要在無法確認舊值已清除的情況
    下寫入新值）。"""
    key = _token_key(site_origin, profile_id)
    try:
        existing = keyring.get_password(_SERVICE_NAME, key)
    except keyring.errors.KeyringError:
        existing = None
    if existing is not None:
        try:
            keyring.delete_password(_SERVICE_NAME, key)
        except keyring.errors.KeyringError:
            return KeyringResult(
                ok=False, error="無法安全更新授權，請至『清除已存憑證』手動清除後重試",
            )
    payload = json.dumps({"token": new_token, "expires_at": expires_at, "username": username})
    try:
        keyring.set_password(_SERVICE_NAME, key, payload)
    except keyring.errors.KeyringError as exc:
        return KeyringResult(
            ok=False, error=f"儲存失敗，請點『重試儲存』；儲存成功前請勿關閉：{exc}",
        )
    return KeyringResult(ok=True)


def load_token(*, site_origin: str, profile_id: str) -> dict | None:
    raw = keyring.get_password(_SERVICE_NAME, _token_key(site_origin, profile_id))
    return json.loads(raw) if raw else None


def save_broker_credentials(
    *, site_origin: str, profile_id: str, api_key: str, secret_key: str,
) -> KeyringResult:
    """逐筆自治：寫入前先讀既有值快照。若快照存在，先把快照值「寫回」同一把 key 一次——
    這一步等同即時確認 backend 現在確實可寫、且舊值當下仍完整落地；寫回失敗就直接
    fail closed，不冒險嘗試覆蓋新值（此時尚未動到任何資料，不宣稱原值已保留，因為
    連「backend 現在能不能寫」都無法確認）。寫回成功後才嘗試寫入新值——這樣一旦新值
    寫入失敗，舊值早已於寫回那一步重新確認落地，keyring 端不需要再補一次復原動作，
    也就不會有「復原本身也失敗」的中間態。首次建立（無快照）則單純寫入一次。
    （呼叫端另外分別呼叫 save_token，兩者互不回滾——這裡只管永豐這一筆自身。）"""
    key = _broker_key(site_origin, profile_id)
    try:
        snapshot = keyring.get_password(_SERVICE_NAME, key)
    except keyring.errors.KeyringError:
        snapshot = None
    if snapshot is not None:
        try:
            keyring.set_password(_SERVICE_NAME, key, snapshot)
        except keyring.errors.KeyringError:
            return KeyringResult(
                ok=False,
                error="儲存失敗且原值可能遺失——請至『清除已存憑證』檢查後重新設定",
            )
    payload = json.dumps({"api_key": api_key, "secret_key": secret_key})
    try:
        keyring.set_password(_SERVICE_NAME, key, payload)
    except keyring.errors.KeyringError as exc:
        return KeyringResult(ok=False, error=f"儲存失敗，原值已保留：{exc}")
    return KeyringResult(ok=True)


def load_broker_credentials(*, site_origin: str, profile_id: str) -> dict | None:
    raw = keyring.get_password(_SERVICE_NAME, _broker_key(site_origin, profile_id))
    return json.loads(raw) if raw else None


def clear_secret(*, site_origin: str, profile_id: str, which: str) -> KeyringResult:
    """which ∈ {"token","broker"}；只刪這一筆，registry entry 由呼叫端另外處理（不動）。"""
    key = _token_key(site_origin, profile_id) if which == "token" else _broker_key(site_origin, profile_id)
    try:
        keyring.delete_password(_SERVICE_NAME, key)
    except keyring.errors.PasswordDeleteError:
        pass  # 本來就沒有這筆，視同已清除
    except keyring.errors.KeyringError as exc:
        return KeyringResult(ok=False, error=f"清除失敗：{exc}")
    return KeyringResult(ok=True)


def clear_profile(*, site_origin: str, profile_id: str) -> KeyringResult:
    """整個 profile 兩筆都刪（供刪除 profile 用）。"""
    token_result = clear_secret(site_origin=site_origin, profile_id=profile_id, which="token")
    broker_result = clear_secret(site_origin=site_origin, profile_id=profile_id, which="broker")
    if not (token_result.ok and broker_result.ok):
        return KeyringResult(ok=False, error=token_result.error or broker_result.error)
    return KeyringResult(ok=True)
