"""GUI 啟動決策樹＋profile fallback 規則（spec §5.3）：`run_gui()`（Task 8/13）在①預綁
socket②建 app 之後、③開瀏覽器之前呼叫 `resolve_gui_startup()`，依回傳的
`GuiStartupDecision` 決定要開哪一頁（`/profiles`／`/setup`／直接背景連線後開 `/status`）。

- `resolve_gui_startup()`：0 筆 profile → 精靈①；`--profile` miss → 精靈①＋提示；
  多筆且無 hint → `/profiles` 選擇頁；單筆（或 hint 命中）→ 依 keyring 憑證完整度決定
  `direct`／精靈①（token 缺/過期）／精靈②（永豐憑證缺）；`--reset` 一律強制精靈①。
- `reconcile_profile_after_approval()`：device flow 核准後拿到的 `profile_id` 若與
  「這輪精靈原本預期的 profile」不符（或本來就沒有預期對象），一律以 registry
  `(site_origin, profile_id)` 唯一鍵查找——命中就沿用其 `buffer_path`（絕不沿用
  `expected_profile` 的任何欄位），沒命中就用 `buffer_path_for()` 隔離新建。這同時是
  「registry 指向的 keyring entry 被外部刪除→回精靈重建→重建後沿用原 buffer」這條
  spec 規則的實作機制：只要重建後拿到同一個 `profile_id`，`find_profile` 自然命中同一筆。
- `check_legacy_buffer_conflict()`：Inc0 單帳號時代遺留在 `~/.quanquant-agent/outbox.db`
  的舊 buffer，若還有未送出的資料，這台機器第一次從 headless 轉出一個全新
  `(site_origin, profile_id)` 時擋下 launch（見 `setup_routes.py::
  _guard_legacy_buffer_before_first_launch`）——絕不自動搬移或忽略。
- `probe_direct_connect()`：`kind="direct"` 啟動時的觀察，見其 docstring——非短窗賭運氣，
  `TokenRejectedError` 只透過 `runner.py::run_once()` 的單一集中判斷點寫入 "rejected"，
  這個狀態一旦出現就是穩定終態。
"""
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from quanquant.agent import keyring_store, profile_registry
from quanquant.agent.buffer import DurableBuffer
from quanquant.agent.profile_registry import ProfileEntry

LEGACY_DEFAULT_BUFFER = Path.home() / ".quanquant-agent" / "outbox.db"


@dataclass(frozen=True)
class GuiStartupDecision:
    kind: str                        # "profile_select" | "setup" | "direct"
    profile: ProfileEntry | None = None
    start_step: int = 1              # 精靈從哪一步開始（1=裝置授權/2=永豐憑證/3=確認）
    notice: str | None = None        # 給 UI 顯示的提示文案


def _is_expired(expires_at_iso: str) -> bool:
    deadline = datetime.fromisoformat(expires_at_iso)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return now >= deadline


def resolve_gui_startup(
    *, site_origin: str, profile_hint: str | None, reset: bool,
) -> GuiStartupDecision:
    entries = profile_registry.list_profiles(site_origin=site_origin)

    if profile_hint is not None:
        match = next((e for e in entries if e.profile_id == profile_hint), None)
        if match is None:
            return GuiStartupDecision(kind="setup", start_step=1, notice="找不到此帳號設定")
        profile = match
    elif len(entries) == 0:
        return GuiStartupDecision(kind="setup", start_step=1)
    elif len(entries) == 1:
        profile = entries[0]
    else:
        return GuiStartupDecision(kind="profile_select")

    if reset:
        return GuiStartupDecision(kind="setup", start_step=1, profile=profile)

    token = keyring_store.load_token(site_origin=site_origin, profile_id=profile.profile_id)
    if token is None or _is_expired(token["expires_at"]):
        # 涵蓋兩種情境：從沒存過 token，或 keyring entry 被外部刪除／已過期——一律回精靈
        # 步驟①重建，profile 帶著原本的 buffer_path 一起傳回去，完成後沿用同一路徑。
        return GuiStartupDecision(kind="setup", start_step=1, profile=profile)

    broker = keyring_store.load_broker_credentials(site_origin=site_origin, profile_id=profile.profile_id)
    if broker is None:
        return GuiStartupDecision(kind="setup", start_step=2, profile=profile)

    return GuiStartupDecision(kind="direct", profile=profile)


def reconcile_profile_after_approval(
    *, site_origin: str, expected_profile: ProfileEntry | None, approved_profile_id: str, username: str,
) -> tuple[ProfileEntry, bool]:
    """核准頁登入了另一個帳號（approved_profile_id 與 expected_profile 不符，或本來就是
    全新精靈沒有 expected_profile）時的切換規則：已存在該 profile_id → 沿用其
    buffer_path（不沿用 expected_profile 的任何東西）；不存在 → 隔離新建。"""
    existing = profile_registry.find_profile(site_origin=site_origin, profile_id=approved_profile_id)
    if existing is not None:
        return existing, False
    buffer_path = str(profile_registry.buffer_path_for(site_origin=site_origin, profile_id=approved_profile_id))
    new_entry = ProfileEntry(profile_id=approved_profile_id, username=username,
                              buffer_path=buffer_path, created_at=datetime.now(timezone.utc).isoformat())
    return new_entry, True


def check_legacy_buffer_conflict() -> str | None:
    if not LEGACY_DEFAULT_BUFFER.exists():
        return None
    pending = DurableBuffer(str(LEGACY_DEFAULT_BUFFER)).unsent_count()
    if pending == 0:
        return None
    return (
        f"偵測到舊路徑 {LEGACY_DEFAULT_BUFFER} 有 {pending} 筆尚未送出的回報——為避免遺漏，"
        "本精靈不會自動搬移或忽略這批資料。請先以原本的指令列方式（headless，沿用舊設定）"
        "啟動 agent 跑到這批資料送完，或聯絡維運人員人工處理，確認清空後再重新執行本精靈。"
    )


async def probe_direct_connect(runner, *, timeout: float = 5.0) -> str:
    """kind="direct" 啟動時的觀察：poll runner.snapshot().connection 直到出現
    connected/rejected 或逾時。TokenRejectedError 只會透過 run_once() 那唯一一個集中點
    寫入 "rejected"，而且呼叫端一律傳 stop_on_token_reject=True，run_forever() 觀察到後
    立刻 stop()＋return，不會再有下一輪 run_once() 把狀態改回 "reconnecting"——"rejected"
    一旦出現就是穩定終態，這個輪詢迴圈保證看得到，不是短窗賭運氣。逾時仍未出現
    connected/rejected（純網路延遲、server 暫時不可達）→ 當暫時性問題處理，交給背景
    run_forever 繼續照既有 backoff 重試。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = await runner.snapshot()
        if snap.connection in ("connected", "rejected"):
            return snap.connection
        await asyncio.sleep(0.1)
    snap = await runner.snapshot()
    return snap.connection
