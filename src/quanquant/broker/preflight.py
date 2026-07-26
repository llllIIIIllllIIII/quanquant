"""下單子系統啟動前檢查（Task 8）。

ORDER_MODE 拼錯是設定錯誤 → raise RuntimeError——這是唯一會讓 web/app.py 的 lifespan
主動判斷「不能悄悄用錯 mode，必須明確處理」的情況；app.py 接住這個例外後仍讓其餘系統
（行情/日誌/一般 web 功能）正常啟動，只是下單子系統關閉並反映在 /healthz（見 app.py 對
本函式呼叫端的 try/except 說明）。

其餘（缺 key/owner/real 模式缺 CA）是「這台環境還沒準備好下單」→ 軟性停用（回傳
(False, reason)，不 raise），app 其餘功能正常運作。
"""
import os
import stat

from quanquant.config import Settings

_VALID_MODES = frozenset(("sim", "real"))


def _ca_file_permissions_ok(ca_path: str) -> tuple[bool, str | None]:
    """CA 檔必須存在、權限恰好 0600、且屬於目前執行 process 的 UID（Task 10, round3 F8）。
    bind-mount 唯讀掛載時仍可能被錯誤地開太寬權限或掛錯 owner，這裡是 CA 檔案存在性檢查
    之外的最後一道防線——單純「檔案存在」不足以防止其他系統帳號/容器讀到私鑰檔。"""
    try:
        st = os.stat(ca_path)
    except OSError:
        return False, f"CA 檔案不存在或無法讀取: {ca_path}"
    mode = stat.S_IMODE(st.st_mode)
    if mode != 0o600:
        return False, f"CA 檔案權限必須是 0600，目前是 {oct(mode)}: {ca_path}"
    if st.st_uid != os.getuid():
        return False, f"CA 檔案 owner 不是目前執行的使用者（UID {os.getuid()}）: {ca_path}"
    return True, None


def order_subsystem_preflight(settings: Settings) -> tuple[bool, str | None]:
    if settings.order_mode not in _VALID_MODES:
        raise RuntimeError(f"ORDER_MODE 必須是 sim/real，收到 {settings.order_mode!r}（拒絕啟動下單子系統）")

    if not settings.shioaji_trade_api_key or not settings.shioaji_trade_secret_key:
        return False, "缺 SHIOAJI_TRADE_API_KEY/SHIOAJI_TRADE_SECRET_KEY，下單子系統停用"
    if not settings.order_owner_user_ids.strip():
        return False, "未設定 ORDER_OWNER_USER_IDS，下單子系統停用"

    if settings.order_mode == "real":
        if not (settings.shioaji_ca_path and settings.shioaji_ca_passwd and settings.shioaji_person_id):
            return False, "real 模式缺 CA 路徑/密碼/身分證字號，下單子系統停用"
        if not os.path.isfile(settings.shioaji_ca_path):
            return False, f"CA 檔案不存在: {settings.shioaji_ca_path}"
        perms_ok, perms_reason = _ca_file_permissions_ok(settings.shioaji_ca_path)
        if not perms_ok:
            return False, perms_reason

    return True, None
