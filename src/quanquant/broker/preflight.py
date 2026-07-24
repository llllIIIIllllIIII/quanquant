"""下單子系統啟動前檢查（Task 8）。

ORDER_MODE 拼錯是設定錯誤 → raise RuntimeError——這是唯一會讓 web/app.py 的 lifespan
主動判斷「不能悄悄用錯 mode，必須明確處理」的情況；app.py 接住這個例外後仍讓其餘系統
（行情/日誌/一般 web 功能）正常啟動，只是下單子系統關閉並反映在 /healthz（見 app.py 對
本函式呼叫端的 try/except 說明）。

其餘（缺 key/owner/real 模式缺 CA）是「這台環境還沒準備好下單」→ 軟性停用（回傳
(False, reason)，不 raise），app 其餘功能正常運作。
"""
import os

from quanquant.config import Settings

_VALID_MODES = frozenset(("sim", "real"))


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

    return True, None
