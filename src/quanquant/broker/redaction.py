"""中央 exception/log sanitizer（Task 10, round3 F8）。

F8 的核心問題：round2 只在 `ShioajiAdapter.place()` 的送單失敗分支手動 redact 一次，其餘
路徑（`update()`、`health_probe()`、watchdog 重連失敗、lifespan shutdown、`/healthz` 的
`OrderSessionState.last_error`、HTTP 表單錯誤訊息）全部原樣把上游例外文字（可能夾帶
api_key/secret_key/ca_passwd/person_id）落地到 log 或直接回顯給使用者/公開端點。

**中央化設計**：本檔是唯一的 redact 實作（`redact_secrets`），所有呼叫端（adapter/
watchdog/lifecycle/web.app/web.routers.orders）一律呼叫這裡的函式，不各自重寫一份。刻意
用**顯式傳入 `secrets` 清單**而非模組層級全域可變狀態——每個呼叫端本來就有一份「這個
adapter instance 認得的秘密清單」可用（`ShioajiAdapter.secrets_to_redact`），顯式傳遞比
全域 registry 更適合單元測試（不會有測試間互相污染全域狀態的問題），且與 plan 草稿
`_redact_secrets(text, *, secrets=[...])` 的既有測試簽章相容。

只抹除『已知配置的秘密值』（api_key/secret_key/ca_passwd 等，見
`ShioajiAdapter.secrets_to_redact`），不是通用日誌 scrubber——上游例外訊息萬一原文帶出
這些值，不能被原樣 log／回顯／寫進 `/healthz`。
"""


def redact_secrets(text: str, *, secrets: list[str]) -> str:
    """把 `secrets` 中每個已知非空秘密值從 `text` 抹除、取代成 `[REDACTED]`。"""
    if text is None:
        return text
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED]")
    return redacted


def sanitize_exception(exc: BaseException, *, secrets: list[str]) -> str:
    """例外 → 字串並套用 `redact_secrets`；供任何要 log／回應／寫入 `/healthz` 的例外訊息
    使用，取代各處自己手寫 `str(exc)`。"""
    return redact_secrets(str(exc), secrets=secrets)
