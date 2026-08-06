"""營運/開發告警管道（T0.3）。

與 K 線價格警示（`telegram_bot_token`/`telegram_chat_id`，目前 3 位使用者共用同一 chat）
**刻意分離**：營運事件（下單失敗/quarantine/feed 停滯/reconcile 漂移/kill switch/connect
失敗）走獨立的 dev chat，不混進共用的價格警示 chat。

設計鐵律：
  - **非阻塞、絕不 raise**：`emit()` 只做節流判斷 + 把一個 fire-and-forget 的 `send_text`
    協程排程到 event loop，立即返回。任何例外一律吞掉並記 log，不得反噬呼叫端的主流程。
  - **跨執行緒安全**：emit 可能來自 event loop 執行緒（lifespan/worker/watchdog）或
    threadpool 執行緒（place/update 在 `asyncio.to_thread` 內失敗時）——一律用
    `loop.call_soon_threadsafe` 把送出排程回 loop 執行緒，不在呼叫端直接 await。
  - **未設定 dev chat → 整體 no-op**：`ops_telegram_chat_id` 留空時 `configured` 為 False，
    emit 直接返回，營運告警不會漏進 3 人共用的價格警示 chat。
  - **節流**：feed 停滯/connect 失敗會高頻重複——同一 event key 在 window 秒內只送一次；
    `throttle<=0` 代表每次都送（下單失敗/kill switch 每筆都重要，不節流）。
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from threading import Lock

from quanquant.notify.telegram import TelegramNotifier

log = logging.getLogger(__name__)
_CST = timezone(timedelta(hours=8))
_SEV_EMOJI = {"info": "ℹ️", "warn": "⚠️", "critical": "🚨"}


class OpsAlerter:
    """營運告警發送器。持有一個獨立 dev-chat 的 TelegramNotifier，提供各事件的語意方法。

    `clock` 可注入（預設 `time.monotonic`）以利測試節流；`now_fn` 可注入牆鐘以利測試訊息時間。
    """

    def __init__(
        self,
        notifier: TelegramNotifier,
        *,
        mode: str = "sim",
        clock=time.monotonic,
        now_fn=lambda: datetime.now(_CST),
    ) -> None:
        self._notifier = notifier
        self._mode = mode
        self._clock = clock
        self._now_fn = now_fn
        self._loop: asyncio.AbstractEventLoop | None = None
        self._last_sent: dict[str, float] = {}
        self._lock = Lock()

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """由 lifespan 在 running loop 上呼叫；emit 靠它把送出排程回 loop 執行緒。"""
        self._loop = loop

    @property
    def configured(self) -> bool:
        return bool(self._notifier is not None and self._notifier.configured)

    # ---- 核心 ----------------------------------------------------------
    def _should_send(self, key: str, throttle: float) -> bool:
        if throttle <= 0:
            return True
        now = self._clock()
        with self._lock:
            last = self._last_sent.get(key)
            if last is not None and (now - last) < throttle:
                return False
            self._last_sent[key] = now
            return True

    def emit(
        self, key: str, title: str, detail: str = "", *, severity: str = "warn", throttle: float = 0.0
    ) -> None:
        """Fire-and-forget；可從任何執行緒呼叫；絕不 raise。"""
        try:
            if not self.configured:
                return
            if not self._should_send(key, throttle):
                return
            text = self._format(severity, title, detail)
            loop = self._loop
            if loop is None:
                log.warning("ops 告警無 loop 可排程，略過：%s", title)
                return
            loop.call_soon_threadsafe(self._spawn, text)
        except Exception:  # 告警本身絕不能反噬主流程
            log.exception("ops 告警 emit 失敗（已吞，不影響主流程）")

    def _spawn(self, text: str) -> None:
        """在 loop 執行緒上排一個 fire-and-forget send（send_text 自己吞錯、不阻塞）。"""
        try:
            asyncio.ensure_future(self._notifier.send_text(text))
        except Exception:
            log.exception("ops 告警排程 send 失敗")

    def _format(self, severity: str, title: str, detail: str) -> str:
        emoji = _SEV_EMOJI.get(severity, "⚠️")
        stamp = self._now_fn().strftime("%Y-%m-%d %H:%M:%S")
        lines = [f"{emoji} QuanQuant 營運告警 [{self._mode}]", f"🕐 {stamp}", "", f"事件：{title}"]
        if detail:
            lines += ["", detail]
        return "\n".join(lines)

    # ---- 語意方法（集中各事件的 key/severity/節流政策）----------------------
    def connect_failed(self, message: str) -> None:
        self.emit("connect_failed", "下單子系統 connect 失敗", message,
                  severity="critical", throttle=300.0)

    def place_failed(self, *, client_order_id: str, symbol: str, action: str, qty,
                     classification: str, detail: str = "") -> None:
        sev = "critical" if classification == "failed" else "warn"
        label = "券商明確拒絕" if classification == "failed" else "結果不明（待 reconcile）"
        body = (f"client_order_id={client_order_id}\n{action} {symbol} x{qty}\n"
                f"分類：{classification}（{label}）")
        if detail:
            body += f"\n{detail}"
        self.emit("place_failed", "下單失敗", body, severity=sev, throttle=0.0)

    def quarantine(self, *, row_id, kind: str, error: str) -> None:
        self.emit("quarantine", "回報進 quarantine（無法落地）",
                  f"raw_inbox id={row_id} kind={kind}\n{error}",
                  severity="warn", throttle=60.0)

    def reconcile_drift(self, *, count: int, context: str = "") -> None:
        body = f"reconcile 補回 {count} 筆券商端有、本地先前未知的委託進展"
        if context:
            body += f"\n{context}"
        self.emit("reconcile_drift", "reconcile 偵測到漂移", body,
                  severity="warn", throttle=300.0)

    def feed_stale(self, *, age_seconds: float) -> None:
        self.emit("feed_stale", "盤中報價停滯",
                  f"距最後一筆新鮮報價已 {age_seconds:.0f} 秒（僅交易時段判定）",
                  severity="critical", throttle=300.0)

    def kill_switch(self, *, enabled: bool, actor_user_id, scope: str = "global",
                    open_order_count: int = 0, detail: str = "") -> None:
        """D3 兩層 kill switch：`scope` 區分「個人（我的）」急停 vs「全站總閘」，記進告警
        本文供人工稽核翻閘者＋範圍（沿用既有 audit 機制寫法，不另開 DB 表）。"""
        state = "啟動（拒絕新單）" if enabled else "解除"
        scope_label = "全站總閘" if scope == "global" else "個人（我的）"
        body = f"範圍：{scope_label}\n操作者 user_id={actor_user_id}"
        if enabled and open_order_count:
            body += f"\n⚠ 當下仍有 {open_order_count} 筆未成交掛單，未自動取消，請人工決定"
        if detail:
            body += f"\n{detail}"
        self.emit("kill_switch", f"kill switch {state}", body,
                  severity="critical", throttle=0.0)


def build_ops_alerter(settings, *, mode: str = "sim") -> OpsAlerter:
    """從 settings 建 OpsAlerter。

    - token 留空 → 沿用 `telegram_bot_token`（同一 bot，只是送到不同 chat）。
    - `ops_telegram_chat_id` 留空 → notifier 未設定 → OpsAlerter 整體 no-op（不誤入共用 chat）。
    """
    token = settings.ops_telegram_bot_token or settings.telegram_bot_token
    notifier = TelegramNotifier(token, settings.ops_telegram_chat_id)
    return OpsAlerter(notifier, mode=mode)
