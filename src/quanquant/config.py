from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env.local", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    poll_interval_seconds: float = 5.0
    symbol: str = "TXF"
    source: str = "taifex"  # data source name; see sources/registry.py ("shioaji" = streaming)
    finmind_token: str = ""  # for FinMind history backfill (and FinMindSource)

    # Shioaji (永豐) streaming live source — used when source == "shioaji".
    # TAIFEX MIS stays as the automatic fallback whenever the stream is silent.
    shioaji_api_key: str = ""
    shioaji_secret_key: str = ""
    shioaji_stale_seconds: float = 20.0  # stream silent this long → MIS fallback (outage net only)

    # Streaming throttles (no-ops under 5s polling; matter at multi-tick/sec).
    quote_write_min_interval: float = 0.5  # raw Quote row inserts ≤ 2/s (candles canonical)
    sse_min_interval: float = 0.25         # live-quote SSE pushes ≤ 4/s (coalesced)
    alert_eval_min_interval: float = 1.0   # alert engine eval cadence (alerts are close-based)
    quote_retention_days: int = 7  # raw quotes pruned after N days (0 = keep forever)
    telegram_bot_token: str = ""  # alerts → Telegram (browser-only when unset)
    telegram_chat_id: str = ""

    # 營運/開發告警（T0.3）：與上面 3 人共用的價格警示 chat 分離，走獨立 dev chat。
    # token 留空 → 沿用 telegram_bot_token（同一 bot，只是送不同 chat）；chat_id 留空 →
    # ops 告警整體 no-op（不誤入共用 chat）。事件：下單失敗/quarantine/feed 停滯/reconcile
    # 漂移/kill switch/connect 失敗。
    ops_telegram_bot_token: str = ""
    ops_telegram_chat_id: str = ""
    feed_stale_alert_seconds: float = 90.0  # 盤中報價停滯逾此秒數 → 營運告警（僅交易時段判定）

    # Market Pulse v0.1 — price-velocity audio + Telegram alerts.
    # Audio/cooldown/toggle live in the browser; the backend only computes the
    # Velocity Level and pushes Telegram when ENTERING the configured high level.
    pulse_enabled: bool = True
    pulse_telegram_level: int = 4        # Telegram only when entering this level (4 = Extreme)
    pulse_telegram_cooldown: float = 60.0  # seconds between Telegram pushes (edge-triggered)
    pulse_telegram_enabled: bool = False  # default OFF; toggle on from the web (persisted in DB)

    # 臨時休市（颱風）覆寫：逗號分隔 ISO 日期（YYYY-MM-DD），與內建假日清單 union。
    # 用 env（非資料檔）以避開 wheel/Docker force-include 限制；VM 設 env 重啟即生效。
    extra_holidays: str = ""

    # Web / dashboard
    # Account system — signs the session cookie. MUST be set in production
    # (.env on the VM); unset → a transient per-process key (dev only).
    session_secret: str = ""
    db_url: str = "sqlite:///./quanquant.db"
    host: str = "127.0.0.1"
    port: int = 8000
    tv_symbol: str = "TAIFEX:TXF1!"  # TradingView chart symbol (TAIEX Futures 台指期大台; 小台=MXF1!)

    # Shioaji 下單風控（Task 7 RiskGuard 建構參數的最小必要欄位；Task 8 lifespan 用
    # broker.risk.parse_owner_ids/parse_whitelist 解析後傳入 RiskGuard，未設定 owner
    # id 時 Task 8 應軟性停用下單子系統，不崩站——本檔只加欄位，不在這裡連線/驗證）。
    order_owner_user_ids: str = ""       # 逗號分隔 user id 白名單（owner-only 授權）
    order_symbol_whitelist: str = "TXF"  # 逗號分隔可下單商品白名單
    order_max_qty_per_order: int = 5
    order_max_qty_per_day: int = 20
    order_max_orders_per_day: int = 20
    order_confirm_token_ttl_seconds: int = 120  # real 兩階段確認 token 有效秒數
    order_kill_switch_initial: bool = False     # 啟動時的 kill switch 初始值（可即時切換，非快照）

    # Shioaji 下單子系統連線 + lifespan（Task 8）——秘密只在 .env，不進 git。
    # order_mode 刻意用 str（非 Literal）：`broker.preflight.order_subsystem_preflight` 才能
    # 對「拼錯值」做出「拒絕啟動下單子系統」的明確 RuntimeError（若這裡用 Literal，pydantic
    # 會在 Settings() 建構當下就整個拒絕，行為上等同讓整個 app 起不來，不符合「app 其餘正常」
    # 的要求，見 web/app.py lifespan 對 preflight RuntimeError 的處理）。
    shioaji_trade_api_key: str = ""
    shioaji_trade_secret_key: str = ""
    shioaji_ca_path: str = ""
    shioaji_ca_passwd: str = ""
    shioaji_person_id: str = ""
    order_mode: str = "sim"                          # "sim" | "real"；拼錯拒絕啟動下單子系統
    order_sim_fee_per_lot: str = "20"                 # sim 成交 fee 缺值時的估算基準（Decimal 字串）
    order_watchdog_interval_seconds: float = 15.0
    order_login_min_interval_seconds: float = 30.0    # login 節流（配額 5連線/1000 login/day）
    order_unquarantine_after_seconds: float = 300.0   # 較慢週期：多久沒解隔離的 raw_inbox 再重試
    order_unknown_reconcile_grace_seconds: float = 300.0  # unknown 委託等多久才判定失敗+釋放配額
    order_confirm_token_cleanup_interval_seconds: float = 3600.0  # 過期/已消費 token 清理週期

    # --- 本機 broker agent 通道（Increment 0，單一信任使用者） ---
    order_channel: str = "inprocess"    # inprocess | agent（agent=Shioaji I/O 在使用者本機執行）
    agent_ws_token: str = ""            # agent WS 靜態 token；空字串=agent 通道停用
    agent_command_timeout_seconds: float = 10.0  # server 等 cmd_ack 逾時（逾時→unknown 保守）


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
