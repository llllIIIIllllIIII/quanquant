"""SQLModel tables.

Monetary/price values use DecimalText (stored as TEXT) so Decimal round-trips
exactly on SQLite and stays portable to Postgres. Trade datetimes are stored as
naive local (Asia/Taipei) values as entered; audit timestamps are naive UTC.
"""
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Index,
    String,
    TypeDecorator,
    UniqueConstraint,
    text,
)
from sqlmodel import Field, SQLModel


class DecimalText(TypeDecorator):
    """Store Decimal as TEXT for exact round-trips (no float drift)."""

    impl = String
    cache_ok = True

    def process_bind_param(self, value: Decimal | None, dialect: object) -> str | None:
        return None if value is None else str(value)

    def process_result_value(self, value: str | None, dialect: object) -> Decimal | None:
        return None if value is None else Decimal(value)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Trade(SQLModel, table=True):
    __tablename__ = "trades"
    # C2（round3 覆核）：fresh DB 走 create_all 建表，不會經過 db/migrate.py 的
    # ALTER ... CHECK，所以要在這裡另外掛一份等價 CheckConstraint，否則 fresh DB
    # 的 Trade 表完全沒有 mode/source 的 DB 層防呆。IS NOT NULL 與 ALTER 版一致
    # （即使本欄位目前是 NOT NULL 而非本 CHECK 唯一防線，仍保留以防未來欄位改
    # nullable 時防線失效）。
    __table_args__ = (
        CheckConstraint("mode IS NOT NULL AND mode IN ('sim','real')", name="ck_trades_mode"),
        CheckConstraint("source IS NOT NULL AND source IN ('manual','shioaji')", name="ck_trades_source"),
    )

    id: int | None = Field(default=None, primary_key=True)

    symbol: str = Field(index=True)                       # 商品名稱
    direction: str                                        # 多單/空單: "long" | "short"
    entry_time: datetime                                  # 開倉時間
    entry_price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))  # 開倉價格

    exit_time: datetime | None = Field(default=None, index=True)                 # 平倉時間 (NULL=未平倉)
    exit_price: Decimal | None = Field(default=None, sa_column=Column(DecimalText))  # 平倉價格

    stop_loss_price: Decimal | None = Field(default=None, sa_column=Column(DecimalText))  # 停損價格
    take_profit_strategy: str | None = None               # 停利策略
    size: int                                             # 部位大小 (口數)
    point_value: Decimal = Field(                         # 每點每口 NT$ (大台 200, 小台 50)
        default=Decimal("200"), sa_column=Column(DecimalText, nullable=False)
    )
    fee: Decimal | None = Field(default=None, sa_column=Column(DecimalText))     # 手續費

    pnl: Decimal | None = Field(default=None, sa_column=Column(DecimalText))     # 損益 (平倉時自動算)
    pnl_is_manual: bool = False

    note: str | None = None                               # 備註
    tags: str | None = None                               # 標籤 (逗號連接, e.g. "突破,均線")

    mode: str = Field(default="real", index=True)          # "real"（正式/手動）| "sim"（模擬/紙上）
    source: str = Field(default="manual", index=True)      # "manual"（人工輸入）| "shioaji"（broker 自動）

    user_id: int | None = Field(default=None, index=True)  # 擁有者（帳戶系統後必填）
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class Quote(SQLModel, table=True):
    """Raw 5s price snapshots — kept short-term (quote_retention_days) for
    debugging and 1m-candle rebuilds; candles are the canonical store."""

    __tablename__ = "quotes"

    id: int | None = Field(default=None, primary_key=True)
    symbol: str = Field(index=True)
    price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    volume: int = 0
    fetched_at: datetime = Field(index=True)


class Candle(SQLModel, table=True):
    """Canonical candles. Only timeframes "1m" (live-built) and "1d" (backfilled)
    are stored; every other timeframe is derived on demand."""

    __tablename__ = "candles"

    symbol: str = Field(primary_key=True)                 # "TXF"
    timeframe: str = Field(primary_key=True)              # "1m" | "1d"
    # bucket start, epoch ms UTC — BigInteger: epoch-ms overflows Postgres' 4-byte INTEGER
    ts: int = Field(sa_column=Column(BigInteger, primary_key=True))
    open: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    high: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    low: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    close: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    volume: int = 0                                       # per-bucket (diffed), not cumulative
    source: str = "live"                                  # "live"|"finmind"|"rebuild"|future "shioaji"
    session: str | None = None                            # "day"|"night" (1d rows: "day")
    trading_date: str | None = Field(default=None, index=True)  # "YYYY-MM-DD" CST
    updated_at: datetime = Field(default_factory=_utcnow)


class ChartState(SQLModel, table=True):
    """Per-symbol persisted chart UI state: indicator configs and drawings."""

    __tablename__ = "chart_states"
    __table_args__ = (UniqueConstraint("symbol", "kind"),)

    id: int | None = Field(default=None, primary_key=True)
    symbol: str = Field(index=True)
    kind: str                                             # "indicators" | "drawings"
    payload: str                                          # JSON TEXT
    updated_at: datetime = Field(default_factory=_utcnow)


class Alert(SQLModel, table=True):
    """A close-based price/indicator alert. Single condition: left OP right,
    evaluated at the bar close of `timeframe`."""

    __tablename__ = "alerts"

    id: int | None = Field(default=None, primary_key=True)
    symbol: str = Field(index=True)                       # "TXF"
    timeframe: str                                        # e.g. "5m"

    left_kind: str                                        # "price" | "indicator"
    left_name: str | None = None                          # "ma"|"wr"|"bias"
    left_period: int | None = None

    op: str                                               # "gte"|"lte"|"cross_up"|"cross_down"

    right_kind: str                                       # "const" | "indicator"
    right_value: Decimal | None = Field(default=None, sa_column=Column(DecimalText))
    right_name: str | None = None
    right_period: int | None = None

    enabled: bool = True
    fire_once: bool = False                               # disable after first fire
    armed: bool = True                                    # gte/lte dedup state
    last_triggered_at: datetime | None = None
    last_triggered_bar_ts: int | None = Field(default=None, sa_column=Column(BigInteger))
    user_id: int | None = Field(default=None, index=True)  # 擁有者（帳戶系統後必填）
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class AlertEvent(SQLModel, table=True):
    """One alert firing — the trigger log (visible even if no browser was open)."""

    __tablename__ = "alert_events"

    id: int | None = Field(default=None, primary_key=True)
    alert_id: int = Field(index=True)
    fired_at: datetime = Field(default_factory=_utcnow)
    bar_ts: int = Field(sa_column=Column(BigInteger))     # closed bar that triggered
    message: str
    left_value: Decimal | None = Field(default=None, sa_column=Column(DecimalText))
    right_value: Decimal | None = Field(default=None, sa_column=Column(DecimalText))


class User(SQLModel, table=True):
    """Account for app-level auth. Deactivation replaces deletion (data ownership)."""

    __tablename__ = "users"

    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(unique=True, index=True)        # 登入帳號
    display_name: str                                     # 顯示名稱（navbar、通知）
    password_hash: str                                    # bcrypt
    role: str = "user"                                    # "admin" | "user"
    is_active: bool = True
    token_version: int = 0                                # bump → 所有舊 cookie 失效
    telegram_chat_id: str | None = None                   # 第二階段深度連結綁定用
    chart_color_scheme: str | None = None                 # "green_up"(綠漲紅跌,預設) | "red_up"(紅漲綠跌)
    theme: str | None = None                              # "dark"(預設) | "light" — 介面主題
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class UserChartState(SQLModel, table=True):
    """Per-user chart UI state (indicators/drawings). The old chart_states table
    stays as the system-level KV store (e.g. kind="pulse") — see the design spec
    for why we don't ALTER its (symbol, kind) unique constraint."""

    __tablename__ = "user_chart_states"
    __table_args__ = (UniqueConstraint("user_id", "symbol", "kind"),)

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    symbol: str = Field(index=True)
    kind: str                                             # "indicators" | "drawings"
    payload: str                                          # JSON TEXT
    updated_at: datetime = Field(default_factory=_utcnow)


# ---- Shioaji 下單整合（Task 3）----
#
# 七張新表，皆走 SQLModel.metadata.create_all 自動建（見 db/engine.py:init_db，import
# quanquant.db.models 觸發註冊）；全部帶完整 DDL（CheckConstraint/UniqueConstraint/
# BigInteger），不需要 db/migrate.py 的 ensure_columns（那是給既有表補欄位用的）。
#
# mode CHECK 一律用 "mode IS NOT NULL AND mode IN ('sim','real')"（即使欄位本身已是
# required/NOT NULL）：與 Trade.__table_args__ 的防禦風格一致（見上方 C2 註解），防未來
# 欄位改 nullable 時防線失效。


class RawInbox(SQLModel, table=True):
    """callback 落地的第一站——在任何解析/去重/業務邏輯之前先 durable 落地原始 payload，
    確保 QueueFull/worker 例外/處理中 crash 都不丟資料（V3-2，修 BLOCKER#2）。
    watchdog 對帳（Task 8）拉到的券商委託/成交也塞進同一張表，走同一套處理管線。"""

    __tablename__ = "raw_inbox"

    id: int | None = Field(default=None, primary_key=True)
    kind: str                                                # "order_report" | "deal_report"
    broker: str = "shioaji"
    payload: str                                             # JSON TEXT（callback stat+msg，已轉可序列化 dict）
    received_at: datetime = Field(default_factory=_utcnow, index=True)
    processed: bool = Field(default=False, index=True)
    quarantine: bool = False
    error: str | None = None
    processed_at: datetime | None = None


class Order(SQLModel, table=True):
    """一張委託單（下單面板／委託列表資料來源）。client_order_id 為下單前伺服器產生的冪等鍵；
    request_hash 是該冪等鍵當初綁定的 canonical_payload_hash（Task 2），同鍵不同 payload 拒絕（V3-3）。
    ordno/broker_order_id 各自以 (broker,account,mode,X) 複合唯一鍵 scope，解析 fill 時不再裸鍵查詢
    （round3 #3：解決 BLOCKER#3 同 ordno 不同帳戶互相污染）。"""

    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("mode IS NOT NULL AND mode IN ('sim','real')", name="ck_orders_mode"),
        UniqueConstraint("broker", "account", "mode", "ordno", name="uq_orders_ordno_scope"),
        UniqueConstraint("broker", "account", "mode", "broker_order_id", name="uq_orders_broker_order_id_scope"),
    )

    id: int | None = Field(default=None, primary_key=True)
    client_order_id: str = Field(unique=True, index=True)   # 冪等鍵：同鍵重送不重建
    request_hash: str = Field(index=True)                    # 建單當下的 canonical_payload_hash
    user_id: int = Field(index=True)                        # 下單者
    mode: str = Field(index=True)                            # "real" | "sim"（server-side 決定，非表單）
    broker: str = "shioaji"
    account: str = ""                                        # futopt_account.account_id
    trading_day: str = Field(index=True)                      # CST 日曆日（trading_day_for），配額/計數用

    symbol: str = Field(index=True)
    action: str                                               # "Buy" | "Sell"
    qty: int
    price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    price_type: str
    order_type: str
    octype: str

    broker_order_id: str | None = Field(default=None)              # 券商委託單號（scope 唯一，見 __table_args__）
    ordno: str | None = Field(default=None)                        # 委託流水（scope 唯一，見 __table_args__）
    status: str = "pending"  # pending|sending|submitted|partfilled|filled|cancelled|failed|unknown

    filled_qty: int = 0
    avg_fill_price: Decimal | None = Field(default=None, sa_column=Column(DecimalText))

    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class Deal(SQLModel, table=True):
    """durable fill **去重**帳本（unique broker+mode+account+trading_day+fill_id）。
    fill_id 與 ordno/broker_order_id 分離：fill_id 去重、ordno/broker_order_id 關聯委託（複合 scope）。
    只收真實 deal id 當 fill_id（V3-4：不用 ordno/seqno fallback 冒充）。broker_order_id（D5）
    與 user_id/mode/account 一路帶完整 context，不需要回頭 join Order 才能知道是誰的成交。"""

    __tablename__ = "deals"
    __table_args__ = (
        UniqueConstraint("broker", "mode", "account", "trading_day", "fill_id", name="uq_deal_fill"),
        CheckConstraint("mode IS NOT NULL AND mode IN ('sim','real')", name="ck_deals_mode"),
    )

    id: int | None = Field(default=None, primary_key=True)
    broker: str
    account: str
    mode: str = Field(index=True)
    trading_day: str = Field(index=True)   # "YYYY-MM-DD"（CST 日曆日，見 repository.trading_day_for）
    fill_id: str = Field(index=True)       # 券商成交唯一識別（去重鍵）
    ordno: str | None = Field(default=None, index=True)            # 委託關聯鍵之一
    broker_order_id: str | None = Field(default=None, index=True)  # 委託關聯鍵之二（D5）

    order_id: int | None = Field(default=None, foreign_key="orders.id", index=True)
    user_id: int | None = Field(default=None, index=True)  # 解析前為 None（quarantine）
    raw_inbox_id: int | None = Field(default=None, foreign_key="raw_inbox.id", index=True)  # 溯源

    symbol: str
    action: str
    price: Decimal = Field(sa_column=Column(DecimalText, nullable=False))
    qty: int
    fee: Decimal | None = Field(default=None, sa_column=Column(DecimalText))
    octype: str
    ts: int = Field(sa_column=Column(BigInteger, nullable=False))  # epoch-ms UTC

    processed: bool = False
    quarantine: bool = False
    error: str | None = None

    created_at: datetime = Field(default_factory=_utcnow)


class BrokerPosition(SQLModel, table=True):
    """持久化 broker 自動部位帳務；與手動 Trade 完全隔離——自動成交只動這張表，
    round-trip 完成才回寫一筆 Trade(source="shioaji")，trade_id 回填於此。

    V3-1（修 BLOCKER#10 PnL 錯算）：total_opened_qty/entry_notional/open_fee_total 只增不減，
    只在每次 New/開倉 fill 累加；closed_qty/exit_notional/close_fee_total 只在每次 Cover/平倉 fill
    累加。round-trip 結算的 entry 永遠是 entry_notional/total_opened_qty（真實加權平均），
    exit 永遠是 exit_notional/closed_qty——兩者都是「總量的加權平均」，數學上等價於對每一筆
    開倉/平倉 lot 做加權平均，不需要額外的逐筆 lot 表。

    round3 #15（invariant 加固）：
    - `uq_broker_positions_active_scope`：status='open' 的 partial unique index，
      同一 (user,broker,account,mode,symbol,direction) scope 同時只能有一個 open 部位——
      historical 的 closed 部位不受此限（本來就該允許同 scope 累積多筆歷史 round-trip）。
      用 partial index 而非全域 UniqueConstraint，因為全域唯一會讓「平倉後再開新倉」永久
      被擋。
    - qty invariant CHECK：total_opened_qty 必須 > 0（一個部位至少要有開倉量才成立）；
      closed_qty 必須落在 [0, total_opened_qty]（不可能平倉超過已開倉量）。
    - `version` 欄：供未來寫入者（Task 5 的 fill worker）做樂觀鎖 CAS
      （`UPDATE ... WHERE id=? AND version=?`），防跨執行緒/協程競態下的 lost update；
      Task 3 只加欄位提供原語，實際的 CAS 更新邏輯屬於寫入 fill 的 task。"""

    __tablename__ = "broker_positions"
    __table_args__ = (
        CheckConstraint("mode IS NOT NULL AND mode IN ('sim','real')", name="ck_broker_positions_mode"),
        CheckConstraint(
            "direction IS NOT NULL AND direction IN ('long','short')",
            name="ck_broker_positions_direction",
        ),
        CheckConstraint("total_opened_qty > 0", name="ck_broker_positions_total_opened_qty_positive"),
        CheckConstraint(
            "closed_qty >= 0 AND closed_qty <= total_opened_qty",
            name="ck_broker_positions_closed_qty_range",
        ),
        Index(
            "uq_broker_positions_active_scope",
            "user_id", "broker", "account", "mode", "symbol", "direction",
            unique=True,
            sqlite_where=text("status = 'open'"),
            postgresql_where=text("status = 'open'"),
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    broker: str
    account: str
    mode: str = Field(index=True)
    symbol: str = Field(index=True)
    direction: str  # "long" | "short"

    total_opened_qty: int                                      # 累計開倉口數（只增不減）
    entry_notional: Decimal = Field(sa_column=Column(DecimalText, nullable=False))  # Σ(qty*price) 開倉 fills
    open_fee_total: Decimal = Field(default=Decimal(0), sa_column=Column(DecimalText, nullable=False))

    closed_qty: int = 0                                        # 累計平倉口數（只增不減）
    exit_notional: Decimal = Field(default=Decimal(0), sa_column=Column(DecimalText, nullable=False))
    close_fee_total: Decimal = Field(default=Decimal(0), sa_column=Column(DecimalText, nullable=False))

    status: str = "open"  # "open"（remaining>0）| "closed"（remaining==0）
    version: int = 0                                           # 樂觀鎖 CAS 用（round3 #15）
    trade_id: int | None = Field(default=None, foreign_key="trades.id")  # 結案回填

    opened_at: datetime = Field(default_factory=_utcnow)       # naive UTC；Trade 化時轉 CST（見 position_tracker）
    updated_at: datetime = Field(default_factory=_utcnow)


class OrderAudit(SQLModel, table=True):
    """append-only 稽核；不含秘密（只存 payload 的 hash，不存原始敏感值）。"""

    __tablename__ = "order_audits"
    __table_args__ = (
        CheckConstraint("mode IS NOT NULL AND mode IN ('sim','real')", name="ck_order_audits_mode"),
    )

    id: int | None = Field(default=None, primary_key=True)
    ts: int = Field(sa_column=Column(BigInteger, nullable=False))  # epoch-ms UTC
    actor_user_id: int | None = None
    mode: str
    action: str      # "place" | "cancel" | "update" | "risk_reject" | "fill" | "reconnect"
    payload_hash: str
    rule: str | None = None
    result: str      # "ok" | "rejected" | "error"
    detail: str | None = None


class ConfirmToken(SQLModel, table=True):
    """兩階段確認（real）用的 TTL + JTI DB 列（round3 #16），取代記憶體 nonce set
    （無界成長/重啟遺失）。itsdangerous 簽出的 token 字串本身即嵌入 jti（防偽造/過期，
    見 Task 7）；本表額外提供「一次性」保證：claim 是一個原子 UPDATE
    （見 repository.claim_confirm_token），rowcount 判定成敗。consumed 用 consumed_at
    可為 NULL 的時間戳表示（NULL=未消費），claim 成功時原子寫入消費時間。
    過期/已消費列的清理留給 Task 8 lifespan 週期工作，Task 3 只提供資料列與 claim 原語。"""

    __tablename__ = "confirm_tokens"
    __table_args__ = (UniqueConstraint("jti", name="uq_confirm_tokens_jti"),)

    id: int | None = Field(default=None, primary_key=True)
    jti: str = Field(index=True)
    actor_user_id: int
    payload_hash: str
    expires_at: datetime            # naive UTC
    consumed_at: datetime | None = None
    created_at: datetime = Field(default_factory=_utcnow)


class QuotaReservation(SQLModel, table=True):
    """round3 #4 重新設計：**每筆下單/改單嘗試各自一列**的配額保留，取代 v3 計畫原稿的
    「每人/mode/交易日一列聚合、reserved_qty 用單列 conditional UPDATE 做 CAS」設計——
    聚合設計下 confirm/release 無法辨識「究竟是哪一筆保留被結算/釋放」，會造成重複
    release 或誤判（round3 覆核原文：「quota 未閉環...release_quota 非原子 read-modify-write、
    無 per-order 身分」）。

    狀態機（reserved 是唯一非終態，confirmed/released 皆終態、不可逆、不可重複轉移）：
        reserved  --confirm-->  confirmed   （委託已確認送出，永久計入當日已用配額）
        reserved  --release-->  released    （送出失敗/取消，退還配額，不再計入已用配額）
    confirm/release 皆是「UPDATE ... WHERE state='reserved'」的原子一次性轉移
    （rowcount==1 才算成功；見 repository.confirm_quota/release_quota），重複呼叫必為 False。

    「當日已用配額」現在是 SUM(qty) WHERE state IN ('reserved','confirmed') 的動態聚合，
    不再是單一列可 conditional UPDATE 的值，因此 reserve 用等價強度的
    INSERT ... SELECT ... WHERE 陳述式在同一條 SQL 內完成「讀已用配額」與「若未超限才寫入」
    （見 repository.reserve_quota 完整原子性論證與 tests/test_broker_repo.py 的多執行緒 +
    檔案 DB 併發驗證）。reservation_id 為呼叫端冪等鍵（建議 place 用 client_order_id）。"""

    __tablename__ = "quota_reservations"
    __table_args__ = (
        UniqueConstraint("reservation_id", name="uq_quota_reservations_reservation_id"),
        CheckConstraint("mode IS NOT NULL AND mode IN ('sim','real')", name="ck_quota_reservations_mode"),
        CheckConstraint(
            "state IS NOT NULL AND state IN ('reserved','confirmed','released')",
            name="ck_quota_reservations_state",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    reservation_id: str = Field(index=True)   # 呼叫端冪等鍵（place: client_order_id）
    user_id: int = Field(index=True)
    mode: str = Field(index=True)
    trading_day: str = Field(index=True)
    qty: int
    state: str = Field(default="reserved", index=True)  # "reserved" | "confirmed" | "released"
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class BrokerReconcileCursor(SQLModel, table=True):
    """Task 8 watchdog 對帳用的持久 watermark（round3 #2：重連後拉券商委託補回 RawInbox 需要
    「上次對帳到哪裡」的持久狀態，才能做到週期補洞+重啟續接，而不是每次都重新灌一次全量
    snapshot、或重啟後完全遺失進度只能從零開始）。每個 (broker,account,mode) scope 各自一列，
    只記最後一次對帳涵蓋到的委託時間戳（naive UTC）；寫入頻率低（watchdog 對帳週期），
    由 supervisor.lock 序列化保護，單一寫入者，不需要 CAS。"""

    __tablename__ = "broker_reconcile_cursors"
    __table_args__ = (
        UniqueConstraint("broker", "account", "mode", name="uq_broker_reconcile_cursor_scope"),
        CheckConstraint("mode IS NOT NULL AND mode IN ('sim','real')", name="ck_broker_reconcile_cursor_mode"),
    )

    id: int | None = Field(default=None, primary_key=True)
    broker: str
    account: str
    mode: str = Field(index=True)
    last_reconciled_at: datetime  # naive UTC watermark：只補這個時間點之後有新進展的委託
    updated_at: datetime = Field(default_factory=_utcnow)
