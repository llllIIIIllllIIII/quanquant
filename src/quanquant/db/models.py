"""SQLModel tables.

Monetary/price values use DecimalText (stored as TEXT) so Decimal round-trips
exactly on SQLite and stays portable to Postgres. Trade datetimes are stored as
naive local (Asia/Taipei) values as entered; audit timestamps are naive UTC.
"""
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import BigInteger, Column, String, TypeDecorator, UniqueConstraint
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
