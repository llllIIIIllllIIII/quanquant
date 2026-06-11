"""Request/response DTOs for the trade journal (kept separate from the DB table)."""
from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, model_validator

Direction = Literal["long", "short"]


def split_tags(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def join_tags(tags: list[str] | None) -> str | None:
    if not tags:
        return None
    cleaned = [t.strip() for t in tags if t.strip()]
    return ",".join(cleaned) or None


class TradeCreate(BaseModel):
    symbol: str
    direction: Direction
    entry_time: datetime
    entry_price: Decimal = Field(gt=0)
    exit_time: datetime | None = None
    exit_price: Decimal | None = Field(default=None, gt=0)
    stop_loss_price: Decimal | None = Field(default=None, gt=0)
    take_profit_strategy: str | None = None
    size: int = Field(gt=0)
    point_value: Decimal = Field(default=Decimal("200"), gt=0)
    fee: Decimal | None = Field(default=None, ge=0)
    pnl: Decimal | None = None  # if provided, treated as a manual override
    note: str | None = None
    tags: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _exit_pair(self) -> "TradeCreate":
        if (self.exit_time is None) != (self.exit_price is None):
            raise ValueError("exit_time 與 exit_price 必須同時填寫（已平倉）或同時留空（未平倉）")
        return self


class TradeUpdate(BaseModel):
    symbol: str | None = None
    direction: Direction | None = None
    entry_time: datetime | None = None
    entry_price: Decimal | None = Field(default=None, gt=0)
    exit_time: datetime | None = None
    exit_price: Decimal | None = Field(default=None, gt=0)
    stop_loss_price: Decimal | None = Field(default=None, gt=0)
    take_profit_strategy: str | None = None
    size: int | None = Field(default=None, gt=0)
    point_value: Decimal | None = Field(default=None, gt=0)
    fee: Decimal | None = Field(default=None, ge=0)
    pnl: Decimal | None = None
    note: str | None = None
    tags: list[str] | None = None


class TradeRead(BaseModel):
    id: int
    symbol: str
    direction: Direction
    entry_time: datetime
    entry_price: Decimal
    exit_time: datetime | None
    exit_price: Decimal | None
    stop_loss_price: Decimal | None
    take_profit_strategy: str | None
    size: int
    point_value: Decimal
    fee: Decimal | None
    pnl: Decimal | None
    pnl_is_manual: bool
    note: str | None
    tags: list[str]
    is_open: bool
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_trade(cls, t: object) -> "TradeRead":
        return cls(
            id=t.id,
            symbol=t.symbol,
            direction=t.direction,  # type: ignore[arg-type]
            entry_time=t.entry_time,
            entry_price=t.entry_price,
            exit_time=t.exit_time,
            exit_price=t.exit_price,
            stop_loss_price=t.stop_loss_price,
            take_profit_strategy=t.take_profit_strategy,
            size=t.size,
            point_value=t.point_value,
            fee=t.fee,
            pnl=t.pnl,
            pnl_is_manual=t.pnl_is_manual,
            note=t.note,
            tags=split_tags(t.tags),
            is_open=t.exit_time is None,
            created_at=t.created_at,
            updated_at=t.updated_at,
        )
