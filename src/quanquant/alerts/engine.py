"""Alert evaluation engine.

Subscribes to the shared poller; on each cycle, for every distinct enabled-alert
timeframe, finds the newest CLOSED bar and evaluates alerts against its close-
settled value (never intra-bar). Always reads the standard series (intraday =
all sessions, daily = day) regardless of the chart's session dropdown.
"""
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from sqlmodel import Session, select

from quanquant.candles.bucketing import bucket_close_ms
from quanquant.config import get_settings
from quanquant.candles.service import get_candles
from quanquant.candles.timeframes import TIMEFRAMES
from quanquant.db.engine import get_engine
from quanquant.db.models import Alert, AlertEvent, _utcnow
from quanquant.indicators import value_at
from quanquant.notify.base import Notification

_NAME_LABEL = {"ma": "MA", "wr": "WR", "bias": "BIAS"}
_OP_LABEL = {"gte": "≥", "lte": "≤", "cross_up": "向上突破", "cross_down": "向下突破"}


@dataclass(frozen=True, slots=True)
class EvalResult:
    fired: bool
    message: str
    left_value: float | None
    right_value: float | None


def newest_closed(bars: list[dict], tf: str, now_ms: int) -> tuple[int | None, list[dict]]:
    """(closed_bar_ts, series ending at the closed bar), or (None, []).

    The last bar in `bars` is the in-progress (live/synthetic) bar. Intraday: the
    last bucket counts as closed once wall-clock passes its bucket close (the
    session's final bar, which never gets a newer bucket). Otherwise the closed
    bar is the second-to-last.
    """
    if not bars:
        return None, []
    if TIMEFRAMES[tf].kind == "intraday":
        last_ts = bars[-1]["timestamp"]
        if now_ms >= bucket_close_ms(last_ts, tf):
            return last_ts, bars
    if len(bars) >= 2:
        return bars[-2]["timestamp"], bars[:-1]
    return None, []


def _left(alert: Alert, series: list[dict]) -> float | None:
    if alert.left_kind == "price":
        return series[-1]["close"]
    return value_at(alert.left_name, alert.left_period, series)


def _right(alert: Alert, series: list[dict]) -> float | None:
    if alert.right_kind == "const":
        return float(alert.right_value) if alert.right_value is not None else None
    return value_at(alert.right_name, alert.right_period, series)


def _operand_label(kind: str, name: str | None, period: int | None, value: float | None) -> str:
    if kind == "price":
        return "收盤"
    if kind == "const":
        return f"{value:g}" if value is not None else "?"
    return f"{_NAME_LABEL.get(name or '', name or '')}({period})"


def _condition_text(alert: Alert) -> str:
    """The triggering condition without values, e.g. '收盤 向上突破 MA(20)'."""
    left_label = _operand_label(alert.left_kind, alert.left_name, alert.left_period, None)
    rv = float(alert.right_value) if alert.right_value is not None else None
    right_label = _operand_label(alert.right_kind, alert.right_name, alert.right_period, rv)
    return f"{left_label} {_OP_LABEL[alert.op]} {right_label}"


def _render_message(alert: Alert, left: float, right: float) -> str:
    return f"{alert.symbol} {alert.timeframe} {_condition_text(alert)}（{left:.1f}）"


def evaluate_alert(alert: Alert, series: list[dict], closed_ts: int) -> EvalResult:
    """Evaluate one alert at a closed bar; mutate armed/last_triggered/enabled."""
    if not series:
        return EvalResult(False, "", None, None)
    left = _left(alert, series)
    right = _right(alert, series)
    if left is None or right is None:
        return EvalResult(False, "", left, right)

    fired = False
    if alert.op == "gte":
        if left >= right:
            if alert.armed:
                fired = True
                alert.armed = False
        else:
            alert.armed = True
    elif alert.op == "lte":
        if left <= right:
            if alert.armed:
                fired = True
                alert.armed = False
        else:
            alert.armed = True
    elif alert.op in ("cross_up", "cross_down"):
        prev = series[:-1]
        pl = _left(alert, prev)
        pr = _right(alert, prev)
        if pl is not None and pr is not None and alert.last_triggered_bar_ts != closed_ts:
            if alert.op == "cross_up" and pl <= pr and left > right:
                fired = True
            elif alert.op == "cross_down" and pl >= pr and left < right:
                fired = True

    if not fired:
        return EvalResult(False, "", left, right)

    alert.last_triggered_at = _utcnow()
    alert.last_triggered_bar_ts = closed_ts
    if alert.fire_once:
        alert.enabled = False
    return EvalResult(True, _render_message(alert, left, right), left, right)


def _evaluate_sync(symbol: str, last_evaluated: dict[tuple[str, str], int]) -> list[Notification]:
    out: list[Notification] = []
    with Session(get_engine()) as db:
        alerts = list(db.exec(select(Alert).where(Alert.symbol == symbol, Alert.enabled)))
        if not alerts:
            return out
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        for tf in {a.timeframe for a in alerts}:
            tf_alerts = [a for a in alerts if a.timeframe == tf]
            periods = [p for a in tf_alerts for p in (a.left_period, a.right_period) if p]
            k = min((max(periods) if periods else 2) + 5, 1000)
            page = get_candles(db, symbol, tf, limit=k)
            closed_ts, series = newest_closed(page.bars, tf, now_ms)
            if closed_ts is None or last_evaluated.get((symbol, tf), 0) >= closed_ts:
                continue
            last_evaluated[(symbol, tf)] = closed_ts

            for alert in tf_alerts:
                res = evaluate_alert(alert, series, closed_ts)  # mutates alert in-session
                if res.fired:
                    db.add(AlertEvent(
                        alert_id=alert.id, bar_ts=closed_ts, message=res.message,
                        left_value=_dec(res.left_value), right_value=_dec(res.right_value),
                    ))
                    out.append(Notification(
                        symbol=alert.symbol, timeframe=alert.timeframe,
                        tf_label=TIMEFRAMES[alert.timeframe].label,
                        condition=_condition_text(alert),
                        left_value=res.left_value, right_value=res.right_value,
                        right_is_indicator=(alert.right_kind == "indicator"),
                        alert_id=alert.id or 0, bar_ts=closed_ts, body=res.message,
                    ))
        db.commit()
    return out


def _dec(value: float | None) -> Decimal | None:
    return None if value is None else Decimal(str(round(value, 4)))


async def run_alert_engine(poller, notify, symbol: str) -> None:
    """Lifespan task: evaluate alerts at each bar close, fire notifications."""
    queue = poller.subscribe()
    last_evaluated: dict[tuple[str, str], int] = {}
    min_interval = get_settings().alert_eval_min_interval
    last_eval = 0.0
    try:
        while True:
            await queue.get()
            # Alerts evaluate on closed bars only, so sub-second tick streaming needs
            # no faster cadence than this — skip ticks within the throttle window to
            # avoid a DB session + worker thread per tick.
            now = time.monotonic()
            if now - last_eval < min_interval:
                continue
            last_eval = now
            try:
                # sync DB/eval off the event loop; fan out notifications on it
                fired = await asyncio.to_thread(_evaluate_sync, symbol, last_evaluated)
                for notification in fired:
                    await notify.send(notification)
            except Exception:
                pass  # never let an eval error kill the engine
    finally:
        poller.unsubscribe(queue)
