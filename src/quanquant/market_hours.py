from datetime import datetime, time, timedelta, timezone

CST = timezone(timedelta(hours=8))

_DAY_OPEN = time(8, 45)
_DAY_CLOSE = time(13, 45)
_NIGHT_OPEN = time(15, 0)
_NIGHT_CLOSE = time(5, 0)   # next calendar day in CST


def get_session(now: datetime | None = None) -> str:
    """Return 'day', 'night', or 'closed' for current TXF session (CST)."""
    if now is None:
        now = datetime.now(CST)
    else:
        now = now.astimezone(CST)

    t = now.time().replace(second=0, microsecond=0)

    if _DAY_OPEN <= t <= _DAY_CLOSE:
        return "day"
    if t >= _NIGHT_OPEN or t <= _NIGHT_CLOSE:
        return "night"
    return "closed"


def is_market_open(now: datetime | None = None) -> bool:
    return get_session(now) != "closed"
