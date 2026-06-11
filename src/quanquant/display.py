from datetime import timedelta, timezone

from quanquant.models import FuturesSnapshot

_CST = timezone(timedelta(hours=8))

_GREEN = "\033[92m"
_RED = "\033[91m"
_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"


_SESSION_LABEL = {
    "day":    "● 日盤",
    "night":  "● 夜盤",
    "closed": "○ 休市",
}


def format_snapshot(snap: FuturesSnapshot, session: str) -> str:
    arrow = "▲" if snap.change >= 0 else "▼"
    color = _GREEN if snap.change >= 0 else _RED
    status = _SESSION_LABEL.get(session, "○ 休市")
    cst_time = snap.fetched_at.astimezone(_CST).strftime("%H:%M:%S")

    return (
        f"\r{_BOLD}TXF{_RESET}  "
        f"{color}{_BOLD}{snap.price:>8}{_RESET}  "
        f"{color}{arrow} {snap.change:+} ({snap.change_pct:+.2f}%){_RESET}  "
        f"{_DIM}Vol:{snap.volume:,}  H:{snap.high_price}  L:{snap.low_price}  "
        f"[{cst_time} CST]  {status}{_RESET}"
    )
