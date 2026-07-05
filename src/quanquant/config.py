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

    # Market Pulse v0.1 — price-velocity audio + Telegram alerts.
    # Audio/cooldown/toggle live in the browser; the backend only computes the
    # Velocity Level and pushes Telegram when ENTERING the configured high level.
    pulse_enabled: bool = True
    pulse_telegram_level: int = 4        # Telegram only when entering this level (4 = Extreme)
    pulse_telegram_cooldown: float = 60.0  # seconds between Telegram pushes (edge-triggered)
    pulse_telegram_enabled: bool = False  # default OFF; toggle on from the web (persisted in DB)

    # Web / dashboard
    # Account system — signs the session cookie. MUST be set in production
    # (.env on the VM); unset → a transient per-process key (dev only).
    session_secret: str = ""
    db_url: str = "sqlite:///./quanquant.db"
    host: str = "127.0.0.1"
    port: int = 8000
    tv_symbol: str = "TAIFEX:TXF1!"  # TradingView chart symbol (TAIEX Futures 台指期大台; 小台=MXF1!)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
