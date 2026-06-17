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
    source: str = "taifex"  # data source name; see sources/registry.py
    finmind_token: str = ""  # for FinMind history backfill (and FinMindSource)
    quote_retention_days: int = 7  # raw quotes pruned after N days (0 = keep forever)
    telegram_bot_token: str = ""  # alerts → Telegram (browser-only when unset)
    telegram_chat_id: str = ""

    # Web / dashboard
    db_url: str = "sqlite:///./quanquant.db"
    host: str = "127.0.0.1"
    port: int = 8000
    tv_symbol: str = "TAIFEX:TXF1!"  # TradingView chart symbol (TAIEX Futures 台指期大台; 小台=MXF1!)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
