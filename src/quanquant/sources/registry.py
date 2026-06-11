"""Maps a source name (from settings) to a DataSource instance.

Keeps CLI and web server resolving `settings.source` the same way.
"""
from quanquant.config import get_settings
from quanquant.sources.base import DataSource
from quanquant.sources.finmind import FinMindSource
from quanquant.sources.taifex import TaifexSource


def make_source(name: str) -> DataSource:
    if name == "taifex":
        return TaifexSource()
    if name == "finmind":
        return FinMindSource(token=get_settings().finmind_token or None)
    raise ValueError(f"Unknown data source {name!r}. Valid: finmind, taifex")
