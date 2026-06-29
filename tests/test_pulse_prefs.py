"""Market Pulse Telegram-toggle persistence + web endpoint."""
from quanquant.pulse.prefs import load_telegram_enabled, save_telegram_enabled


def test_telegram_pref_defaults_to_none(session):
    assert load_telegram_enabled(session, "TXF") is None  # use config default


def test_telegram_pref_roundtrip(session):
    save_telegram_enabled(session, "TXF", True)
    assert load_telegram_enabled(session, "TXF") is True
    save_telegram_enabled(session, "TXF", False)  # upsert, not duplicate
    assert load_telegram_enabled(session, "TXF") is False


def test_pulse_telegram_endpoint(client):
    assert client.get("/api/pulse/state").json() == {"telegramEnabled": False}
    r = client.put("/api/pulse/telegram", json={"enabled": True})
    assert r.status_code == 200
    assert r.json() == {"telegramEnabled": True}
