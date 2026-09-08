import time
from types import SimpleNamespace

import pytest

from quanquant.agent import keyring_store, profile_registry
from quanquant.agent.gui.startup_flow import (
    check_legacy_buffer_conflict, probe_direct_connect, reconcile_profile_after_approval,
    resolve_gui_startup,
)


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    monkeypatch.setattr(profile_registry, "REGISTRY_PATH", tmp_path / "profiles.json")
    monkeypatch.setattr(profile_registry, "LOCK_PATH", tmp_path / "profiles.lock")
    import quanquant.agent.gui.startup_flow as sf
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", tmp_path / "legacy_outbox.db")


@pytest.fixture
def fake_keyring_backend(monkeypatch):
    import keyring

    class _Fake(keyring.backend.KeyringBackend):
        priority = 1

        def __init__(self): self._store = {}
        def get_password(self, service, key): return self._store.get((service, key))
        def set_password(self, service, key, value): self._store[(service, key)] = value
        def delete_password(self, service, key): self._store.pop((service, key), None)
    backend = _Fake()
    monkeypatch.setattr(keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(keyring, "get_password", backend.get_password)
    monkeypatch.setattr(keyring, "set_password", backend.set_password)
    monkeypatch.setattr(keyring, "delete_password", backend.delete_password)
    return backend


def test_zero_profiles_goes_to_setup_step1():
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "setup" and decision.start_step == 1


def test_profile_hint_miss_shows_notice_and_goes_to_setup():
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint="ghost", reset=False)
    assert decision.kind == "setup" and decision.start_step == 1
    assert "找不到此帳號設定" in decision.notice


def test_multiple_profiles_without_hint_goes_to_profile_select():
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="2", username="b", buffer_path="/x2")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "profile_select"


def test_single_profile_with_complete_unexpired_credentials_goes_direct(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    keyring_store.save_token(site_origin="https://q.example", profile_id="1", token="t",
                              expires_at="2099-01-01T00:00:00", username="a")
    keyring_store.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                           api_key="k", secret_key="s")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "direct" and decision.profile.profile_id == "1"


def test_single_profile_missing_token_restarts_at_step1(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "setup" and decision.start_step == 1


def test_single_profile_missing_broker_creds_restarts_at_step2(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    keyring_store.save_token(site_origin="https://q.example", profile_id="1", token="t",
                              expires_at="2099-01-01T00:00:00", username="a")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "setup" and decision.start_step == 2


def test_expired_token_restarts_at_step1_even_with_broker_creds_present(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    keyring_store.save_token(site_origin="https://q.example", profile_id="1", token="t",
                              expires_at="2000-01-01T00:00:00", username="a")
    keyring_store.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                           api_key="k", secret_key="s")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint=None, reset=False)
    assert decision.kind == "setup" and decision.start_step == 1


def test_reset_flag_forces_setup_even_when_profile_complete(fake_keyring_backend):
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    keyring_store.save_token(site_origin="https://q.example", profile_id="1", token="t",
                              expires_at="2099-01-01T00:00:00", username="a")
    keyring_store.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                           api_key="k", secret_key="s")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint="1", reset=True)
    assert decision.kind == "setup" and decision.start_step == 1


def test_externally_deleted_keyring_entry_falls_back_to_setup_not_crash(fake_keyring_backend):
    # registry 還在，但沒寫 keyring（模擬外部工具清空了 keychain）——不得 crash，回精靈。
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x1")
    decision = resolve_gui_startup(site_origin="https://q.example", profile_hint="1", reset=False)
    assert decision.kind == "setup" and decision.profile.profile_id == "1"  # 沿用原 buffer 路徑


def test_reconcile_reuses_existing_profile_when_approved_id_already_registered():
    profile_registry.upsert_profile(site_origin="https://q.example", profile_id="42", username="bob", buffer_path="/x42")
    entry, is_new = reconcile_profile_after_approval(
        site_origin="https://q.example", expected_profile=None, approved_profile_id="42", username="bob",
    )
    assert is_new is False and entry.buffer_path == "/x42"


def test_reconcile_creates_isolated_new_profile_when_approved_id_unknown():
    entry, is_new = reconcile_profile_after_approval(
        site_origin="https://q.example", expected_profile=None, approved_profile_id="99", username="carol",
    )
    assert is_new is True and entry.profile_id == "99"


def test_legacy_buffer_conflict_blocks_when_unsent_rows_present(tmp_path, monkeypatch):
    import quanquant.agent.gui.startup_flow as sf
    from quanquant.agent.buffer import DurableBuffer
    legacy = tmp_path / "legacy_outbox.db"
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", legacy)
    buf = DurableBuffer(str(legacy))
    buf.append("order_report", {"x": 1})
    notice = check_legacy_buffer_conflict()
    assert notice is not None and "不會自動搬移" in notice


def test_legacy_buffer_conflict_none_when_no_legacy_file(tmp_path, monkeypatch):
    import quanquant.agent.gui.startup_flow as sf
    monkeypatch.setattr(sf, "LEGACY_DEFAULT_BUFFER", tmp_path / "nope.db")
    assert check_legacy_buffer_conflict() is None


# ---------------------------------------------------------------------------
# Reviewer 必修（Important）：probe_direct_connect 的 "rejected"/"connected" 分支
# 零測試覆蓋——這正是根因分析裡 race-prone 的整合點，鎖住『一旦出現終態就立即返回，
# 不是靠 timeout 兜底』這個行為，未來重構才不會無聲重新引入競態。
# ---------------------------------------------------------------------------

class _FlippingRunner:
    """`snapshot()` 依序回傳 `sequence` 裡的值，最後一個值之後持續回傳同一個值
    （模擬『後來穩定在某個終態』）。"""

    def __init__(self, sequence):
        self._sequence = sequence
        self._calls = 0

    async def snapshot(self):
        idx = min(self._calls, len(self._sequence) - 1)
        self._calls += 1
        return SimpleNamespace(connection=self._sequence[idx])


async def test_probe_direct_connect_returns_immediately_once_rejected_appears():
    runner = _FlippingRunner(["reconnecting", "reconnecting", "rejected", "reconnecting"])
    start = time.monotonic()
    result = await probe_direct_connect(runner, timeout=5.0)
    elapsed = time.monotonic() - start
    assert result == "rejected"
    assert elapsed < 1.0  # 遠低於 5.0s timeout——證明是輪詢中途發現終態就返回，不是撞 timeout


async def test_probe_direct_connect_returns_immediately_once_connected_appears():
    runner = _FlippingRunner(["connecting", "connected", "reconnecting"])
    start = time.monotonic()
    result = await probe_direct_connect(runner, timeout=5.0)
    elapsed = time.monotonic() - start
    assert result == "connected"
    assert elapsed < 1.0
