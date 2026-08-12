import keyring
import keyring.errors
import pytest

from quanquant.agent import keyring_store as ks


class _FakeSecureBackend(keyring.backend.KeyringBackend):
    priority = 1

    def __init__(self):
        self._store: dict[tuple[str, str], str] = {}

    def get_password(self, service, key):
        return self._store.get((service, key))

    def set_password(self, service, key, value):
        self._store[(service, key)] = value

    def delete_password(self, service, key):
        if (service, key) not in self._store:
            raise keyring.errors.PasswordDeleteError("not found")
        del self._store[(service, key)]


class _FlakyBackend(_FakeSecureBackend):
    """第二次 set_password 必失敗，模擬寫入失敗場景。"""

    def __init__(self):
        super().__init__()
        self._set_calls = 0

    def set_password(self, service, key, value):
        self._set_calls += 1
        if self._set_calls == 2:
            raise keyring.errors.PasswordSetError("disk full")
        super().set_password(service, key, value)


@pytest.fixture
def fake_backend(monkeypatch):
    backend = _FakeSecureBackend()
    monkeypatch.setattr(keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(keyring, "get_password", backend.get_password)
    monkeypatch.setattr(keyring, "set_password", backend.set_password)
    monkeypatch.setattr(keyring, "delete_password", backend.delete_password)
    return backend


def test_check_secure_backend_rejects_fail_backend(monkeypatch):
    import keyring.backends.fail

    monkeypatch.setattr(keyring, "get_keyring", lambda: keyring.backends.fail.Keyring())
    assert ks.check_secure_backend() is False


def test_check_secure_backend_accepts_fake_secure_backend(fake_backend):
    assert ks.check_secure_backend() is True


def test_save_and_load_token_roundtrip(fake_backend):
    result = ks.save_token(site_origin="https://q.example", profile_id="1",
                            token="tok123", expires_at="2099-01-01T00:00:00", username="u")
    assert result.ok
    loaded = ks.load_token(site_origin="https://q.example", profile_id="1")
    assert loaded == {"token": "tok123", "expires_at": "2099-01-01T00:00:00", "username": "u"}


def test_rotate_token_deletes_old_before_writing_new(fake_backend):
    ks.save_token(site_origin="https://q.example", profile_id="1",
                   token="old", expires_at="2020-01-01T00:00:00", username="u")
    result = ks.rotate_token_secret(site_origin="https://q.example", profile_id="1",
                                     new_token="new", expires_at="2099-01-01T00:00:00", username="u")
    assert result.ok
    assert ks.load_token(site_origin="https://q.example", profile_id="1")["token"] == "new"


def test_rotate_token_delete_failure_is_fail_closed(fake_backend, monkeypatch):
    ks.save_token(site_origin="https://q.example", profile_id="1",
                   token="old", expires_at="2020-01-01T00:00:00", username="u")

    def _boom(service, key):
        raise keyring.errors.PasswordDeleteError("locked")

    monkeypatch.setattr(keyring, "delete_password", _boom)
    result = ks.rotate_token_secret(site_origin="https://q.example", profile_id="1",
                                     new_token="new", expires_at="2099-01-01T00:00:00", username="u")
    assert result.ok is False and "手動清除" in result.error


def test_broker_credentials_partial_write_failure_keeps_successful_field_and_restores_only_failed_snapshot(
    monkeypatch,
):
    backend = _FlakyBackend()
    monkeypatch.setattr(keyring, "get_keyring", lambda: backend)
    monkeypatch.setattr(keyring, "get_password", backend.get_password)
    monkeypatch.setattr(keyring, "set_password", backend.set_password)
    monkeypatch.setattr(keyring, "delete_password", backend.delete_password)
    # 先寫一次成功值當快照基準
    ks.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                api_key="k1", secret_key="s1")
    backend._set_calls = 0  # 重置：下一次 save_broker_credentials 呼叫時第二個 set_password 失敗

    result = ks.save_broker_credentials(site_origin="https://q.example", profile_id="1",
                                         api_key="k2", secret_key="s2")
    assert result.ok is False
    loaded = ks.load_broker_credentials(site_origin="https://q.example", profile_id="1")
    assert loaded == {"api_key": "k1", "secret_key": "s1"}  # 復原成寫入前快照
