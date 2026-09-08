import hashlib



def _initiate(client, verifier="v" * 43):
    challenge = hashlib.sha256(verifier.encode()).hexdigest()
    resp = client.post("/api/agent/device-code", json={"code_challenge": challenge})
    return resp, verifier


def test_initiate_returns_device_code_and_fixed_verification_path(anon_client):
    resp, _ = _initiate(anon_client)
    assert resp.status_code == 200
    body = resp.json()
    assert body["verification_path"] == "/agent/authorize"
    assert body["interval"] == 5 and body["expires_in"] == 600
    assert len(body["device_code"]) > 20 and len(body["user_code"]) == 9


def test_initiate_does_not_require_login(anon_client):
    resp, _ = _initiate(anon_client)
    assert resp.status_code == 200


def test_initiate_rate_limited_after_burst(anon_client, monkeypatch):
    from quanquant.config import get_settings
    get_settings.cache_clear()
    monkeypatch.setenv("AGENT_DEVICE_CODE_RATE_BURST", "2")
    monkeypatch.setenv("AGENT_DEVICE_CODE_RATE_PER_MINUTE", "2")
    get_settings.cache_clear()
    for _ in range(2):
        assert _initiate(anon_client)[0].status_code == 200
    assert _initiate(anon_client)[0].status_code == 429
    get_settings.cache_clear()


def test_initiate_per_ip_active_cap_returns_429(anon_client, monkeypatch):
    from quanquant.config import get_settings
    get_settings.cache_clear()
    monkeypatch.setenv("AGENT_DEVICE_CODE_MAX_ACTIVE_PER_IP", "2")
    get_settings.cache_clear()
    for _ in range(2):
        assert _initiate(anon_client)[0].status_code == 200
    assert _initiate(anon_client)[0].status_code == 429
    get_settings.cache_clear()


def test_initiate_global_active_cap_returns_429(anon_client, monkeypatch):
    from quanquant.config import get_settings
    get_settings.cache_clear()
    # per-IP cap 留預設值（10，遠高於下面的 3 次呼叫）——排除 per-IP cap 干擾，確保觸發的
    # 是全域上限而不是同 IP 上限。
    monkeypatch.setenv("AGENT_DEVICE_CODE_MAX_ACTIVE_GLOBAL", "2")
    get_settings.cache_clear()
    for _ in range(2):
        assert _initiate(anon_client)[0].status_code == 200
    assert _initiate(anon_client)[0].status_code == 429
    get_settings.cache_clear()


def test_poll_unknown_device_code_returns_404(anon_client):
    resp = anon_client.post("/api/agent/device-token", json={"device_code": "nope", "code_verifier": "v"})
    assert resp.status_code == 404


def test_poll_pending_then_approved_flow(anon_client, session, user):
    resp, verifier = _initiate(anon_client)
    body = resp.json()
    poll = anon_client.post("/api/agent/device-token",
                             json={"device_code": body["device_code"], "code_verifier": verifier})
    assert poll.json()["state"] == "pending"

    from quanquant.db.models import AgentDeviceCode
    from sqlmodel import select
    row = session.exec(select(AgentDeviceCode).where(AgentDeviceCode.user_code == body["user_code"])).first()
    row.status, row.user_id, row.current_interval = "approved", user.id, 0
    session.add(row)
    session.commit()

    poll2 = anon_client.post("/api/agent/device-token",
                              json={"device_code": body["device_code"], "code_verifier": verifier})
    assert poll2.json()["state"] == "approved" and poll2.json()["username"] == user.username
