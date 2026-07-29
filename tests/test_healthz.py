"""/healthz 健康碼（T0.3）：下單子系統真故障（connect 失敗/設定錯）→ 503；刻意停用
（缺金鑰的本機/唯讀部署）與 ready → 200；無 order_state（無法判斷）→ 200。feed 停滯不翻
503（由 OpsAlerter 盤中告警負責）。"""
from quanquant.broker.session_state import OrderSessionState
from quanquant.web.deps import get_order_session_state


def _with_state(anon_client, state):
    anon_client.app.dependency_overrides[get_order_session_state] = lambda: state
    return anon_client


def test_healthz_503_when_order_subsystem_unhealthy(anon_client):
    st = OrderSessionState()
    st.mark_unhealthy("connect 失敗（已 redact）")
    r = _with_state(anon_client, st).get("/healthz")
    assert r.status_code == 503
    body = r.json()
    assert body["status"] == "unhealthy"
    assert body["order_subsystem"]["status"] == "unhealthy"


def test_healthz_200_when_order_subsystem_disabled(anon_client):
    st = OrderSessionState()
    st.mark_disabled("缺下單金鑰（本機/唯讀部署，刻意停用）")
    r = _with_state(anon_client, st).get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["order_subsystem"]["status"] == "disabled"


def test_healthz_200_when_order_subsystem_ready(anon_client):
    st = OrderSessionState()
    st.mark_ready()
    r = _with_state(anon_client, st).get("/healthz")
    assert r.status_code == 200
    assert r.json()["order_subsystem"]["status"] == "ready"


def test_healthz_200_when_no_order_state(anon_client):
    # 未設 order_state（無法判斷下單子系統健康）→ 維持 200（healthz 為公開 uptime 檢查）
    anon_client.app.dependency_overrides[get_order_session_state] = lambda: None
    r = anon_client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["order_subsystem"] is None
