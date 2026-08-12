import hashlib

from quanquant.auth.device_flow import create_device_code


def _pending_code(session, *, verifier="v" * 43):
    challenge = hashlib.sha256(verifier.encode()).hexdigest()
    raw, row = create_device_code(session, request_ip="203.0.113.5", code_challenge=challenge)
    return raw, row


def test_get_authorize_page_requires_login(anon_client):
    resp = anon_client.get("/agent/authorize", follow_redirects=False)
    assert resp.status_code in (303, 401)


def test_lookup_shows_confirmation_for_valid_pending_code(client, session):
    raw, row = _pending_code(session)
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize", data={"user_code": row.user_code, "csrf_token": csrf})
    assert resp.status_code == 200 and row.user_code in resp.text
    assert "核准" in resp.text and "拒絕" in resp.text


def test_lookup_unknown_code_shows_generic_error(client):
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize", data={"user_code": "ZZZZ-0000", "csrf_token": csrf})
    assert resp.status_code == 200 and "找不到" in resp.text


def test_decide_without_valid_csrf_rejected(client, session):
    raw, row = _pending_code(session)
    resp = client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "approve", "csrf_token": "forged"})
    assert resp.status_code == 403
    session.refresh(row)
    assert row.status == "pending"


def test_decide_approve_binds_current_user_and_conditional_update(client, session, user):
    raw, row = _pending_code(session)
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "approve", "csrf_token": csrf})
    assert resp.status_code == 200
    session.refresh(row)
    assert row.status == "approved" and row.user_id == user.id


def test_decide_deny_sets_denied(client, session):
    raw, row = _pending_code(session)
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "deny", "csrf_token": csrf})
    assert resp.status_code == 200
    session.refresh(row)
    assert row.status == "denied"


def test_decide_already_processed_is_rejected_not_overwritten(client, session, user):
    raw, row = _pending_code(session)
    row.status, row.user_id = "approved", user.id
    session.add(row); session.commit()
    get_resp = client.get("/agent/authorize")
    csrf = get_resp.cookies.get("qq_csrf_authorize")
    resp = client.post("/agent/authorize/decide",
                        data={"device_code_id": row.id, "decision": "deny", "csrf_token": csrf})
    assert resp.status_code == 200 and "已處理" in resp.text
    session.refresh(row)
    assert row.status == "approved"  # 沒被 deny 蓋掉
