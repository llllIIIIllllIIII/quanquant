def _payload(**over) -> dict:
    base = {
        "symbol": "TXF", "timeframe": "5m", "left_kind": "price",
        "op": "gte", "right_kind": "const", "right_value": 18000,
    }
    base.update(over)
    return base


def test_alert_crud(client):
    r = client.post("/api/alerts", json=_payload())
    assert r.status_code == 201
    aid = r.json()["id"]

    listed = client.get("/api/alerts").json()
    assert any(a["id"] == aid for a in listed)

    p = client.patch(f"/api/alerts/{aid}", json={"enabled": False})
    assert p.status_code == 200 and p.json()["enabled"] is False

    assert client.delete(f"/api/alerts/{aid}").status_code == 204
    assert client.get("/api/alerts").json() == []


def test_alert_indicator_operands_ok(client):
    r = client.post("/api/alerts", json=_payload(
        left_kind="indicator", left_name="ma", left_period=20,
        op="cross_up", right_kind="indicator", right_name="ma", right_period=60,
    ))
    assert r.status_code == 201


def test_alert_validation_422(client):
    assert client.post("/api/alerts", json=_payload(op="bogus")).status_code == 422
    assert client.post("/api/alerts", json=_payload(timeframe="7m")).status_code == 422
    assert client.post("/api/alerts", json=_payload(
        left_kind="indicator", left_name="ma")).status_code == 422  # no period
    bad = _payload()
    del bad["right_value"]
    assert client.post("/api/alerts", json=bad).status_code == 422  # const w/o value
    assert client.post("/api/alerts", json=_payload(
        right_kind="indicator")).status_code == 422  # indicator w/o name+period


def test_events_and_stream(client):
    assert client.get("/api/alerts/events").json() == []
    assert client.get("/alerts/stream").status_code == 200  # empty when notify unset (tests)
