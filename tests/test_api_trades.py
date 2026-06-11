def _closed_payload(**over):
    data = {
        "symbol": "TXF", "direction": "long",
        "entry_time": "2026-06-01T09:00", "entry_price": "18000",
        "exit_time": "2026-06-01T10:00", "exit_price": "18100",
        "size": "1", "point_value": "200", "tags": "突破",
    }
    data.update(over)
    return data


def test_pages_render(client):
    for path in ("/", "/journal", "/stats", "/quote", "/trades/new"):
        assert client.get(path).status_code == 200


def test_create_list_update_delete(client):
    # Mutations return an empty body + HX-Trigger; the table is re-fetched by the
    # filter form (refreshtable). State is verified via GET /trades.
    r = client.post("/trades", data=_closed_payload())
    assert r.status_code == 200
    assert r.headers.get("HX-Trigger") == "closemodal, refreshtable"
    listed = client.get("/trades").text
    assert "TXF" in listed and "20,000" in listed

    u = client.put("/trades/1", data=_closed_payload(exit_price="18200"))
    assert u.status_code == 200 and u.headers.get("HX-Trigger") == "closemodal, refreshtable"
    assert "40,000" in client.get("/trades").text

    d = client.delete("/trades/1")
    assert d.status_code == 200 and d.headers.get("HX-Trigger") == "refreshtable"
    assert "尚無交易" in client.get("/trades").text


def test_validation_error_retargets_form(client):
    # exit_time without exit_price -> validation error
    bad = _closed_payload()
    bad.pop("exit_price")
    r = client.post("/trades", data=bad)
    assert r.status_code == 200
    assert r.headers.get("HX-Retarget") == ".form-error-slot"
    assert "error-banner" in r.text


def test_status_filter(client):
    client.post("/trades", data=_closed_payload(symbol="TXF"))
    client.post(
        "/trades",
        data={"symbol": "MTX", "direction": "long", "entry_time": "2026-06-02T09:00",
              "entry_price": "18000", "size": "1", "point_value": "200", "tags": ""},
    )
    open_rows = client.get("/trades", params={"status": "open"}).text
    assert "MTX" in open_rows and "TXF" not in open_rows


def test_export_endpoints(client):
    client.post("/trades", data=_closed_payload())
    csv = client.get("/stats/export.csv")
    assert csv.status_code == 200
    assert "attachment" in csv.headers.get("content-disposition", "")
    xlsx = client.get("/stats/export.xlsx")
    assert xlsx.status_code == 200 and len(xlsx.content) > 4000
