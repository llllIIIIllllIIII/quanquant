# tests/test_agent_protocol.py
"""協定 v2（Inc1 Task 2，D7）測試：硬升 PROTOCOL_VERSION=2，v1 訊息一律 validation error；
UpReport 必填 account/mode（D5 回報歸屬蓋章，I7）；UpCmdAck 必填 event_id、error_kind 新增
expired/scope_mismatch；新增 volatile UpQueryResult/UpCommandRejected；UpHealth 必填
status/health_epoch；DownPlace/DownCancel/DownUpdate 必填 account/expires_at；新增
DownQueryQty。"""
import pytest
from pydantic import ValidationError
from quanquant.broker.agent_protocol import (
    PROTOCOL_VERSION, DownCancel, DownPlace, DownQueryQty, DownUpdate, PlaceNative,
    UpCmdAck, UpCommandRejected, UpHealth, UpLogin, UpQueryResult, UpReport,
    parse_downlink, parse_uplink,
)


def _place(**overrides):
    kwargs = dict(cmd_id="c1", account="F1", mode="sim", expires_at="2026-08-07T00:00:00",
                  native=PlaceNative(action="Buy", price="21500", qty=1,
                                     price_type="LMT", order_type="ROD", octype="Auto"))
    kwargs.update(overrides)
    return DownPlace(**kwargs)


def test_protocol_version_is_2():
    assert PROTOCOL_VERSION == 2


# ---------- 下行：DownPlace/DownCancel/DownUpdate +account/expires_at ----------

def test_place_roundtrip():
    cmd = _place()
    parsed = parse_downlink(cmd.model_dump())
    assert isinstance(parsed, DownPlace) and parsed.native.price == "21500"
    assert parsed.account == "F1" and parsed.expires_at == "2026-08-07T00:00:00"


def test_mode_real_rejected():
    with pytest.raises(ValidationError):
        DownPlace(cmd_id="c1", account="F1", mode="real", expires_at="2026-08-07T00:00:00",
                  native=PlaceNative(action="Buy", price="0", qty=1,
                                     price_type="MKT", order_type="IOC", octype="Auto"))


def test_place_missing_account_rejected():
    with pytest.raises(ValidationError):
        DownPlace(cmd_id="c1", mode="sim", expires_at="2026-08-07T00:00:00",
                  native=PlaceNative(action="Buy", price="0", qty=1,
                                     price_type="MKT", order_type="IOC", octype="Auto"))


def test_place_missing_expires_at_rejected():
    with pytest.raises(ValidationError):
        DownPlace(cmd_id="c1", account="F1", mode="sim",
                  native=PlaceNative(action="Buy", price="0", qty=1,
                                     price_type="MKT", order_type="IOC", octype="Auto"))


def test_cancel_requires_account_and_expires_at():
    with pytest.raises(ValidationError):
        DownCancel(cmd_id="c1", mode="sim", ordno="101AA1")
    cancel = DownCancel(cmd_id="c1", account="F1", mode="sim",
                        expires_at="2026-08-07T00:00:00", ordno="101AA1")
    assert cancel.account == "F1" and cancel.expires_at == "2026-08-07T00:00:00"


def test_update_requires_account_and_expires_at():
    with pytest.raises(ValidationError):
        DownUpdate(cmd_id="c1", mode="sim", ordno="101AA1", qty=1)
    update = DownUpdate(cmd_id="c1", account="F1", mode="sim",
                        expires_at="2026-08-07T00:00:00", ordno="101AA1", qty=1)
    assert update.account == "F1" and update.expires_at == "2026-08-07T00:00:00"


# ---------- 新下行：DownQueryQty（G3，D8）----------

def test_query_qty_roundtrip():
    cmd = DownQueryQty(cmd_id="c1", ordno="101AA1", mode="sim")
    parsed = parse_downlink(cmd.model_dump())
    assert isinstance(parsed, DownQueryQty) and parsed.ordno == "101AA1"


def test_query_qty_mode_real_rejected():
    with pytest.raises(ValidationError):
        DownQueryQty(cmd_id="c1", ordno="101AA1", mode="real")


# ---------- 上行 discriminator / 未知類型 ----------

def test_uplink_discriminates_by_type():
    assert isinstance(parse_uplink({"type": "report", "event_id": 3, "kind": "deal_report",
                                    "account": "F1", "mode": "sim", "payload": {"a": 1}}),
                       UpReport)
    assert isinstance(parse_uplink({"type": "cmd_ack", "cmd_id": "c1", "event_id": 1,
                                    "ok": True, "result": {"ordno": "101AA1"}}), UpCmdAck)


def test_unknown_type_raises():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "evil"})


# ---- codex round1 fix5：login 帶協定版本，錯版直接 ValidationError；Inc1 D7 硬升 v2，
# 不做 v1 相容——protocol=1 現在也必須拒收（原本 protocol=1 是可接受的當期版本）。----

def test_uplink_login_rejects_unknown_protocol_version():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "login", "account": "F1", "mode": "sim", "protocol": 999,
                      "health_epoch": 0})


def test_uplink_login_rejects_protocol_v1():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "login", "account": "F1", "mode": "sim", "protocol": 1,
                      "health_epoch": 0})


def test_uplink_login_accepts_current_protocol_version():
    msg = parse_uplink({"type": "login", "account": "F1", "mode": "sim", "protocol": 2,
                        "health_epoch": 0})
    assert isinstance(msg, UpLogin) and msg.account == "F1" and msg.health_epoch == 0


# ---- codex round2 fix1：protocol 版本必填化——UpLogin.protocol 原本有 default=1，缺欄位
# 的 login 會自動補齊通過驗證，等於版本協商可被繞過。改成必填後，缺欄位必須 ValidationError。

def test_uplink_login_missing_protocol_field_rejected():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "login", "account": "F1", "mode": "sim", "health_epoch": 0})


# ---- R3-2：health_epoch 為本 session 健康狀態基準宣告，同樣必填（不可靜默補 0 繞過）。----

def test_uplink_login_missing_health_epoch_rejected():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "login", "account": "F1", "mode": "sim", "protocol": 2})


# ---------- S#12：UpReport 缺 account/mode 拒收（D5 回報歸屬蓋章，不可變，I7）----------

def test_uplink_report_missing_account_rejected():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "report", "event_id": 1, "kind": "deal_report",
                      "mode": "sim", "payload": {}})


def test_uplink_report_missing_mode_rejected():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "report", "event_id": 1, "kind": "deal_report",
                      "account": "F1", "payload": {}})


def test_uplink_report_mode_real_rejected():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "report", "event_id": 1, "kind": "deal_report",
                      "account": "F1", "mode": "real", "payload": {}})


# ---------- UpCmdAck +event_id；error_kind 新值合法（D4 kind×outcome 轉移表）----------

def test_cmd_ack_missing_event_id_rejected():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "cmd_ack", "cmd_id": "c1", "ok": True})


@pytest.mark.parametrize("error_kind", [
    "trade_not_found", "exception", "timeout", "mode_mismatch", "expired", "scope_mismatch",
])
def test_cmd_ack_error_kind_values_accepted(error_kind):
    ack = UpCmdAck(cmd_id="c1", event_id=1, ok=False, error_kind=error_kind)
    assert ack.error_kind == error_kind


def test_cmd_ack_invalid_error_kind_rejected():
    with pytest.raises(ValidationError):
        UpCmdAck(cmd_id="c1", event_id=1, ok=False, error_kind="not_a_real_kind")


# ---------- 新訊息 round-trip：UpQueryResult（volatile，reconcile 快照與 query_qty 共用，
# R1-7：無 event_id，不進 outbox）----------

def test_query_result_roundtrip():
    msg = UpQueryResult(cmd_id="c1", result={"payloads": [], "newest": None})
    parsed = parse_uplink(msg.model_dump())
    assert isinstance(parsed, UpQueryResult) and parsed.cmd_id == "c1"
    assert parsed.result == {"payloads": [], "newest": None}
    assert "event_id" not in msg.model_dump()   # volatile：不帶 outbox 追蹤鍵


def test_query_result_missing_result_rejected():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "query_result", "cmd_id": "c1"})


# ---------- 新訊息 round-trip：UpCommandRejected（volatile，failstop 專用，R2-5）----------

def test_command_rejected_roundtrip():
    msg = UpCommandRejected(cmd_id="c1", error_kind="failstop")
    parsed = parse_uplink(msg.model_dump())
    assert isinstance(parsed, UpCommandRejected) and parsed.error_kind == "failstop"
    assert "event_id" not in msg.model_dump()   # volatile：不帶 outbox 追蹤鍵


def test_command_rejected_only_accepts_failstop():
    with pytest.raises(ValidationError):
        UpCommandRejected(cmd_id="c1", error_kind="timeout")


# ---------- UpHealth +status/detail/health_epoch（G2 R2-3；generation 不進 payload）----------

def test_up_health_roundtrip_with_status_and_epoch():
    msg = UpHealth(status="ok", detail=None, health_epoch=3)
    parsed = parse_uplink(msg.model_dump())
    assert isinstance(parsed, UpHealth) and parsed.status == "ok" and parsed.health_epoch == 3


def test_up_health_failstop_status_with_detail():
    msg = UpHealth(status="failstop", detail="buffer 寫入失敗", health_epoch=4)
    parsed = parse_uplink(msg.model_dump())
    assert parsed.status == "failstop" and parsed.detail == "buffer 寫入失敗"


def test_up_health_missing_status_rejected():
    with pytest.raises(ValidationError):
        UpHealth(health_epoch=0)


def test_up_health_missing_health_epoch_rejected():
    with pytest.raises(ValidationError):
        UpHealth(status="ok")


def test_up_health_invalid_status_rejected():
    with pytest.raises(ValidationError):
        UpHealth(status="degraded", health_epoch=0)
