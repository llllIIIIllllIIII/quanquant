# tests/test_agent_protocol.py
import pytest
from pydantic import ValidationError
from quanquant.broker.agent_protocol import (
    DownPlace, PlaceNative, UpCmdAck, UpReport, parse_downlink, parse_uplink,
)


def test_place_roundtrip():
    cmd = DownPlace(cmd_id="c1", mode="sim",
                    native=PlaceNative(action="Buy", price="21500", qty=1,
                                       price_type="LMT", order_type="ROD", octype="Auto"))
    parsed = parse_downlink(cmd.model_dump())
    assert isinstance(parsed, DownPlace) and parsed.native.price == "21500"


def test_mode_real_rejected():
    with pytest.raises(ValidationError):
        DownPlace(cmd_id="c1", mode="real",
                  native=PlaceNative(action="Buy", price="0", qty=1,
                                     price_type="MKT", order_type="IOC", octype="Auto"))


def test_uplink_discriminates_by_type():
    assert isinstance(parse_uplink({"type": "report", "event_id": 3,
                                    "kind": "deal_report", "payload": {"a": 1}}), UpReport)
    assert isinstance(parse_uplink({"type": "cmd_ack", "cmd_id": "c1", "ok": True,
                                    "result": {"ordno": "101AA1"}}), UpCmdAck)


def test_unknown_type_raises():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "evil"})


# ---- codex round1 fix5：login 帶協定版本，錯版直接 ValidationError（端點既有
# invalid-frame 路徑會忽略，agent 永不 ready，避免版本不合的 agent 誤連上）----

def test_uplink_login_rejects_unknown_protocol_version():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "login", "account": "F1", "mode": "sim", "protocol": 999})


def test_uplink_login_accepts_current_protocol_version():
    from quanquant.broker.agent_protocol import UpLogin
    msg = parse_uplink({"type": "login", "account": "F1", "mode": "sim", "protocol": 1})
    assert isinstance(msg, UpLogin) and msg.account == "F1"


# ---- codex round2 fix1：protocol 版本必填化——UpLogin.protocol 原本有 default=1，缺欄位
# 的 login 會自動補齊通過驗證，等於版本協商可被繞過。改成必填後，缺欄位必須 ValidationError。

def test_uplink_login_missing_protocol_field_rejected():
    with pytest.raises(ValidationError):
        parse_uplink({"type": "login", "account": "F1", "mode": "sim"})
