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
