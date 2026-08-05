import asyncio
import pytest
from quanquant.broker.agent_channel import AgentChannel, AgentNativeGateway
from quanquant.broker.agent_protocol import UpCmdAck
from quanquant.broker.base import (
    AgentCommandTimeoutError, AgentUnavailableError, OrderError, TradeNotFoundError,
)
from quanquant.broker.shioaji_adapter import _classify_place_failure


class _Sink:
    def __init__(self):
        self.msgs = []
    async def __call__(self, msg):
        self.msgs.append(msg)


def _ready_channel():
    ch, sink = AgentChannel(), _Sink()
    ch.attach(sink)
    ch.mark_logged_in("F1")
    return ch, sink


async def test_request_before_attach_raises_unavailable():
    with pytest.raises(AgentUnavailableError):
        await AgentChannel().request({"type": "health", "cmd_id": "c1"}, cmd_id="c1", timeout=1)


async def test_request_resolves_when_ack_arrives():
    ch, sink = _ready_channel()
    task = asyncio.create_task(ch.request({"type": "health", "cmd_id": "c1"},
                                          cmd_id="c1", timeout=1))
    await asyncio.sleep(0)
    ch.resolve_ack(UpCmdAck(cmd_id="c1", ok=True, result={"x": 1}))
    ack = await task
    assert ack.ok and sink.msgs[0]["cmd_id"] == "c1"


async def test_request_timeout_raises_command_timeout():
    ch, _ = _ready_channel()
    with pytest.raises(AgentCommandTimeoutError):
        await ch.request({"type": "health", "cmd_id": "c1"}, cmd_id="c1", timeout=0.01)


async def test_detach_fails_pending_with_timeout_error():
    ch, _ = _ready_channel()
    task = asyncio.create_task(ch.request({"type": "health", "cmd_id": "c1"},
                                          cmd_id="c1", timeout=5))
    await asyncio.sleep(0)
    ch.detach()
    with pytest.raises(AgentCommandTimeoutError):
        await task


class _StubChannel(AgentChannel):
    """request 直接回 canned ack / raise，測 gateway 對映。"""
    def __init__(self, ack=None, exc=None):
        super().__init__()
        self._ack, self._exc = ack, exc
        self.sent = []
    async def request(self, cmd, *, cmd_id, timeout):
        self.sent.append(cmd)
        if self._exc:
            raise self._exc
        return self._ack


def _req():
    from decimal import Decimal
    from quanquant.broker.types import OrderRequest
    return OrderRequest(client_order_id="c-1", symbol="TXF", action="Buy", qty=1,
                        price=Decimal("21500"), price_type="LMT", order_type="ROD",
                        octype="Auto", user_id=1)


async def test_gateway_place_maps_ack_fields_and_serializes_price_as_str():
    ch = _StubChannel(ack=UpCmdAck(cmd_id="x", ok=True,
                                   result={"ordno": "101AA1", "broker_order_id": "101AA1"}))
    gw = AgentNativeGateway(ch, timeout_seconds=1)
    assert await gw.place(_req()) == {"ordno": "101AA1", "broker_order_id": "101AA1"}
    assert ch.sent[0]["native"]["price"] == "21500"


async def test_gateway_trade_not_found_raises_typed():
    ch = _StubChannel(ack=UpCmdAck(cmd_id="x", ok=False, error_kind="trade_not_found",
                                   result={"ordno": "NOPE"}))
    gw = AgentNativeGateway(ch, timeout_seconds=1)
    with pytest.raises(TradeNotFoundError):
        await gw.cancel("NOPE")


async def test_gateway_error_message_preserves_broker_code_for_classification():
    ch = _StubChannel(ack=UpCmdAck(cmd_id="x", ok=False, error_kind="exception",
                                   message="code: 406 Please sign F002 first"))
    gw = AgentNativeGateway(ch, timeout_seconds=1)
    with pytest.raises(OrderError) as ei:
        await gw.place(_req())
    assert _classify_place_failure(ei.value) == "failed"
