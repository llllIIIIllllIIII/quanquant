import pytest
import websockets
import websockets.frames

from quanquant.agent.ws_client import TokenRejectedError, WebsocketsTransport


class _FakeWs:
    def __init__(self, exc):
        self._exc = exc

    async def recv(self):
        raise self._exc

    async def send(self, data): ...
    async def close(self): ...


async def test_receive_raises_token_rejected_on_close_code_1008():
    transport = WebsocketsTransport("ws://x", token="t")
    transport._ws = _FakeWs(websockets.exceptions.ConnectionClosedError(
        rcvd=websockets.frames.Close(1008, "policy violation"), sent=None,
    ))
    with pytest.raises(TokenRejectedError):
        await transport.receive()


async def test_receive_reraises_other_close_codes_unchanged():
    transport = WebsocketsTransport("ws://x", token="t")
    transport._ws = _FakeWs(websockets.exceptions.ConnectionClosedError(
        rcvd=websockets.frames.Close(1011, "internal error"), sent=None,
    ))
    with pytest.raises(websockets.exceptions.ConnectionClosed):
        await transport.receive()
