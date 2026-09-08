"""agent → server 的 WS 傳輸層（Inc0 Task 12）：純 I/O，不含協定語意——訊息形狀由
`quanquant.broker.agent_protocol` 定義、由 `runner.py` 組裝/解析。

`Transport` 是 runner 依賴的最小介面（結構型別，`AgentRunner` 建構時注入），測試用
`_FakeTransport` 替身即可，不需真的連網。`WebsocketsTransport` 是唯一的正式實作，用
`websockets` 套件連 server 的 `/ws/agent` 端點，header `x-agent-token` 帶認證 token
（server 端見 `quanquant.web.routers.agent_ws`）。

`additional_headers` 是 `websockets` 14+ 才有的參數名（舊版 <14 用 `extra_headers`）；pyproject
已顯式宣告 `websockets>=14` 對齊此用法，不在程式碼加版本分支相容舊版。
"""
import json
from typing import Protocol

import websockets


class Transport(Protocol):
    async def connect(self) -> None: ...
    async def send(self, msg: dict) -> None: ...
    async def receive(self) -> dict: ...
    async def close(self) -> None: ...


class TokenRejectedError(RuntimeError):
    """WS 握手被 server 以 close code 1008 拒絕（token 無效/停用/非 owner，見
    web/routers/agent_ws.py::_authenticate 的既有語意）。GUI 決策樹用它判斷『不能再信
    metadata 說 token 未過期』，headless 路徑不特別處理（沿用既有例外傳播/backoff 行為，
    不影響 G5）。"""


class WebsocketsTransport:
    """websockets 套件實作；header x-agent-token 帶 token。"""

    def __init__(self, url: str, *, token: str) -> None:
        self._url = url
        self._token = token
        self._ws = None

    async def connect(self) -> None:
        self._ws = await websockets.connect(
            self._url, additional_headers={"x-agent-token": self._token},
        )

    async def send(self, msg: dict) -> None:
        await self._ws.send(json.dumps(msg))

    async def receive(self) -> dict:
        try:
            data = await self._ws.recv()
        except websockets.exceptions.ConnectionClosed as exc:
            code = getattr(exc, "code", None)
            if code is None:
                code = getattr(getattr(exc, "rcvd", None), "code", None)
            if code == 1008:
                raise TokenRejectedError("agent WS 握手被拒（token 無效/停用/非 owner）") from exc
            raise
        return json.loads(data)

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
