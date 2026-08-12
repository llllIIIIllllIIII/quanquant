import hashlib

import httpx
import pytest

from quanquant.agent.device_flow_client import (
    AGENT_AUTHORIZE_PATH, DeviceFlowClient, VerificationPathMismatchError,
)


def _mock_transport(responses):
    calls = []

    def handler(request):
        calls.append(request)
        return responses.pop(0)

    return httpx.MockTransport(handler), calls


@pytest.mark.asyncio
async def test_initiate_rejects_verification_path_mismatch():
    resp = httpx.Response(200, json={
        "device_code": "d", "user_code": "AAAA-BBBB",
        "verification_path": "//evil.example/agent/authorize",  # network-path reference 攻擊
        "interval": 5, "expires_in": 600,
    })
    transport, _ = _mock_transport([resp])
    async with httpx.AsyncClient(transport=transport, base_url="https://quant.example") as http:
        client = DeviceFlowClient(site_origin="https://quant.example", http_client=http)
        with pytest.raises(VerificationPathMismatchError):
            await client.initiate()


@pytest.mark.asyncio
async def test_initiate_builds_approval_url_from_builtin_constant_not_server_value():
    resp = httpx.Response(200, json={
        "device_code": "d", "user_code": "AAAA-BBBB",
        "verification_path": AGENT_AUTHORIZE_PATH, "interval": 5, "expires_in": 600,
    })
    transport, calls = _mock_transport([resp])
    async with httpx.AsyncClient(transport=transport, base_url="https://quant.example") as http:
        client = DeviceFlowClient(site_origin="https://quant.example", http_client=http)
        state = await client.initiate()
        assert client.approval_url == f"https://quant.example{AGENT_AUTHORIZE_PATH}"
    body = calls[0].content
    import json
    challenge = json.loads(body)["code_challenge"]
    assert challenge == hashlib.sha256(client._code_verifier.encode()).hexdigest()


@pytest.mark.asyncio
async def test_poll_until_done_auto_restarts_on_consumed():
    init_resp = httpx.Response(200, json={
        "device_code": "d1", "user_code": "AAAA-BBBB",
        "verification_path": AGENT_AUTHORIZE_PATH, "interval": 0, "expires_in": 600,
    })
    consumed_resp = httpx.Response(200, json={"state": "consumed"})
    reinit_resp = httpx.Response(200, json={
        "device_code": "d2", "user_code": "CCCC-DDDD",
        "verification_path": AGENT_AUTHORIZE_PATH, "interval": 0, "expires_in": 600,
    })
    approved_resp = httpx.Response(200, json={
        "state": "approved", "token": "tok", "profile_id": "1",
        "username": "tester", "token_expires_at": "2099-01-01T00:00:00",
    })
    transport, calls = _mock_transport([init_resp, consumed_resp, reinit_resp, approved_resp])
    async with httpx.AsyncClient(transport=transport, base_url="https://quant.example") as http:
        client = DeviceFlowClient(site_origin="https://quant.example", http_client=http)
        await client.initiate()
        result = await client.poll_until_done()
    assert result["token"] == "tok"
    assert client.user_code == "CCCC-DDDD"  # 已切到重開後的新一輪
