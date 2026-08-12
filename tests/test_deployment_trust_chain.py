"""Task 16：部署信任鏈驗收——Caddy 段依賴預設忽略外來 XFF＋uvicorn 段
`forwarded_allow_ips`＝固定 Caddy IP/CIDR 字面值（見 docs/superpowers/specs/
2026-08-12-agent-setup-gui-design.md §4.3/§7）。

四項驗收：
1. 本機開發（無 proxy 信任設定）：偽造 XFF 不被採信，回真實 socket peer IP。
2. 雙來源測試：兩個不同來源 IP 都「經過」同一個被信任的 Caddy IP 送出、各自帶自己的
   XFF，ProxyHeadersMiddleware 換算出的 client IP 必須彼此不同、且都不等於 Caddy IP
   本身——per-IP 限流不合流。
3. 偽造來源測試：非被信任 IP 直連送來的 XFF 不被採信，client IP 仍是連線本身的 peer。
4. 靜態設定驗證：Settings 預設值與 docker-compose.yml 的 ipam/服務網路設定互相一致
   （FORWARDED_ALLOW_IPS 必須等於 compose 裡固定寫死的 caddy ipv4_address 字面值，
   不得用服務別名——uvicorn 只做 IP/CIDR 字面比對不解析 DNS）。
"""
import httpx
import pytest

from quanquant.web.routers.agent_device import client_ip
from fastapi import FastAPI, Request


def _build_probe_app():
    app = FastAPI()

    @app.get("/whoami")
    def whoami(request: Request):
        return {"ip": client_ip(request)}

    return app


@pytest.mark.asyncio
async def test_client_ip_reflects_direct_peer_when_no_proxy_trusted():
    """本機開發情境（無 proxy 設定）：不解析任何 X-Forwarded-For，直接回 socket peer IP，
    即使外部偽造 XFF 也不被採信（spec §4.3「app 端不得解析任意 XFF」）。"""
    app = _build_probe_app()
    transport = httpx.ASGITransport(app=app, client=("203.0.113.5", 12345))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        resp = await c.get("/whoami", headers={"X-Forwarded-For": "9.9.9.9"})
    assert resp.json()["ip"] == "203.0.113.5"   # 偽造的 9.9.9.9 未被採信


@pytest.mark.asyncio
async def test_two_different_source_ips_are_recorded_distinctly_through_caddy_ip():
    """雙來源測試（spec §4.3 驗收）：模擬兩個不同來源 IP 都「經過」同一個 Caddy 容器 IP
    （172.28.0.10）送出、各自帶自己的 X-Forwarded-For；uvicorn 的
    ProxyHeadersMiddleware（forwarded_allow_ips=172.28.0.10）採信該標頭時，兩者記到的
    client IP 必須不同、都不等於 Caddy 自己的 IP——限流不合流。"""
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    app = ProxyHeadersMiddleware(_build_probe_app(), trusted_hosts=["172.28.0.10"])
    transport = httpx.ASGITransport(app=app, client=("172.28.0.10", 443))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        r1 = await c.get("/whoami", headers={"X-Forwarded-For": "198.51.100.1"})
        r2 = await c.get("/whoami", headers={"X-Forwarded-For": "198.51.100.2"})
    ip1, ip2 = r1.json()["ip"], r2.json()["ip"]
    assert ip1 != ip2
    assert "172.28.0.10" not in (ip1, ip2)


@pytest.mark.asyncio
async def test_untrusted_proxy_ip_is_not_honored():
    """偽造來源測試：request 不是從被信任的 172.28.0.10 送來（例如攻擊者直連 app 容器），
    ProxyHeadersMiddleware 不採信其 XFF，client IP 仍是連線本身的 peer。"""
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    app = ProxyHeadersMiddleware(_build_probe_app(), trusted_hosts=["172.28.0.10"])
    transport = httpx.ASGITransport(app=app, client=("6.6.6.6", 555))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        resp = await c.get("/whoami", headers={"X-Forwarded-For": "1.2.3.4"})
    assert resp.json()["ip"] == "6.6.6.6"   # 未被信任來源送的 XFF 不採信


def test_settings_forwarded_allow_ips_defaults_to_loopback():
    from quanquant.config import Settings
    assert Settings().forwarded_allow_ips == "127.0.0.1"


def test_docker_compose_pins_caddy_ip_and_app_forwarded_allow_ips():
    import yaml
    compose = yaml.safe_load(open("docker-compose.yml"))
    assert compose["networks"]["quanquant_net"]["ipam"]["config"][0]["subnet"] == "172.28.0.0/24"
    caddy_ip = compose["services"]["caddy"]["networks"]["quanquant_net"]["ipv4_address"]
    assert compose["services"]["app"]["environment"]["FORWARDED_ALLOW_IPS"] == caddy_ip
