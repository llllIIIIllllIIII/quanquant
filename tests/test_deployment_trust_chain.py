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
async def test_multi_hop_xff_uses_rightmost_untrusted_real_peer_not_leftmost_forged():
    """opus 終審測試強化：既有測試只驗過單筆 XFF，補一個多筆案例。header 帶兩段——
    最左 `1.2.3.4` 是攻擊者可自由偽造、自己塞進請求裡的假位址；最右
    `198.51.100.9` 模擬真正經過信任 Caddy 轉發時、由 Caddy 附加上去的真實來源
    （現實中 reverse_proxy 一律「附加」而非覆寫既有 XFF，惡意使用者能左邊塞任何值，
    但塞不了最右邊那格）。uvicorn `ProxyHeadersMiddleware.get_trusted_client_address()`
    從右往左掃，找到第一個不在 `trusted_hosts` 的值就採信為 client——這裡驗證結果是
    最右側那個真實 peer，偽造的最左值完全不被採信，也不等於 Caddy 自己的 IP（避免
    per-IP 限流被三個位址中的任一個錯誤合流／誤判）。"""
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    app = ProxyHeadersMiddleware(_build_probe_app(), trusted_hosts=["172.28.0.10"])
    transport = httpx.ASGITransport(app=app, client=("172.28.0.10", 443))
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as c:
        resp = await c.get("/whoami", headers={"X-Forwarded-For": "1.2.3.4, 198.51.100.9"})
    resolved_ip = resp.json()["ip"]
    assert resolved_ip == "198.51.100.9"          # 最右側、非信任的真實 peer 被採信
    assert resolved_ip != "1.2.3.4"                # 偽造的最左值不被誤判為 client
    assert resolved_ip != "172.28.0.10"            # 也不會跟 Caddy 自己的位址混淆


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
