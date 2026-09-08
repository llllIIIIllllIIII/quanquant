"""OpsAlerter（T0.3 營運告警管道）：非阻塞、跨執行緒安全、未設定 dev chat 時整體 no-op、
同 key 節流。與 3 人共用的價格警示 chat 分離。"""
import asyncio
from types import SimpleNamespace

from quanquant.notify.ops_alerter import OpsAlerter, build_ops_alerter


class _FakeNotifier:
    def __init__(self, configured: bool = True) -> None:
        self._configured = configured
        self.texts: list[str] = []

    @property
    def configured(self) -> bool:
        return self._configured

    async def send_text(self, text: str) -> None:
        self.texts.append(text)


def _drain(alerter: OpsAlerter, thunk) -> _FakeNotifier:
    """在一個 running loop 上 attach + 執行 thunk（可能排程 send），讓排程的 send task 跑完。"""
    async def _run():
        alerter.attach_loop(asyncio.get_running_loop())
        thunk()
        await asyncio.sleep(0.02)  # 讓 call_soon_threadsafe → ensure_future(send_text) 跑完
    asyncio.run(_run())
    return alerter._notifier  # type: ignore[return-value]


def test_emit_delivers_formatted_text_when_configured():
    fake = _FakeNotifier(configured=True)
    alerter = OpsAlerter(fake, mode="real")
    _drain(alerter, lambda: alerter.emit("k", "下單失敗", "client_order_id=abc", severity="critical"))
    assert len(fake.texts) == 1
    body = fake.texts[0]
    assert "下單失敗" in body and "client_order_id=abc" in body
    assert "[real]" in body and "🚨" in body  # mode 標記 + critical emoji


def test_emit_is_noop_when_unconfigured_never_leaks_to_shared_chat():
    fake = _FakeNotifier(configured=False)  # 未設定 dev chat
    alerter = OpsAlerter(fake)
    _drain(alerter, lambda: alerter.emit("k", "任何營運事件", "detail"))
    assert fake.texts == []  # 絕不送出（不誤入共用價格警示 chat）


def test_emit_never_raises_when_no_loop_attached():
    # 沒 attach_loop（loop=None）→ emit 不得 raise，只記 log 略過
    alerter = OpsAlerter(_FakeNotifier(configured=True))
    alerter.emit("k", "title", "detail")  # 不得拋


def test_emit_is_threadsafe_from_worker_thread():
    fake = _FakeNotifier(configured=True)
    alerter = OpsAlerter(fake)

    async def _run():
        alerter.attach_loop(asyncio.get_running_loop())
        await asyncio.to_thread(alerter.emit, "k", "threadpool 來的告警", "x")  # 從別的執行緒 emit
        await asyncio.sleep(0.02)
    asyncio.run(_run())
    assert len(fake.texts) == 1 and "threadpool 來的告警" in fake.texts[0]


def test_should_send_throttles_same_key_within_window():
    t = {"v": 1000.0}
    alerter = OpsAlerter(_FakeNotifier(), clock=lambda: t["v"])
    assert alerter._should_send("feed_stale", 300.0) is True   # 首次放行
    t["v"] = 1100.0
    assert alerter._should_send("feed_stale", 300.0) is False  # window 內壓下
    t["v"] = 1400.0
    assert alerter._should_send("feed_stale", 300.0) is True   # 逾 window 再放行


def test_should_send_never_throttles_when_window_zero():
    alerter = OpsAlerter(_FakeNotifier(), clock=lambda: 0.0)
    assert alerter._should_send("place_failed", 0.0) is True
    assert alerter._should_send("place_failed", 0.0) is True   # throttle<=0：每次都送


def test_place_failed_wrapper_marks_severity_by_classification():
    fake = _FakeNotifier(configured=True)
    alerter = OpsAlerter(fake)
    _drain(alerter, lambda: alerter.place_failed(
        client_order_id="c1", symbol="TXF", action="Buy", qty=1, classification="failed"))
    assert "🚨" in fake.texts[0] and "券商明確拒絕" in fake.texts[0]

    fake2 = _FakeNotifier(configured=True)
    alerter2 = OpsAlerter(fake2)
    _drain(alerter2, lambda: alerter2.place_failed(
        client_order_id="c2", symbol="TXF", action="Sell", qty=2, classification="unknown"))
    assert "⚠️" in fake2.texts[0] and "待 reconcile" in fake2.texts[0]


def test_kill_switch_wrapper_surfaces_open_orders_when_enabling():
    fake = _FakeNotifier(configured=True)
    alerter = OpsAlerter(fake)
    _drain(alerter, lambda: alerter.kill_switch(enabled=True, actor_user_id=7, open_order_count=3))
    assert "kill switch 啟動" in fake.texts[0] and "3 筆未成交掛單" in fake.texts[0]


def test_kill_switch_wrapper_records_scope_self_vs_global():
    """D3：兩層 kill switch 的翻閘告警要能分辨 scope（個人急停 vs 全站總閘），供人工稽核。"""
    fake_global = _FakeNotifier(configured=True)
    alerter_global = OpsAlerter(fake_global)
    _drain(alerter_global, lambda: alerter_global.kill_switch(
        enabled=True, actor_user_id=7, scope="global", open_order_count=0))
    assert "全站" in fake_global.texts[0]

    fake_self = _FakeNotifier(configured=True)
    alerter_self = OpsAlerter(fake_self)
    _drain(alerter_self, lambda: alerter_self.kill_switch(
        enabled=True, actor_user_id=7, scope="self", open_order_count=0))
    assert "個人" in fake_self.texts[0]


def test_failstop_wrapper_enabled_and_disabled_messages():
    """Inc1 D9/G2（Task 12）：agent fail-stop latch 事件——進入/解除各自的文案與 severity。"""
    fake = _FakeNotifier(configured=True)
    alerter = OpsAlerter(fake)
    _drain(alerter, lambda: alerter.failstop(user_id=7, enabled=True, detail="buffer 落地失敗"))
    assert "進入 fail-stop" in fake.texts[0] and "user_id=7" in fake.texts[0]
    assert "buffer 落地失敗" in fake.texts[0]
    assert "🚨" in fake.texts[0]   # critical

    fake2 = _FakeNotifier(configured=True)
    alerter2 = OpsAlerter(fake2)
    _drain(alerter2, lambda: alerter2.failstop(user_id=7, enabled=False))
    assert "解除 fail-stop" in fake2.texts[0]


def test_build_ops_alerter_token_fallback_and_configured_gate():
    # ops token 空 → 沿用 telegram_bot_token；chat_id 有值 → configured
    s = SimpleNamespace(ops_telegram_bot_token="", telegram_bot_token="MAINTOK",
                        ops_telegram_chat_id="devchat")
    a = build_ops_alerter(s, mode="sim")
    assert a.configured is True

    # chat_id 空 → 整體 no-op（不誤入共用 chat）
    s2 = SimpleNamespace(ops_telegram_bot_token="X", telegram_bot_token="MAINTOK",
                         ops_telegram_chat_id="")
    assert build_ops_alerter(s2).configured is False
