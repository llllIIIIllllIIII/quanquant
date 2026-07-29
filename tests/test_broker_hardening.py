"""Tier 0 真錢硬化：券商回報落地路徑不得靜默丟單。

_json_safe 必須保證輸出「完全 JSON 可序列化」（否則 commit_raw_callback 的 json.dumps
會在 Solace/.NET callback thread 拋例外、整包成交/委託回報遺失）；_on_order_cb 必須在
序列化/落地失敗時退化保存 + 記錄，永不把例外拋進 callback thread、也不靜默丟單。
"""
import json
import logging
from datetime import datetime
from decimal import Decimal

from quanquant.broker.shioaji_adapter import ShioajiAdapter


class _Weird:
    """無 to_dict / 無 keys 的物件——舊 _json_safe 會原封返回，害下游 json.dumps 炸。"""
    def __repr__(self):
        return "WEIRD"


def test_json_safe_coerces_nonserializable_leaf_so_dumps_never_throws():
    msg = {"order": {"price": 43737.0, "obj": _Weird()}, "qty": 1, "action": "Buy"}
    safe = ShioajiAdapter._json_safe(msg)
    json.dumps(safe)  # 關鍵：不得拋 TypeError
    assert safe["order"]["obj"] == "WEIRD"      # 非可序列化葉節點 → 字串
    assert safe["order"]["price"] == 43737.0    # 原生數值保留
    assert safe["qty"] == 1 and safe["action"] == "Buy"


def test_json_safe_handles_datetime_and_decimal_leaves():
    safe = ShioajiAdapter._json_safe({"ts": datetime(2026, 7, 28, 18, 8), "px": Decimal("41536.5")})
    json.dumps(safe)  # 不炸（datetime/Decimal 皆非 JSON 原生型別）


def test_json_safe_preserves_nested_mapping_and_primitives():
    """既有正常路徑不回退：巢狀 mapping/原生值照舊完整保留。"""
    safe = ShioajiAdapter._json_safe({"a": {"b": [1, 2.0, "x", True, None]}})
    assert safe == {"a": {"b": [1, 2.0, "x", True, None]}}


def _bare_adapter(session_factory):
    """繞過 __init__ 建最小 adapter：_on_order_cb 只用到 _session_factory / broker / _json_safe。"""
    adapter = object.__new__(ShioajiAdapter)
    adapter._session_factory = session_factory
    adapter.broker = "shioaji"
    return adapter


def test_on_order_cb_never_raises_into_callback_thread_when_staging_fails(caplog):
    """DB 全掛時，callback 不得把例外拋回券商背景執行緒（會殺掉整條回報通道），且要記錄。"""
    def _boom():
        raise RuntimeError("DB down")

    adapter = _bare_adapter(_boom)
    with caplog.at_level(logging.ERROR):
        adapter._on_order_cb("FuturesDeal", {"trade_id": "X", "price": 1.0})  # 不得拋
    assert caplog.records  # 有記錄下來（非靜默）


def test_on_order_cb_stages_degraded_payload_when_primary_commit_fails(caplog):
    """主落地失敗→退化保存一筆帶原始 repr 的紀錄（供 reconcile/人工補救），不整包遺失。"""
    staged = []
    calls = {"n": 0}

    def _factory():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("first commit path boom")  # 主路徑落地失敗
        class _S:
            def __enter__(self_): return self_
            def __exit__(self_, *a): return False
        return _S()

    import quanquant.broker.shioaji_adapter as mod
    orig = mod.commit_raw_callback

    def _spy(session_factory, *, kind, broker, payload):
        staged.append(payload)
        session_factory()  # 第一次 raise（主路徑），第二次成功（退化保存）

    mod.commit_raw_callback = _spy
    try:
        adapter = _bare_adapter(_factory)
        with caplog.at_level(logging.ERROR):
            adapter._on_order_cb("FuturesOrder", {"id": "O1"})
    finally:
        mod.commit_raw_callback = orig

    assert any(isinstance(p, dict) and p.get("_unparsed") for p in staged)  # 有退化保存
