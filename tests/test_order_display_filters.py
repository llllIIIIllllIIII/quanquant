"""006（委託頁搬家順修 P1-8/P1-9）：委託狀態中文化、倉別中文化、委託時間（epoch-ms →
台灣本地時間字串）三個新 Jinja filter 的純函式單元測試。比照 `test_quote_status.py` 對
`_symbol_label` 的測法——直接 import 私有函式測，不經過完整頁面渲染。

`_dt_cst`（終審必修 HIGH-3）：naive-UTC datetime（如 `Order.created_at`）→ 台灣本地
時間字串，修正 `dt` filter 直接套用在 UTC 欄位上裸印 UTC 時刻（差 8 小時）的問題。"""
from datetime import datetime

from quanquant.web.templating import _dt_cst, _ms_dt, _octype_label, _order_type_badge, _status_label


def test_status_label_maps_known_statuses_to_chinese():
    assert _status_label("submitted") == "已委託"
    assert _status_label("partfilled") == "部分成交"
    assert _status_label("filled") == "全部成交"
    assert _status_label("cancelled") == "已取消"
    assert _status_label("failed") == "失敗"
    assert _status_label("unknown") == "狀態不明"


def test_status_label_unknown_value_falls_back_to_raw_not_blank():
    assert _status_label("some_new_status") == "some_new_status"
    assert _status_label(None) == "—"


def test_octype_label_maps_new_cover_auto():
    assert _octype_label("New") == "新倉"
    assert _octype_label("Cover") == "平倉"
    assert _octype_label("Auto") == "自動"


def test_octype_label_unknown_falls_back_to_raw():
    assert _octype_label("Weird") == "Weird"
    assert _octype_label(None) == "—"


def test_order_type_badge_combines_price_order_octype():
    assert _order_type_badge("LMT", "ROD", "New") == "LMT・ROD・新倉"
    assert _order_type_badge("MKT", "IOC", "Cover") == "MKT・IOC・平倉"


def test_order_type_badge_handles_missing_values_gracefully():
    assert _order_type_badge(None, None, None) == "—・—・—"


def test_ms_dt_converts_epoch_ms_utc_to_taiwan_local_string():
    # 2026-06-16 10:00:00 UTC → +08:00 → 2026-06-16 18:00:00
    ms = 1781604000000  # 2026-06-16T10:00:00Z
    assert _ms_dt(ms) == "2026-06-16 18:00:00"


def test_ms_dt_none_returns_placeholder_not_crash():
    assert _ms_dt(None) == "—"


def test_dt_cst_converts_naive_utc_datetime_to_taiwan_local_string():
    """終審必修 HIGH-3：naive datetime（無 tzinfo，代表 UTC，如 Order.created_at）
    +8 小時後才是台灣本地顯示值——不是原封不動 strftime。"""
    naive_utc = datetime(2026, 6, 16, 10, 0, 0)
    assert _dt_cst(naive_utc) == "2026-06-16 18:00:00"


def test_dt_cst_none_returns_placeholder_not_crash():
    assert _dt_cst(None) == "—"


def test_dt_cst_respects_custom_format():
    naive_utc = datetime(2026, 6, 16, 23, 30, 0)  # 跨日：UTC 23:30 → CST 隔天 07:30
    assert _dt_cst(naive_utc, "%Y-%m-%d %H:%M") == "2026-06-17 07:30"
