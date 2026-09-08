"""R2-10：正式／模擬共用 segmented control macro（templates/macros/segmented.html）。
直接渲染 macro 本身驗證 aria 單選結構＋兩態 class 不同；頁面層級驗收（P0-1 選中態正確、
套用範圍涵蓋委託／成交／未平倉／交易績效／交易日記）另見 test_orders_routes.py 與
test_journal_routes.py / test_stats_routes.py 的對應測試。"""
from quanquant.web.templating import templates

_TEMPLATE_SRC = """
{% from "macros/segmented.html" import mode_tabs %}
{{ mode_tabs(mode, base_path) }}
"""


def _render(mode: str, base_path: str = "/orders/queue") -> str:
    tmpl = templates.env.from_string(_TEMPLATE_SRC)
    return tmpl.render(mode=mode, base_path=base_path)


def test_macro_produces_aria_single_choice_radiogroup_structure():
    html = _render("real")
    assert 'role="radiogroup"' in html
    assert html.count('role="radio"') == 2
    assert 'aria-checked="true"' in html
    assert 'aria-checked="false"' in html


def test_macro_two_states_have_different_selected_class():
    real_selected = _render("real")
    sim_selected = _render("sim")
    # 正式被選中時：正式那顆有 is-selected，模擬沒有；反之亦然——兩態的 class 組合不同。
    assert "is-selected" in real_selected
    assert "is-selected" in sim_selected
    assert real_selected != sim_selected


def test_macro_real_mode_selects_real_tab_only():
    html = _render("real")
    real_tag = html.split("正式")[0].rsplit("<a", 1)[-1]
    sim_tag = html.split("模擬")[0].rsplit("<a", 1)[-1]
    assert "is-selected" in real_tag
    assert "is-selected" not in sim_tag


def test_macro_sim_mode_selects_sim_tab_only():
    html = _render("sim")
    real_tag = html.split("正式")[0].rsplit("<a", 1)[-1]
    sim_tag = html.split("模擬")[0].rsplit("<a", 1)[-1]
    assert "is-selected" not in real_tag
    assert "is-selected" in sim_tag


def test_macro_links_use_base_path_and_mode_query():
    html = _render("real", base_path="/orders/deals")
    assert 'href="/orders/deals?mode=real"' in html
    assert 'href="/orders/deals?mode=sim"' in html
