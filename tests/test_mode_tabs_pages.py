"""R2-10：委託／成交／未平倉／交易績效／交易日記等頁面套用共用 mode segmented control
後，選中態必須正確（P0-1 那類「條件寫反」的永久解——這裡驗證的是套用結果，macro 本身的
單元測試見 test_mode_tabs_macro.py）。"""


def _selected_tab(html: str, label: str) -> bool:
    tag = html.split(label)[0].rsplit("<a", 1)[-1]
    return "is-selected" in tag


def test_journal_page_real_mode_selects_real_tab_only(client):
    html = client.get("/journal?mode=real").text
    assert _selected_tab(html, "正式") is True
    assert _selected_tab(html, "模擬") is False


def test_journal_page_sim_mode_selects_sim_tab_only(client):
    html = client.get("/journal?mode=sim").text
    assert _selected_tab(html, "正式") is False
    assert _selected_tab(html, "模擬") is True


def test_stats_page_real_mode_selects_real_tab_only(client):
    html = client.get("/stats?mode=real").text
    assert _selected_tab(html, "正式") is True
    assert _selected_tab(html, "模擬") is False


def test_stats_page_sim_mode_selects_sim_tab_only(client):
    html = client.get("/stats?mode=sim").text
    assert _selected_tab(html, "正式") is False
    assert _selected_tab(html, "模擬") is True


def test_journal_and_stats_use_shared_segmented_control_markup(client):
    """不要每頁手寫條件——兩頁的 mode 分頁都應該是同一份 macro 輸出（radiogroup 結構）。"""
    journal_html = client.get("/journal?mode=real").text
    stats_html = client.get("/stats?mode=real").text
    assert 'role="radiogroup"' in journal_html
    assert 'role="radiogroup"' in stats_html
