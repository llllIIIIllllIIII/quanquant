"""011（2026-09-10，需求 A）：指標設定 dialog 動作區固定——參數一多，「儲存／取消」與
「＋ 新增」不再被推到卷軸底下。`chart.js` 的 Alpine 元件（`addIndicatorLine` 等）依賴
瀏覽器 DOM／Alpine 執行環境，不像 `indicators.js`／`chart-guards.js` 那樣是可
`require()` 的純邏輯模組（見 `tests/js/*.test.mjs` 的既有慣例——那些檔案本身就有
`module.exports`），故這裡改用內容斷言：讀 `app.css`／`dashboard.html`／`chart.js`
原始碼文字，確認關鍵 CSS 規則、dashboard 結構、`addIndicatorLine` 關鍵行為標記都存在。
"""
from pathlib import Path

STATIC_DIR = Path(__file__).resolve().parent.parent / "src/quanquant/web/static"
TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "src/quanquant/web/templates"

APP_CSS = (STATIC_DIR / "app.css").read_text(encoding="utf-8")
CHART_JS = (STATIC_DIR / "chart.js").read_text(encoding="utf-8")
DASHBOARD_HTML = (TEMPLATES_DIR / "dashboard.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# CSS：article 限高＋flex column＋header/footer 固定、兩欄各自捲、sticky 欄頭／sticky footer
# ---------------------------------------------------------------------------

def test_ind_settings_is_flex_column_with_max_height_and_hidden_overflow():
    """病灶修正核心：article 不再讓 Pico 預設的 `overflow: auto` 把 header/footer 一起捲走。
    （`.ind-settings {` 選擇器在檔內出現兩次——舊的 `min-width` 規則與這裡新增的規則，
    故直接比對完整多行區塊，不用 split 抓第一個容易抓錯舊規則。）"""
    assert (
        ".ind-settings {\n"
        "  display: flex; flex-direction: column;\n"
        "  max-height: min(86vh, 780px); overflow: hidden;\n"
        "}"
    ) in APP_CSS


def test_ind_settings_header_and_footer_do_not_shrink():
    assert ".ind-settings > header, .ind-settings > footer { flex: 0 0 auto; }" in APP_CSS


def test_ind_split_is_the_scrolling_layer_with_min_height_zero():
    """踩點提示：`.ind-split` 沒有 `min-height: 0` 的話 flex item 預設 `min-height: auto`
    不會縮，內捲不會生效——這是最常見的失敗原因，明確斷言它存在。"""
    assert ".ind-settings > .ind-split { flex: 1 1 auto; min-height: 0; overflow: hidden; }" in APP_CSS


def test_left_and_right_columns_scroll_independently():
    assert ".ind-settings .ind-list, .ind-settings .ind-detail { overflow-y: auto; min-height: 0; }" in APP_CSS


def test_detail_head_is_sticky_with_opaque_background():
    """sticky 列要給不透明底色，否則捲動時底下的參數列會透出來（踩點提示）。"""
    assert ".ind-detail-head {" in APP_CSS
    block = APP_CSS.split(".ind-detail-head {", 1)[1].split("}", 1)[0]
    assert "position: sticky" in block
    assert "top: 0" in block
    assert "background: var(--qq-surface)" in block


def test_narrow_breakpoint_keeps_whole_dialog_scrolling_but_sticks_footer():
    """<768px：維持整個視窗一起捲（沿用既有斷點，不改），但「儲存／取消」sticky 在底部。"""
    assert ".ind-settings { max-height: min(90vh, 780px); overflow-y: auto; }" in APP_CSS
    assert (
        ".ind-settings > footer.form-actions { position: sticky; bottom: 0; "
        "background: var(--qq-surface); }"
    ) in APP_CSS


# ---------------------------------------------------------------------------
# dashboard.html：「＋ 新增」移到右欄欄頂固定列，與指標名同一列
# ---------------------------------------------------------------------------

def test_add_line_button_lives_in_sticky_detail_head_with_title():
    assert '<div class="ind-detail-head">' in DASHBOARD_HTML
    # 只抓到「＋ 新增」鈕收尾（頭列本身含巢狀 <div>，不能用第一個 </div> 當邊界）。
    head_block = DASHBOARD_HTML.split('<div class="ind-detail-head">', 1)[1].split("＋ 新增</button>", 1)[0]
    assert 'x-text="e.title"' in head_block
    assert 'addIndicatorLine(e.key, $el)' in head_block


def test_add_line_button_no_longer_inline_after_param_rows():
    """原本的 `form[e.key].params.push({period: 10, color: '#f0b90b'})` 內嵌寫法要整個
    移除——新增邏輯收斂進 `chart.js::addIndicatorLine`，不再由模板直接 push 固定物件。"""
    assert "form[e.key].params.push({period: 10, color: '#f0b90b'})" not in DASHBOARD_HTML


def test_save_and_cancel_stay_in_bottom_footer_not_header():
    """使用者已拍板：儲存留在底部，不搬去標題列（右上角已是 ✕，不相鄰兩個相反動作）。"""
    dialog = DASHBOARD_HTML.split('<dialog :open="settingsOpen">', 1)[1].split("</dialog>", 1)[0]
    header = dialog.split("<header>", 1)[1].split("</header>", 1)[0]
    footer = dialog.split('<footer class="form-actions">', 1)[1].split("</footer>", 1)[0]
    assert "saveSettings()" not in header  # 標題列只有關閉鈕本身（rel="prev" 的 ✕）
    assert "saveSettings()" in footer
    assert "取消" in footer


# ---------------------------------------------------------------------------
# chart.js：addIndicatorLine 捲到新列並聚焦；新線預設值取上一條兩倍週期＋未用色（附帶）
# ---------------------------------------------------------------------------

def test_add_indicator_line_method_exists_between_open_and_save_settings():
    assert "addIndicatorLine(key, el) {" in CHART_JS
    # 依參考實作位置：插在 openSettings() 與 saveSettings() 之間
    open_idx = CHART_JS.index("openSettings(key) {")
    add_idx = CHART_JS.index("addIndicatorLine(key, el) {")
    save_idx = CHART_JS.index("async saveSettings() {")
    assert open_idx < add_idx < save_idx


def test_add_indicator_line_scrolls_to_new_row_and_focuses_period_input():
    block = CHART_JS.split("addIndicatorLine(key, el) {", 1)[1].split("\n    },", 1)[0]
    assert "$nextTick" in block
    assert "scrollIntoView" in block
    assert ".focus()" in block
    assert "conf.params.push(" in block


def test_add_indicator_line_default_value_avoids_stacking_identical_lines():
    """附帶（可不做，這次一併做）：新線預設值不再固定 10／黃——避免同週期同色疊在一起
    看不出新增有效果。取「上一條週期 × 2」＋還沒用過的顏色。"""
    block = CHART_JS.split("addIndicatorLine(key, el) {", 1)[1].split("\n    },", 1)[0]
    assert "last.period * 2" in block
    assert "used.includes" in block
