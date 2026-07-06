# K 線漲跌配色個人化偏好

**日期：** 2026-07-06
**狀態：** 已核准設計，待實作計畫

## 問題

K 線圖的漲跌顏色目前寫死在 `chart.js` 的 `DARK_STYLES`（`upColor:#16c784` 綠＝漲、`downColor:#ea3943` 紅＝跌），是歐美慣例。但台灣／亞洲市場慣例相反（紅＝漲、綠＝跌）。不同使用者對「哪個顏色代表漲」的直覺不同，需要讓**每個使用者**能選擇自己的配色，並跨裝置／跨 session 記住。

## 範圍

**包含：**
- 兩種預設配色二選一：`green_up`（綠漲紅跌，維持現狀，預設）／ `red_up`（紅漲綠跌，台灣慣例）。
- 偏好綁定使用者帳號，持久化於 DB，跨裝置一致。
- 兩個切換入口：帳戶設定頁、K 線圖工具列快捷鈕；兩者寫同一欄位天然同步。
- 圖表工具列切換為即時套用（不需重整）。

**刻意排除（YAGNI）：**
- 自訂 HEX 色票。
- per-symbol 配色（本系統目前僅監控 TXF）。
- 指標線、背景、格線等其他元素變色 —— 僅 candle 的漲跌相關色。

## 資料模型

`User` 新增欄位：

```python
chart_color_scheme: str | None = None   # "green_up" | "red_up"；None 視為 green_up
```

- 值域限定 `{"green_up", "red_up"}`，於 service 層驗證。
- `None`（既有使用者、未設定過）在讀取時一律視為預設 `green_up` —— 不驚動現有使用者。
- 遷移：`migrate.py` 的 `_MIGRATIONS` 加入 `("users", "chart_color_scheme", "VARCHAR")`。startup 時 `ALTER TABLE users ADD COLUMN chart_color_scheme VARCHAR`（nullable，SQLite／Postgres 皆可攜）。`ensure_columns` 已具冪等性（欄位存在則跳過）。

## 後端

### Service

`auth/service.py` 新增：

```python
VALID_COLOR_SCHEMES = {"green_up", "red_up"}

def set_color_scheme(db: Session, user: User, scheme: str) -> bool:
    """設定使用者 K 線配色。回傳是否成功（scheme 非法則 False，不寫入）。"""
    if scheme not in VALID_COLOR_SCHEMES:
        return False
    user.chart_color_scheme = scheme
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()
    return True
```

不 bump `token_version`（配色與登入無關，不應登出其他裝置）。

### 端點

單一真相來源＝`User.chart_color_scheme`；兩入口共用 `set_color_scheme`。

1. **帳戶頁表單** —— `auth.py`：
   `POST /account/color-scheme`，`scheme: str = Form(...)`。
   成功 → `RedirectResponse("/account", 303)`；非法值 → 重render `account.html` 帶 error。

2. **圖表工具列即時切換** —— 放在 `candles.py`（與其他 `/api/chart/*`、per-user 端點同群）：
   `PUT /api/user/color-scheme`，JSON body `{"scheme": "..."}`。
   合法 → `Response(204)`；非法 → `HTTPException(422)`。

兩端點皆 `Depends(get_current_user)`。

## 前端

### 配色定義

基色沿用現有色碼，scheme 只決定綁定方向：

- `#16c784`（綠）與 `#ea3943`（紅）為兩個基色。
- `green_up`：up=綠、down=紅（現狀）。
- `red_up`：up=紅、down=綠。
- `noChange` 維持 `#888888`。

於 `chart.js` 以一個純函式產生 candle 樣式片段：

```js
function candleColorStyles(scheme) {
  const up = scheme === "red_up" ? "#ea3943" : "#16c784";
  const down = scheme === "red_up" ? "#16c784" : "#ea3943";
  return {
    candle: {
      bar: { upColor: up, downColor: down, noChangeColor: "#888888",
             upBorderColor: up, downBorderColor: down, noChangeBorderColor: "#888888",
             upWickColor: up, downWickColor: down, noChangeWickColor: "#888888" },
      priceMark: { last: { upColor: up, downColor: down } },
    },
  };
}
```

（實際 `candle` 樣式的巢狀結構以現有 `DARK_STYLES.candle` 為準對齊。）

### 注入與即時套用

- `dashboard.html`：仿現有 `window.QQ_SYMBOL`，加 `<script>window.QQ_COLOR_SCHEME = "{{ color_scheme }}";</script>`。`color_scheme` 由 `dashboard.py` 從 `request.state.user.chart_color_scheme or "green_up"` 帶入 context。
- `chart.js`：
  - init 時把 `candleColorStyles(window.QQ_COLOR_SCHEME || "green_up")` 併入 `DARK_STYLES` 後傳給 `klinecharts.init`。
  - 新增 `applyColorScheme(scheme)`：`this.chart.setStyles(candleColorStyles(scheme))` 即時重繪，無需 reload。
  - 工具列快捷鈕（Alpine `@click`）：翻轉 scheme → `applyColorScheme()` 立即套用 → `fetch(PUT /api/user/color-scheme)` 存回。按鈕圖示／title 反映目前配色。

### 帳戶頁

`account.html`：於密碼表單下方加一組 radio（或 `<select>`），預選目前值（`user.chart_color_scheme or "green_up"`，由 `account_page` 帶入 context），`POST /account/color-scheme`。

## 測試

**後端（pytest，必須全綠）：**
- `set_color_scheme` 接受 `green_up`／`red_up` 並持久化；拒絕非法值（回 False、不寫入）。
- 不 bump `token_version`。
- `POST /account/color-scheme`：合法 303 redirect＋DB 已更新；非法值 render error。
- `PUT /api/user/color-scheme`：合法 204＋持久化；非法 422；未登入 401/redirect。
- migration 冪等：欄位已存在時 `ensure_columns` 不報錯。

**前端（手動驗證）：**
- 工具列切換即時翻色、不需重整。
- 重整後配色保留。
- 帳戶頁切換後，儀表板反映新配色。
- 另一裝置／瀏覽器登入同帳號，配色一致。

## 不變式（勿回退）

- candle 讀取路徑（raw-SQL→float、sync routes）不受影響 —— 本功能只改樣式，不碰資料路徑。
- KLineCharts 釘版 v9.8.12；`chart.setStyles` 為 v9 API，升版前需重驗。
- 新增欄位走 `ensure_columns` nullable ADD COLUMN，維持 SQLite／Postgres 雙方言可攜。
