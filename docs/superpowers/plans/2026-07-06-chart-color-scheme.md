# K 線漲跌配色個人化偏好 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓每個使用者選擇 K 線漲跌配色（`green_up` 綠漲紅跌／`red_up` 紅漲綠跌），綁定帳號、跨裝置持久化，帳戶頁與圖表工具列雙入口即時切換。

**Architecture:** 於 `User` 新增 `chart_color_scheme` 欄位（走 `ensure_columns` nullable ALTER），service 層集中驗證與寫入，兩個薄端點（表單 redirect／JSON 204）共用同一 helper。前端以純函式 `candleColorStyles(scheme)` 產生 candle 樣式，init 時併入 `DARK_STYLES`，切換時用 `chart.setStyles()` 即時重繪並 PUT 存回。

**Tech Stack:** FastAPI、SQLModel、Jinja2、KLineCharts v9.8.12、Alpine.js、pytest。

## Global Constraints

- 值域限定 `{"green_up", "red_up"}`；`None`／未設定一律視為預設 `green_up`。
- 新增欄位必須 nullable，經 `migrate.py` 的 `ensure_columns` ALTER，維持 SQLite／Postgres 雙方言可攜。
- 配色變更**不** bump `token_version`（不影響登入 cookie）。
- 基色沿用現有：綠 `#16c784`、紅 `#ea3943`、noChange `#888888`。
- KLineCharts 釘版 v9.8.12；`chart.setStyles` 為 v9 API，勿升版。
- 只改 candle 漲跌相關樣式；勿碰 candle 資料讀取路徑（raw-SQL→float、sync routes）。
- `uv run pytest` 必須全綠才可部署。

## File Structure

- `src/quanquant/db/models.py` — `User` 加 `chart_color_scheme` 欄位。
- `src/quanquant/db/migrate.py` — `_MIGRATIONS` 加 users 欄位。
- `src/quanquant/auth/service.py` — `VALID_COLOR_SCHEMES` + `set_color_scheme()`。
- `src/quanquant/web/routers/auth.py` — `POST /account/color-scheme`；`account_page` 帶 `color_scheme` context。
- `src/quanquant/web/routers/candles.py` — `PUT /api/user/color-scheme`。
- `src/quanquant/web/routers/dashboard.py` — dashboard context 帶 `color_scheme`。
- `src/quanquant/web/templates/account.html` — 配色 radio 表單。
- `src/quanquant/web/templates/dashboard.html` — 注入 `QQ_COLOR_SCHEME` + 工具列快捷鈕。
- `src/quanquant/web/static/chart.js` — `candleColorStyles()`、init 併入、`applyColorScheme()`、Alpine 切換。
- 測試：`tests/test_migrate.py`、`tests/test_auth_service.py`、`tests/test_auth_routes.py`。

---

### Task 1: `User` 欄位 + 遷移

**Files:**
- Modify: `src/quanquant/db/models.py:167` (User，`telegram_chat_id` 之後)
- Modify: `src/quanquant/db/migrate.py:11-14` (`_MIGRATIONS`)
- Test: `tests/test_migrate.py`

**Interfaces:**
- Produces: `User.chart_color_scheme: str | None`（`"green_up" | "red_up" | None`）；migration tuple `("users", "chart_color_scheme", "VARCHAR")`。

- [ ] **Step 1: Write the failing test**

在 `tests/test_migrate.py` 末尾新增，自建無此欄位的 users 表：

```python
def test_adds_chart_color_scheme_to_users(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_users.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)"
        ))
    ensure_columns(eng)
    insp = inspect(eng)
    assert "chart_color_scheme" in {c["name"] for c in insp.get_columns("users")}


def test_chart_color_scheme_idempotent(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_users2.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)"
        ))
    ensure_columns(eng)
    ensure_columns(eng)  # 第二次不得 raise
    insp = inspect(eng)
    assert "chart_color_scheme" in {c["name"] for c in insp.get_columns("users")}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_migrate.py::test_adds_chart_color_scheme_to_users -v`
Expected: FAIL（欄位不存在，assert 失敗）

- [ ] **Step 3: 加 migration tuple**

`src/quanquant/db/migrate.py`，把 `_MIGRATIONS` 改為：

```python
_MIGRATIONS = [
    ("trades", "user_id", "INTEGER"),
    ("alerts", "user_id", "INTEGER"),
    ("users", "chart_color_scheme", "VARCHAR"),
]
```

- [ ] **Step 4: 加 model 欄位**

`src/quanquant/db/models.py`，`User` 類別 `telegram_chat_id` 那行之後加：

```python
    chart_color_scheme: str | None = None   # "green_up"(綠漲紅跌,預設) | "red_up"(紅漲綠跌)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_migrate.py -v`
Expected: PASS（全部 migrate 測試綠）

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/db/models.py src/quanquant/db/migrate.py tests/test_migrate.py
git commit -m "feat: add User.chart_color_scheme column + migration"
```

---

### Task 2: `service.set_color_scheme`

**Files:**
- Modify: `src/quanquant/auth/service.py` (檔尾，`list_users` 之後)
- Test: `tests/test_auth_service.py`

**Interfaces:**
- Consumes: `User.chart_color_scheme`（Task 1）；`_utcnow`（已 import 於 service.py）。
- Produces: `service.VALID_COLOR_SCHEMES: set[str]`；`service.set_color_scheme(db: Session, user: User, scheme: str) -> bool`。

- [ ] **Step 1: Write the failing test**

在 `tests/test_auth_service.py` 末尾新增（該檔已有 `henry` fixture＝以 session 建立的 admin user）：

```python
def test_set_color_scheme_valid_persists(session, henry):
    assert service.set_color_scheme(session, henry, "red_up") is True
    reloaded = service.get_by_username(session, "henry")
    assert reloaded.chart_color_scheme == "red_up"


def test_set_color_scheme_rejects_invalid(session, henry):
    assert service.set_color_scheme(session, henry, "rainbow") is False
    reloaded = service.get_by_username(session, "henry")
    assert reloaded.chart_color_scheme is None  # 未寫入


def test_set_color_scheme_does_not_bump_token_version(session, henry):
    before = henry.token_version
    service.set_color_scheme(session, henry, "green_up")
    assert henry.token_version == before
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_auth_service.py::test_set_color_scheme_valid_persists -v`
Expected: FAIL with `AttributeError: module 'quanquant.auth.service' has no attribute 'set_color_scheme'`

- [ ] **Step 3: 實作**

`src/quanquant/auth/service.py` 檔尾加：

```python
VALID_COLOR_SCHEMES = {"green_up", "red_up"}


def set_color_scheme(db: Session, user: User, scheme: str) -> bool:
    """設定使用者 K 線配色。scheme 非法則回 False 且不寫入。"""
    if scheme not in VALID_COLOR_SCHEMES:
        return False
    user.chart_color_scheme = scheme
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()
    return True
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_auth_service.py -v -k color_scheme`
Expected: PASS（3 個測試綠）

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/auth/service.py tests/test_auth_service.py
git commit -m "feat: service.set_color_scheme with validation"
```

---

### Task 3: 帳戶頁表單端點 + 模板

**Files:**
- Modify: `src/quanquant/web/routers/auth.py:58-62` (`account_page`)，檔尾加 `POST /account/color-scheme`
- Modify: `src/quanquant/web/templates/account.html`
- Test: `tests/test_auth_routes.py`

**Interfaces:**
- Consumes: `service.set_color_scheme`（Task 2）；`user.chart_color_scheme`（Task 1）。`Form`、`RedirectResponse`、`Session`、`get_session`、`service` 均已於 auth.py import。
- Produces: route `POST /account/color-scheme`（Form `scheme`）；`account.html` 讀 context `color_scheme`。

- [ ] **Step 1: Write the failing test**

在 `tests/test_auth_routes.py` 末尾新增（`client` fixture 已登入為 `tester`）：

```python
def test_account_page_shows_color_scheme_radio(client):
    body = client.get("/account").text
    assert 'name="scheme"' in body
    assert "green_up" in body and "red_up" in body


def test_set_color_scheme_via_account_form(client):
    r = client.post("/account/color-scheme", data={"scheme": "red_up"},
                    follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/account"
    # 重新載入帳戶頁，red_up 被預選
    body = client.get("/account").text
    assert 'value="red_up" checked' in body


def test_set_color_scheme_invalid_shows_error(client):
    r = client.post("/account/color-scheme", data={"scheme": "bad"})
    assert r.status_code == 200
    assert "配色設定無效" in r.text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_auth_routes.py::test_set_color_scheme_via_account_form -v`
Expected: FAIL（404，端點未定義）

- [ ] **Step 3: 更新 `account_page` context + 新增端點**

`src/quanquant/web/routers/auth.py`，`account_page` 改為帶 `color_scheme`：

```python
@router.get("/account", response_class=HTMLResponse)
def account_page(request: Request, user: User = Depends(get_current_user)):
    return templates.TemplateResponse(
        request, "account.html",
        {"active": "account", "error": None,
         "color_scheme": user.chart_color_scheme or "green_up"},
    )
```

檔尾加（`Form` 已於 auth.py 從 fastapi import；`service`、`get_session`、`Session`、`RedirectResponse` 亦已 import）：

```python
@router.post("/account/color-scheme")
def change_color_scheme(
    request: Request,
    scheme: str = Form(...),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    if not service.set_color_scheme(session, user, scheme):
        return templates.TemplateResponse(
            request, "account.html",
            {"active": "account", "error": "配色設定無效",
             "color_scheme": user.chart_color_scheme or "green_up"},
        )
    return RedirectResponse("/account", status_code=303)
```

- [ ] **Step 4: 更新 `account.html`**

在密碼表單 `</form>`（第 13 行）之後、`{% endblock %}` 之前插入：

```html
<h3>K 線配色</h3>
<form method="post" action="/account/color-scheme" style="max-width: 24rem;">
  <fieldset>
    <label>
      <input type="radio" name="scheme" value="green_up"
             {% if color_scheme == 'green_up' %}checked{% endif %}>
      綠漲紅跌（歐美慣例）
    </label>
    <label>
      <input type="radio" name="scheme" value="red_up"
             {% if color_scheme == 'red_up' %}checked{% endif %}>
      紅漲綠跌（台灣慣例）
    </label>
  </fieldset>
  <button type="submit">儲存配色</button>
</form>
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/test_auth_routes.py -v -k color_scheme`
Expected: PASS（3 個測試綠）

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/routers/auth.py src/quanquant/web/templates/account.html tests/test_auth_routes.py
git commit -m "feat: account page color-scheme form"
```

---

### Task 4: 圖表工具列 JSON 端點

**Files:**
- Modify: `src/quanquant/web/routers/candles.py` (檔尾，緊接 `put_chart_state` 之後)
- Test: `tests/test_auth_routes.py`

**Interfaces:**
- Consumes: `service.set_color_scheme`（Task 2）；`get_current_user`、`get_session`、`Request`、`HTTPException`、`Response`、`User`、`Session`（candles.py 已使用）。
- Produces: route `PUT /api/user/color-scheme`，JSON body `{"scheme": "..."}` → 204／422。

- [ ] **Step 1: Write the failing test**

在 `tests/test_auth_routes.py` 末尾新增：

```python
def test_put_color_scheme_api_persists(client):
    r = client.put("/api/user/color-scheme", json={"scheme": "red_up"})
    assert r.status_code == 204
    # 帳戶頁反映新值（同一欄位）
    assert 'value="red_up" checked' in client.get("/account").text


def test_put_color_scheme_api_rejects_invalid(client):
    r = client.put("/api/user/color-scheme", json={"scheme": "nope"})
    assert r.status_code == 422


def test_put_color_scheme_api_requires_auth(anon_client):
    r = anon_client.put("/api/user/color-scheme", json={"scheme": "red_up"},
                        follow_redirects=False)
    assert r.status_code in (401, 303, 307)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_auth_routes.py::test_put_color_scheme_api_persists -v`
Expected: FAIL（405/404，端點未定義）

- [ ] **Step 3: 確認 candles.py import 並實作端點**

檢查 `src/quanquant/web/routers/candles.py` 頂端是否已 `from quanquant.auth import service`；若無則加上。`User`、`get_current_user`、`get_session`、`Request`、`HTTPException`、`Response`、`Session`、`Depends` 皆已於該檔使用。檔尾加：

```python
@router.put("/api/user/color-scheme")
async def put_color_scheme(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    body = await request.json()
    scheme = body.get("scheme") if isinstance(body, dict) else None
    if not isinstance(scheme, str) or not service.set_color_scheme(session, user, scheme):
        raise HTTPException(status_code=422, detail="invalid color scheme")
    return Response(status_code=204)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_auth_routes.py -v -k "put_color_scheme"`
Expected: PASS（3 個測試綠）

- [ ] **Step 5: 全套後端測試回歸**

Run: `uv run pytest -q`
Expected: 全綠（含既有 79+ 測試）

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/routers/candles.py tests/test_auth_routes.py
git commit -m "feat: PUT /api/user/color-scheme endpoint"
```

---

### Task 5: 前端即時套色 + 雙入口

**Files:**
- Modify: `src/quanquant/web/routers/dashboard.py:46-56` (`dashboard` context)
- Modify: `src/quanquant/web/templates/dashboard.html:20-30` (工具列)、`:223` (script 注入)
- Modify: `src/quanquant/web/static/chart.js:57-78` (DARK_STYLES / 新純函式)、`:113-122` (init)、加 `applyColorScheme` 與 Alpine 切換
- Test: 手動驗證（前端無單元測試框架）

**Interfaces:**
- Consumes: `PUT /api/user/color-scheme`（Task 4）；`request.state.user.chart_color_scheme`（Task 1，protected route 已由 `Depends(get_current_user)` 於 `deps.py:57` 設 `request.state.user`）。
- Produces: `window.QQ_COLOR_SCHEME`；`chart.js` 內 `candleColorStyles(scheme)`、`QQChart.colorScheme`、`QQChart.applyColorScheme(scheme)`、Alpine `chartPanel()` 的 `colorScheme` 與 `toggleColorScheme()`。

- [ ] **Step 1: dashboard context 帶 color_scheme**

`src/quanquant/web/routers/dashboard.py`，`dashboard` 路由 context 改為：

```python
@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    settings = get_settings()
    user = request.state.user
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "active": "dashboard",
            "symbol": settings.symbol,
            "color_scheme": (user.chart_color_scheme if user else None) or "green_up",
        },
    )
```

- [ ] **Step 2: dashboard.html 注入 + 工具列鈕**

`src/quanquant/web/templates/dashboard.html`：把第 223 行的注入 script 改為同時注入配色：

```html
<script>window.QQ_SYMBOL = "{{ symbol }}"; window.QQ_COLOR_SCHEME = "{{ color_scheme }}";</script>
```

並在工具列（`pulse-toggle` 那群按鈕附近，約第 22 行前後）加一顆快捷鈕：

```html
<button type="button" class="outline mini color-scheme-toggle"
        @click="toggleColorScheme()"
        :title="colorScheme === 'red_up' ? 'K線配色：紅漲綠跌（點擊切換）' : 'K線配色：綠漲紅跌（點擊切換）'"
        x-text="colorScheme === 'red_up' ? '🔴漲' : '🟢漲'"></button>
```

- [ ] **Step 3: chart.js — 純函式 + init 併入**

`src/quanquant/web/static/chart.js`，在 `DARK_STYLES` 定義（第 78 行 `};` 之後）新增純函式：

```js
  function candleColorStyles(scheme) {
    const up = scheme === "red_up" ? "#ea3943" : "#16c784";
    const down = scheme === "red_up" ? "#16c784" : "#ea3943";
    return {
      candle: {
        bar: {
          upColor: up, downColor: down, noChangeColor: "#888888",
          upBorderColor: up, downBorderColor: down, noChangeBorderColor: "#888888",
          upWickColor: up, downWickColor: down, noChangeWickColor: "#888888",
        },
        priceMark: { last: { upColor: up, downColor: down } },
      },
    };
  }
```

在 `QQChart` 物件加一個狀態欄位（例如 `tf: "1m",` 附近）：

```js
    colorScheme: "green_up",   // 由 Alpine 於 init 前依 window.QQ_COLOR_SCHEME 設定
```

`init()` 內把 `klinecharts.init` 的 styles（第 118-122 行）改為深層合併配色，保留 `DARK_STYLES.candle.tooltip`：

```js
      this.colorScheme = window.QQ_COLOR_SCHEME || "green_up";
      const styles = JSON.parse(JSON.stringify(DARK_STYLES));
      const cc = candleColorStyles(this.colorScheme).candle;
      styles.candle.bar = cc.bar;
      styles.candle.priceMark = cc.priceMark;
      this.chart = klinecharts.init("kchart", {
        timezone: "Asia/Taipei",
        locale: "zh-CN", // v9 built-ins: en-US / zh-CN only (zh-TW unregistered)
        styles,
      });
```

- [ ] **Step 4: chart.js — applyColorScheme 即時套用**

在 `QQChart` 加方法（例如 `_lineStyle` 附近）：

```js
    applyColorScheme(scheme) {
      this.colorScheme = scheme;
      if (this.chart && this.chart.setStyles) {
        this.chart.setStyles(candleColorStyles(scheme));
      }
    },
```

- [ ] **Step 5: chart.js — Alpine chartPanel() 切換**

找到 `chartPanel()` 的 Alpine 定義（回傳含 `deductionOn`、`drawScope` 等狀態的物件）。加入狀態與方法，並在其 init（把 `drawScope`/`drawingsVisible` 交給 `QQChart` 的同一處）加 `QQChart.colorScheme = this.colorScheme;`：

```js
    colorScheme: window.QQ_COLOR_SCHEME || "green_up",

    toggleColorScheme() {
      this.colorScheme = this.colorScheme === "red_up" ? "green_up" : "red_up";
      QQChart.applyColorScheme(this.colorScheme);
      fetch("/api/user/color-scheme", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scheme: this.colorScheme }),
      }).catch(() => { /* 即時已套用；存回失敗僅影響下次載入 */ });
    },
```

- [ ] **Step 6: 手動驗證（RED→GREEN 對照）**

```bash
uv run quanquant-web   # → http://127.0.0.1:8000
```

逐項確認：
1. 登入後儀表板 K 線預設為綠漲紅跌（未設定使用者）。
2. 點工具列配色鈕 → K 棒立即翻成紅漲綠跌，**不需重整**；圖示變 `🔴漲`。
3. 重整頁面 → 仍為紅漲綠跌（已存回 DB）。
4. 進「帳戶設定」→ 配色 radio 預選「紅漲綠跌」；改選「綠漲紅跌」儲存 → 回帳戶頁預選正確。
5. 回儀表板重整 → 綠漲紅跌（帳戶頁與圖表同步同一欄位）。
6. 另一瀏覽器（或無痕）登入同帳號 → 配色一致。
7. tooltip 文字色（`#b8b8c0`）與其他樣式未跑掉。

- [ ] **Step 7: 後端回歸（確認前端改動未破壞 route 測試）**

Run: `uv run pytest -q`
Expected: 全綠。

- [ ] **Step 8: Commit**

```bash
git add src/quanquant/web/routers/dashboard.py src/quanquant/web/templates/dashboard.html src/quanquant/web/static/chart.js
git commit -m "feat: live K-line color-scheme toggle on chart toolbar"
```

---

## 部署備註

依 `CLAUDE.md`：`uv run pytest` 全綠後 `git push` → `./scripts/deploy.sh`。雲端 Postgres 首次啟動時 `ensure_columns` 會自動 `ALTER TABLE users ADD COLUMN chart_color_scheme`（nullable，既有列為 NULL＝預設 green_up）。無需手動遷移。
