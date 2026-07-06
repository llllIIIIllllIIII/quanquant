# QuanQuant UI/UX 大改版 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 依 `docs/superpowers/specs/2026-07-06-ui-ux-redesign-design.md`，將全站改造為專業交易終端風格（深色為主 + 淺色切換），重組儀表板工具列，並加入 logo 回首頁、全螢幕、指標調參入口等流程優化——全程不影響既有功能。

**Architecture:** 保留 Pico CSS 2 為基底，新增 `static/tokens.css`（design tokens，深淺主題以 `html[data-theme]` 換值並覆寫 `--pico-*` 變數）。模板只改 class 與 DOM 排列，**所有 id、Alpine x-data 結構、HTMX 屬性、SSE 端點、API 路由不動**。主題偏好完全複製既有 `chart_color_scheme` 三層模式（models → migrate → service → PUT API → 前端）。

**Tech Stack:** FastAPI + Jinja2、Pico CSS 2（CDN）、HTMX 2 + Alpine 3（CDN）、KLineCharts 9.8.12（釘版）、Google Fonts（Noto Sans TC + JetBrains Mono）、Lucide inline SVG（手貼、無套件）。

## Global Constraints

- KLineCharts 釘版 **9.8.12**，不升版；`chart.js` 的四道渲染防線（stale-response guard、_watchdog、_nextFrame 分幀、完整 lineStyle）**不可移除或改動**
- candle 讀取路徑 raw-SQL→float、routes 為 sync `def` —— 本計畫完全不碰
- 新增 migration 必須 SQLite/Postgres 雙方言可攜（本計畫只用 nullable `ALTER TABLE ... ADD COLUMN`，走既有 `migrate.py` 機制）
- pyproject hatch wheel 設定不可加 force-include；`.env` 與 `*.dump` 不進 git
- 以下 DOM id 為 JS 綁定點，**必須原樣保留**：`#quote`、`#kchart`、`#chart-empty`、`#pulse-toggle`、`#pulse-tg-toggle`、`#trade-tbody`、`#modal-body`、`#logout-form`
- 模板注入 JS 變數一律用 `| tojson`（比照 QQ_COLOR_SCHEME 的 T5-M2 加固）
- 每個 task 結尾 `uv run pytest` 必須全綠才可 commit；commit 訊息格式 `<type>: <description>`（無 attribution）
- 部署不自動：本計畫完成後由使用者決定何時 `./scripts/deploy.sh`

---

### Task 1: Design tokens + 字體 + 導航列改造

**Files:**
- Create: `src/quanquant/web/static/tokens.css`
- Modify: `src/quanquant/web/templates/base.html`（全檔重寫，43 行 → 約 95 行）
- Modify: `src/quanquant/web/static/app.css`（頂部 topnav 區塊替換）
- Test: `tests/test_auth_routes.py`（新增模板斷言）

**Interfaces:**
- Produces: CSS custom properties `--qq-bg`、`--qq-surface`、`--qq-surface-2`、`--qq-border`、`--qq-border-strong`、`--qq-text`、`--qq-text-muted`、`--qq-accent`、`--up`、`--down`、`--qq-font-mono`、`--qq-radius-sm`(6px)、`--qq-radius-lg`(10px)、`--qq-shadow-1`、`--qq-shadow-2` —— 後續所有 task 的 CSS 都引用這些名稱
- Produces: base.html 的 `.topnav`、`.brand`、`.nav-burger`、`.user-menu` 結構；`<html data-theme>` 仍固定 `dark`（Task 5 才接使用者偏好）

- [ ] **Step 1: 寫失敗測試（logo 連結 + tokens.css 載入）**

在 `tests/test_auth_routes.py` 檔尾加入：

```python
def test_navbar_brand_links_home(client):
    r = client.get("/account")  # 任一套用 base.html 的頁面
    assert r.status_code == 200
    assert '<a href="/" class="brand"' in r.text


def test_base_loads_tokens_css(client):
    r = client.get("/account")
    assert '/static/tokens.css' in r.text
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_auth_routes.py -k "brand or tokens" -v`
Expected: 2 FAILED（`assert ... in r.text` 不成立）

- [ ] **Step 3: 建立 `src/quanquant/web/static/tokens.css`**

完整內容：

```css
/* QuanQuant design tokens — trading-terminal restyle (2026-07 redesign).
   Dark is the primary theme; light swaps the same token names.
   Tokens are mapped onto Pico's --pico-* vars so stock Pico widgets
   (forms, dialogs, dropdowns) pick up the palette for free. */

:root {
  --qq-font-mono: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, monospace;
  --qq-radius-sm: 6px;
  --qq-radius-lg: 10px;
  --qq-shadow-1: 0 4px 12px rgba(0, 0, 0, 0.35);
  --qq-shadow-2: 0 8px 24px rgba(0, 0, 0, 0.45);
  --up: #16c784;
  --down: #ea3943;
}

html[data-theme="dark"] {
  --qq-bg: #020617;
  --qq-surface: #0e1223;
  --qq-surface-2: #1a1e2f;
  --qq-border: #26304a;
  --qq-border-strong: #334155;
  --qq-text: #f8fafc;
  --qq-text-muted: #94a3b8;
  --qq-accent: #3b82f6;
}

html[data-theme="light"] {
  --qq-bg: #f1f5f9;
  --qq-surface: #ffffff;
  --qq-surface-2: #e2e8f0;
  --qq-border: #cbd5e1;
  --qq-border-strong: #94a3b8;
  --qq-text: #0f172a;
  --qq-text-muted: #475569;
  --qq-accent: #2563eb;
  --qq-shadow-1: 0 4px 12px rgba(15, 23, 42, 0.12);
  --qq-shadow-2: 0 8px 24px rgba(15, 23, 42, 0.18);
}

/* map onto Pico so its widgets follow the theme */
html[data-theme="dark"], html[data-theme="light"] {
  --pico-background-color: var(--qq-bg);
  --pico-card-background-color: var(--qq-surface);
  --pico-card-sectioning-background-color: var(--qq-surface-2);
  --pico-color: var(--qq-text);
  --pico-muted-color: var(--qq-text-muted);
  --pico-muted-border-color: var(--qq-border);
  --pico-primary: var(--qq-accent);
  --pico-primary-hover: color-mix(in srgb, var(--qq-accent) 85%, white);
  --pico-primary-focus: color-mix(in srgb, var(--qq-accent) 40%, transparent);
  --pico-border-radius: var(--qq-radius-sm);
  --pico-font-family: "Noto Sans TC", system-ui, -apple-system, "Segoe UI",
    "PingFang TC", "Microsoft JhengHei", sans-serif;
}

body { font-family: var(--pico-font-family); }

/* data numerals: prices, stats, table number cells */
.num, .quote .price, .quote .chg, .cards .big, .numcell {
  font-family: var(--qq-font-mono);
  font-variant-numeric: tabular-nums;
}

/* motion discipline */
button, a, select, input, [role="button"] { transition: border-color 150ms ease-out,
  background-color 150ms ease-out, color 150ms ease-out; }
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after { animation: none !important; transition: none !important; }
}

[x-cloak] { display: none !important; }
```

- [ ] **Step 4: 重寫 `src/quanquant/web/templates/base.html`**

完整內容（保留 `#logout-form` id、`active` 變數契約、`request.state.user` guard）：

```html
<!DOCTYPE html>
<html lang="zh-Hant" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QuanQuant</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@400;500;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
  <link rel="stylesheet" href="/static/tokens.css">
  <link rel="stylesheet" href="/static/app.css">
  <script src="https://unpkg.com/htmx.org@2.0.4"></script>
  <script src="https://unpkg.com/htmx-ext-sse@2.2.2/sse.js"></script>
  <script defer src="https://unpkg.com/alpinejs@3.14.8/dist/cdn.min.js"></script>
</head>
<body>
  {% set u = request.state.user if request.state.user is defined else None %}
  <nav class="container-fluid topnav" x-data="{ navOpen: false }">
    <a href="/" class="brand" aria-label="回到儀表板首頁">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2" stroke-linecap="round" aria-hidden="true">
        <path d="M8 6v12M8 9h-3v6h3M16 4v16M16 7h3v8h-3"/>
      </svg>
      <strong>QuanQuant</strong>
    </a>
    <button class="nav-burger" @click="navOpen = !navOpen" aria-label="開啟選單"
            :aria-expanded="navOpen">
      <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2" stroke-linecap="round" aria-hidden="true">
        <line x1="4" y1="7" x2="20" y2="7"/><line x1="4" y1="12" x2="20" y2="12"/>
        <line x1="4" y1="17" x2="20" y2="17"/>
      </svg>
    </button>
    <div class="nav-links" :class="{ open: navOpen }">
      <a href="/" class="nav-link {% if active == 'dashboard' %}active{% endif %}">儀表板</a>
      <a href="/journal" class="nav-link {% if active == 'journal' %}active{% endif %}">交易日記</a>
      <a href="/stats" class="nav-link {% if active == 'stats' %}active{% endif %}">績效統計</a>
    </div>
    <div class="nav-right">
      {% if u %}
      <details class="dropdown user-menu">
        <summary>
          <span class="avatar">{{ u.display_name[:1] }}</span>
          <span class="user-name">{{ u.display_name }}</span>
        </summary>
        <ul dir="rtl">
          {% if u.role == 'admin' %}<li><a href="/admin/users">使用者管理</a></li>{% endif %}
          <li><a href="/account">帳戶設定</a></li>
          <li class="menu-sep"><a href="#" class="danger"
              onclick="document.getElementById('logout-form').submit(); return false;">登出</a></li>
        </ul>
      </details>
      <form id="logout-form" method="post" action="/logout" hidden></form>
      {% endif %}
    </div>
  </nav>
  <main class="container">
    {% block content %}{% endblock %}
  </main>
  <script src="/static/app.js"></script>
</body>
</html>
```

- [ ] **Step 5: 替換 `app.css` 的 topnav 區塊**

把 `app.css` 頂部的兩行 topnav 規則（`.topnav { align-items: center; }` 與 `.topnav a.active { font-weight: 700; }`）替換為：

```css
/* ---- top navigation ---- */
.topnav {
  position: sticky; top: 0; z-index: 100;
  display: flex; align-items: center; gap: 1.25rem;
  background: var(--qq-surface);
  border-bottom: 1px solid var(--qq-border);
  padding-top: 0.4rem; padding-bottom: 0.4rem;
}
.brand {
  display: inline-flex; align-items: center; gap: 0.45rem;
  color: var(--qq-text); text-decoration: none;
}
.brand:hover { color: var(--qq-accent); }
.brand svg { color: var(--qq-accent); }
.nav-links { display: flex; gap: 0.25rem; flex: 1 1 auto; }
.nav-link {
  padding: 0.55rem 0.8rem; color: var(--qq-text-muted); text-decoration: none;
  border-bottom: 2px solid transparent; font-size: 0.92rem;
}
.nav-link:hover { color: var(--qq-text); }
.nav-link.active {
  color: var(--qq-text); font-weight: 700;
  border-bottom-color: var(--qq-accent);
}
.nav-right { display: flex; align-items: center; gap: 0.6rem; margin-left: auto; }
.nav-burger { display: none; margin: 0; width: auto; padding: 0.35rem 0.5rem;
  background: transparent; border: 1px solid transparent; color: var(--qq-text); }
.user-menu summary {
  display: inline-flex; align-items: center; gap: 0.45rem;
  border: 1px solid var(--qq-border); border-radius: var(--qq-radius-sm);
  background: transparent; color: var(--qq-text);
}
.avatar {
  display: inline-flex; align-items: center; justify-content: center;
  width: 1.6rem; height: 1.6rem; border-radius: 50%;
  background: var(--qq-accent); color: #fff; font-size: 0.8rem; font-weight: 700;
}
.user-menu .menu-sep { border-top: 1px solid var(--qq-border); margin-top: 0.25rem; padding-top: 0.25rem; }
.user-menu a.danger { color: var(--down); }

@media (max-width: 767px) {
  .nav-burger { display: inline-flex; }
  .nav-links {
    display: none; position: absolute; top: 100%; left: 0; right: 0;
    flex-direction: column; background: var(--qq-surface);
    border-bottom: 1px solid var(--qq-border); padding: 0.4rem 1rem;
  }
  .nav-links.open { display: flex; }
  .user-name { display: none; }
}
```

- [ ] **Step 6: 跑測試確認通過 + 全套件綠**

Run: `uv run pytest tests/test_auth_routes.py -v && uv run pytest -q`
Expected: 新增 2 測試 PASS；全套件綠（既有模板斷言如 `value="green_up" checked` 不受影響——account.html 未動）

- [ ] **Step 7: 手動驗證**

Run: `uv run quanquant-web`，瀏覽 http://127.0.0.1:8000 。確認：logo 可點回 `/`、active 頁籤有底線、使用者選單開合正常、登出可用、視窗縮到 <768px 出現漢堡選單、頁面底色變 `#020617`。

- [ ] **Step 8: Commit**

```bash
git add src/quanquant/web/static/tokens.css src/quanquant/web/templates/base.html src/quanquant/web/static/app.css tests/test_auth_routes.py
git commit -m "feat: design tokens + trading-terminal navbar (clickable brand, active indicator, burger)"
```

---

### Task 2: `User.theme` 欄位 + migration

**Files:**
- Modify: `src/quanquant/db/models.py:168`（User model，`chart_color_scheme` 下一行）
- Modify: `src/quanquant/db/migrate.py:11-15`（`_MIGRATIONS` 清單）
- Test: `tests/test_migrate.py`

**Interfaces:**
- Produces: `User.theme: str | None`（`"dark"` | `"light"`，None 視為 dark）；DB 欄位 `users.theme VARCHAR`

- [ ] **Step 1: 寫失敗測試**

`tests/test_migrate.py` 檔尾加入（`create_engine`、`inspect`、`text`、`ensure_columns` 該檔已 import）：

```python
def test_adds_theme_to_users(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_users3.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)"
        ))
    ensure_columns(eng)
    insp = inspect(eng)
    assert "theme" in {c["name"] for c in insp.get_columns("users")}


def test_theme_idempotent(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old_users4.db'}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)"
        ))
    ensure_columns(eng)
    ensure_columns(eng)  # 第二次不得 raise
    insp = inspect(eng)
    assert "theme" in {c["name"] for c in insp.get_columns("users")}
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_migrate.py -k theme -v`
Expected: FAILED（`"theme" not in columns`）

- [ ] **Step 3: 實作**

`models.py` User class 內，`chart_color_scheme` 那行（168 行）之後加：

```python
    theme: str | None = None                              # "dark"(預設) | "light" — 介面主題
```

`migrate.py` `_MIGRATIONS` 清單（11-15 行）加一項：

```python
    ("users", "theme", "VARCHAR"),
```

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_migrate.py -v && uv run pytest -q`
Expected: 全 PASS

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/db/models.py src/quanquant/db/migrate.py tests/test_migrate.py
git commit -m "feat: add User.theme column + startup migration"
```

---

### Task 3: `service.set_theme`

**Files:**
- Modify: `src/quanquant/auth/service.py`（檔尾，`set_color_scheme`（119-127 行）之後）
- Test: `tests/test_auth_service.py`

**Interfaces:**
- Consumes: `User.theme`（Task 2）
- Produces: `service.set_theme(db: Session, user: User, theme: str) -> bool`、`service.VALID_THEMES = {"dark", "light"}`

- [ ] **Step 1: 寫失敗測試**

`tests/test_auth_service.py` 檔尾加入（`session`、`henry` fixtures 該檔已有；重載寫法比照該檔 81-95 行 `test_set_color_scheme_*` 的實際寫法）：

```python
def test_set_theme_valid_persists(session, henry):
    assert service.set_theme(session, henry, "light") is True
    reloaded = session.get(User, henry.id)
    assert reloaded.theme == "light"


def test_set_theme_rejects_invalid(session, henry):
    assert service.set_theme(session, henry, "neon") is False
    reloaded = session.get(User, henry.id)
    assert reloaded.theme is None  # 未寫入


def test_set_theme_does_not_bump_token_version(session, henry):
    before = henry.token_version
    service.set_theme(session, henry, "dark")
    assert session.get(User, henry.id).token_version == before
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_auth_service.py -k theme -v`
Expected: FAILED with `AttributeError: ... has no attribute 'set_theme'`

- [ ] **Step 3: 實作**

`service.py` 檔尾（`set_color_scheme` 之後）加：

```python
VALID_THEMES = {"dark", "light"}


def set_theme(db: Session, user: User, theme: str) -> bool:
    """設定介面主題。theme 非法則回 False 且不寫入。"""
    if theme not in VALID_THEMES:
        return False
    user.theme = theme
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()
    return True
```

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_auth_service.py -v && uv run pytest -q`
Expected: 全 PASS

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/auth/service.py tests/test_auth_service.py
git commit -m "feat: service.set_theme with validation"
```

---

### Task 4: `PUT /api/user/theme` endpoint

**Files:**
- Modify: `src/quanquant/web/routers/candles.py`（檔尾，`put_color_scheme`（128-141 行）之後——與 color-scheme PUT 同檔同模式）
- Test: `tests/test_auth_routes.py`

**Interfaces:**
- Consumes: `service.set_theme`（Task 3）
- Produces: `PUT /api/user/theme`，body `{"theme": "dark"|"light"}` → 204；非法/壞 body → 422；未登入比照 color-scheme API 的行為

- [ ] **Step 1: 寫失敗測試**

`tests/test_auth_routes.py` 檔尾加入（結構比照該檔 96-117 行 `test_put_color_scheme_*` 四件組）：

```python
def test_put_theme_api_persists(client):
    r = client.put("/api/user/theme", json={"theme": "light"})
    assert r.status_code == 204


def test_put_theme_api_rejects_invalid(client):
    r = client.put("/api/user/theme", json={"theme": "neon"})
    assert r.status_code == 422


def test_put_theme_api_requires_auth(anon_client):
    r = anon_client.put("/api/user/theme", json={"theme": "light"},
                        follow_redirects=False)
    assert r.status_code in (401, 303, 307)


def test_put_theme_api_bad_body(client):
    r = client.put("/api/user/theme", content=b"not json",
                   headers={"Content-Type": "application/json"})
    assert r.status_code == 422
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_auth_routes.py -k put_theme -v`
Expected: FAILED（404/405，endpoint 尚不存在）

- [ ] **Step 3: 實作**

`candles.py` 檔尾（`put_color_scheme` 之後）加：

```python
@router.put("/api/user/theme")
async def put_theme(
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    try:
        body = await request.json()
    except Exception:
        body = None
    theme = body.get("theme") if isinstance(body, dict) else None
    if not isinstance(theme, str) or not service.set_theme(session, user, theme):
        raise HTTPException(status_code=422, detail="invalid theme")
    return Response(status_code=204)
```

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_auth_routes.py -v && uv run pytest -q`
Expected: 全 PASS

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/routers/candles.py tests/test_auth_routes.py
git commit -m "feat: PUT /api/user/theme endpoint"
```

---

### Task 5: 主題切換前端（navbar 按鈕 + chart 同步 + login localStorage）

**Files:**
- Modify: `src/quanquant/web/templates/base.html`（`data-theme` 接使用者偏好 + navbar 加切換鈕）
- Modify: `src/quanquant/web/static/app.js`（主題切換邏輯）
- Modify: `src/quanquant/web/static/chart.js`（新增 light styles + `applyTheme`，**不動四道防線**）
- Modify: `src/quanquant/web/static/app.css`（切換鈕樣式）
- Modify: `src/quanquant/web/templates/login.html`（localStorage 預載 script；整頁重造在 Task 11）
- Test: `tests/test_auth_routes.py`

**Interfaces:**
- Consumes: `PUT /api/user/theme`（Task 4）、tokens.css 的 `html[data-theme]`（Task 1）
- Produces: `window.QQTheme.toggle()`（app.js）；`qq:theme-changed` CustomEvent（detail = `"dark"|"light"`）；`QQChart.applyTheme(theme)`（chart.js）

- [ ] **Step 1: 寫失敗測試**

`tests/test_auth_routes.py` 檔尾加入：

```python
def test_base_renders_user_theme(client):
    client.put("/api/user/theme", json={"theme": "light"})
    r = client.get("/account")
    assert 'data-theme="light"' in r.text


def test_base_theme_defaults_dark(client):
    r = client.get("/account")
    assert 'data-theme="dark"' in r.text
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_auth_routes.py -k base_renders_user_theme -v`
Expected: FAILED（永遠 `data-theme="dark"`）

- [ ] **Step 3: base.html 接偏好 + 加切換鈕**

`base.html` 開頭改為（`{% set u %}` 移到 `<html>` 之前、刪除 `<body>` 內重複的那行）：

```html
<!DOCTYPE html>
{% set u = request.state.user if request.state.user is defined else None %}
<html lang="zh-Hant" data-theme="{{ (u.theme if u else none) or 'dark' }}">
```

`nav-right` div 內、user-menu 之前加：

```html
      <button type="button" class="theme-toggle" onclick="QQTheme.toggle()"
              aria-label="切換深淺主題" title="切換深淺主題">
        <svg class="icon-sun" width="18" height="18" viewBox="0 0 24 24" fill="none"
             stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true">
          <circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.9 4.9l1.4 1.4m11.4 11.4 1.4 1.4M2 12h2m16 0h2M4.9 19.1l1.4-1.4m11.4-11.4 1.4-1.4"/>
        </svg>
        <svg class="icon-moon" width="18" height="18" viewBox="0 0 24 24" fill="none"
             stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true">
          <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/>
        </svg>
      </button>
```

- [ ] **Step 4: app.js 實作 QQTheme**

`app.js` 檔尾加（保留原註解區塊）：

```javascript
// Theme toggle. Server renders the authoritative data-theme for logged-in
// users; this just flips it live, persists to the API, and lets the chart
// re-skin via the qq:theme-changed event.
window.QQTheme = {
  current() {
    return document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
  },
  apply(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("qq_theme", theme); // login 頁（匿名）預載用
    window.dispatchEvent(new CustomEvent("qq:theme-changed", { detail: theme }));
  },
  toggle() {
    const next = this.current() === "dark" ? "light" : "dark";
    this.apply(next);
    fetch("/api/user/theme", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ theme: next }),
    }).catch(() => { /* 即時已套用；存回失敗僅影響下次載入 */ });
  },
};
```

- [ ] **Step 5: app.css 加切換鈕樣式**

```css
/* theme toggle: show the icon of the mode you'd switch TO */
.theme-toggle { margin: 0; width: auto; padding: 0.4rem 0.55rem; background: transparent;
  border: 1px solid var(--qq-border); color: var(--qq-text-muted); border-radius: var(--qq-radius-sm); }
.theme-toggle:hover { color: var(--qq-text); border-color: var(--qq-border-strong); }
html[data-theme="dark"] .theme-toggle .icon-moon { display: none; }
html[data-theme="light"] .theme-toggle .icon-sun { display: none; }
```

- [ ] **Step 6: chart.js 加主題樣式同步**

(a) `DARK_STYLES` 常數（57-78 行）之後新增：

```javascript
  const LIGHT_STYLES = {
    grid: {
      horizontal: { color: "#e5e9f0" },
      vertical: { color: "#e5e9f0" },
    },
    candle: {
      tooltip: { text: { color: "#475569" } },
    },
    xAxis: { axisLine: { color: "#cbd5e1" }, tickText: { color: "#475569" } },
    yAxis: { axisLine: { color: "#cbd5e1" }, tickText: { color: "#475569" } },
    crosshair: {
      horizontal: { line: { color: "#94a3b8" } },
      vertical: { line: { color: "#94a3b8" } },
    },
    separator: { color: "#cbd5e1" },
  };

  function themeStyles(theme) {
    return theme === "light" ? LIGHT_STYLES : DARK_STYLES;
  }
```

(b) QQChart 上加方法（放在 `applyColorScheme`（375-380 行）旁）：

```javascript
    applyTheme(theme) {
      if (this.chart && this.chart.setStyles) {
        this.chart.setStyles(themeStyles(theme));
        // K 棒漲跌色由紅漲/綠漲偏好控制，主題切換後重套一次以免被覆蓋
        this.chart.setStyles(candleColorStyles(this.colorScheme));
      }
    },
```

(c) `init()` 內兩處修改。135-138 行的：

```javascript
      const styles = JSON.parse(JSON.stringify(DARK_STYLES));
      const cc = candleColorStyles(this.colorScheme).candle;
      styles.candle.bar = cc.bar;
      styles.candle.priceMark = cc.priceMark;
```

改為（LIGHT_STYLES 沒有 `candle.bar`，直接賦值會炸 undefined——用 Object.assign 合併）：

```javascript
      const initTheme = document.documentElement.getAttribute("data-theme") === "light" ? "light" : "dark";
      const styles = JSON.parse(JSON.stringify(themeStyles(initTheme)));
      const cc = candleColorStyles(this.colorScheme).candle;
      styles.candle = Object.assign({}, styles.candle, { bar: cc.bar, priceMark: cc.priceMark });
```

並在 `window.addEventListener("resize", ...)`（169 行）旁加：

```javascript
      window.addEventListener("qq:theme-changed", (e) => this.applyTheme(e.detail));
```

- [ ] **Step 7: login.html 加 localStorage 預載**

`login.html` 的 `<link rel="stylesheet" href="/static/app.css">` 之後加（同時把 base.html head 的三行 font link 與 tokens.css link 抄進 login.html——login 不繼承 base）：

```html
  <script>
    // anonymous page: honor the last chosen theme before first paint (no FOUC)
    var t = localStorage.getItem("qq_theme");
    if (t === "light" || t === "dark") document.documentElement.setAttribute("data-theme", t);
  </script>
```

- [ ] **Step 8: 跑測試 + 手動驗證**

Run: `uv run pytest -q`
Expected: 全 PASS

手動：登入後點切換鈕 → 整頁變淺色、K 線格線/軸文字變淺色、K 棒漲跌色不變；重新整理仍是淺色（DB 記住了）；登出到 login 頁也是淺色（localStorage）；再切回深色。

- [ ] **Step 9: Commit**

```bash
git add src/quanquant/web/templates/base.html src/quanquant/web/templates/login.html src/quanquant/web/static/app.js src/quanquant/web/static/chart.js src/quanquant/web/static/app.css tests/test_auth_routes.py
git commit -m "feat: live dark/light theme toggle synced to user pref and chart"
```

---

### Task 6: 報價面板改造

**Files:**
- Modify: `src/quanquant/web/templates/partials/quote.html`（全檔重寫）
- Modify: `src/quanquant/web/static/app.css`（quote 區塊（10-31 行）替換）
- Test: 既有 pytest + 手動

**Interfaces:**
- Consumes: tokens（Task 1）
- 保留契約: `.quote` root、`data-qq-price`、`data-qq-session`、`data-qq-pulse-level`、`data-qq-pulse-state` 屬性、`up|down|err` class（chart.js `_bindQuoteSync`（295-308 行）與 pulse.js 依賴）

- [ ] **Step 1: 重寫 `partials/quote.html`**

```html
{% if snap %}
<div class="quote {{ 'up' if snap.change >= 0 else 'down' }}"
     data-qq-price="{{ snap.price }}" data-qq-session="{{ session }}"
     data-qq-pulse-level="{{ pulse_level }}" data-qq-pulse-state="{{ pulse_state }}">
  <div class="quote-main">
    <span class="sym">{{ snap.symbol }}</span>
    <span class="price">{{ snap.price | num }}</span>
    <span class="chg-pill">{{ '▲' if snap.change >= 0 else '▼' }} {{ snap.change | signed }} ({{ snap.change_pct | pct }}%)</span>
  </div>
  <div class="quote-fields">
    <span class="qf"><small>量</small><span class="num">{{ snap.volume | comma }}</span></span>
    <span class="qf"><small>高</small><span class="num">{{ snap.high_price | num }}</span></span>
    <span class="qf"><small>低</small><span class="num">{{ snap.low_price | num }}</span></span>
  </div>
  <div class="quote-side">
    <span class="sess sess-{{ session }}"><i class="dot"></i>{{ session_label }}</span>
    <span class="time">{{ (as_of or snap.fetched_at) | cst_time }} CST</span>
  </div>
</div>
{% elif error %}
<div class="quote err"><span>⚠ 報價錯誤：{{ error }}</span></div>
{% else %}
<div class="quote"><span>等待報價…</span></div>
{% endif %}
```

- [ ] **Step 2: 替換 app.css 的 quote 區塊（原 10-31 行）**

```css
/* ---- Live quote panel ---- */
.quote-panel { margin: 1rem 0 1.25rem; }
.quote {
  display: flex; flex-wrap: wrap; align-items: center; gap: 0.75rem 2rem;
  padding: 0.9rem 1.25rem;
  border-radius: var(--qq-radius-lg);
  background: var(--qq-surface);
  border: 1px solid var(--qq-border);
}
.quote-main { display: flex; align-items: baseline; gap: 0.8rem; }
.quote .sym { font-weight: 700; font-size: 1rem; color: var(--qq-text-muted); }
.quote .price { font-size: 1.75rem; font-weight: 700; line-height: 1.2; }
.chg-pill {
  font-size: 0.9rem; font-weight: 600; padding: 0.15rem 0.6rem;
  border-radius: 999px; font-family: var(--qq-font-mono);
  font-variant-numeric: tabular-nums;
}
.quote.up .price { color: var(--up); }
.quote.down .price { color: var(--down); }
.quote.up .chg-pill { color: var(--up); background: color-mix(in srgb, var(--up) 12%, transparent); }
.quote.down .chg-pill { color: var(--down); background: color-mix(in srgb, var(--down) 12%, transparent); }
.quote.err { color: var(--down); }
.quote-fields { display: flex; gap: 1.25rem; }
.qf { display: flex; flex-direction: column; line-height: 1.25; }
.qf small { color: var(--qq-text-muted); font-size: 0.72rem; }
.qf .num { font-size: 0.95rem; font-weight: 600; }
.quote-side { margin-left: auto; display: flex; flex-direction: column; align-items: flex-end; gap: 0.15rem; }
.quote .time { color: var(--qq-text-muted); font-size: 0.78rem; }
.sess { display: inline-flex; align-items: center; gap: 0.35rem; font-size: 0.85rem; }
.sess .dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
.sess-day { color: var(--up); }
.sess-night { color: #f0b90b; }
.sess-closed { color: var(--qq-text-muted); }
/* SSE swap flash: subtle background pulse on update */
@keyframes qq-quote-flash { from { background: var(--qq-surface-2); } to { background: var(--qq-surface); } }
#quote > .quote { animation: qq-quote-flash 300ms ease-out; }
@media (prefers-reduced-motion: reduce) { #quote > .quote { animation: none; } }
```

- [ ] **Step 3: 跑測試 + 手動驗證**

Run: `uv run pytest -q`
Expected: 全 PASS

手動：報價每 5 秒更新、閃底提示、漲跌 pill 顏色正確、chart 最後價跟著 quote 動（`_bindQuoteSync` 契約沒破）、市場脈搏開啟時音效照常（`data-qq-pulse-*` 沒動）。

- [ ] **Step 4: Commit**

```bash
git add src/quanquant/web/templates/partials/quote.html src/quanquant/web/static/app.css
git commit -m "feat: restyle live quote panel (mono price, change pill, labeled fields)"
```

---

### Task 7: 工具列重組 + ⋯ 選單 + 繪圖 rail 改 SVG

**Files:**
- Modify: `src/quanquant/web/templates/dashboard.html`（10-63 行的 chart-head + ded-legend + tf-bar 區塊重排；draw-rail 按鈕（66-91 行）換 SVG）
- Modify: `src/quanquant/web/static/chart.js`（Alpine `chartPanel()` 加 `moreOpen`）
- Modify: `src/quanquant/web/static/app.css`（chart-head-actions、tf-bar 區塊改寫 + 新增 more-menu）
- Test: 既有 pytest + 手動驗證清單

**Interfaces:**
- Consumes: tokens（Task 1）
- 保留契約: `#pulse-toggle`、`#pulse-tg-toggle`（pulse.js `getElementById`（171、187 行），會 toggle `.active` class 與 title；不設 textContent，按鈕內放 SVG+span 安全）、Alpine `chartPanel()` 的所有既有 method（`setTf/setSession/openSettings/openAlerts/toggleColorScheme/toggleDeduction/toggleDrawScope/toggleDrawingsVisible/draw/clearDrawings`）與 state 名稱一律不改
- Produces: `.chart-toolbar` 結構、`.more-menu`、Alpine `moreOpen: false`；Task 8 依賴的全螢幕鈕（先以 guard 隱藏）

- [ ] **Step 1: 重寫 dashboard.html 的圖表工具區**

`<section x-data="chartPanel()">` 起到 `.chart-row` 前（原 10-63 行，含 ded-legend 與 tf-bar）替換為以下內容 + 原樣保留的 ded-legend 區塊（44-56 行內容不變，位置移到 toolbar 之後）。tf-bar 獨立一排移除（併入 toolbar）：

```html
<section x-data="chartPanel()">
  <div class="chart-toolbar" @click.outside="moreOpen = false">
    <div class="tb-left">
      <h3 class="chart-title">{{ symbol }}<small class="muted">即時</small></h3>
      <select class="session-sel" x-model="session" @change="setSession(session)"
              aria-label="價格時段">
        <template x-for="s in sessions" :key="s.code">
          <option :value="s.code" x-text="s.label"></option>
        </template>
      </select>
    </div>
    <div class="tb-tfs" role="group" aria-label="時間週期">
      <template x-for="t in tfs" :key="t.code">
        <button class="tf-btn" :class="{active: tf === t.code}"
                @click="setTf(t.code)" x-text="t.label"></button>
      </template>
    </div>
    <div class="tb-right">
      <button type="button" class="tb-btn" x-show="!!toggleFullscreen" x-cloak
              @click="toggleFullscreen()" aria-label="全螢幕" title="全螢幕（Esc 退出）">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" aria-hidden="true">
          <path d="M8 3H5a2 2 0 0 0-2 2v3m18 0V5a2 2 0 0 0-2-2h-3m0 18h3a2 2 0 0 0 2-2v-3M3 16v3a2 2 0 0 0 2 2h3"/>
        </svg>
      </button>
      <button type="button" class="tb-btn" @click="openSettings()">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <circle cx="12" cy="12" r="3"/>
          <path d="M12 2v3m0 14v3M2 12h3m14 0h3M4.9 4.9l2.1 2.1m10 10 2.1 2.1M19.1 4.9l-2.1 2.1m-10 10-2.1 2.1"/>
        </svg>
        指標
      </button>
      <button type="button" class="tb-btn" @click="openAlerts()">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <path d="M6 8a6 6 0 0 1 12 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10.3 21a1.94 1.94 0 0 0 3.4 0"/>
        </svg>
        警示
      </button>
      <div class="more-wrap">
        <button type="button" class="tb-btn more-btn" @click="moreOpen = !moreOpen"
                :aria-expanded="moreOpen" aria-label="更多圖表功能" title="更多圖表功能">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
            <circle cx="5" cy="12" r="1.6"/><circle cx="12" cy="12" r="1.6"/><circle cx="19" cy="12" r="1.6"/>
          </svg>
          <span class="more-dot" aria-hidden="true"></span>
        </button>
        <div class="more-menu" x-show="moreOpen" x-cloak>
          <button type="button" id="pulse-toggle" class="menu-row pulse-toggle"
                  aria-pressed="false" aria-label="市場脈搏音效"
                  title="市場脈搏音效：關（點擊開啟）">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z"/>
            </svg>
            <span>市場脈搏音效</span><span class="row-state"></span>
          </button>
          <button type="button" id="pulse-tg-toggle" class="menu-row pulse-tg-toggle"
                  aria-pressed="false" aria-label="Telegram 通知"
                  title="Telegram 通知：載入中…">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <path d="m22 2-7 20-4-9-9-4Z"/><path d="M22 2 11 13"/>
            </svg>
            <span>Telegram 通知</span><span class="row-state"></span>
          </button>
          <button type="button" class="menu-row" @click="toggleColorScheme()">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 stroke-width="2" stroke-linecap="round" aria-hidden="true">
              <path d="M8 6v12M8 9h-3v6h3M16 4v16M16 7h3v8h-3"/>
            </svg>
            <span>K 線配色</span>
            <span class="row-state" x-text="colorScheme === 'red_up' ? '紅漲' : '綠漲'"></span>
          </button>
          <button type="button" class="menu-row" :class="{active: deductionOn}"
                  @click="toggleDeduction()" :aria-pressed="deductionOn">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <path d="M12 19V5M5 12l7-7 7 7"/>
            </svg>
            <span>均線扣抵</span>
            <span class="row-state" x-text="deductionOn ? '開' : '關'"></span>
          </button>
          <button type="button" class="menu-row" @click="toggleDrawScope()"
                  :aria-pressed="drawScope === 'all'"
                  :title="drawScope === 'hybrid'
                    ? '畫線跨時框：智慧混合（水平線/價格線跨所有時框；趨勢線只在繪製時框顯示）。點擊改為全部時框'
                    : '畫線跨時框：全部時框顯示。點擊改回智慧混合'">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 stroke-width="2" stroke-linecap="round" aria-hidden="true">
              <path d="M2 12h20M12 2v20"/>
            </svg>
            <span>畫線跨時框</span>
            <span class="row-state" x-text="drawScope === 'hybrid' ? '智慧' : '全部'"></span>
          </button>
        </div>
      </div>
    </div>
  </div>
```

注意全螢幕鈕的 `x-show="!!toggleFullscreen"`：Task 8 加上 `toggleFullscreen()` method 前，Alpine 對 undefined property 求值為 falsy → 鈕隱藏，不會拋錯（`@click` 未觸發就不會呼叫）。

- [ ] **Step 2: draw-rail 按鈕換 SVG（dashboard.html 原 66-91 行）**

七顆繪圖鈕的文字字元（╱ ↗ ━ ┃ ∥ ⊢ ≣ ✕）改為 16px stroke SVG，外層結構不變（class、@click、title、aria-label 全保留）。SVG 外殼統一為：

```html
<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
     stroke-width="2" stroke-linecap="round" aria-hidden="true">…</svg>
```

各鈕內容 path：
- 趨勢線 `segment`: `<line x1="4" y1="20" x2="20" y2="4"/><circle cx="4" cy="20" r="1.5" fill="currentColor"/><circle cx="20" cy="4" r="1.5" fill="currentColor"/>`
- 射線 `rayLine`: `<line x1="4" y1="20" x2="17" y2="7"/><path d="M14 4h6v6"/>`
- 水平線 `horizontalStraightLine`: `<line x1="3" y1="12" x2="21" y2="12"/>`
- 垂直線 `verticalStraightLine`: `<line x1="12" y1="3" x2="12" y2="21"/>`
- 平行通道 `priceChannelLine`: `<line x1="3" y1="16" x2="21" y2="8"/><line x1="3" y1="21" x2="21" y2="13"/>`
- 價格線 `priceLine`: `<line x1="3" y1="12" x2="15" y2="12"/><path d="M18 9v6M16.5 10.5h3"/>`
- Fibonacci `fibonacciLine`: `<line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/>`
- 清除全部（`.danger`）: `<path d="M3 6h18M8 6V4a1 1 0 0 1 1-1h6a1 1 0 0 1 1 1v2m3 0v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/>`

眼睛顯示/隱藏鈕（74-89 行）已是 SVG，原樣保留。

- [ ] **Step 3: chart.js 的 Alpine `chartPanel()` 加 `moreOpen` state**

`window.chartPanel = () => ({` 內（`settingsOpen: false,`（724 行）旁）加一行：

```javascript
    moreOpen: false,
```

- [ ] **Step 4: app.css 改寫工具列樣式**

刪除原 36-84 行（`.chart-head` 到 `.pulse-tg-toggle.active` 及 qq-heartbeat keyframes）與 128-151 行（tf-bar 區塊），替換為：

```css
/* ---- chart toolbar: [title+session] [timeframes] [actions] ---- */
.chart-toolbar {
  display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap;
  padding: 0.5rem 0.75rem; margin-bottom: 0.4rem;
  background: var(--qq-surface); border: 1px solid var(--qq-border);
  border-radius: var(--qq-radius-lg);
}
.tb-left { display: flex; align-items: center; gap: 0.6rem; }
.chart-title { margin: 0; font-size: 1rem; display: flex; align-items: baseline; gap: 0.4rem; }
.chart-title small { font-size: 0.72rem; }
.session-sel {
  margin: 0; width: auto; height: 2rem; font-size: 0.82rem; line-height: 1;
  border-radius: var(--qq-radius-sm);
  padding: 0 1.8rem 0 0.7rem; background-position: right 0.6rem center;
  background-color: transparent; border-color: var(--qq-border-strong); color: var(--qq-text);
}
.tb-tfs { display: flex; flex-wrap: wrap; gap: 2px; }
.tf-btn {
  margin: 0; width: auto; padding: 0.25rem 0.55rem; font-size: 0.78rem;
  background: transparent; border: 1px solid transparent; color: var(--qq-text-muted);
  border-radius: var(--qq-radius-sm);
}
.tf-btn:hover { color: var(--qq-text); background: var(--qq-surface-2); }
.tf-btn.active { background: var(--qq-accent); color: #fff; font-weight: 700; }
.tb-right { display: flex; align-items: center; gap: 0.35rem; margin-left: auto; }
.tb-btn {
  margin: 0; width: auto; height: 2rem; padding: 0 0.65rem;
  display: inline-flex; align-items: center; gap: 0.35rem;
  font-size: 0.82rem; line-height: 1; white-space: nowrap;
  background: transparent; border: 1px solid var(--qq-border-strong);
  color: var(--qq-text); border-radius: var(--qq-radius-sm);
}
.tb-btn:hover { border-color: var(--qq-accent); color: var(--qq-accent); }

/* ⋯ more menu */
.more-wrap { position: relative; }
.more-btn { position: relative; padding: 0 0.5rem; }
.more-dot {
  display: none; position: absolute; top: 4px; right: 4px;
  width: 6px; height: 6px; border-radius: 50%; background: var(--qq-accent);
}
/* any tracked toggle active → dot on the trigger (pulse.js toggles .active itself) */
.more-wrap:has(.menu-row.active) .more-dot,
.more-wrap:has(.pulse-toggle.active) .more-dot,
.more-wrap:has(.pulse-tg-toggle.active) .more-dot { display: block; }
.more-menu {
  position: absolute; right: 0; top: calc(100% + 4px); z-index: 50;
  min-width: 13.5rem; padding: 0.3rem;
  background: var(--qq-surface); border: 1px solid var(--qq-border-strong);
  border-radius: var(--qq-radius-sm); box-shadow: var(--qq-shadow-1);
  display: flex; flex-direction: column; gap: 2px;
}
.menu-row {
  margin: 0; width: 100%; display: flex; align-items: center; gap: 0.5rem;
  padding: 0.45rem 0.6rem; font-size: 0.82rem; text-align: left;
  background: transparent; border: 1px solid transparent;
  color: var(--qq-text); border-radius: var(--qq-radius-sm);
}
.menu-row:hover { background: var(--qq-surface-2); }
.menu-row > span:nth-child(2) { flex: 1 1 auto; }
.row-state { font-size: 0.72rem; color: var(--qq-text-muted); }
.menu-row.active, .menu-row.active .row-state { color: var(--qq-accent); }
.menu-row.pulse-toggle.active { color: var(--down); animation: qq-heartbeat 1s ease-in-out infinite; }
.menu-row.pulse-tg-toggle.active { color: var(--up); }
@keyframes qq-heartbeat {
  0%, 100% { transform: scale(1); }
  15% { transform: scale(1.04); }
  45% { transform: scale(1.02); }
}
```

rail 區塊（原 87-104 行）微調：`.rail-btn` 刪除 `font-size: 1rem`、`color` 改 `var(--qq-text-muted)`、加 `.rail-btn:hover { color: var(--qq-text); }`；其餘保留。

- [ ] **Step 5: 跑測試 + 手動驗證（本 task 最關鍵的一步）**

Run: `uv run pytest -q`
Expected: 全 PASS

手動逐項：時框切換、時段切換、指標設定 dialog 開合與儲存、警示 dialog CRUD、⋯ 選單內——市場脈搏開關（開啟後該列變紅心跳、⋯ 鈕出現圓點）、TG 通知開關、K 線配色切換（K 棒變色 + 狀態字變）、均線扣抵開關（legend 出現）、跨框切換；七種繪圖工具各畫一條、Delete 刪線、眼睛隱藏、清除全部；重新整理後畫線與指標仍在。

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/templates/dashboard.html src/quanquant/web/static/chart.js src/quanquant/web/static/app.css
git commit -m "feat: regroup chart toolbar (tf merge, overflow menu, SVG icons)"
```

---

### Task 8: 全螢幕模式 + 圖表高度 clamp

**Files:**
- Modify: `src/quanquant/web/static/chart.js`（Alpine `chartPanel()` 加 fullscreen state/method + Esc）
- Modify: `src/quanquant/web/static/app.css`（`#kchart` 高度 + fullscreen 樣式）
- Test: 既有 pytest + 手動

**Interfaces:**
- Consumes: Task 7 的全螢幕鈕（`x-show="!!toggleFullscreen"` guard 已就位，本 task 完成後自動顯示）
- Produces: Alpine `fullscreen: false` state、`toggleFullscreen()` method；`body.chart-fullscreen` CSS class

- [ ] **Step 1: chart.js 的 `chartPanel()` 加 state 與 method**

state 區（`moreOpen: false,` 旁）加：

```javascript
    fullscreen: false,
```

methods 區（`toggleDrawingsVisible()` 之後）加：

```javascript
    toggleFullscreen() {
      this.fullscreen = !this.fullscreen;
      document.body.classList.toggle("chart-fullscreen", this.fullscreen);
      // relayout after the CSS takes effect
      requestAnimationFrame(() => { if (QQChart.chart) QQChart.chart.resize(); });
    },
```

Alpine `init()`（762-774 行）內加 Esc 退出（獨立 listener，不動 chart.js 的 `_bindDrawingKeys`；畫線中按 Esc 由 `_bindDrawingKeys` 先取消畫線，再按一次才退全螢幕——兩者獨立不衝突）：

```javascript
      window.addEventListener("keydown", (e) => {
        if (e.key === "Escape" && this.fullscreen) this.toggleFullscreen();
      });
```

- [ ] **Step 2: app.css**

`#kchart` 的 `height: 680px;` 改為：

```css
  height: clamp(420px, calc(100vh - 320px), 760px);
```

檔尾加：

```css
/* ---- chart fullscreen mode ---- */
body.chart-fullscreen .topnav,
body.chart-fullscreen .quote-panel { display: none; }
body.chart-fullscreen main.container { max-width: none; padding: 0.5rem 0.75rem; }
body.chart-fullscreen #kchart { height: calc(100vh - 130px); }
```

- [ ] **Step 3: 跑測試 + 手動驗證**

Run: `uv run pytest -q`
Expected: 全 PASS

手動：點 ⛶ → 導航與報價列消失、圖表撐滿；再點或按 Esc → 還原且圖表無變形（resize 生效）；一般視窗高度下圖表不超出視野（clamp 生效）。

- [ ] **Step 4: Commit**

```bash
git add src/quanquant/web/static/chart.js src/quanquant/web/static/app.css
git commit -m "feat: chart fullscreen mode + viewport-aware chart height"
```

---

### Task 9: 指標調參入口（spike + 實作）

**Files:**
- Modify: `src/quanquant/web/static/chart.js`（主方案時）
- Modify: `src/quanquant/web/templates/dashboard.html` + `src/quanquant/web/static/app.css`（退階方案時）
- Test: 手動（純前端入口；指標儲存邏輯不動）

**Interfaces:**
- Consumes: 既有 Alpine `openSettings()`（905-908 行，打開指標 dialog）
- Produces: 點擊圖上指標區域（或懸浮鈕）→ `openSettings()` 的入口

- [ ] **Step 1: Spike——驗證 KLineCharts 9.8.12 tooltip 事件支援度**

開 dev server，瀏覽器 console 對 `window.QQChart.chart` 驗證：

1. `klinecharts.version()` 確認 9.8.12
2. `console.log(Object.values(klinecharts.ActionType || {}))` 列出可 subscribe 的 action
3. 若有 tooltip icon click 類 action（如 `onTooltipIconClick`）→ 主方案（Step 2A）；並用 `chart.setStyles({indicator:{tooltip:{icons:[...]}}})` 實測 icon 有無出現
4. 若不存在 → 退階方案（Step 2B）

把結論（支援/不支援、實測輸出）記錄到本檔此處再繼續。

- [ ] **Step 2A（主方案，若 spike 成功）: tooltip icon 開設定**

QQChart 物件加欄位（`onDeductionUpdate` 旁）：`onEditIndicator: null,`

`init()`（chart 建立後）加：

```javascript
      // 指標 tooltip 上的「編輯」icon → 打開指標設定 dialog
      this.chart.setStyles({
        indicator: { tooltip: { icons: [{
          id: "qq-edit", position: "middle", marginLeft: 8, marginTop: 6,
          marginRight: 0, marginBottom: 0, paddingLeft: 2, paddingTop: 2,
          paddingRight: 2, paddingBottom: 2, size: 14, color: "#94a3b8",
          activeColor: "#3b82f6", backgroundColor: "transparent",
          activeBackgroundColor: "rgba(59,130,246,0.15)",
          icon: "✎", fontFamily: "sans-serif",
        }] } },
      });
      this.chart.subscribeAction("onTooltipIconClick", (data) => {
        if (data && data.iconId === "qq-edit" && this.onEditIndicator) this.onEditIndicator();
      });
```

Alpine `chartPanel().init()` 內加：

```javascript
      QQChart.onEditIndicator = () => this.openSettings();
```

（action 名稱以 spike 實測為準。）

- [ ] **Step 2B（退階方案，若 spike 失敗）: 圖表角落懸浮編輯鈕**

dashboard.html 的 `#kchart` div 內（`#chart-empty` 之後）加：

```html
      <button type="button" class="chart-edit-ind" @click="openSettings()"
              title="編輯指標參數" aria-label="編輯指標參數">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
          <path d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5Z"/>
        </svg>
      </button>
```

app.css 加：

```css
/* floating "edit indicators" affordance next to the on-chart tooltip text */
.chart-edit-ind {
  position: absolute; top: 6px; left: 6px; z-index: 3;
  margin: 0; width: auto; padding: 0.3rem;
  background: color-mix(in srgb, var(--qq-surface) 80%, transparent);
  border: 1px solid var(--qq-border); color: var(--qq-text-muted);
  border-radius: var(--qq-radius-sm); opacity: 0;
}
#kchart:hover .chart-edit-ind { opacity: 1; }
.chart-edit-ind:hover { color: var(--qq-accent); border-color: var(--qq-accent); }
@media (hover: none) { .chart-edit-ind { opacity: 1; } } /* 觸控裝置常駐 */
```

（KLineCharts 的指標 tooltip 文字畫在 canvas 左上角，此鈕定位在同一角落緊鄰之。）

- [ ] **Step 3: 手動驗證 + 全套件**

Run: `uv run pytest -q`
Expected: 全 PASS

手動：hover 圖表出現編輯入口（或 tooltip icon）→ 點擊打開指標設定 dialog → 改 MA 週期/顏色 → 儲存生效。畫線、十字線、捲動不受影響。

- [ ] **Step 4: Commit**

```bash
git add -A src/quanquant/web/static src/quanquant/web/templates
git commit -m "feat: on-chart entry point for indicator parameter editing"
```

---

### Task 10: 交易日記 + 績效統計改造

**Files:**
- Modify: `src/quanquant/web/templates/journal.html`（filterbar 加 class）
- Modify: `src/quanquant/web/templates/stats.html`（metric_cards macro 加語意 class）
- Modify: `src/quanquant/web/static/app.css`（journal/stats 區塊（原 188-234 行）改寫）
- Test: 既有 pytest（trade CRUD、export、stats 路由測試已存在）

**Interfaces:**
- Consumes: tokens（Task 1）
- 保留契約: `#trade-tbody`、`#modal-body`、filterbar 的 hx-* 屬性、`refreshtable`/`closemodal` 事件流、export URL 的 qs 組裝

- [ ] **Step 1: journal.html 微調**

12 行 `<form class="filterbar" ...>` 的 class 改為 `filterbar card-bar`（hx-* 屬性原樣不動）。

- [ ] **Step 2: stats.html 微調**

`metric_cards` macro（3-15 行）：總損益卡改為

```html
  <article class="stat-card focus {{ 'up' if m.total_pnl >= 0 else 'down' }}"><small>總損益</small><div class="big {{ 'up' if m.total_pnl >= 0 else 'down' }}">{{ m.total_pnl | signed }}</div></article>
```

其餘八張 `<article>` 一律加 `class="stat-card"`；語意色條：平均獲利/最大單筆獲利加 `stat-card up`、平均虧損/最大單筆虧損/最大回撤加 `stat-card down`、交易次數/勝率/獲利因子維持 `stat-card`。52 行 `<form class="filterbar" ...>` 加 `card-bar`。

- [ ] **Step 3: app.css 改寫 journal/stats 區塊（原 188-234 行）**

```css
/* ---- Journal & Stats ---- */
.journal-head { display: flex; justify-content: space-between; align-items: center; gap: 1rem; }
.journal-head h3 { margin: 0; }
.filterbar { display: flex; flex-wrap: wrap; gap: 0.5rem; align-items: center; margin: 1rem 0; }
.filterbar.card-bar {
  padding: 0.6rem 0.75rem; background: var(--qq-surface);
  border: 1px solid var(--qq-border); border-radius: var(--qq-radius-lg);
}
.filterbar select, .filterbar input { margin: 0; width: auto; min-width: 9rem; font-size: 0.85rem; }

.table-wrap { overflow-x: auto; border: 1px solid var(--qq-border); border-radius: var(--qq-radius-lg); }
table { font-size: 0.88rem; margin-bottom: 0; }
thead th { position: sticky; top: 0; background: var(--qq-surface); z-index: 1;
  font-size: 0.78rem; color: var(--qq-text-muted); }
tbody tr:hover { background: var(--qq-surface-2); }
th, td { white-space: normal; }
td.nowrap, th { white-space: nowrap; }
.numcell { text-align: right; }
td.note { max-width: 14rem; }

.dir.long { color: var(--up); font-weight: 700; }
.dir.short { color: var(--down); font-weight: 700; }
.pnl.up { color: var(--up); font-weight: 600; }
.pnl.down { color: var(--down); font-weight: 600; }

.badge { font-size: 0.72rem; padding: 0.1rem 0.45rem; border-radius: 999px; }
.badge.open { background: rgba(240, 185, 11, 0.18); color: #f0b90b; }
.tag { display: inline-block; font-size: 0.72rem; padding: 0.1rem 0.45rem; margin: 0.1rem;
  border-radius: var(--qq-radius-sm); background: var(--qq-surface-2);
  border: 1px solid var(--qq-border); }
.empty td { text-align: center; color: var(--qq-text-muted); padding: 2rem; }
a.mini { padding: 0.15rem 0.5rem; font-size: 0.78rem; }

/* stat cards */
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 0.75rem; margin: 1rem 0 1.5rem; }
.cards article { margin: 0; padding: 0.9rem 1rem; border-radius: var(--qq-radius-lg); }
.stat-card { border-left: 2px solid var(--qq-border-strong); }
.stat-card.up { border-left-color: var(--up); }
.stat-card.down { border-left-color: var(--down); }
.stat-card.focus { grid-column: span 2; }
.stat-card.focus .big { font-size: 2rem; }
.cards small { color: var(--qq-text-muted); }
.cards .big { font-size: 1.5rem; font-weight: 700; }
.cards .big.up { color: var(--up); }
.cards .big.down { color: var(--down); }
.export-actions { display: flex; gap: 0.5rem; }
.export-actions [role="button"] { font-size: 0.82rem; padding: 0.35rem 0.8rem; }
```

- [ ] **Step 4: 跑測試 + 手動驗證**

Run: `uv run pytest -q`
Expected: 全 PASS

手動：日記——篩選（商品/標籤/狀態/日期）即時更新表格、新增/編輯/刪除交易、modal 開合、表頭 sticky、行 hover。統計——套用篩選、卡片語意色條、總損益卡放大、CSV/Excel 匯出下載成功。深淺兩主題都看一遍。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/templates/journal.html src/quanquant/web/templates/stats.html src/quanquant/web/static/app.css
git commit -m "feat: restyle journal and stats pages (card filter bar, sticky header, semantic stat cards)"
```

---

### Task 11: 登入 + 帳戶 + 使用者管理改造

**Files:**
- Modify: `src/quanquant/web/templates/login.html`（全檔重寫）
- Modify: `src/quanquant/web/templates/account.html`（卡片化 + 錯誤樣式）
- Modify: `src/quanquant/web/templates/admin_users.html`（表格自動吃 Task 10 樣式，預期零改動；過一眼確認）
- Modify: `src/quanquant/web/static/app.css`（login/settings 區塊）
- Test: 既有 pytest（login flow、account form、admin 路由測試已存在）

**Interfaces:**
- Consumes: tokens（Task 1）、Task 5 的 login localStorage script（重寫時保留）
- 保留契約: `/login`、`/account/password`、`/account/color-scheme` 的 form `name` 屬性（`username`/`password`/`old_password`/`new_password`/`scheme`）；測試斷言字串 `帳號或密碼錯誤`、`舊密碼錯誤`、`value="green_up" checked`

- [ ] **Step 1: 重寫 login.html**

```html
<!DOCTYPE html>
<html lang="zh-Hant" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>登入 — QuanQuant</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@400;500;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
  <link rel="stylesheet" href="/static/tokens.css">
  <link rel="stylesheet" href="/static/app.css">
  <script>
    // anonymous page: honor the last chosen theme before first paint (no FOUC)
    var t = localStorage.getItem("qq_theme");
    if (t === "light" || t === "dark") document.documentElement.setAttribute("data-theme", t);
  </script>
</head>
<body class="login-body">
  <main class="login-card">
    <div class="brand login-brand">
      <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="2" stroke-linecap="round" aria-hidden="true">
        <path d="M8 6v12M8 9h-3v6h3M16 4v16M16 7h3v8h-3"/>
      </svg>
      <strong>QuanQuant</strong>
    </div>
    <form method="post" action="/login">
      {% if error %}<p class="field-error">{{ error }}</p>{% endif %}
      <label>帳號
        <input type="text" name="username" autocomplete="username" required autofocus>
      </label>
      <label>密碼
        <div class="pw-wrap">
          <input type="password" name="password" id="login-pw" autocomplete="current-password" required>
          <button type="button" class="pw-toggle" aria-label="顯示/隱藏密碼"
                  onclick="var i=document.getElementById('login-pw'); i.type = i.type==='password'?'text':'password';">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                 stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
              <path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7-11-7-11-7z"/><circle cx="12" cy="12" r="3"/>
            </svg>
          </button>
        </div>
      </label>
      <button type="submit">登入</button>
    </form>
  </main>
</body>
</html>
```

- [ ] **Step 2: account.html 卡片化**

```html
{% extends "base.html" %}
{% block content %}
<h2>帳戶設定</h2>
<div class="settings-grid">
  <article class="settings-card">
    <h3>修改密碼</h3>
    {% if error %}<p class="field-error">{{ error }}</p>{% endif %}
    <form method="post" action="/account/password">
      <label>舊密碼
        <input type="password" name="old_password" autocomplete="current-password" required>
      </label>
      <label>新密碼
        <input type="password" name="new_password" autocomplete="new-password" required>
      </label>
      <button type="submit">修改密碼（其他裝置將被登出）</button>
    </form>
  </article>
  <article class="settings-card">
    <h3>K 線配色</h3>
    <form method="post" action="/account/color-scheme">
      <fieldset>
        <label>
          <input type="radio" name="scheme" value="green_up" {% if color_scheme == 'green_up' %}checked{% endif %}>
          綠漲紅跌（歐美慣例）
        </label>
        <label>
          <input type="radio" name="scheme" value="red_up" {% if color_scheme == 'red_up' %}checked{% endif %}>
          紅漲綠跌（台灣慣例）
        </label>
      </fieldset>
      <button type="submit">儲存配色</button>
    </form>
  </article>
</div>
{% endblock %}
```

（`error` 變數由密碼與配色兩個 POST 共用，統一顯示在密碼卡的 error 區——與現行為一致，測試斷言 `舊密碼錯誤` 仍會出現在頁面上。）

- [ ] **Step 3: app.css 檔尾加 login/settings 樣式**

```css
/* ---- login & settings ---- */
.login-body { display: flex; align-items: flex-start; justify-content: center; min-height: 100dvh; padding-top: 14vh; }
.login-card {
  width: min(24rem, 92vw); padding: 2rem;
  background: var(--qq-surface); border: 1px solid var(--qq-border);
  border-radius: var(--qq-radius-lg); box-shadow: var(--qq-shadow-1);
}
.login-brand { justify-content: center; display: flex; font-size: 1.3rem; margin-bottom: 1.25rem; }
.pw-wrap { position: relative; }
.pw-wrap input { padding-right: 2.6rem; }
.pw-toggle {
  position: absolute; right: 0.35rem; top: 50%; transform: translateY(-50%);
  margin: 0; width: auto; padding: 0.3rem 0.45rem;
  background: transparent; border: none; color: var(--qq-text-muted);
}
.pw-toggle:hover { color: var(--qq-text); }
.field-error {
  background: color-mix(in srgb, var(--down) 12%, transparent); color: var(--down);
  padding: 0.5rem 0.8rem; border-radius: var(--qq-radius-sm); font-size: 0.85rem;
}
.settings-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 24rem)); gap: 1rem; }
.settings-card { margin: 0; padding: 1.25rem; border-radius: var(--qq-radius-lg); }
.settings-card h3 { font-size: 1rem; margin-bottom: 0.75rem; }
```

- [ ] **Step 4: 跑測試 + 手動驗證**

Run: `uv run pytest -q`
Expected: 全 PASS——特別注意 `test_login_page_renders`（斷言 `"password" in r.text`）、`test_change_password_flow`（斷言 `舊密碼錯誤` 與 `value="green_up" checked`）、`test_account_page_shows_color_scheme_radio` 必須仍通過。

手動：登入頁卡片置中、密碼眼睛切換、錯誤訊息樣式；帳戶頁兩張卡、改密碼流程、配色儲存；admin 使用者列表表格吃到新樣式。

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/templates/login.html src/quanquant/web/templates/account.html src/quanquant/web/static/app.css
git commit -m "feat: restyle login and account pages (card layout, password toggle)"
```

---

### Task 12: RWD 收尾 + 全站驗證

**Files:**
- Modify: `src/quanquant/web/static/app.css`（media queries 收尾）
- Test: 全套件 + 全站手動清單

**Interfaces:**
- Consumes: 全部前置 task

- [ ] **Step 1: app.css 檔尾加 RWD 收尾**

```css
/* ---- responsive polish ---- */
@media (max-width: 767px) {
  .chart-toolbar { gap: 0.4rem; }
  .tb-tfs { order: 3; width: 100%; }           /* 時框列自成一列 */
  .tb-right { margin-left: auto; }
  .tf-btn { padding: 0.5rem 0.7rem; }          /* 觸控目標放大 */
  .chart-row { flex-direction: column; }
  .draw-rail {
    flex-direction: row; flex: none; width: 100%;
    border-right: 1px solid var(--qq-border); border-bottom: none;
    border-radius: var(--qq-radius-sm) var(--qq-radius-sm) 0 0;
  }
  .rail-btn { width: auto; aspect-ratio: auto; padding: 0.45rem 0.6rem; }
  #kchart { border-radius: 0 0 var(--qq-radius-sm) var(--qq-radius-sm); }
  .quote { gap: 0.5rem 1rem; }
  .quote-side { margin-left: 0; width: 100%; flex-direction: row; justify-content: space-between; }
  .cards { grid-template-columns: repeat(2, 1fr); }
  .stat-card.focus { grid-column: span 2; }
}
@media (min-width: 768px) and (max-width: 1023px) {
  .cards { grid-template-columns: repeat(3, 1fr); }
}
```

- [ ] **Step 2: 全套件測試**

Run: `uv run pytest -q`
Expected: 全 PASS

- [ ] **Step 3: 全站手動驗證清單（spec §8）**

以 375px、768px、1440px 三種寬度 × 深/淺兩主題過一遍：

1. 報價 SSE 即時更新與閃底
2. 指標增刪改（含圖上編輯入口）
3. 警示 CRUD + toast
4. 畫線七工具、跨框切換、眼睛、清除、Delete/Esc
5. K 線配色切換（chart + 帳戶頁 radio 一致）
6. 市場脈搏音效 + TG 通知開關（⋯ 選單內、圓點指示）
7. 全螢幕進出
8. 主題切換（頁面 + chart 同步 + 重整記憶 + login 頁 localStorage）
9. 日記 CRUD、篩選、modal
10. 統計篩選 + CSV/Excel 匯出
11. 登入/登出、改密碼（他裝置登出）、admin 使用者管理
12. 手機寬度無水平捲動（表格容器除外）、漢堡選單、工具列換行

- [ ] **Step 4: Commit**

```bash
git add src/quanquant/web/static/app.css
git commit -m "feat: responsive polish for mobile/tablet breakpoints"
```

---

## 部署備註

全部 task 完成、pytest 全綠、手動清單過完後，由使用者決定執行 `git push && ./scripts/deploy.sh`（僅新增 nullable `users.theme` 欄位，由既有 `ensure_columns()` 啟動時自動加；本機 `quanquant.db` 與 VM Postgres 資料皆不受影響）。
