# 帳戶系統 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 為 QuanQuant 加上應用層帳戶系統：session 登入、admin 管理、trades／chart state／alerts 每人一份，取代 Caddy basic_auth 共用帳號。

**Architecture:** 自建認證（bcrypt 密碼＋itsdangerous 簽章 cookie，無 server-side session 表）；`get_current_user` FastAPI dependency 掛在所有既有 router 上；既有表加 nullable `user_id` 欄位（啟動時輕量遷移），chart state 改走新表 `user_chart_states`；`quanquant-user bootstrap` 建第一個 admin 並認領舊資料。

**Tech Stack:** FastAPI（sync routes）、SQLModel（SQLite 本機／Postgres 雲端）、Jinja2＋HTMX＋Pico CSS、bcrypt、itsdangerous、pytest。

**Spec:** `docs/superpowers/specs/2026-07-05-account-system-design.md`

## Global Constraints

- Routes 一律 sync `def`（candle 讀取路徑勿改 async/ORM）— 專案 CLAUDE.md 約束
- 新增 raw SQL 必須 SQLite／Postgres 兩方言可攜（named params、dialect-aware）
- `Candle.ts` 維持 `BigInteger`；KLineCharts 釘版 v9.8.12 勿動；`chart.js` 四道渲染防線勿移除
- pyproject hatch wheel 設定不可加 force-include
- `.env` 與 `*.dump` 不進 git；本機 `quanquant.db` 勿刪
- 部署前 `uv run pytest` 必須全綠；push 不會自動部署（`./scripts/deploy.sh`）
- Commit message 格式 `<type>: <description>`，無 attribution
- 密碼雜湊 bcrypt cost 12；cookie 有效期 30 天；登入失敗 5 次鎖 60 秒
- 操作他人資源回 **404**（非 403，避免洩漏存在性）；非 admin 訪問 admin 頁回 403
- 舊 `chart_states` 表不動（降級為系統 KV store，Market Pulse 開關續住）

---

### Task 1: 資料模型（User、UserChartState、user_id 欄位）＋啟動輕量遷移

**Files:**
- Modify: `src/quanquant/db/models.py`（加 `User`、`UserChartState`；`Trade`、`Alert` 加 `user_id`）
- Create: `src/quanquant/db/migrate.py`
- Modify: `src/quanquant/db/engine.py:41-45`（`init_db` 呼叫遷移）
- Test: `tests/test_migrate.py`

**Interfaces:**
- Consumes: 既有 `SQLModel.metadata`、`get_engine()`
- Produces: `User`（欄位 `id, username, display_name, password_hash, role, is_active, token_version, telegram_chat_id, created_at, updated_at`）、`UserChartState`（`id, user_id, symbol, kind, payload, updated_at`，unique `(user_id, symbol, kind)`）、`Trade.user_id: int | None`、`Alert.user_id: int | None`、`ensure_columns(engine) -> None`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_migrate.py
"""Startup migration: ALTER TABLE adds user_id to pre-account-era tables."""
from sqlalchemy import create_engine, inspect, text

from quanquant.db.migrate import ensure_columns


def _old_engine(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'old.db'}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE trades (id INTEGER PRIMARY KEY, symbol TEXT)"))
        conn.execute(text("CREATE TABLE alerts (id INTEGER PRIMARY KEY, symbol TEXT)"))
    return eng


def test_adds_user_id_columns(tmp_path):
    eng = _old_engine(tmp_path)
    ensure_columns(eng)
    insp = inspect(eng)
    assert "user_id" in {c["name"] for c in insp.get_columns("trades")}
    assert "user_id" in {c["name"] for c in insp.get_columns("alerts")}


def test_idempotent(tmp_path):
    eng = _old_engine(tmp_path)
    ensure_columns(eng)
    ensure_columns(eng)  # second run must not raise
    insp = inspect(eng)
    assert "user_id" in {c["name"] for c in insp.get_columns("trades")}


def test_missing_table_skipped(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'empty.db'}")
    ensure_columns(eng)  # no tables at all — must not raise
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_migrate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'quanquant.db.migrate'`

- [ ] **Step 3: 實作**

`src/quanquant/db/migrate.py`（新檔）：

```python
"""Startup-time lightweight migrations (the project has no alembic).

`create_all` only creates missing tables — it never adds columns to existing
ones. ensure_columns() inspects each table and issues `ALTER TABLE ... ADD
COLUMN` for columns the ORM model has but the DB lacks. Plain ADD COLUMN of a
nullable column is portable across SQLite and Postgres.
"""
from sqlalchemy import inspect, text

# (table, column, DDL type) — nullable so ALTER works on populated tables.
_MIGRATIONS = [
    ("trades", "user_id", "INTEGER"),
    ("alerts", "user_id", "INTEGER"),
]


def ensure_columns(engine) -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    with engine.begin() as conn:
        for table, column, ddl_type in _MIGRATIONS:
            if table not in tables:
                continue
            cols = {c["name"] for c in inspector.get_columns(table)}
            if column not in cols:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl_type}"))
```

`src/quanquant/db/models.py` — 檔尾加兩個新 model（放在 `AlertEvent` 之後）：

```python
class User(SQLModel, table=True):
    """Account for app-level auth. Deactivation replaces deletion (data ownership)."""

    __tablename__ = "users"

    id: int | None = Field(default=None, primary_key=True)
    username: str = Field(unique=True, index=True)        # 登入帳號
    display_name: str                                     # 顯示名稱（navbar、通知）
    password_hash: str                                    # bcrypt
    role: str = "user"                                    # "admin" | "user"
    is_active: bool = True
    token_version: int = 0                                # bump → 所有舊 cookie 失效
    telegram_chat_id: str | None = None                   # 第二階段深度連結綁定用
    created_at: datetime = Field(default_factory=_utcnow)
    updated_at: datetime = Field(default_factory=_utcnow)


class UserChartState(SQLModel, table=True):
    """Per-user chart UI state (indicators/drawings). The old chart_states table
    stays as the system-level KV store (e.g. kind="pulse") — see the design spec
    for why we don't ALTER its (symbol, kind) unique constraint."""

    __tablename__ = "user_chart_states"
    __table_args__ = (UniqueConstraint("user_id", "symbol", "kind"),)

    id: int | None = Field(default=None, primary_key=True)
    user_id: int = Field(index=True)
    symbol: str = Field(index=True)
    kind: str                                             # "indicators" | "drawings"
    payload: str                                          # JSON TEXT
    updated_at: datetime = Field(default_factory=_utcnow)
```

`Trade` 加欄位（放在 `created_at` 之前）：

```python
    user_id: int | None = Field(default=None, index=True)  # 擁有者（帳戶系統後必填）
```

`Alert` 同樣加（放在 `created_at` 之前）：

```python
    user_id: int | None = Field(default=None, index=True)  # 擁有者（帳戶系統後必填）
```

`src/quanquant/db/engine.py` 的 `init_db` 改為：

```python
def init_db() -> None:
    """Create all tables, then apply lightweight column migrations."""
    from quanquant.db import models  # noqa: F401
    from quanquant.db.migrate import ensure_columns

    engine = get_engine()
    SQLModel.metadata.create_all(engine)
    ensure_columns(engine)
```

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_migrate.py -v && uv run pytest -q`
Expected: 全 PASS（既有測試不受影響——新欄位 nullable、新表由 conftest 的 `create_all` 建出）

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/db/models.py src/quanquant/db/migrate.py src/quanquant/db/engine.py tests/test_migrate.py
git commit -m "feat: add User/UserChartState models, user_id columns, startup migration"
```

---

### Task 2: 認證基元 — 密碼雜湊與簽章 cookie

**Files:**
- Modify: `pyproject.toml:10-23`（dependencies 加 `bcrypt`、`itsdangerous`）
- Modify: `src/quanquant/config.py`（加 `session_secret`）
- Create: `src/quanquant/auth/__init__.py`（空檔）
- Create: `src/quanquant/auth/passwords.py`
- Create: `src/quanquant/auth/tokens.py`
- Test: `tests/test_auth_primitives.py`

**Interfaces:**
- Consumes: `get_settings()`
- Produces: `hash_password(plain: str) -> str`、`verify_password(plain: str, hashed: str) -> bool`、`SESSION_COOKIE = "qq_session"`、`MAX_AGE_SECONDS = 30*24*3600`、`sign_session(user_id: int, token_version: int) -> str`、`load_session(value: str) -> dict | None`（回 `{"uid": int, "tv": int}`）

- [ ] **Step 1: 加依賴**

`pyproject.toml` dependencies 陣列加兩行（依字母序插入）：

```toml
    "bcrypt>=4.1",
    "itsdangerous>=2.2",
```

Run: `uv sync --extra dev`
Expected: 安裝成功

- [ ] **Step 2: 寫失敗測試**

```python
# tests/test_auth_primitives.py
"""Password hashing + signed session cookies."""
from quanquant.auth.passwords import hash_password, verify_password
from quanquant.auth.tokens import load_session, sign_session


def test_hash_roundtrip():
    h = hash_password("s3cret-pw")
    assert h != "s3cret-pw"
    assert verify_password("s3cret-pw", h)
    assert not verify_password("wrong", h)


def test_verify_garbage_hash_is_false():
    assert not verify_password("pw", "not-a-bcrypt-hash")


def test_session_roundtrip():
    token = sign_session(42, 3)
    data = load_session(token)
    assert data == {"uid": 42, "tv": 3}


def test_tampered_token_rejected():
    token = sign_session(42, 3)
    assert load_session(token[:-2] + "xx") is None
    assert load_session("garbage") is None
```

- [ ] **Step 3: 跑測試確認失敗**

Run: `uv run pytest tests/test_auth_primitives.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'quanquant.auth'`

- [ ] **Step 4: 實作**

`src/quanquant/config.py` — `Settings` 加欄位（放在 `# Web / dashboard` 區塊、`db_url` 之前）：

```python
    # Account system — signs the session cookie. MUST be set in production
    # (.env on the VM); unset → a transient per-process key (dev only).
    session_secret: str = ""
```

`src/quanquant/auth/__init__.py`：空檔。

`src/quanquant/auth/passwords.py`：

```python
"""bcrypt password hashing (cost 12)."""
import bcrypt

_ROUNDS = 12


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt(rounds=_ROUNDS)).decode("ascii")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("ascii"))
    except ValueError:
        return False  # malformed hash — treat as mismatch, never raise
```

`src/quanquant/auth/tokens.py`：

```python
"""Signed session-cookie payloads (itsdangerous). No server-side session table:
the cookie carries {uid, tv}; every request re-loads the user and checks
is_active + token_version, so deactivation and password changes revoke
immediately."""
import logging
import secrets

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from quanquant.config import get_settings

SESSION_COOKIE = "qq_session"
MAX_AGE_SECONDS = 30 * 24 * 3600  # 30 days

log = logging.getLogger(__name__)
_fallback_secret: str | None = None  # stable within the process (dev/tests)


def _secret() -> str:
    global _fallback_secret
    configured = get_settings().session_secret
    if configured:
        return configured
    if _fallback_secret is None:
        _fallback_secret = secrets.token_urlsafe(32)
        log.warning("SESSION_SECRET unset — transient signing key (dev only); "
                    "a restart logs everyone out")
    return _fallback_secret


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(_secret(), salt="qq-session")


def sign_session(user_id: int, token_version: int) -> str:
    return _serializer().dumps({"uid": user_id, "tv": token_version})


def load_session(value: str) -> dict | None:
    """{'uid': int, 'tv': int}, or None when invalid/expired/tampered."""
    try:
        data = _serializer().loads(value, max_age=MAX_AGE_SECONDS)
    except (BadSignature, SignatureExpired):
        return None
    if not isinstance(data, dict) or "uid" not in data or "tv" not in data:
        return None
    return data
```

- [ ] **Step 5: 跑測試確認通過**

Run: `uv run pytest tests/test_auth_primitives.py -v`
Expected: 4 PASS

- [ ] **Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/quanquant/config.py src/quanquant/auth/ tests/test_auth_primitives.py
git commit -m "feat: auth primitives — bcrypt hashing + itsdangerous session cookies"
```

---

### Task 3: auth service — 登入驗證（含失敗鎖定）與使用者管理

**Files:**
- Create: `src/quanquant/auth/service.py`
- Test: `tests/test_auth_service.py`

**Interfaces:**
- Consumes: Task 1 `User`、Task 2 `hash_password`/`verify_password`
- Produces:
  - `authenticate(db: Session, username: str, password: str, *, now: float | None = None) -> User | None`
  - `create_user(db, username: str, password: str, *, display_name: str | None = None, role: str = "user") -> User`（重複 username → `ValueError`）
  - `reset_password(db, user: User, new_password: str) -> None`（bump `token_version`）
  - `change_password(db, user: User, old_password: str, new_password: str) -> bool`
  - `set_active(db, user: User, active: bool) -> None`
  - `set_role(db, user: User, role: str) -> None`
  - `get_by_username(db, username: str) -> User | None`
  - `list_users(db) -> list[User]`
  - 測試用：`clear_failures()`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_auth_service.py
"""authenticate() + user management + login-failure lockout."""
import pytest

from quanquant.auth import service


@pytest.fixture(autouse=True)
def _fresh_lockout():
    service.clear_failures()
    yield
    service.clear_failures()


@pytest.fixture
def henry(session):
    return service.create_user(session, "henry", "pw12345", display_name="Henry", role="admin")


def test_create_and_authenticate(session, henry):
    assert henry.id is not None
    u = service.authenticate(session, "henry", "pw12345")
    assert u is not None and u.username == "henry"


def test_wrong_password_rejected(session, henry):
    assert service.authenticate(session, "henry", "nope") is None


def test_unknown_user_rejected(session):
    assert service.authenticate(session, "ghost", "pw") is None


def test_duplicate_username_raises(session, henry):
    with pytest.raises(ValueError):
        service.create_user(session, "henry", "other")


def test_inactive_user_rejected(session, henry):
    service.set_active(session, henry, False)
    assert service.authenticate(session, "henry", "pw12345") is None


def test_lockout_after_5_failures(session, henry):
    for _ in range(5):
        assert service.authenticate(session, "henry", "bad", now=100.0) is None
    # locked: even the CORRECT password fails inside the 60s window
    assert service.authenticate(session, "henry", "pw12345", now=130.0) is None
    # after the window it works again
    assert service.authenticate(session, "henry", "pw12345", now=161.0) is not None


def test_success_clears_failures(session, henry):
    for _ in range(4):
        service.authenticate(session, "henry", "bad", now=100.0)
    assert service.authenticate(session, "henry", "pw12345", now=101.0) is not None
    # counter reset — 4 more failures still below the threshold
    for _ in range(4):
        service.authenticate(session, "henry", "bad", now=102.0)
    assert service.authenticate(session, "henry", "pw12345", now=103.0) is not None


def test_reset_password_bumps_token_version(session, henry):
    old_tv = henry.token_version
    service.reset_password(session, henry, "newpw999")
    assert henry.token_version == old_tv + 1
    assert service.authenticate(session, "henry", "newpw999") is not None


def test_change_password_needs_old(session, henry):
    assert service.change_password(session, henry, "WRONG", "x") is False
    assert service.change_password(session, henry, "pw12345", "newpw999") is True
    assert service.authenticate(session, "henry", "newpw999") is not None


def test_list_users_sorted(session, henry):
    service.create_user(session, "amy", "pw")
    names = [u.username for u in service.list_users(session)]
    assert names == ["amy", "henry"]
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_auth_service.py -v`
Expected: FAIL — `cannot import name 'service'`

- [ ] **Step 3: 實作**

`src/quanquant/auth/service.py`：

```python
"""User management + login with an in-memory failure lockout.

Lockout is per-username, in-memory (single-process deployment): 5 consecutive
failures lock the account for 60s. `now` is injectable for tests; production
uses time.monotonic().
"""
import time

from sqlmodel import Session, select

from quanquant.auth.passwords import hash_password, verify_password
from quanquant.db.models import User, _utcnow

_MAX_FAILURES = 5
_LOCK_SECONDS = 60.0
# username -> (consecutive failures, locked_until monotonic timestamp)
_failures: dict[str, tuple[int, float]] = {}


def clear_failures() -> None:
    """Test helper: reset lockout state."""
    _failures.clear()


def _is_locked(username: str, now: float) -> bool:
    _count, until = _failures.get(username, (0, 0.0))
    return now < until


def _record_failure(username: str, now: float) -> None:
    count, until = _failures.get(username, (0, 0.0))
    count += 1
    if count >= _MAX_FAILURES:
        _failures[username] = (0, now + _LOCK_SECONDS)  # lock and reset the counter
    else:
        _failures[username] = (count, until)


def authenticate(
    db: Session, username: str, password: str, *, now: float | None = None
) -> User | None:
    """The user on success; None on unknown user / bad password / inactive / locked."""
    t = time.monotonic() if now is None else now
    if _is_locked(username, t):
        return None
    user = get_by_username(db, username)
    if user is None or not user.is_active or not verify_password(password, user.password_hash):
        _record_failure(username, t)
        return None
    _failures.pop(username, None)
    return user


def get_by_username(db: Session, username: str) -> User | None:
    return db.exec(select(User).where(User.username == username)).first()


def create_user(
    db: Session,
    username: str,
    password: str,
    *,
    display_name: str | None = None,
    role: str = "user",
) -> User:
    if get_by_username(db, username) is not None:
        raise ValueError(f"username {username!r} already exists")
    user = User(
        username=username,
        display_name=display_name or username,
        password_hash=hash_password(password),
        role=role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def reset_password(db: Session, user: User, new_password: str) -> None:
    user.password_hash = hash_password(new_password)
    user.token_version += 1  # revoke every existing cookie
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()
    db.refresh(user)


def change_password(db: Session, user: User, old_password: str, new_password: str) -> bool:
    if not verify_password(old_password, user.password_hash):
        return False
    reset_password(db, user, new_password)
    return True


def set_active(db: Session, user: User, active: bool) -> None:
    user.is_active = active
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()


def set_role(db: Session, user: User, role: str) -> None:
    if role not in ("admin", "user"):
        raise ValueError(f"unknown role {role!r}")
    user.role = role
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()


def list_users(db: Session) -> list[User]:
    return list(db.exec(select(User).order_by(User.username)))
```

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_auth_service.py -v`
Expected: 10 PASS

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/auth/service.py tests/test_auth_service.py
git commit -m "feat: auth service — authenticate with lockout + user management"
```

---

### Task 4: 登入／登出路由、login 頁、`get_current_user`／`require_admin`

此任務先把認證路由與 dependencies 建好並註冊；**既有 router 的保護在 Task 5 才開**（讓既有測試維持綠）。

**Files:**
- Modify: `src/quanquant/web/deps.py`（加 `get_current_user`、`require_admin`）
- Create: `src/quanquant/web/routers/auth.py`
- Create: `src/quanquant/web/templates/login.html`
- Modify: `src/quanquant/web/app.py:196-205`（`create_app` 註冊 auth router）
- Test: `tests/test_auth_routes.py`

**Interfaces:**
- Consumes: Task 2 `SESSION_COOKIE`/`MAX_AGE_SECONDS`/`sign_session`/`load_session`、Task 3 `service.authenticate`
- Produces:
  - `get_current_user(request, session) -> User`（dependency；同時設 `request.state.user` 供模板用；失敗 raise：HTMX/`/api/` → 401＋`HX-Redirect: /login`，一般頁 → 303 `Location: /login`）
  - `require_admin(user) -> User`（非 admin → 403）
  - Routes：`GET /login`、`POST /login`、`POST /logout`
  - `set_session_cookie(response, request, user) -> None`（auth router 內，Task 12 改密碼也用）

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_auth_routes.py
"""Login/logout flow. Router protection is asserted in test_route_protection.py (Task 5)."""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth import service
from quanquant.auth.tokens import SESSION_COOKIE
from quanquant.web.app import create_app
from quanquant.web.deps import get_poller, get_session


@pytest.fixture(autouse=True)
def _fresh_lockout():
    service.clear_failures()
    yield
    service.clear_failures()


@pytest.fixture
def anon_client(engine):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    return TestClient(app)


@pytest.fixture
def henry(engine):
    with Session(engine) as s:
        return service.create_user(s, "henry", "pw12345", display_name="Henry")


def test_login_page_renders(anon_client):
    r = anon_client.get("/login")
    assert r.status_code == 200
    assert "password" in r.text


def test_login_success_sets_cookie_and_redirects(anon_client, henry):
    r = anon_client.post(
        "/login", data={"username": "henry", "password": "pw12345"}, follow_redirects=False
    )
    assert r.status_code == 303
    assert r.headers["location"] == "/"
    assert SESSION_COOKIE in r.cookies


def test_login_failure_shows_error_no_cookie(anon_client, henry):
    r = anon_client.post("/login", data={"username": "henry", "password": "bad"})
    assert r.status_code == 200
    assert SESSION_COOKIE not in r.cookies
    assert "帳號或密碼錯誤" in r.text


def test_logout_clears_cookie(anon_client, henry):
    anon_client.post("/login", data={"username": "henry", "password": "pw12345"})
    r = anon_client.post("/logout", follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/login"
    assert anon_client.cookies.get(SESSION_COOKIE) is None
```

（Task 5 會把 `anon_client` 移進 conftest；屆時刪掉本檔的區域版本。）

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_auth_routes.py -v`
Expected: FAIL — `GET /login` 404（router 不存在）

- [ ] **Step 3: 實作 dependencies**

`src/quanquant/web/deps.py` — imports 區改為：

```python
"""FastAPI dependencies and small request helpers."""
from datetime import date, datetime, time

from fastapi import Depends, HTTPException, Request
from sqlmodel import Session

from quanquant.auth.tokens import SESSION_COOKIE, load_session
from quanquant.db.engine import get_session  # re-exported for routers
from quanquant.db.models import User
from quanquant.poller import QuotePoller
from quanquant.pulse.engine import PulseEngine

__all__ = ["get_session", "get_poller", "get_pulse", "parse_date",
           "get_current_user", "require_admin"]
```

檔尾加：

```python
def _auth_failure(request: Request) -> HTTPException:
    """Full-page loads get a 303 to /login; HTMX/API calls get 401 + HX-Redirect
    (htmx performs a full-page redirect on that header)."""
    if request.headers.get("HX-Request") or request.url.path.startswith("/api/"):
        return HTTPException(status_code=401, headers={"HX-Redirect": "/login"})
    return HTTPException(status_code=303, headers={"Location": "/login"})


def get_current_user(request: Request, session: Session = Depends(get_session)) -> User:
    """Resolve the logged-in user from the signed session cookie.

    Re-checks is_active and token_version on every request, so deactivating an
    account or changing a password revokes existing cookies immediately. Also
    stashes the user on request.state for templates (base.html user menu).
    """
    raw = request.cookies.get(SESSION_COOKIE)
    data = load_session(raw) if raw else None
    if data is None:
        raise _auth_failure(request)
    user = session.get(User, data["uid"])
    if user is None or not user.is_active or user.token_version != data["tv"]:
        raise _auth_failure(request)
    request.state.user = user
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin only")
    return user
```

- [ ] **Step 4: 實作 auth router 與 login 頁**

`src/quanquant/web/routers/auth.py`：

```python
"""Login/logout. The login page is standalone (not base.html) — it must render
for anonymous users."""
from fastapi import APIRouter, Depends, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from quanquant.auth import service
from quanquant.auth.tokens import MAX_AGE_SECONDS, SESSION_COOKIE, sign_session
from quanquant.db.models import User
from quanquant.web.deps import get_session
from quanquant.web.templating import templates

router = APIRouter()


def set_session_cookie(response: Response, request: Request, user: User) -> None:
    # Behind Caddy the app sees plain HTTP; trust X-Forwarded-Proto for `secure`.
    secure = request.headers.get("x-forwarded-proto", request.url.scheme) == "https"
    response.set_cookie(
        SESSION_COOKIE,
        sign_session(user.id or 0, user.token_version),
        max_age=MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
        secure=secure,
    )


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {"error": None})


@router.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    session: Session = Depends(get_session),
):
    user = service.authenticate(session, username.strip(), password)
    if user is None:
        return templates.TemplateResponse(
            request, "login.html", {"error": "帳號或密碼錯誤（連續失敗會暫時鎖定）"}
        )
    response = RedirectResponse("/", status_code=303)
    set_session_cookie(response, request, user)
    return response


@router.post("/logout")
def logout():
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE)
    return response
```

`src/quanquant/web/templates/login.html`：

```html
<!DOCTYPE html>
<html lang="zh-Hant" data-theme="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>登入 — QuanQuant</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
  <link rel="stylesheet" href="/static/app.css">
</head>
<body>
  <main class="container" style="max-width: 24rem; padding-top: 8vh;">
    <h1>QuanQuant</h1>
    {% if error %}<p style="color: var(--pico-color-red-500);">{{ error }}</p>{% endif %}
    <form method="post" action="/login">
      <label>帳號
        <input type="text" name="username" autocomplete="username" required autofocus>
      </label>
      <label>密碼
        <input type="password" name="password" autocomplete="current-password" required>
      </label>
      <button type="submit">登入</button>
    </form>
  </main>
</body>
</html>
```

`src/quanquant/web/app.py` — routers import 區加：

```python
from quanquant.web.routers import auth as auth_routes
```

`create_app()` 內、`include_router(dashboard.router)` 之前加：

```python
    app.include_router(auth_routes.router)  # public: /login, /logout
```

- [ ] **Step 5: 跑測試確認通過**

Run: `uv run pytest tests/test_auth_routes.py -v && uv run pytest -q`
Expected: 全 PASS（保護尚未開啟，既有測試不動）

- [ ] **Step 6: Commit**

```bash
git add src/quanquant/web/deps.py src/quanquant/web/routers/auth.py src/quanquant/web/templates/login.html src/quanquant/web/app.py tests/test_auth_routes.py
git commit -m "feat: login/logout routes + get_current_user/require_admin dependencies"
```

---

### Task 5: 開啟全站保護＋搬移 /healthz＋測試基礎設施

**Files:**
- Create: `src/quanquant/web/routers/health.py`（`/healthz` 從 candles.py 搬出——candles router 掛保護後 healthz 必須留在免驗證區）
- Modify: `src/quanquant/web/routers/candles.py:116-124`（移除 healthz）
- Modify: `src/quanquant/web/app.py`（受保護 router 全掛 `Depends(get_current_user)`；註冊 health router）
- Modify: `tests/conftest.py`（`user` fixture＋`client` 改為已登入、加 `anon_client`）
- Modify: `tests/test_auth_routes.py`（刪區域 `anon_client`，改用 conftest 版）
- Test: `tests/test_route_protection.py`

**Interfaces:**
- Consumes: Task 4 `get_current_user`、`sign_session`、`SESSION_COOKIE`
- Produces: conftest fixtures — `user`（admin「tester」，密碼 `test-pw`）、`client`（已以 tester 登入）、`anon_client`（未登入）、`_build_app(engine)`（隔離測試建多 client 用）

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_route_protection.py
"""Every app surface requires login; /healthz and /login stay public."""
import pytest

PROTECTED_PAGES = ["/", "/journal", "/stats"]
PROTECTED_APIS = ["/api/candles?tf=1m", "/api/alerts", "/api/chart/state", "/api/pulse/state"]


@pytest.mark.parametrize("path", PROTECTED_PAGES)
def test_pages_redirect_anonymous(anon_client, path):
    r = anon_client.get(path, follow_redirects=False)
    assert r.status_code == 303
    assert r.headers["location"] == "/login"


@pytest.mark.parametrize("path", PROTECTED_APIS)
def test_apis_401_anonymous(anon_client, path):
    r = anon_client.get(path, follow_redirects=False)
    assert r.status_code == 401
    assert r.headers.get("hx-redirect") == "/login"


def test_healthz_public(anon_client):
    assert anon_client.get("/healthz").status_code == 200


def test_login_public(anon_client):
    assert anon_client.get("/login").status_code == 200


def test_logged_in_client_passes(client):
    assert client.get("/").status_code == 200
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_route_protection.py -v`
Expected: FAIL — 受保護路徑對匿名回 200，且 conftest 沒有 `anon_client`

- [ ] **Step 3: 搬移 /healthz**

`src/quanquant/web/routers/health.py`（新檔）：

```python
"""Unauthenticated health check (GCP uptime check hits this without credentials)."""
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from quanquant.poller import QuotePoller
from quanquant.web.deps import get_poller

router = APIRouter()


@router.get("/healthz")
async def healthz(poller: QuotePoller | None = Depends(get_poller)):
    age: float | None = None
    if poller is not None and poller.last is not None:
        age = (datetime.now(timezone.utc) - poller.last.fetched_at).total_seconds()
    return JSONResponse({"status": "ok", "last_quote_age_s": age})
```

`src/quanquant/web/routers/candles.py`：刪掉檔尾 `# --- health ---` 區塊（`healthz` 函式），並清掉因此不再使用的 imports（`datetime`/`timezone`、`QuotePoller`、`get_poller`——先確認檔內無其他使用處）。

- [ ] **Step 4: app.py 開啟保護**

`src/quanquant/web/app.py` imports 加：

```python
from fastapi import Depends, FastAPI
from quanquant.web.deps import get_current_user
from quanquant.web.routers import health
```

`create_app()` 改為：

```python
def create_app() -> FastAPI:
    app = FastAPI(title="QuanQuant", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    app.include_router(auth_routes.router)    # public: /login, /logout
    app.include_router(health.router)         # public: /healthz

    protected = [Depends(get_current_user)]
    app.include_router(dashboard.router, dependencies=protected)
    app.include_router(trades.router, dependencies=protected)
    app.include_router(stats.router, dependencies=protected)
    app.include_router(candles.router, dependencies=protected)
    app.include_router(alerts.router, dependencies=protected)
    app.include_router(pulse_routes.router, dependencies=protected)
    return app
```

- [ ] **Step 5: conftest 加登入基礎設施**

`tests/conftest.py` — imports 加：

```python
from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
```

`client` fixture 改為以下（並新增 `user`、`anon_client`、`_build_app`）：

```python
@pytest.fixture
def user(engine):
    """Default logged-in account for route tests. Admin so admin-only surfaces
    (pulse toggle, /admin) work without a second fixture in most tests."""
    with Session(engine) as s:
        return auth_service.create_user(
            s, "tester", "test-pw", display_name="Tester", role="admin"
        )


def _build_app(engine):
    def _session_override():
        with Session(engine) as s:
            yield s

    app = create_app()
    app.dependency_overrides[get_session] = _session_override
    app.dependency_overrides[get_poller] = lambda: None
    return app


@pytest.fixture
def client(engine, user):
    """TestClient already logged in as `user` (signed cookie, no /login round-trip)."""
    c = TestClient(_build_app(engine))
    c.cookies.set(SESSION_COOKIE, sign_session(user.id, user.token_version))
    return c


@pytest.fixture
def anon_client(engine):
    return TestClient(_build_app(engine))
```

`tests/test_auth_routes.py`：刪掉檔內區域 `anon_client` fixture 與其 imports（改用 conftest 的）。

- [ ] **Step 6: 跑全部測試、修既有失敗**

Run: `uv run pytest -q`
Expected: 全 PASS。若有既有測試自建 TestClient（不經 conftest `client`）而收到 303/401，改用 conftest fixtures。

- [ ] **Step 7: Commit**

```bash
git add src/quanquant/web/routers/health.py src/quanquant/web/routers/candles.py src/quanquant/web/app.py tests/conftest.py tests/test_route_protection.py tests/test_auth_routes.py
git commit -m "feat: require login on all app routes; move /healthz to public router"
```

---

### Task 6: trades／stats 資料隔離

**Files:**
- Modify: `src/quanquant/journal/repository.py`（全函式加 `user_id` keyword-only 參數）
- Modify: `src/quanquant/web/routers/trades.py`（各 route 取 `current_user` 並傳入 repo）
- Modify: `src/quanquant/web/routers/stats.py`（同上）
- Modify: `tests/conftest.py`（`sample_trades`、`make_create` 相關傳 `user_id`）
- Modify: `tests/test_repository.py`、`tests/test_export.py`、`tests/test_metrics.py`、`tests/test_api_trades.py` 等直接呼叫 repo 的既有測試（補 `user_id`）
- Test: `tests/test_isolation.py`（新檔；本任務先放 trades/stats 案例）

**Interfaces:**
- Consumes: Task 4 `get_current_user`、Task 5 conftest `user`／`_build_app`
- Produces: repo 新簽名（後續任務與測試依此）：
  - `create_trade(session, data, *, user_id: int) -> Trade`
  - `get_trade(session, trade_id, *, user_id: int) -> Trade | None`（非本人 → None）
  - `update_trade(session, trade_id, data, *, user_id: int) -> Trade | None`
  - `delete_trade(session, trade_id, *, user_id: int) -> bool`
  - `list_trades(session, *, user_id: int, symbol=None, tag=None, date_from=None, date_to=None, status="all")`
  - `list_for_stats(session, *, user_id: int, ...)`、`list_symbols(session, *, user_id: int)`、`list_all_tags(session, *, user_id: int)`

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_isolation.py
"""Cross-user isolation: A must not see or touch B's data."""
import datetime as dt
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session
from quanquant.journal import repository as repo
from quanquant.journal.schemas import TradeCreate

from .conftest import _build_app


@pytest.fixture
def user_b(engine):
    with Session(engine) as s:
        return auth_service.create_user(s, "bob", "bob-pw", display_name="Bob")


@pytest.fixture
def client_b(engine, user_b):
    c = TestClient(_build_app(engine))
    c.cookies.set(SESSION_COOKIE, sign_session(user_b.id, user_b.token_version))
    return c


def _trade(session, user_id, price="18000"):
    return repo.create_trade(
        session,
        TradeCreate(
            symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 1, 9, 0),
            entry_price=Decimal(price), size=1, point_value=Decimal("200"),
        ),
        user_id=user_id,
    )


def test_repo_list_filters_by_user(session, user, user_b):
    _trade(session, user.id)
    _trade(session, user_b.id)
    assert len(repo.list_trades(session, user_id=user.id)) == 1
    assert len(repo.list_trades(session, user_id=user_b.id)) == 1


def test_repo_get_update_delete_scoped(session, user, user_b):
    t = _trade(session, user.id)
    assert repo.get_trade(session, t.id, user_id=user_b.id) is None
    assert repo.delete_trade(session, t.id, user_id=user_b.id) is False
    assert repo.get_trade(session, t.id, user_id=user.id) is not None


def test_journal_page_shows_only_own(client, client_b, session, user, user_b):
    _trade(session, user.id, price="11111")
    _trade(session, user_b.id, price="22222")
    assert "11,111" in client.get("/journal").text
    assert "22,222" not in client.get("/journal").text
    assert "11,111" not in client_b.get("/journal").text


def test_cannot_delete_others_trade_via_route(client_b, session, user):
    t = _trade(session, user.id)
    client_b.delete(f"/trades/{t.id}")
    assert repo.get_trade(session, t.id, user_id=user.id) is not None


def test_stats_only_own(client, client_b, session, user, user_b):
    repo.create_trade(
        session,
        TradeCreate(
            symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 1, 9, 0),
            entry_price=Decimal("18000"), exit_time=dt.datetime(2026, 6, 1, 10, 0),
            exit_price=Decimal("18100"), size=1, point_value=Decimal("200"),
        ),
        user_id=user.id,
    )
    own = client.get("/stats/data").json()
    other = client_b.get("/stats/data").json()
    # 鍵名以 quanquant/stats/metrics.py compute_stats 實際回傳為準（實作前先讀該檔確認）
    assert own != other
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_isolation.py -v`
Expected: FAIL — `create_trade() got an unexpected keyword argument 'user_id'`

- [ ] **Step 3: 改 repository**

`src/quanquant/journal/repository.py` 全部函式加 `user_id`。`create_trade`／`get_trade`／`delete_trade` 完整新版：

```python
def create_trade(session: Session, data: TradeCreate, *, user_id: int) -> Trade:
    trade = Trade(
        user_id=user_id,
        symbol=data.symbol,
        direction=data.direction,
        entry_time=data.entry_time,
        entry_price=data.entry_price,
        exit_time=data.exit_time,
        exit_price=data.exit_price,
        stop_loss_price=data.stop_loss_price,
        take_profit_strategy=data.take_profit_strategy,
        size=data.size,
        point_value=data.point_value,
        fee=data.fee,
        note=data.note,
        tags=join_tags(data.tags),
        pnl_is_manual=data.pnl is not None,
        pnl=data.pnl,
    )
    _recompute_pnl(trade)
    session.add(trade)
    session.commit()
    session.refresh(trade)
    return trade


def get_trade(session: Session, trade_id: int, *, user_id: int) -> Trade | None:
    trade = session.get(Trade, trade_id)
    if trade is None or trade.user_id != user_id:
        return None  # not found OR someone else's — identical from the caller's view
    return trade


def delete_trade(session: Session, trade_id: int, *, user_id: int) -> bool:
    trade = get_trade(session, trade_id, user_id=user_id)
    if trade is None:
        return False
    session.delete(trade)
    session.commit()
    return True
```

`update_trade`：簽名改 `(session, trade_id, data, *, user_id: int)`，第一行改 `trade = get_trade(session, trade_id, user_id=user_id)`，其餘（fields/tags/pnl 檢核、commit、refresh）不動。

`list_trades`：簽名加 `*, user_id: int`（放最前），`stmt = select(Trade).where(Trade.user_id == user_id)`，其餘 where/order_by/tag 過濾不動。

`list_for_stats`：簽名加 `*, user_id: int`，轉傳 `list_trades(session, user_id=user_id, ...)`。

`list_symbols`／`list_all_tags`：簽名加 `*, user_id: int`，select 加 `.where(Trade.user_id == user_id)`：

```python
def list_symbols(session: Session, *, user_id: int) -> list[str]:
    rows = session.exec(select(Trade.symbol).where(Trade.user_id == user_id).distinct())
    return sorted(set(rows))


def list_all_tags(session: Session, *, user_id: int) -> list[str]:
    rows = session.exec(select(Trade.tags).where(Trade.user_id == user_id))
    tags: set[str] = set()
    for raw in rows:
        tags.update(split_tags(raw))
    return sorted(tags)
```

- [ ] **Step 4: 改 trades／stats routes**

`src/quanquant/web/routers/trades.py`：imports 改

```python
from quanquant.db.models import User
from quanquant.web.deps import get_current_user, get_poller, get_session, parse_date
```

每個 route 函式加參數 `user: User = Depends(get_current_user)`，所有 `repo.xxx(session, ...)` 呼叫補 `user_id=user.id`。範例（`journal_page`；`list_trades_fragment`、`new_trade_form`、`edit_trade_form`、`create_trade_route`、`update_trade_route`、`delete_trade_route` 同型改法）：

```python
@router.get("/journal", response_class=HTMLResponse)
async def journal_page(
    request: Request,
    session: Session = Depends(get_session),
    poller: QuotePoller | None = Depends(get_poller),
    user: User = Depends(get_current_user),
    symbol: str | None = Query(None),
    tag: str | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    status: str = Query("all"),
):
    trades = repo.list_trades(
        session,
        user_id=user.id,
        symbol=symbol or None,
        tag=tag or None,
        date_from=parse_date(date_from),
        date_to=parse_date(date_to, end=True),
        status=status,
    )
    return templates.TemplateResponse(
        request,
        "journal.html",
        {
            "active": "journal",
            "rows": _rows(trades, _mark_price(poller)),
            "symbols": repo.list_symbols(session, user_id=user.id),
            "all_tags": repo.list_all_tags(session, user_id=user.id),
            "f": {"symbol": symbol or "", "tag": tag or "", "date_from": date_from or "",
                  "date_to": date_to or "", "status": status},
        },
    )
```

`src/quanquant/web/routers/stats.py`：`_filtered` 簽名改 `(session, user_id, symbol, tag, date_from, date_to)` 並往下傳 `user_id=user_id`；四個 route 各加 `user: User = Depends(get_current_user)`，呼叫改 `_filtered(session, user.id, symbol, tag, date_from, date_to)`；`stats_page` 內 `repo.list_symbols`／`repo.list_all_tags` 補 `user_id=user.id`。imports 加 `from quanquant.db.models import User`、`from quanquant.web.deps import get_current_user, get_session, parse_date`。

（FastAPI 對同一 request 的 dependency 有快取——route 參數的 `get_current_user` 與 router 層掛的是同一次執行，無重複查詢。）

- [ ] **Step 5: 修 conftest 與既有測試**

`tests/conftest.py`：`sample_trades` fixture 改為吃 `(session, user)`，所有 `repo.create_trade(session, make_create(...))` 補 `user_id=user.id`，回傳 `repo.list_trades(session, user_id=user.id)`。

`tests/test_repository.py`、`tests/test_export.py`、`tests/test_metrics.py`、`tests/test_api_trades.py` 等直接呼叫 repo 的地方一律引入 `user` fixture 並補 `user_id=user.id`。

- [ ] **Step 6: 跑全部測試**

Run: `uv run pytest -q`
Expected: 全 PASS

- [ ] **Step 7: Commit**

```bash
git add src/quanquant/journal/repository.py src/quanquant/web/routers/trades.py src/quanquant/web/routers/stats.py tests/
git commit -m "feat: per-user isolation for trade journal and stats"
```

---

### Task 7: chart state 改走 user_chart_states

**Files:**
- Modify: `src/quanquant/web/routers/candles.py:63-113`（chart-state 端點改用 `UserChartState`）
- Test: `tests/test_isolation.py`（追加 chart-state 案例）

**Interfaces:**
- Consumes: Task 1 `UserChartState`、Task 4 `get_current_user`
- Produces: `GET /api/chart/state`、`PUT /api/chart/state/{kind}` 對外行為不變（前端 `chart.js` 無須改動），但資料按 user 分開

- [ ] **Step 1: 寫失敗測試（追加到 tests/test_isolation.py）**

```python
def test_chart_state_isolated(client, client_b):
    client.put("/api/chart/state/indicators", json={"ma": [5, 10]})
    assert client.get("/api/chart/state").json()["indicators"] == {"ma": [5, 10]}
    assert client_b.get("/api/chart/state").json()["indicators"] is None


def test_chart_state_upsert_per_user(client, client_b):
    client.put("/api/chart/state/drawings", json=[{"type": "line"}])
    client_b.put("/api/chart/state/drawings", json=[{"type": "rect"}])
    assert client.get("/api/chart/state").json()["drawings"] == [{"type": "line"}]
    assert client_b.get("/api/chart/state").json()["drawings"] == [{"type": "rect"}]
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_isolation.py -v -k chart_state`
Expected: FAIL — 兩個 user 讀到同一份 state

- [ ] **Step 3: 實作**

`src/quanquant/web/routers/candles.py`：imports 改 `from quanquant.db.models import User, UserChartState, _utcnow`、deps import 加 `get_current_user`。chart-state 區塊改為：

```python
def _get_state(session: Session, user_id: int, symbol: str, kind: str) -> dict | list | None:
    stmt = select(UserChartState).where(
        UserChartState.user_id == user_id,
        UserChartState.symbol == symbol,
        UserChartState.kind == kind,
    )
    row = session.exec(stmt).first()
    if row is None:
        return None
    try:
        return json.loads(row.payload)
    except json.JSONDecodeError:
        return None


@router.get("/api/chart/state")
def chart_state(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str = Query("TXF"),
):
    return JSONResponse(
        {
            "indicators": _get_state(session, user.id, symbol, "indicators"),
            "drawings": _get_state(session, user.id, symbol, "drawings"),
        }
    )


@router.put("/api/chart/state/{kind}")
async def put_chart_state(
    kind: str,
    request: Request,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str = Query("TXF"),
):
    if kind not in _STATE_KINDS:
        raise HTTPException(status_code=404, detail=f"unknown state kind {kind!r}")
    body = await request.body()
    if len(body) > _MAX_STATE_BYTES:
        raise HTTPException(status_code=413, detail="state payload too large")
    try:
        json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=422, detail="payload must be valid JSON") from None

    stmt = select(UserChartState).where(
        UserChartState.user_id == user.id,
        UserChartState.symbol == symbol,
        UserChartState.kind == kind,
    )
    row = session.exec(stmt).first()
    if row is None:
        row = UserChartState(user_id=user.id, symbol=symbol, kind=kind, payload=body.decode("utf-8"))
    else:
        row.payload = body.decode("utf-8")
        row.updated_at = _utcnow()
    session.add(row)
    session.commit()
    return Response(status_code=204)
```

（`put_chart_state` 原本就是 async `def`（要 await request body），維持原樣——「sync def」約束針對 candle 讀取路徑。`ChartState` import 若 candles.py 已無他處使用則移除。）

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_isolation.py tests/test_api_candles.py tests/test_chart_history.py -v`
Expected: 全 PASS（既有 chart-state 測試走已登入 `client`，行為不變）

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/routers/candles.py tests/test_isolation.py
git commit -m "feat: per-user chart state via user_chart_states table"
```

---

### Task 8: alerts 資料隔離

**Files:**
- Modify: `src/quanquant/web/routers/alerts.py`（CRUD＋events 以 user 過濾；他人資源 404）
- Modify: `tests/test_api_alerts.py`（delete-missing 從 204 改斷言 404，其他案例補登入 client 已由 Task 5 涵蓋）
- Test: `tests/test_isolation.py`（追加 alerts 案例）

**Interfaces:**
- Consumes: Task 4 `get_current_user`
- Produces: `/api/alerts*` 全部 user-scoped；`Alert.user_id` 建立時寫入；`_owned_alert(session, alert_id, user) -> Alert`（404 on missing/others'）

- [ ] **Step 1: 寫失敗測試（追加到 tests/test_isolation.py）**

```python
_ALERT_BODY = {
    "symbol": "TXF", "timeframe": "5m", "left_kind": "price",
    "op": "gte", "right_kind": "const", "right_value": "18000",
}


def test_alerts_isolated(client, client_b):
    client.post("/api/alerts", json=_ALERT_BODY)
    assert len(client.get("/api/alerts").json()) == 1
    assert len(client_b.get("/api/alerts").json()) == 0


def test_others_alert_404(client, client_b):
    aid = client.post("/api/alerts", json=_ALERT_BODY).json()["id"]
    assert client_b.patch(f"/api/alerts/{aid}", json={"enabled": False}).status_code == 404
    assert client_b.delete(f"/api/alerts/{aid}").status_code == 404
    assert client.get("/api/alerts").json()[0]["enabled"] is True  # owner unaffected


def test_alert_events_scoped(client, client_b, session):
    from quanquant.db.models import AlertEvent

    aid = client.post("/api/alerts", json=_ALERT_BODY).json()["id"]
    session.add(AlertEvent(alert_id=aid, bar_ts=0, message="fired"))
    session.commit()
    assert len(client.get("/api/alerts/events").json()) == 1
    assert len(client_b.get("/api/alerts/events").json()) == 0
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_isolation.py -v -k alert`
Expected: FAIL — B 看得到 A 的 alert

- [ ] **Step 3: 實作**

`src/quanquant/web/routers/alerts.py`：imports 加 `User`（models）與 `get_current_user`（deps）。routes 改為：

```python
@router.get("/api/alerts")
def list_alerts(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    symbol: str = Query("TXF"),
):
    rows = session.exec(
        select(Alert)
        .where(Alert.symbol == symbol, Alert.user_id == user.id)
        .order_by(Alert.id.desc())  # type: ignore[union-attr]
    ).all()
    return JSONResponse([_alert_dict(a) for a in rows])


@router.post("/api/alerts")
def create_alert(
    body: AlertCreate,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    alert = Alert(**body.model_dump(), user_id=user.id)
    session.add(alert)
    session.commit()
    session.refresh(alert)
    return JSONResponse(_alert_dict(alert), status_code=201)


def _owned_alert(session: Session, alert_id: int, user: User) -> Alert:
    alert = session.get(Alert, alert_id)
    if alert is None or alert.user_id != user.id:
        # 404 (not 403): another user's alert is indistinguishable from a missing one
        raise HTTPException(status_code=404, detail="alert not found")
    return alert


@router.patch("/api/alerts/{alert_id}")
def patch_alert(
    alert_id: int,
    body: AlertPatch,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    alert = _owned_alert(session, alert_id, user)
    data = body.model_dump(exclude_unset=True)
    for key, value in data.items():
        setattr(alert, key, value)
    if data.get("enabled"):
        alert.armed = True  # re-arm when re-enabled
    alert.updated_at = _utcnow()
    session.add(alert)
    session.commit()
    session.refresh(alert)
    return JSONResponse(_alert_dict(alert))


@router.delete("/api/alerts/{alert_id}")
def delete_alert(
    alert_id: int,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    alert = _owned_alert(session, alert_id, user)
    session.delete(alert)
    session.commit()
    return Response(status_code=204)


@router.get("/api/alerts/events")
def list_events(
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    limit: int = Query(50, ge=1, le=200),
):
    own_alert_ids = select(Alert.id).where(Alert.user_id == user.id)
    events = session.exec(
        select(AlertEvent)
        .where(AlertEvent.alert_id.in_(own_alert_ids))  # type: ignore[union-attr]
        .order_by(AlertEvent.id.desc())  # type: ignore[union-attr]
        .limit(limit)
    ).all()
    return JSONResponse([
        {
            "id": e.id, "alertId": e.alert_id, "barTs": e.bar_ts, "message": e.message,
            "firedAt": e.fired_at.isoformat() if e.fired_at else None,
        }
        for e in events
    ])
```

**行為變更**：`delete_alert` 從「不存在也回 204」改為「不存在／非本人回 404」——`tests/test_api_alerts.py` 若有 204-on-missing 斷言，改為 404。

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_isolation.py tests/test_api_alerts.py -v`
Expected: 全 PASS

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/routers/alerts.py tests/
git commit -m "feat: per-user alerts — scoped CRUD and trigger log, 404 on others'"
```

---

### Task 9: 通知分流 — 瀏覽器 toast 只推擁有者、Telegram 標註擁有者

**Files:**
- Modify: `src/quanquant/notify/base.py`（`Notification` 加 `user_id`、`owner_name`）
- Modify: `src/quanquant/notify/browser.py`（per-user 訂閱與投遞）
- Modify: `src/quanquant/notify/telegram.py`（訊息加 👤 擁有者行）
- Modify: `src/quanquant/alerts/engine.py:132-166`（`_evaluate_sync` 帶擁有者資訊）
- Modify: `src/quanquant/web/routers/alerts.py:122-136`（`alerts_stream` 以 user 訂閱）
- Test: `tests/test_notify.py`（追加）、`tests/test_alert_engine.py`（依新欄位微調）

**Interfaces:**
- Consumes: Task 8 的 `Alert.user_id`
- Produces: `Notification(..., user_id: int | None = None, owner_name: str | None = None)`、`BrowserNotifier.subscribe(user_id: int | None = None)`

- [ ] **Step 1: 寫失敗測試（追加到 tests/test_notify.py）**

```python
from quanquant.notify.base import Notification
from quanquant.notify.browser import BrowserNotifier
from quanquant.notify.telegram import format_alert


def _owned_n(user_id=None, owner_name=None) -> Notification:
    return Notification(
        symbol="TXF", timeframe="5m", tf_label="5分", condition="收盤 ≥ 18000",
        left_value=18001.0, right_value=18000.0, right_is_indicator=False,
        alert_id=1, bar_ts=0, body="TXF 5m 收盤 ≥ 18000（18001.0）",
        user_id=user_id, owner_name=owner_name,
    )


async def test_browser_routes_to_owner_only():
    notifier = BrowserNotifier()
    q_owner = notifier.subscribe(user_id=1)
    q_other = notifier.subscribe(user_id=2)
    await notifier.send(_owned_n(user_id=1))
    assert q_owner.qsize() == 1
    assert q_other.qsize() == 0


async def test_browser_ownerless_notification_broadcasts():
    notifier = BrowserNotifier()
    q1 = notifier.subscribe(user_id=1)
    q2 = notifier.subscribe(user_id=2)
    await notifier.send(_owned_n(user_id=None))
    assert q1.qsize() == 1 and q2.qsize() == 1


def test_telegram_message_names_owner():
    text = format_alert(_owned_n(user_id=1, owner_name="Henry"))
    assert "👤 擁有者：Henry" in text


def test_telegram_message_without_owner_unchanged():
    assert "👤" not in format_alert(_owned_n())
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_notify.py -v`
Expected: FAIL — `Notification.__init__() got an unexpected keyword argument 'user_id'`

- [ ] **Step 3: 實作**

`src/quanquant/notify/base.py` — `Notification` 加兩個欄位（放在 `body` 之後；有預設值，既有建構不破）：

```python
    user_id: int | None = None      # alert owner (None = system-wide, broadcast)
    owner_name: str | None = None   # display name for the shared Telegram chat
```

`src/quanquant/notify/browser.py` 改為：

```python
class BrowserNotifier:
    def __init__(self) -> None:
        self._subscribers: dict[asyncio.Queue[str], int | None] = {}

    def subscribe(self, user_id: int | None = None) -> asyncio.Queue[str]:
        """user_id=None subscribes to everything (tests/system streams); a real
        id only receives that user's notifications plus ownerless broadcasts."""
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        self._subscribers[queue] = user_id
        return queue

    def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        self._subscribers.pop(queue, None)

    async def send(self, n: Notification) -> None:
        payload = json.dumps({
            "body": n.body, "condition": n.condition, "symbol": n.symbol,
            "tf": n.timeframe, "alertId": n.alert_id, "barTs": n.bar_ts,
        })
        for queue, uid in list(self._subscribers.items()):
            if n.user_id is not None and uid is not None and uid != n.user_id:
                continue  # someone else's alert
            try:
                queue.put_nowait(payload)
            except asyncio.QueueFull:  # slow consumer: drop oldest
                try:
                    queue.get_nowait()
                    queue.put_nowait(payload)
                except (asyncio.QueueEmpty, asyncio.QueueFull):
                    pass
```

`src/quanquant/notify/telegram.py` — `format_alert` 的 lines 組裝改為：

```python
    lines = [
        "🔔 QuanQuant 警示觸發",
        "",
    ]
    if n.owner_name:
        lines.append(f"👤 擁有者：{n.owner_name}")
    lines += [
        f"📊 商品：{n.symbol}",
        f"🕐 週期：{n.tf_label}",
        f"🎯 條件：{n.condition}",
        f"💲 結算值：{_fmt(n.left_value)}",
    ]
```

（其後的 `right_is_indicator`／`K棒` 行不動。）

`src/quanquant/alerts/engine.py` — import 加 `User`；`_evaluate_sync` 內、`alerts` 載入後（`if not alerts: return out` 之後）加：

```python
        owner_ids = {a.user_id for a in alerts if a.user_id is not None}
        owners: dict[int, str] = {}
        if owner_ids:
            owners = {
                u.id: u.display_name
                for u in db.exec(select(User).where(User.id.in_(owner_ids)))  # type: ignore[union-attr]
            }
```

`Notification(...)` 建構加兩個參數：

```python
                        user_id=alert.user_id,
                        owner_name=owners.get(alert.user_id) if alert.user_id else None,
```

`src/quanquant/web/routers/alerts.py` — `alerts_stream` 改為：

```python
@router.get("/alerts/stream")
async def alerts_stream(request: Request, user: User = Depends(get_current_user)):
    notify = getattr(request.app.state, "notify", None)
    if notify is None:
        return EventSourceResponse(iter(()))
    queue = notify.browser.subscribe(user_id=user.id)

    async def event_generator():
        try:
            while True:
                yield {"data": await queue.get()}
        finally:
            notify.browser.unsubscribe(queue)

    return EventSourceResponse(event_generator())
```

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_notify.py tests/test_alert_engine.py -v && uv run pytest -q`
Expected: 全 PASS（`test_alert_engine.py` 若直接建 `Notification` 或斷言 TG 格式，依新欄位微調）

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/notify/ src/quanquant/alerts/engine.py src/quanquant/web/routers/alerts.py tests/
git commit -m "feat: route alert toasts to owners; name owner in shared Telegram chat"
```

---

### Task 10: CLI `quanquant-user`（bootstrap／create／reset-password／list）

**Files:**
- Create: `src/quanquant/user_cli.py`
- Modify: `pyproject.toml:33-37`（`[project.scripts]` 加 `quanquant-user`）
- Test: `tests/test_user_cli.py`

**Interfaces:**
- Consumes: Task 3 `service.*`、Task 1 models
- Produces: console script `quanquant-user`；核心函式 `bootstrap_admin(db: Session, username: str, password: str) -> User`（CLI 與測試共用；已有 admin → `SystemExit(1)`）

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_user_cli.py
"""bootstrap: first admin + claiming pre-account data."""
import datetime as dt
from decimal import Decimal

import pytest
from sqlmodel import select

from quanquant.db.models import Alert, ChartState, Trade, UserChartState
from quanquant.user_cli import bootstrap_admin


@pytest.fixture
def legacy_data(session):
    session.add(Trade(
        symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 1, 9, 0),
        entry_price=Decimal("18000"), size=1, point_value=Decimal("200"),
    ))
    session.add(Alert(
        symbol="TXF", timeframe="5m", left_kind="price", op="gte",
        right_kind="const", right_value=Decimal("18000"),
    ))
    session.add(ChartState(symbol="TXF", kind="indicators", payload='{"ma":[5]}'))
    session.add(ChartState(symbol="TXF", kind="pulse", payload='{"telegramEnabled":true}'))
    session.commit()


def test_bootstrap_creates_admin_and_claims(session, legacy_data):
    admin = bootstrap_admin(session, "henry", "pw12345")
    assert admin.role == "admin"
    assert session.exec(select(Trade)).first().user_id == admin.id
    assert session.exec(select(Alert)).first().user_id == admin.id
    copied = session.exec(select(UserChartState)).all()
    assert len(copied) == 1  # indicators copied; kind="pulse" stays system-level
    assert copied[0].user_id == admin.id and copied[0].kind == "indicators"
    # legacy chart_states rows untouched (rollback safety)
    assert len(session.exec(select(ChartState)).all()) == 2


def test_bootstrap_refuses_second_run(session):
    bootstrap_admin(session, "henry", "pw12345")
    with pytest.raises(SystemExit):
        bootstrap_admin(session, "again", "pw")


def test_bootstrap_claims_only_orphans(session, legacy_data, user):
    # a row that already has an owner must keep it
    owned = Trade(
        symbol="TXF", direction="long", entry_time=dt.datetime(2026, 6, 2, 9, 0),
        entry_price=Decimal("18000"), size=1, point_value=Decimal("200"), user_id=user.id,
    )
    session.add(owned)
    session.commit()
    # `user` fixture is admin — bootstrap refuses; demote first to test claiming
    user_row = session.get(type(user), user.id)
    user_row.role = "user"
    session.add(user_row)
    session.commit()

    admin = bootstrap_admin(session, "henry", "pw12345")
    session.refresh(owned)
    assert owned.user_id == user.id  # untouched
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_user_cli.py -v`
Expected: FAIL — `No module named 'quanquant.user_cli'`

- [ ] **Step 3: 實作**

`src/quanquant/user_cli.py`：

```python
"""User management CLI: `quanquant-user bootstrap|create|reset-password|list`.

bootstrap = create the FIRST admin and claim all pre-account data (user_id IS
NULL trades/alerts; legacy chart_states indicators/drawings copied — not moved —
to user_chart_states). Refuses to run when an admin already exists.
"""
import argparse
import getpass
import sys

from sqlalchemy import update
from sqlmodel import Session, select

from quanquant.auth import service
from quanquant.db.engine import get_engine, init_db
from quanquant.db.models import Alert, ChartState, Trade, User, UserChartState


def bootstrap_admin(db: Session, username: str, password: str) -> User:
    existing_admin = db.exec(select(User).where(User.role == "admin")).first()
    if existing_admin is not None:
        print(f"已存在 admin（{existing_admin.username}），bootstrap 拒絕重跑", file=sys.stderr)
        raise SystemExit(1)

    admin = service.create_user(db, username, password, role="admin")

    db.execute(update(Trade).where(Trade.user_id.is_(None)).values(user_id=admin.id))
    db.execute(update(Alert).where(Alert.user_id.is_(None)).values(user_id=admin.id))

    legacy = db.exec(
        select(ChartState).where(ChartState.kind.in_(("indicators", "drawings")))  # type: ignore[union-attr]
    ).all()
    for row in legacy:
        dup = db.exec(
            select(UserChartState).where(
                UserChartState.user_id == admin.id,
                UserChartState.symbol == row.symbol,
                UserChartState.kind == row.kind,
            )
        ).first()
        if dup is None:
            db.add(UserChartState(
                user_id=admin.id, symbol=row.symbol, kind=row.kind, payload=row.payload
            ))
    db.commit()
    return admin


def _prompt_password(args) -> str:
    if args.password:
        return args.password
    pw = getpass.getpass("密碼: ")
    if getpass.getpass("再輸入一次: ") != pw:
        print("兩次輸入不一致", file=sys.stderr)
        raise SystemExit(1)
    return pw


def main() -> None:
    parser = argparse.ArgumentParser(prog="quanquant-user")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_boot = sub.add_parser("bootstrap", help="建第一個 admin 並認領舊資料")
    p_boot.add_argument("username")
    p_boot.add_argument("--password", help="未提供則互動輸入")

    p_create = sub.add_parser("create", help="建一般帳號")
    p_create.add_argument("username")
    p_create.add_argument("--display-name")
    p_create.add_argument("--role", choices=("admin", "user"), default="user")
    p_create.add_argument("--password")

    p_reset = sub.add_parser("reset-password", help="重設密碼（bump token_version）")
    p_reset.add_argument("username")
    p_reset.add_argument("--password")

    sub.add_parser("list", help="列出帳號")

    args = parser.parse_args()
    init_db()
    with Session(get_engine()) as db:
        if args.cmd == "bootstrap":
            admin = bootstrap_admin(db, args.username, _prompt_password(args))
            print(f"admin '{admin.username}' 建立完成，舊資料已認領")
        elif args.cmd == "create":
            user = service.create_user(
                db, args.username, _prompt_password(args),
                display_name=args.display_name, role=args.role,
            )
            print(f"使用者 '{user.username}'（{user.role}）建立完成")
        elif args.cmd == "reset-password":
            user = service.get_by_username(db, args.username)
            if user is None:
                print(f"找不到使用者 '{args.username}'", file=sys.stderr)
                raise SystemExit(1)
            service.reset_password(db, user, _prompt_password(args))
            print("密碼已重設（所有裝置需重新登入）")
        elif args.cmd == "list":
            for u in service.list_users(db):
                flag = "" if u.is_active else "（停用）"
                print(f"{u.id}\t{u.username}\t{u.role}\t{u.display_name}{flag}")
```

`pyproject.toml` `[project.scripts]` 加：

```toml
quanquant-user = "quanquant.user_cli:main"
```

- [ ] **Step 4: 跑測試確認通過**

Run: `uv sync --extra dev && uv run pytest tests/test_user_cli.py -v`
Expected: 3 PASS

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/user_cli.py pyproject.toml uv.lock tests/test_user_cli.py
git commit -m "feat: quanquant-user CLI — bootstrap first admin + claim legacy data"
```

---

### Task 11: Web 管理頁 `/admin/users`

**Files:**
- Create: `src/quanquant/web/routers/admin.py`
- Create: `src/quanquant/web/templates/admin_users.html`
- Modify: `src/quanquant/web/app.py`（註冊 admin router）
- Test: `tests/test_admin_routes.py`

**Interfaces:**
- Consumes: Task 3 `service.*`、Task 4 `require_admin`
- Produces: `GET /admin/users`（頁面）、`POST /admin/users`（建立）、`POST /admin/users/{user_id}/reset-password`、`POST /admin/users/{user_id}/toggle-active`、`POST /admin/users/{user_id}/role` — 全部表單 POST → 303 回列表（不用 HTMX，管理頁保持最簡）

- [ ] **Step 1: 寫失敗測試**

```python
# tests/test_admin_routes.py
"""/admin/users: admin-only management surface."""
import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from quanquant.auth import service as auth_service
from quanquant.auth.tokens import SESSION_COOKIE, sign_session

from .conftest import _build_app


@pytest.fixture
def plain_client(engine):
    with Session(engine) as s:
        u = auth_service.create_user(s, "plain", "pw", role="user")
    c = TestClient(_build_app(engine))
    c.cookies.set(SESSION_COOKIE, sign_session(u.id, u.token_version))
    return c


def _amy(engine):
    with Session(engine) as s:
        return auth_service.get_by_username(s, "amy")


def test_non_admin_403(plain_client):
    assert plain_client.get("/admin/users").status_code == 403


def test_page_lists_users(client):
    r = client.get("/admin/users")
    assert r.status_code == 200 and "tester" in r.text


def test_create_user(client, engine):
    r = client.post(
        "/admin/users",
        data={"username": "amy", "display_name": "Amy", "password": "amy-pw", "role": "user"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert _amy(engine) is not None


def test_create_duplicate_shows_error(client, engine):
    body = {"username": "amy", "display_name": "Amy", "password": "pw", "role": "user"}
    client.post("/admin/users", data=body)
    r = client.post("/admin/users", data=body)
    assert "already exists" in r.text


def test_toggle_active(client, engine):
    client.post("/admin/users", data={"username": "amy", "display_name": "Amy",
                                      "password": "pw", "role": "user"})
    client.post(f"/admin/users/{_amy(engine).id}/toggle-active")
    assert _amy(engine).is_active is False


def test_reset_password_bumps_tv(client, engine):
    client.post("/admin/users", data={"username": "amy", "display_name": "Amy",
                                      "password": "pw", "role": "user"})
    amy = _amy(engine)
    client.post(f"/admin/users/{amy.id}/reset-password", data={"password": "new-pw"})
    assert _amy(engine).token_version == amy.token_version + 1
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_admin_routes.py -v`
Expected: FAIL — `/admin/users` 404

- [ ] **Step 3: 實作**

`src/quanquant/web/routers/admin.py`：

```python
"""Admin-only user management. Plain forms + 303 redirects (no HTMX — this page
is rare-use; keep it dead simple)."""
from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session

from quanquant.auth import service
from quanquant.db.models import User
from quanquant.web.deps import get_session, require_admin
from quanquant.web.templating import templates

router = APIRouter(prefix="/admin", dependencies=[Depends(require_admin)])


def _user_or_404(session: Session, user_id: int) -> User:
    user = session.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    return user


@router.get("/users", response_class=HTMLResponse)
def users_page(request: Request, session: Session = Depends(get_session), error: str | None = None):
    return templates.TemplateResponse(
        request,
        "admin_users.html",
        {"active": "admin", "users": service.list_users(session), "error": error},
    )


@router.post("/users")
def create_user(
    request: Request,
    session: Session = Depends(get_session),
    username: str = Form(...),
    display_name: str = Form(""),
    password: str = Form(...),
    role: str = Form("user"),
):
    try:
        service.create_user(
            session, username.strip(), password,
            display_name=display_name.strip() or None, role=role,
        )
    except ValueError as exc:
        return users_page(request, session, error=str(exc))
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/users/{user_id}/reset-password")
def reset_password(
    user_id: int, session: Session = Depends(get_session), password: str = Form(...)
):
    service.reset_password(session, _user_or_404(session, user_id), password)
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/users/{user_id}/toggle-active")
def toggle_active(user_id: int, session: Session = Depends(get_session)):
    user = _user_or_404(session, user_id)
    service.set_active(session, user, not user.is_active)
    return RedirectResponse("/admin/users", status_code=303)


@router.post("/users/{user_id}/role")
def change_role(user_id: int, session: Session = Depends(get_session), role: str = Form(...)):
    service.set_role(session, _user_or_404(session, user_id), role)
    return RedirectResponse("/admin/users", status_code=303)
```

`src/quanquant/web/templates/admin_users.html`：

```html
{% extends "base.html" %}
{% block content %}
<h2>使用者管理</h2>
{% if error %}<p style="color: var(--pico-color-red-500);">{{ error }}</p>{% endif %}

<table>
  <thead>
    <tr><th>帳號</th><th>顯示名稱</th><th>角色</th><th>狀態</th><th>建立時間</th><th>操作</th></tr>
  </thead>
  <tbody>
  {% for u in users %}
    <tr>
      <td>{{ u.username }}</td>
      <td>{{ u.display_name }}</td>
      <td>
        <form method="post" action="/admin/users/{{ u.id }}/role" style="display:inline">
          <select name="role" onchange="this.form.submit()">
            <option value="user" {% if u.role == 'user' %}selected{% endif %}>user</option>
            <option value="admin" {% if u.role == 'admin' %}selected{% endif %}>admin</option>
          </select>
        </form>
      </td>
      <td>{{ '啟用' if u.is_active else '停用' }}</td>
      <td>{{ u.created_at | dt }}</td>
      <td>
        <form method="post" action="/admin/users/{{ u.id }}/toggle-active" style="display:inline">
          <button class="secondary" type="submit">{{ '停用' if u.is_active else '啟用' }}</button>
        </form>
        <form method="post" action="/admin/users/{{ u.id }}/reset-password" style="display:inline">
          <input type="password" name="password" placeholder="新密碼" required style="width:8rem">
          <button class="secondary" type="submit">重設密碼</button>
        </form>
      </td>
    </tr>
  {% endfor %}
  </tbody>
</table>

<h3>建立帳號</h3>
<form method="post" action="/admin/users">
  <div class="grid">
    <input type="text" name="username" placeholder="帳號" required>
    <input type="text" name="display_name" placeholder="顯示名稱">
    <input type="password" name="password" placeholder="初始密碼" required>
    <select name="role"><option value="user">user</option><option value="admin">admin</option></select>
  </div>
  <button type="submit">建立</button>
</form>
{% endblock %}
```

`src/quanquant/web/app.py`：routers import 加 `from quanquant.web.routers import admin as admin_routes`；`create_app` 的 protected 區塊後加：

```python
    app.include_router(admin_routes.router)   # self-guarded: require_admin
```

- [ ] **Step 4: 跑測試確認通過**

Run: `uv run pytest tests/test_admin_routes.py -v && uv run pytest -q`
Expected: 全 PASS

- [ ] **Step 5: Commit**

```bash
git add src/quanquant/web/routers/admin.py src/quanquant/web/templates/admin_users.html src/quanquant/web/app.py tests/test_admin_routes.py
git commit -m "feat: /admin/users management page (create/reset/toggle/role)"
```

---

### Task 12: navbar 使用者選單＋修改密碼＋Caddy 收尾

**Files:**
- Modify: `src/quanquant/web/templates/base.html`（navbar 加使用者選單）
- Modify: `src/quanquant/web/routers/auth.py`（加 `GET /account`、`POST /account/password`）
- Create: `src/quanquant/web/templates/account.html`
- Modify: `Caddyfile`（移除 basic_auth）
- Modify: `docs/deployment.md`（部署順序＋SESSION_SECRET＋bootstrap）
- Modify: `CLAUDE.md`（常用指令加 `quanquant-user`）
- Test: `tests/test_auth_routes.py`（追加改密碼與 navbar 案例）

**Interfaces:**
- Consumes: Task 3 `service.change_password`、Task 4 `get_current_user`／`set_session_cookie`／`request.state.user`
- Produces: `GET /account`、`POST /account/password`

- [ ] **Step 1: 寫失敗測試（追加到 tests/test_auth_routes.py）**

```python
def test_change_password_flow(client):
    # wrong old password → error page
    r = client.post("/account/password",
                    data={"old_password": "WRONG", "new_password": "brand-new"})
    assert "舊密碼錯誤" in r.text

    r = client.post("/account/password",
                    data={"old_password": "test-pw", "new_password": "brand-new"},
                    follow_redirects=False)
    assert r.status_code == 303
    # a fresh cookie was issued for this device — protected pages still work
    assert client.get("/journal").status_code == 200


def test_navbar_shows_user_menu(client):
    body = client.get("/").text
    assert "Tester" in body           # display_name
    assert "/logout" in body
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `uv run pytest tests/test_auth_routes.py -v -k "change_password or navbar"`
Expected: FAIL — `/account/password` 404、navbar 無使用者選單

- [ ] **Step 3: 實作 /account**

`src/quanquant/web/routers/auth.py` — deps import 加 `get_current_user`，檔尾追加：

```python
@router.get("/account", response_class=HTMLResponse)
def account_page(request: Request, user: User = Depends(get_current_user)):
    return templates.TemplateResponse(
        request, "account.html", {"active": "account", "error": None}
    )


@router.post("/account/password")
def change_password(
    request: Request,
    old_password: str = Form(...),
    new_password: str = Form(...),
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
):
    if not service.change_password(session, user, old_password, new_password):
        return templates.TemplateResponse(
            request, "account.html", {"active": "account", "error": "舊密碼錯誤"}
        )
    # token_version was bumped — re-issue THIS device's cookie; other devices log out
    response = RedirectResponse("/", status_code=303)
    set_session_cookie(response, request, user)
    return response
```

`src/quanquant/web/templates/account.html`：

```html
{% extends "base.html" %}
{% block content %}
<h2>帳戶設定</h2>
{% if error %}<p style="color: var(--pico-color-red-500);">{{ error }}</p>{% endif %}
<form method="post" action="/account/password" style="max-width: 24rem;">
  <label>舊密碼
    <input type="password" name="old_password" autocomplete="current-password" required>
  </label>
  <label>新密碼
    <input type="password" name="new_password" autocomplete="new-password" required>
  </label>
  <button type="submit">修改密碼（其他裝置將被登出）</button>
</form>
{% endblock %}
```

- [ ] **Step 4: navbar**

`src/quanquant/web/templates/base.html` — 第二個 `<ul>` 改為：

```html
    <ul>
      <li><a href="/" {% if active == 'dashboard' %}class="active"{% endif %}>儀表板</a></li>
      <li><a href="/journal" {% if active == 'journal' %}class="active"{% endif %}>交易日記</a></li>
      <li><a href="/stats" {% if active == 'stats' %}class="active"{% endif %}>績效統計</a></li>
      {% set u = request.state.user if request.state.user is defined else None %}
      {% if u %}
      <li>
        <details class="dropdown">
          <summary>{{ u.display_name }}</summary>
          <ul dir="rtl">
            {% if u.role == 'admin' %}<li><a href="/admin/users">使用者管理</a></li>{% endif %}
            <li><a href="/account">帳戶設定</a></li>
            <li><a href="#" onclick="document.getElementById('logout-form').submit(); return false;">登出</a></li>
          </ul>
        </details>
        <form id="logout-form" method="post" action="/logout" hidden></form>
      </li>
      {% endif %}
    </ul>
```

- [ ] **Step 5: Caddyfile 與文件**

`Caddyfile` 全檔改為（app 已有登入，healthz 例外不再需要）：

```
# DOMAIN comes from the environment (compose). Unset → https://localhost (internal CA).
# Domain swap later = change DOMAIN in .env and `docker compose up -d caddy`.
# Auth is app-level (session login) since the account system — no basic_auth here.
{$DOMAIN:localhost} {
	reverse_proxy app:8000
}
```

`docs/deployment.md` 追加「帳戶系統部署」一節，內容（照抄）：

```markdown
## 帳戶系統部署（首次啟用）

依序執行：

1. VM 的 `.env` 加 `SESSION_SECRET`（`openssl rand -base64 32` 產生）。
2. 照固定三步部署 app：`git commit → git push → ./scripts/deploy.sh`。
3. VM 上建第一個 admin 並認領舊資料：
   `docker compose exec app quanquant-user bootstrap <帳號>`（互動輸入密碼）。
4. 瀏覽器登入驗證（此時 Caddy basic_auth 與 app 登入並存，安全無虞）。
5. 確認可登入後部署移除 basic_auth 的 Caddyfile：`docker compose up -d caddy`。

日常帳號管理：Web `/admin/users`（admin），或 SSH 備援
`docker compose exec app quanquant-user create|reset-password|list`。
密碼重設會 bump token_version，所有裝置立即登出。
```

`CLAUDE.md` 常用指令區塊加一行：

```bash
uv run quanquant-user bootstrap|create|reset-password|list   # 帳戶管理（首次部署先 bootstrap）
```

- [ ] **Step 6: 跑全部測試**

Run: `uv run pytest -q`
Expected: 全 PASS

- [ ] **Step 7: Commit**

```bash
git add src/quanquant/web/routers/auth.py src/quanquant/web/templates/ Caddyfile docs/deployment.md CLAUDE.md tests/test_auth_routes.py
git commit -m "feat: user menu + change password; drop Caddy basic_auth for app-level auth"
```

---

## 部署（計畫完成後、人工執行）

依規格 §10 與 `docs/deployment.md` 新增章節的五步順序執行；**執行前 `uv run pytest` 必須全綠**。

## Self-Review 紀錄

- **Spec 覆蓋**：§2 資料模型→Task 1；§3 認證→Task 2-5；§4 管理→Task 10-12；§5 隔離→Task 6-9；§6 遷移→Task 10；§7 TG 第一版→Task 9；§8 前端相容→無需程式碼（設計已相容）；§9 測試→各 task 內建＋isolation 專檔；§10 部署→Task 12。
- **型別一致性**：repo `user_id` 一律 keyword-only；`Notification` 新欄位有預設值（既有呼叫不破）；`BrowserNotifier.subscribe(user_id=None)` 向後相容；`_build_app` 由 Task 5 產出、Task 6/8/11 測試引用。
- **已知取捨**：登入鎖定為 in-memory（單機部署，重啟歸零可接受）；既有表由 ALTER 加入的 `user_id` 無 index（新裝機才有，資料量小可接受）；`delete_alert` 行為從 204-on-missing 改為 404（隔離語意需要，已註記修測試）。
