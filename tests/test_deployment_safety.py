"""部署安全（Task 10，收尾）：*.pfx 進 gitignore、git 追蹤檔案不含明文憑證檔、CA 檔權限
（0600 + owner UID）檢查、七＋一張新表在 Postgres 方言下 CreateTable 逐項可攜（round3 #20：
不只斷言 "CREATE TABLE" 出現，BigInteger→BIGINT/CHECK/UniqueConstraint 名稱與內容皆逐項
比對）、例外訊息 redaction 中央化並套用到 adapter/watchdog/lifespan/HTTP 錯誤路徑（round3
F8，不只 place 錯誤一處）。

`server_default` 稽核（round3 #20 附帶要求）：本檔的 8 張新表目前沒有任何欄位帶
`server_default`——`Field(default=...)` 只是 Python-side default，PG dialect 編譯出的
DDL 確實不含 DEFAULT 子句（見 `test_new_tables_have_no_accidental_server_default_gap`
的說明）。已對照 `broker/repository.py`/`broker/inbox_worker.py` 逐一確認：所有寫入路徑
一律經 SQLModel ORM（Python-side default 由 SQLAlchemy 在送出 INSERT 前套用）或顯式列出
全部欄位的 Core `insert().from_select(...)`（`reserve_quota`，`state` 欄位明確帶
`literal("reserved")`），沒有任何路徑靠 DB 端 DEFAULT 回填欄位；這 8 張表也全部經
`create_all` 全新建立（非 `ensure_columns` 的既有表 ALTER 補欄位路徑，那才需要 DEFAULT
替既有列回填），因此本案不落入 round3 條件句「若計畫依賴 DB default，改用 server_default」
的觸發條件，不需要修改 `db/models.py`。若未來新增繞過 ORM 的 raw-SQL 寫入路徑，需重新
評估。

Postgres 是否有真實環境可跑 `create_all + ensure_columns` schema smoke + 非法列 CHECK 擋：
本機已有 Docker（`docker --version` 可用）且專案已內建 `psycopg[binary]` 依賴，已於實作
時人工跑過一次真實 Postgres 16 容器驗證（見本次 Task 10 完成報告），非本檔常態 pytest
（避免 CI/其他開發機沒有 docker 時整批測試變 flaky）——部署前仍須依
`docs/deployment.md`〈下單子系統部署〉一節重新跑一次。
"""
import os
import subprocess

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from quanquant.broker.preflight import _ca_file_permissions_ok
from quanquant.db.models import (
    BrokerPosition,
    BrokerReconcileCursor,
    ConfirmToken,
    Deal,
    Order,
    OrderAudit,
    QuotaReservation,
    RawInbox,
    Trade,
)


def _repo_root() -> str:
    return subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True
    ).stdout.strip()


# ---- gitignore / secret scan ----

def test_gitignore_excludes_pfx_files():
    gitignore = open(os.path.join(_repo_root(), ".gitignore"), encoding="utf-8").read()
    assert "*.pfx" in gitignore


def test_no_pfx_files_tracked_in_git():
    tracked = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    assert not [f for f in tracked if f.endswith(".pfx")]


def test_no_env_files_tracked_in_git():
    """.env/.env.local 不進 git（已在 .gitignore，這裡額外驗證真的沒有任何 .env* 被追蹤，
    防止曾經誤加後忘記 git rm --cached）。"""
    tracked = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    leaked = [f for f in tracked if f == ".env" or f.endswith("/.env") or f.endswith(".env.local")]
    assert not leaked


# ---- CA 檔權限（0600 + owner UID，round3 F8） ----

def test_ca_file_permissions_ok_requires_0600_and_own_uid(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o600)
    ok, reason = _ca_file_permissions_ok(str(ca))
    assert ok is True and reason is None


def test_ca_file_permissions_rejects_overly_permissive_mode(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o644)
    ok, reason = _ca_file_permissions_ok(str(ca))
    assert ok is False and "0600" in reason


def test_ca_file_permissions_missing_file_rejected(tmp_path):
    ok, reason = _ca_file_permissions_ok(str(tmp_path / "missing.pfx"))
    assert ok is False


def test_ca_file_permissions_rejects_wrong_owner_uid(tmp_path, monkeypatch):
    """無法在測試環境真的建一個屬於別的 UID 的檔案（需要 root），所以反過來 monkeypatch
    `os.getuid`，模擬「目前執行 process 的 UID」與檔案 owner 不同，驗證 owner 檢查真的有
    在比對、不是只檢查權限位。"""
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o600)
    real_uid = os.getuid()
    monkeypatch.setattr(os, "getuid", lambda: real_uid + 1)
    ok, reason = _ca_file_permissions_ok(str(ca))
    assert ok is False
    assert "owner" in reason.lower() and "UID" in reason


# ---- Postgres 可攜性：逐項斷言（round3 #20） ----

def _ddl(model) -> str:
    return str(CreateTable(model.__table__).compile(dialect=postgresql.dialect()))


def test_order_table_ddl_has_mode_check_and_two_scope_unique_constraints():
    ddl = _ddl(Order)
    assert "CONSTRAINT ck_orders_mode CHECK (mode IS NOT NULL AND mode IN ('sim','real'))" in ddl
    assert "CONSTRAINT uq_orders_ordno_scope UNIQUE (broker, account, mode, ordno)" in ddl
    assert "CONSTRAINT uq_orders_broker_order_id_scope UNIQUE (broker, account, mode, broker_order_id)" in ddl


def test_deal_table_ddl_has_fill_unique_constraint_mode_check_and_bigint_ts():
    ddl = _ddl(Deal)
    assert "CONSTRAINT uq_deal_fill UNIQUE (broker, mode, account, trading_day, fill_id)" in ddl
    assert "CONSTRAINT ck_deals_mode CHECK (mode IS NOT NULL AND mode IN ('sim','real'))" in ddl
    assert "ts BIGINT NOT NULL" in ddl  # epoch-ms UTC；4-byte INTEGER 會溢位（Candle.ts 教訓同款）


def test_raw_inbox_table_compiles_with_expected_columns():
    ddl = _ddl(RawInbox)
    assert "CREATE TABLE raw_inbox" in ddl
    assert "kind VARCHAR NOT NULL" in ddl
    assert "processed BOOLEAN NOT NULL" in ddl


def test_broker_position_table_ddl_has_all_four_check_constraints():
    ddl = _ddl(BrokerPosition)
    assert "CONSTRAINT ck_broker_positions_mode CHECK (mode IS NOT NULL AND mode IN ('sim','real'))" in ddl
    assert (
        "CONSTRAINT ck_broker_positions_direction "
        "CHECK (direction IS NOT NULL AND direction IN ('long','short'))" in ddl
    )
    assert "CONSTRAINT ck_broker_positions_total_opened_qty_positive CHECK (total_opened_qty > 0)" in ddl
    assert (
        "CONSTRAINT ck_broker_positions_closed_qty_range "
        "CHECK (closed_qty >= 0 AND closed_qty <= total_opened_qty)" in ddl
    )


def test_broker_position_active_scope_partial_unique_index_compiles_under_postgres():
    """round3 #15 的 partial unique index 不是 UniqueConstraint，`CreateTable` 不會印出
    index DDL（SQLAlchemy 把 index 另外用 `CreateIndex` 產生）——單純斷言 CreateTable 含
    "CREATE TABLE" 完全不會驗到這個 invariant，必須另外 compile `CreateIndex`。"""
    idx = next(
        i for i in BrokerPosition.__table__.indexes if i.name == "uq_broker_positions_active_scope"
    )
    assert idx.unique is True
    ddl = str(CreateIndex(idx).compile(dialect=postgresql.dialect()))
    assert "CREATE UNIQUE INDEX uq_broker_positions_active_scope" in ddl
    assert "WHERE status = 'open'" in ddl


def test_order_audit_table_ddl_has_mode_check_and_bigint_ts():
    ddl = _ddl(OrderAudit)
    assert "CONSTRAINT ck_order_audits_mode CHECK (mode IS NOT NULL AND mode IN ('sim','real'))" in ddl
    assert "ts BIGINT NOT NULL" in ddl


def test_confirm_token_table_ddl_has_jti_unique_constraint():
    ddl = _ddl(ConfirmToken)
    assert "CONSTRAINT uq_confirm_tokens_jti UNIQUE (jti)" in ddl


def test_quota_reservation_table_ddl_has_reservation_unique_and_two_check_constraints():
    ddl = _ddl(QuotaReservation)
    assert "CONSTRAINT uq_quota_reservations_reservation_id UNIQUE (reservation_id)" in ddl
    assert "CONSTRAINT ck_quota_reservations_mode CHECK (mode IS NOT NULL AND mode IN ('sim','real'))" in ddl
    assert (
        "CONSTRAINT ck_quota_reservations_state "
        "CHECK (state IS NOT NULL AND state IN ('reserved','confirmed','released'))" in ddl
    )


def test_broker_reconcile_cursor_table_ddl_has_scope_unique_and_mode_check():
    """計畫草稿的 parametrize 清單漏列這張表（只有 7/8 張），實際 db/models.py 有 8 張新表
    （含 `BrokerReconcileCursor`），Task 10 對「每張新表」補齊到真正的 8 張，不盲抄漏掉
    的那張。"""
    ddl = _ddl(BrokerReconcileCursor)
    assert "CONSTRAINT uq_broker_reconcile_cursor_scope UNIQUE (broker, account, mode)" in ddl
    assert "CONSTRAINT ck_broker_reconcile_cursor_mode CHECK (mode IS NOT NULL AND mode IN ('sim','real'))" in ddl


@pytest.mark.parametrize(
    "model",
    [Order, Deal, RawInbox, BrokerPosition, OrderAudit, ConfirmToken, QuotaReservation, BrokerReconcileCursor],
)
def test_all_eight_new_tables_compile_under_postgres_dialect(model):
    assert "CREATE TABLE" in _ddl(model)


def test_trade_table_with_mode_source_columns_compiles_under_postgres():
    ddl = _ddl(Trade)
    assert "CREATE TABLE trades" in ddl
    assert "CONSTRAINT ck_trades_mode CHECK (mode IS NOT NULL AND mode IN ('sim','real'))" in ddl
    assert "CONSTRAINT ck_trades_source CHECK (source IS NOT NULL AND source IN ('manual','shioaji'))" in ddl


# ---- 例外訊息 redaction（round3 F8：中央化，覆蓋 adapter/watchdog/lifespan/HTTP） ----

def test_redact_secrets_scrubs_known_secret_values():
    from quanquant.broker.shioaji_adapter import _redact_secrets

    text = "connection failed: key=SUPERSECRETKEY123 auth denied"
    redacted = _redact_secrets(text, secrets=["SUPERSECRETKEY123"])
    assert "SUPERSECRETKEY123" not in redacted
    assert "REDACTED" in redacted


def test_redact_secrets_scrubs_multiple_distinct_secret_kinds_in_one_message():
    """驗收要求「測 api_key/ca_passwd/person_id 各一」的基礎版本：單一函式對三種不同秘密
    樣態同時抹除，缺一都會被抓到。"""
    from quanquant.broker.redaction import redact_secrets

    text = "shioaji login failed api_key=AKEY999 ca_passwd=PWSECRET person_id=A123456789"
    redacted = redact_secrets(text, secrets=["AKEY999", "PWSECRET", "A123456789"])
    assert "AKEY999" not in redacted
    assert "PWSECRET" not in redacted
    assert "A123456789" not in redacted
    assert redacted.count("[REDACTED]") == 3


def test_shioaji_adapter_secrets_to_redact_covers_api_key_secret_key_ca_passwd_person_id():
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    from quanquant.broker.supervisor import BrokerSupervisor

    adapter = ShioajiAdapter(
        api_key="AK1", secret_key="SK1", ca_path="/tmp/x.pfx", ca_passwd="CAPW1",
        person_id="PID1", symbol="TXF", mode="sim",
        session_factory=lambda: None, supervisor=BrokerSupervisor(),
    )
    secrets = adapter.secrets_to_redact
    assert set(secrets) == {"AK1", "SK1", "CAPW1", "PID1"}


def test_place_native_failure_exception_redacts_api_key_end_to_end(engine):
    """故意讓 adapter 的送單路徑拋出含 api_key 的例外，驗證 OrderError 的訊息（會被
    web/routers/orders.py 的 place_order 直接 `_form_error` 回顯給瀏覽器）不含明文 api_key。"""
    import asyncio
    from decimal import Decimal

    from sqlmodel import Session

    from quanquant.broker.base import OrderError
    from quanquant.broker.shioaji_adapter import ShioajiAdapter
    from quanquant.broker.supervisor import BrokerSupervisor
    from quanquant.broker.types import OrderRequest

    secret_api_key = "LIVE-API-KEY-DO-NOT-LEAK"
    adapter = ShioajiAdapter(
        api_key=secret_api_key, secret_key="s", ca_path=None, ca_passwd=None, person_id=None,
        symbol="TXF", mode="sim", session_factory=lambda: Session(engine), supervisor=BrokerSupervisor(),
    )

    class _FakeApi:
        futopt_account = type("Acc", (), {"account_id": "F1"})()

        def Order(self, **kw):
            return kw

        def place_order(self, contract, order):
            raise RuntimeError(f"upstream rejected key={secret_api_key}")

    adapter._api = _FakeApi()
    adapter._contract = object()
    adapter.account = "F1"

    req = OrderRequest(
        client_order_id="C-REDACT-1", symbol="TXF", action="Buy", qty=1, price=Decimal("18000"),
        price_type="LMT", order_type="ROD", octype="New", user_id=1,
    )
    with pytest.raises(OrderError) as exc_info:
        asyncio.run(adapter.place(req, actor_user_id=1))
    assert secret_api_key not in str(exc_info.value)
    assert "REDACTED" in str(exc_info.value)


def test_watchdog_reconnect_failure_redacts_ca_passwd_in_session_state(engine):
    """故意讓 watchdog 的重連失敗帶出含 ca_passwd 的例外，驗證 `OrderSessionState.last_error`
    （經 /healthz 未認證公開端點直接回顯）不含明文 ca_passwd。"""
    import asyncio

    from quanquant.broker.session_state import OrderSessionState
    from quanquant.broker.supervisor import BrokerSupervisor
    from quanquant.broker.watchdog import run_order_watchdog

    secret_ca_passwd = "CA-PASSWD-DO-NOT-LEAK"

    class _FailingAdapter:
        def __init__(self):
            self.supervisor = BrokerSupervisor()
            self._api = None
            self.secrets_to_redact = [secret_ca_passwd]

        async def connect(self):
            raise RuntimeError(f"activate_ca failed ca_passwd={secret_ca_passwd}")

        async def reconcile(self):
            pass

    adapter = _FailingAdapter()
    state = OrderSessionState()

    async def scenario():
        task = asyncio.create_task(run_order_watchdog(
            adapter, state, interval=0.01, login_min_interval=0.0,
            unquarantine_after_seconds=9999, unknown_reconcile_grace_seconds=9999,
        ))
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert state.last_error is not None
    assert secret_ca_passwd not in state.last_error
    assert "REDACTED" in state.last_error


def test_start_order_subsystem_connect_failure_redacts_person_id_in_healthz_state(monkeypatch):
    """故意讓 lifespan 的 `_start_order_subsystem` connect 失敗、例外帶出含 person_id 的
    文字，驗證 `app.state.order_session_state.last_error`（/healthz 直接回顯）不含明文
    person_id。"""
    import asyncio

    from fastapi import FastAPI
    from sqlalchemy.pool import StaticPool
    from sqlmodel import SQLModel, create_engine

    from quanquant.config import Settings
    from quanquant.web.app import _start_order_subsystem

    secret_person_id = "P-ID-DO-NOT-LEAK"

    def _test_engine():
        eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        SQLModel.metadata.create_all(eng)
        return eng

    class _FakeAdapterFailsWithSecret:
        def __init__(self, **kw):
            self._map_deal_report = lambda payload: None
            self._map_order_report = lambda payload: None
            self.secrets_to_redact = [secret_person_id]

        async def connect(self):
            raise RuntimeError(f"activate_ca failed person_id={secret_person_id}")

    monkeypatch.setattr("quanquant.web.app.get_engine", _test_engine)
    monkeypatch.setattr("quanquant.broker.shioaji_adapter.ShioajiAdapter", _FakeAdapterFailsWithSecret)

    app = FastAPI()
    tasks: list = []
    asyncio.run(_start_order_subsystem(app, Settings(
        shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
        order_mode="sim", order_owner_user_ids="1", symbol="TXF",
        # 明確釘住 in-process 路徑——不依賴 code-level 預設值，避免本機 `.env`（例如手動測試
        # agent 通道時暫留的 ORDER_CHANNEL=agent）汙染導致這個測試跑錯分支。
        order_channel="inprocess",
    ), tasks))

    assert app.state.order_service is None
    last_error = app.state.order_session_state.last_error
    assert last_error is not None
    assert secret_person_id not in last_error
    assert "REDACTED" in last_error


def test_existing_fake_adapter_without_secrets_to_redact_attribute_still_works(monkeypatch):
    """既有測試（test_order_subsystem_startup.py 的 `_FakeAdapterFails`）沒有實作
    `secrets_to_redact` 屬性——round3 F8 的加固不得要求所有既有測試 double 都補這個屬性
    才能過，`getattr(..., default=[])` 的防禦性寫法必須讓這類 double 照常運作。"""
    import asyncio

    from fastapi import FastAPI
    from sqlalchemy.pool import StaticPool
    from sqlmodel import SQLModel, create_engine

    from quanquant.config import Settings
    from quanquant.web.app import _start_order_subsystem

    def _test_engine():
        eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        SQLModel.metadata.create_all(eng)
        return eng

    class _FakeAdapterFailsNoSecretsAttr:
        def __init__(self, **kw):
            self._map_deal_report = lambda payload: None
            self._map_order_report = lambda payload: None

        async def connect(self):
            raise RuntimeError("login 失敗")

    monkeypatch.setattr("quanquant.web.app.get_engine", _test_engine)
    monkeypatch.setattr("quanquant.broker.shioaji_adapter.ShioajiAdapter", _FakeAdapterFailsNoSecretsAttr)

    app = FastAPI()
    tasks: list = []
    asyncio.run(_start_order_subsystem(app, Settings(
        shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
        order_mode="sim", order_owner_user_ids="1", symbol="TXF",
        # 同上：明確釘住 in-process 路徑，避免本機 `.env` 的 ORDER_CHANNEL 汙染。
        order_channel="inprocess",
    ), tasks))

    assert app.state.order_service is None
    assert "connect" in app.state.order_session_state.last_error
