"""ShioajiAdapter：OrderService 的 Shioaji 落地。

本檔不直接碰 Shioaji SDK（Task 2 委派重構，見下方說明），一般測試/CLI 載入這個模組不會
拉進原生 client。所有 native 呼叫（connect/close/place/cancel/update/positions）一律經
`BrokerSupervisor.run()` 這個單一 command executor（round3 #11）——**禁止**呼叫端各自
`async with supervisor.lock:`，否則 watchdog reconnect（Task 8）可能與 place 交錯替換
`_api`。鎖內、native 呼叫前最後一次 `_send_gate()` 檢查 kill switch（V3-2，修
BLOCKER#13 TOCTOU）；取消單刻意不檢查 kill switch（spec 明文：取消單仍允許）。

冪等（V3-3）：place 先以 client_order_id 查既有 Order，命中且 request_hash 相符 → 直接
回既有狀態，不重跑風控、不消費 confirm token、不再送單；不符 → 拒絕（同鍵不同 payload）；
命中但 owner 不符 → 拒絕（round3 開放清單 #6：非 owner 猜到 client_order_id+payload 不得
在授權前拿到他人 OrderAck）。canonical_payload_hash 一律用 self.mode/self.account
（server-side 真相），不接受外部輸入——`OrderRequest`（Task 2）結構上就沒有 mode 欄位，
外部混入 mode 在建構當下就會被 dataclass 拒絕（TypeError）。

callback-before-ack：place 在**送出 native 呼叫之前**就已經把 Order（client_order_id→
user_id/mode，ordno/broker_order_id 皆為 NULL 佔位）commit 進 DB，所以即使成交回報早於
ack 抵達（callback 在獨立執行緒，與本協程的 native 呼叫完成順序無關），RawInboxWorker
（Task 5）事後仍能透過 ordno/broker_order_id 補齊時解析到這筆委託（一開始解不到就
quarantine，等 ack 補上 ordno 後由 watchdog unquarantine 重試，不會遺失）。

callback → durable（round3 BLOCKER#2）：`set_order_callback` 的 handler
（`_on_order_cb`，跑在 Solace/.NET 背景執行緒）**只呼叫 `commit_raw_callback`**——
獨立 Session、同步、返回前保證落地，不做 `loop.call_soon_threadsafe` + `ensure_future`
排程協程晚點才 commit 的作法（那正是 round3 抓到的漏洞：loop 未就緒/排程後
crash/等 supervisor lock 時 payload 仍會遺失）；callback 內**不**碰部位/DB 業務邏輯，
那是 RawInboxWorker 之後才做的事。

**注意**：Shioaji SDK 的確切回呼欄位名稱（`order.id`/`order.seqno`/`status.id` 等）以
官方文件公開語意為準，本檔用 `getattr`/`payload.get` 防禦性存取（比照
`shioaji_stream.py::_dec` 既有慣例）；實作時若已安裝套件的 type stub 顯示不同屬性名，
只需局部調整以下函式內的欄位鍵，不影響本檔其餘架構：`_map_deal_report` /
`_map_order_report`（`ack_fields_from_trade` 已隨 native 呼叫一併搬到
`quanquant.broker.native.ShioajiNativeClient`，本檔不再直接碰 Shioaji SDK 物件）。

**Inc0 委派重構（Task 2）**：本檔所有直接碰 Shioaji SDK 的呼叫已搬到
`quanquant.broker.native.ShioajiNativeClient`（`self._native`）——`ShioajiAdapter` 只負責
DB/風控/冪等/告警業務邏輯，經 `self._native` 的方法送出實際 native 呼叫，本檔不再直接載入
Shioaji SDK 套件（Task 1/2 的 import 邊界：全 broker 子系統只有 `native.py` 允許載入該套件）。
`_api`/`_contract`/`account` 是委派 `self._native` 對應屬性的 property（相容既有測試
`a._api = _FakeApi()` 的注入寫法）。
"""
import asyncio
import json
import logging
import re
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Protocol

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from quanquant.broker import agent_commands, repository as brepo
from quanquant.broker.base import (
    AgentCommandTimeoutError,
    AgentUnavailableError,
    AuthorizationError,
    OrderError,
    RiskError,
    TradeNotFoundError,
)
from quanquant.broker.inbox_worker import (
    OrderReport,
    RawInboxDeadLetterError,
    commit_raw_callback,
    stage_scoped_raw_inbox,
)
from quanquant.broker.native import ShioajiNativeClient
from quanquant.broker.redaction import redact_secrets as _redact_secrets
from quanquant.broker.supervisor import BrokerSupervisor
from quanquant.broker.types import Fill, Mode, OrderAck, OrderRequest, Position, canonical_payload_hash
from quanquant.db.models import Order

log = logging.getLogger(__name__)

# 券商「明確拒絕」偵測（本次精進）：HTTP 慣例的 4xx 代表「請求已被伺服器端處理、且明確
# 拒絕」（例如 400 Bad Request、401/403 認證/授權失敗、404 Not Found、406 Not Acceptable、
# 409 Conflict），語意上不同於逾時/連線中斷（那些代表「不確定伺服器有沒有處理到」）。
# 真實踩過的案例：Shioaji `place_order` 丟出的例外訊息形如
# `"place_order: ... code: 406, detail: Please sign ... first."`。
_BROKER_REJECT_CODE_RE = re.compile(r"code[:=]\s*(4\d{2})\b", re.IGNORECASE)


def _classify_place_failure(exc: Exception) -> str:
    """分類送單/改單失敗的例外：回傳 `"failed"` 或 `"unknown"`。

    - `"failed"`：**只在能確定券商已收到這筆委託並明確拒絕**時才回傳——代表這筆委託
      **確定沒送出**，呼叫端可以安全地立即標記委託 failed 並釋放保留的配額（不必等
      Task 8 watchdog reconcile）。
    - `"unknown"`：其餘所有情況（逾時、連線中斷、無法辨識的例外形狀）——結果不明，
      委託可能其實已經送達券商，必須維持既有 fail-safe（標 unknown、保留配額不動，
      留給 watchdog reconcile 決議）。

    **保守是鐵律**：危險方向是把「模稜兩可」的失敗誤判成 `"failed"`——那會讓其實已經
    送達的委託被誤退還配額，變相突破日限。因此本函式的判斷刻意寧可漏判成 `"unknown"`，
    也不可誤判成 `"failed"`：只有在例外訊息中辨認出「HTTP 式 4xx 回應碼」（`code: 4xx`）
    這種代表券商端已明確處理並拒絕的訊號時，才會判定 `"failed"`；辨認不出來、或碼落在
    4xx 以外（例如 5xx 伺服器錯誤、逾時)，一律回傳 `"unknown"`。

    **待實機驗證**：這裡依賴的「Shioaji 例外訊息含 `code: 4xx`」字串形狀，是從實際踩到的
    `place_order` 例外訊息歸納出來的，不是官方文件保證的介面——與模組頂部說明的其餘欄位
    名稱（`order.id`/`order.seqno`/`status.id` 等）同屬「待實機驗證」等級：若正式 SDK
    版本的例外訊息格式不同，只需局部調整這裡的判斷邏輯，不影響呼叫端（place/update）的
    分支結構。

    **本機 broker agent 通道例外**：`AgentUnavailableError` 代表指令送出前 agent 即不在
    線——保證這筆委託沒有離開本機、沒送到券商，因此是唯一另一個能安全判定 `"failed"`
    的訊號（同 `code: 4xx`，可放心退配額）。`AgentCommandTimeoutError`（指令可能已送達
    agent/券商但未收到 ack）不特別分支處理——沿用上述「辨認不出來一律 unknown」的預設
    路徑，保守保留配額。
    """
    if isinstance(exc, AgentUnavailableError):
        return "failed"
    # 結構性早退（非靠訊息不含 code: 4xx 的隱含保證）：AgentChannel.request 逾時/送出失敗
    # 時會把底層例外訊息包進 AgentCommandTimeoutError（例如 f"下行送出失敗: {exc}"），若
    # 底層訊息恰好含 `code: 4xx` 字樣，靠字串比對會被誤判成 "failed"。型別優先於字串內容，
    # 保證 AgentCommandTimeoutError 一律 unknown（保留配額，留給 watchdog reconcile 決議）。
    if isinstance(exc, AgentCommandTimeoutError):
        return "unknown"
    text = str(exc)
    # SessionNotEstablished（券商 Solace/SolClient session 未建立就送單，2026-08 實測）：
    # place_order 的請求因 session 建立不起來而**根本沒離開 SDK、沒送到券商**，與
    # `AgentUnavailableError`/`code: 4xx` 同屬「確定沒送達券商」的安全 failed 訊號，可立即
    # 標 failed 退配額（不必等 watchdog reconcile）。實測字串：`code: NotReady, ...
    # sub_code: SubCode(SessionNotEstablished), error_str: "Unable to wait for session
    # '(c0,s1)_sinopac' to be established"`。比對 sub_code 名（穩定、非秘密、不隨帳號變）。
    if "SessionNotEstablished" in text:
        return "failed"
    match = _BROKER_REJECT_CODE_RE.search(text)
    if match is None:
        return "unknown"
    code = int(match.group(1))
    return "failed" if 400 <= code <= 499 else "unknown"


# Task 2 委派重構：native.py 的 `update()` 找不到對應 Trade 時拋 base.py 的公開
# `TradeNotFoundError`——這裡保留舊的模組私有名稱 `_TradeNotFoundError` 當別名，本檔下面
# `update()` 的 `except _TradeNotFoundError:` 分支與既有測試（若有 import 這個私有名）都
# 不必改。語意同舊 docstring：cancel/update 前刷新 + `list_trades()` 比對不到對應 `Trade`
# （bug 2/3 收尾）——這種情況根本**沒有**送出任何 native cancel_order/update_order 呼叫，
# 不屬於 `_classify_place_failure` 設計要處理的「結果不明」unknown fail-safe 範疇。
_TradeNotFoundError = TradeNotFoundError


class _RiskGuardLike(Protocol):
    """Task 7 RiskGuard 的結構型別（避免對 Task 7 模組的 import-time 相依）。

    D3（Inc1 兩層 kill switch）：`kill_switch: bool` 單一旗標改成 `blocked(user_id)`——
    `_send_gate` 與 `check_place`/`check_update` 都改查這個，判定 = 全站總閘 OR 該
    user 自己的個人急停。"""

    def assert_owner(self, actor_user_id: int) -> None: ...
    def blocked(self, user_id: int) -> bool: ...
    def check_place(self, session: Session, req: OrderRequest, **kw) -> Order: ...
    def check_update(self, session: Session, order: Order, **kw) -> None: ...


class _NativeGatewayLike(Protocol):
    """Task 5 `AgentNativeGateway` 的結構型別（避免對 agent_channel 模組的 import-time
    相依，同 `_RiskGuardLike` 鬆耦合寫法）。三段切：adapter 只認得這個介面，不知道背後是
    in-process native 還是跨網路 WS 通道；`ready=False` 時代表 agent 未連線，呼叫端一律
    fail-fast，不得誤送。"""

    @property
    def ready(self) -> bool: ...
    @property
    def admission_ready(self) -> bool: ...
    async def place(self, req: OrderRequest, *, cmd_id: str, expires_at: str | None = None) -> dict: ...
    async def cancel(self, ordno: str, *, cmd_id: str, expires_at: str | None = None) -> None: ...
    async def update(
        self, ordno: str, *, price, qty: int, price_type: str | None = None, cmd_id: str,
        expires_at: str | None = None,
    ) -> None: ...
    async def trades_snapshot(self, after): ...
    async def query_qty(self, ordno: str) -> "int | None": ...


class ShioajiAdapter:
    def __init__(
        self,
        *,
        api_key: str,
        secret_key: str,
        ca_path: str | None,
        ca_passwd: str | None,
        person_id: str | None,
        symbol: str,
        mode: Mode,
        session_factory: Callable[[], Session],
        supervisor: BrokerSupervisor,
        risk_guard: "_RiskGuardLike | None" = None,
        broker: str = "shioaji",
        sim_fee_per_lot: Decimal | None = None,
        ops_alerter=None,
        remote_gateway: "_NativeGatewayLike | None" = None,
        agent_user_id: int | None = None,
        agent_command_expiry_seconds: int = agent_commands.DEFAULT_COMMAND_EXPIRY_SECONDS,
    ) -> None:
        # Inc1 D5：這個 adapter instance「屬於誰」——in-process 與目前仍是單一共享 channel 的
        # agent 模式一律不傳（None），callback/reconcile 落地的 RawInbox 蓋章 user_id=None，
        # 行為與現行完全一致。Task 7 建 per-user `UserAgentSlot` 後，每個 slot 建構自己的
        # adapter 時會傳 `agent_user_id=slot.user_id`，這個屬性才會真的生效——本 task 只負責
        # 把這個 scope 一路傳進 commit_raw_callback/stage_scoped_raw_inbox，不改變任何現行呼叫端
        # 的實際行為。
        self._agent_user_id = agent_user_id
        self._api_key = api_key
        self._secret_key = secret_key
        self._ca_path = ca_path
        self._ca_passwd = ca_passwd
        self._person_id = person_id
        # F8（round3 中央化 redaction）：這個 adapter instance 認得的所有秘密值，供
        # place/update/health_probe 的例外訊息、以及 watchdog/lifespan/HTTP 路由（經
        # `secrets_to_redact` 這個 public property 讀取）呼叫 broker.redaction.redact_secrets
        # 時使用，確保任何可能夾帶這些值的上游例外文字落地/回顯前一律先過濾。
        self._secrets_to_redact = [s for s in (api_key, secret_key, ca_passwd, person_id) if s]
        self.symbol = symbol
        self.mode: Mode = mode
        self.broker = broker
        self._session_factory = session_factory
        self._supervisor = supervisor
        self._risk_guard = risk_guard
        self._sim_fee_per_lot = sim_fee_per_lot  # A6：sim 成交 fee 缺值時依口數估算，不留 None/0
        self._ops = ops_alerter  # T0.3：營運告警（fire-and-forget、絕不 raise），純疊加
        # Task 6：三段切——None 時維持 in-process 行為零變化（原路徑）；非 None 時所有
        # native 呼叫（place/cancel/update/reconcile 的 trades_snapshot）改經這個 gateway
        # 下行，DB 決策/寫回/風控/冪等仍留在 adapter（server 端）不變。
        self._remote_gateway = remote_gateway
        # Task 10：config.py `agent_command_expiry_seconds`（預設 120，唯一接線到 Settings
        # 的權威來源，見該設定欄位註解）——`new_command(...)` 建 ledger 列時的
        # `expires_at = created_at + 這個值`，取代 Task 8 的模組層常數字面值。
        self._agent_command_expiry_seconds = agent_command_expiry_seconds
        self._fill_handler: Callable[[Fill], None] | None = None
        # 事件喚醒（RawInboxWorker 從 idle_interval 純逾時輪詢改事件喚醒）：這個 adapter
        # instance 構造當下還不知道之後會被哪個 RawInboxWorker 認領（app.py 兩個接線點都是
        # adapter 先建、worker 後建，生命週期順序問題），故留一個可事後設定的 public 掛勾——
        # 預設 None＝no-op，行為與現行完全一致；接線後（`adapter.raw_committed_hook =
        # inbox_worker.request_wake`）`_persist_raw` commit 成功後才會呼叫。
        self.raw_committed_hook: Callable[[], None] | None = None
        # Task 2 委派重構：所有直接碰 Shioaji SDK 的呼叫交給 native（`_api`/`_contract`/
        # `account` 三個 property 墊片委派讀寫 native 對應屬性，見下方）；`on_raw` 落地責任
        # 交回這個 adapter 的 `_persist_raw`（等價原本 `_on_order_cb` 直呼 commit_raw_callback
        # 落地 RawInbox 那半段）。
        self._native = ShioajiNativeClient(
            api_key=api_key, secret_key=secret_key, ca_path=ca_path, ca_passwd=ca_passwd,
            person_id=person_id, symbol=symbol, mode=mode, on_raw=self._persist_raw,
        )

    # ---- native 委派 property 墊片（相容既有測試 `a._api = _FakeApi()` 注入寫法） ----

    @property
    def _api(self):
        return self._native.api

    @_api.setter
    def _api(self, value) -> None:
        self._native.api = value

    @property
    def _contract(self):
        return self._native.contract

    @_contract.setter
    def _contract(self, value) -> None:
        self._native.contract = value

    @property
    def account(self) -> str:
        return self._native.account

    @account.setter
    def account(self, value: str) -> None:
        self._native.account = value

    def _persist_raw(self, kind: str, payload: dict) -> None:
        """`self._native` 的 `on_raw` callback：落地責任留在 adapter（DB/session 相依），
        native 端零 DB 相依（見 native.py 模組頂部說明）。

        Inc1 D5：在落地當下蓋章 `account=self.account, mode=self.mode`（事件產生瞬間的
        immutable snapshot，S3 拆除的另一半——mapper 之後讀的是這個蓋章值，不是處理當下可能
        已經被換帳號覆寫過的 `self.account`）；`user_id=self._agent_user_id`——in-process 與
        目前仍未拆分的 agent 單例模式一律是 None，行為與現行完全一致。

        事件喚醒：`on_committed=self.raw_committed_hook` 一路傳進 `commit_raw_callback`——
        commit 成功後才可能觸發，未接線時是 None（no-op），行為與現行完全一致（見
        `raw_committed_hook` 欄位註解／`commit_raw_callback` docstring）。"""
        commit_raw_callback(
            self._session_factory, kind=kind, broker=self.broker, payload=payload,
            user_id=self._agent_user_id, account=self.account, mode=self.mode,
            ops_alerter=self._ops, on_committed=self.raw_committed_hook,
        )

    # ---- T0.3 營運告警（純疊加，絕不反噬既有 fail-closed/冪等/redaction 行為） ----

    def _redact(self, text: str) -> str:
        """比照本檔既有 `_redact_secrets(str(exc), secrets=self._secrets_to_redact)` 用法，
        統一遮蔽這個 adapter instance 認得的所有秘密值（api_key/secret_key/ca_passwd/
        person_id）——任何可能夾帶秘密的上游例外文字進告警/日誌前必經此步。"""
        return _redact_secrets(text, secrets=self._secrets_to_redact)

    def _alert_place_failed(self, *, client_order_id, action, qty, classification, exc) -> None:
        """把 place/update 失敗送進 OpsAlerter（`if self._ops is not None:` 保護、detail 一律
        redact）。取值/redact 皆包 try/except 吞掉——告警絕不能反噬下單主流程（此刻多半正要
        raise OrderError，若這裡拋例外會遮蓋掉真正的失敗）。"""
        if self._ops is None:
            return
        try:
            self._ops.place_failed(
                client_order_id=client_order_id, symbol=self.symbol,
                action=action, qty=qty, classification=classification,
                detail=self._redact(str(exc)),
            )
        except Exception:
            log.exception("place_failed 告警失敗（已吞，不影響下單流程）")

    # ---- 連線生命週期（round3 #11：connect/close 都經 supervisor.run，同一通道） ----

    async def connect(self) -> None:
        await self._supervisor.run(lambda: asyncio.to_thread(self._connect_blocking))

    def _connect_blocking(self) -> None:
        # native.connect() 內部登入/CA 啟用/註冊 callback（真正註冊的是 native 自己的
        # `_on_order_cb`，會呼叫 `self._native._on_raw`＝adapter 的 `_persist_raw`——與原本
        # 這裡直接 `api.set_order_callback(self._on_order_cb)` 落地到同一個 commit_raw_callback
        # 等價）全部搬到 native.py（Task 1）；adapter 端不再直接碰 Shioaji SDK。
        self._native.connect()

    async def close(self) -> None:
        async def _do_close() -> None:
            await asyncio.to_thread(self._native.close)

        await self._supervisor.run(_do_close)

    def on_fill(self, handler: Callable[[Fill], None]) -> None:
        self._fill_handler = handler  # 目前無呼叫端；保留供未來 push 通知擴充

    @property
    def supervisor(self) -> BrokerSupervisor:
        """Task 8 watchdog 需要直接拿鎖做 DB-only 背景工作（unquarantine/unknown reconcile），
        不經過 `.run()`（那是給 native shioaji API 呼叫用的通道，round3 #11）。"""
        return self._supervisor

    @property
    def secrets_to_redact(self) -> list[str]:
        """F8：watchdog/lifespan/HTTP 路由沒有直接持有 api_key/secret_key/ca_passwd/
        person_id，但持有這個 adapter instance——經這個 property 取得同一份秘密清單，統一
        呼叫 `broker.redaction.redact_secrets`，不需要各自另外接收/保存一份秘密。回傳
        copy（不是內部 list 的參照），避免呼叫端意外修改到 adapter 內部狀態。"""
        return list(self._secrets_to_redact)

    # ---- health probe（round3 #10：序列化 broker health probe，不只看 `_api is not None`） ----

    async def health_probe(self) -> bool:
        """探測底層連線是否還活著。`self._api` 是 python object，就算底層 TCP/會話已經斷線，
        object 本身通常還在——只檢查 `_api is not None` 會讓 watchdog 永遠以為連線健康、
        永不重連（round3 #10 抓到的漏洞）。這裡經 supervisor.run() 同一通道做一次輕量、
        無副作用的 native 呼叫，失敗（含任何例外）一律視為不健康。"""

        async def _do_probe() -> bool:
            if self._api is None:
                return False
            try:
                await asyncio.to_thread(self._probe_blocking)
                return True
            except Exception as exc:
                log.warning(
                    "health_probe 失敗（視為不健康，觸發重連）: %s",
                    _redact_secrets(str(exc), secrets=self._secrets_to_redact),
                )
                return False

        return await self._supervisor.run(_do_probe)

    def _probe_blocking(self) -> None:
        """實際探測邏輯已搬到 native.py `ShioajiNativeClient.probe()`（同一套 getattr 防禦性
        存取慣例：優先 `list_accounts()`，找不到 fallback 讀 `futopt_account`）；這裡純委派。"""
        self._native.probe()

    # ---- reconcile（round3 #2：重連後對帳，持久 cursor + 分辨委託/成交，不只 retry 本地 quarantine） ----

    async def reconcile(self) -> None:
        """重連後對帳：拉券商目前委託回報補回 RawInbox（kind="order_report"，不是 round3 覆核前
        草稿版本把所有列都當 deal_report 那個 bug）。用持久 `BrokerReconcileCursor` watermark
        只補「上次對帳後有新進展」的委託——重啟後從 DB 讀回 cursor 續接，不會每次都重新灌一次
        全量 snapshot，也不會因為重啟就遺失對帳進度。

        已知限制（誠實記錄，非本檔可單方面解決）：Shioaji 的 `list_trades()` 只回傳委託層級的
        彙總狀態（含 `deal_quantity` 累計數），不含逐筆真實 deal_id；V3-4 明文禁止用
        ordno/seqno 等 fallback 冒充 fill_id 建構 Fill（見 broker/types.py），因此本函式刻意
        不嘗試從這裡重建成交明細——成交回報一律只信任 durable callback
        （`_on_order_cb`→`commit_raw_callback`，已保證同步落地不遺失，見模組頂部說明）。若
        真的發生「斷線期間券商 callback 完全沒送達」的成交缺口，需要券商提供逐筆歷史回放
        API 才能完整補齊，超出目前高階 SDK 介面下可靠實作的範圍。
        """
        # supervisor.run 回傳被包協程/callable 的結果（見 broker/supervisor.py），故
        # `_reconcile_inner` 回傳的「本次補回筆數」能直接在這裡取得。>0 才發漂移告警：
        # sim 下 list_trades() 空 → count 0 → 不發（見對應測試）。count>0 的 ops 告警邏輯
        # 在 remote/in-process 兩條路徑下完全不動（Task 6：只有 `_reconcile_inner` 內部
        # 依 `self._remote_gateway` 分流讀取來源，這裡不知道、也不必知道走哪條）。
        count = await self._supervisor.run(lambda: self._reconcile_inner())
        if count and self._ops is not None:
            self._ops.reconcile_drift(count=count, context=f"mode={self.mode} account={self.account}")

    async def _reconcile_inner(self) -> int:
        """Task 6：remote 模式下改讀 gateway 而非本機 native——鎖 semantics 不變（仍全程在
        `supervisor.run` 包住的 lambda 內執行，見上方 `reconcile()`）。gateway 未連線時視同
        本次無新進展（回 0，不 raise）：對帳是背景維護動作，watchdog 週期性重跑即可，不需要
        像 place 那樣 fail-fast 拒絕呼叫端。"""
        if self._remote_gateway is None:
            return await asyncio.to_thread(self._reconcile_blocking)
        if not self._remote_gateway.ready:
            return 0
        after = await asyncio.to_thread(self._read_reconcile_cursor)
        payloads, newest = await self._remote_gateway.trades_snapshot(after)
        return await asyncio.to_thread(self._stage_reconcile_results, payloads, newest)

    def _read_reconcile_cursor(self) -> "datetime | None":
        """Task 6/8 依賴：讀取持久 `BrokerReconcileCursor` watermark（沿用原
        `_reconcile_blocking` 的呼叫寫法，單獨拆出供 watchdog/agent 端重用）。"""
        with self._session_factory() as session:
            return brepo.get_reconcile_cursor(
                session, broker=self.broker, account=self.account, mode=self.mode
            )

    def _stage_reconcile_results(self, payloads: list[dict], newest: "datetime | None") -> int:
        """Task 6/8 依賴：把 native 端 `trades_snapshot()` 篩出的委託進展 payload 落地
        RawInbox + 推進 cursor（沿用原 `_reconcile_blocking` L326-333 的呼叫寫法與
        `newest is None` 時的 `_utcnow_naive()` fallback——`payloads` 非空時一律推進 cursor，
        不留下「有新委託進展卻沒有記錄任何 cursor」的半殘狀態，行為與重構前完全一致）。

        Inc1 D5（R1-6）：改走 `stage_scoped_raw_inbox`（唯一 scoped staging 邏輯，不是直接呼叫
        `repository.stage_raw_inbox`），每筆 payload 都蓋章 `user_id=self._agent_user_id`（目前
        單例模式下恆為 None，Task 7 per-slot adapter 才會帶真正的 user_id）、
        `account=self.account`、`mode=self.mode`；整批落列與 cursor 推進**必須同一交易**——
        中途任何一筆失敗（例如 DB 短暫故障）都要讓已落地的列與 cursor 推進一起回滾，不留下
        「列已落地但 cursor 沒推進」或反過來的半殘狀態（同一個 `with ... as session:` 區塊、
        直到最後才 `session.commit()` 一次，中途 raise 就整段不 commit，見呼叫端測試 S#18）。"""
        if not payloads:
            return 0
        with self._session_factory() as session:
            for p in payloads:
                stage_scoped_raw_inbox(
                    session, kind="order_report", broker=self.broker, payload=json.dumps(p),
                    user_id=self._agent_user_id, account=self.account, mode=self.mode,
                    ops_alerter=self._ops,
                )
            brepo.upsert_reconcile_cursor(
                session, broker=self.broker, account=self.account, mode=self.mode,
                at=newest if newest is not None else _utcnow_naive(),
            )
            session.commit()
        return len(payloads)

    def _reconcile_blocking(self) -> int:
        """回傳本次實際補回 RawInbox 的委託進展筆數。讀 SDK + watermark 過濾已搬到 native.py
        `trades_snapshot()`（Task 1）；這裡只負責讀 cursor → 委派 native 讀取 → 落地/推進
        cursor，三段拆開供 Task 6/8 重用（`_read_reconcile_cursor`/`_stage_reconcile_results`）。"""
        if self._api is None:
            return 0
        after = self._read_reconcile_cursor()
        payloads, newest = self._native.trades_snapshot(after)
        return self._stage_reconcile_results(payloads, newest)

    def _query_order_qty_blocking(self, ordno: str) -> int | None:
        """Task 8 watchdog「quota unknown reconcile」收尾用（round3 殘留1）：查詢券商目前對
        這筆委託回報的口數（quantity），供 watchdog 判斷 update 逾時（unknown）後這次改單
        究竟是否真的生效——比對這個回傳值與「改單前口數」/「改單後目標口數」，才能決定
        對應的 delta QuotaReservation 該 confirm 還是 release（見 watchdog.py
        `_reconcile_unknown_quota_blocking`）。

        **呼叫端責任（避免鎖重入死結）**：本函式刻意設計成單純同步、自己不取
        `supervisor` 鎖——watchdog 對這批「DB-only 背景工作」是直接
        `async with adapter.supervisor.lock:` 整段包住（見本檔 `supervisor` property 說明、
        watchdog.py 模組頂部），本函式若再呼叫 `supervisor.run()` 會在同一顆
        `asyncio.Lock` 上重入，永久卡死；因此只能在「呼叫端已經持有鎖」的前提下直接呼叫。

        找不到這筆委託（已從 `list_trades()` 目前清單消失，例如已完全結案）回 None，
        呼叫端視為無法判斷、不猜測。實際查詢邏輯已搬到 native.py
        `ShioajiNativeClient.query_order_qty()`（同檔一貫 getattr 防禦性慣例）；本函式純委派，
        呼叫端持鎖責任不變（不得再經 `supervisor.run()`，見上方 docstring）。"""
        return self._native.query_order_qty(ordno)

    # ---- send gate（V3-2，鎖內、native 呼叫前的最後線性化點） ----

    async def _send_gate(self, user_id: int) -> None:
        """D3：`user_id` 是這筆指令的 actor（呼叫端一律傳 place/update 當下的
        `actor_user_id`，adapter 綁定的 owner 身分不再是唯一判準）——kill switch 判定
        改查 `blocked(user_id)` = 全站總閘 OR 該 user 自己的個人急停。"""
        if self._remote_gateway is not None:
            # Task 6：remote 模式下「session 是否就緒」的問法變成「gateway 是否連線」——
            # 未連線視同 AgentUnavailableError（保證這筆委託沒有離開本機/送達券商，
            # `_classify_place_failure` 會判 failed 並安全退配額），語意對齊 native 模式
            # `_api is None` 那支「下單 session 尚未就緒」的 fail-fast，但用 agent 自己的
            # 例外型別，讓呼叫端的失敗分類邏輯自然落到既有 failed 分支，不必額外特判。
            # C2（HIGH，codex 終審）：這裡是 place/update 唯一會建新 DB 決策列的送單前
            # 最後守門（_send_gate 只給 place/update 呼叫，cancel 不經這裡）——改查
            # `admission_ready`（pending_health/failstop/lease 過期三態下皆 False），不再
            # 用寬鬆的 `ready`（那個定義留給 reconcile/query_qty 等唯讀背景動作）。
            if not self._remote_gateway.admission_ready:
                raise AgentUnavailableError("agent 未連線或未登入")
        elif self._api is None:
            raise OrderError("下單 session 尚未就緒")
        if self._risk_guard is not None and self._risk_guard.blocked(user_id):
            raise RiskError("kill switch 已啟動，拒絕送出")

    # ---- place ----

    async def place(
        self, req: OrderRequest, *, actor_user_id: int, confirm_token: str | None = None
    ) -> OrderAck:
        request_hash = canonical_payload_hash(
            symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
            price_type=req.price_type, order_type=req.order_type, octype=req.octype,
            account=self.account, mode=self.mode,
        )
        with self._session_factory() as session:
            existing = brepo.find_order_by_client_order_id(session, req.client_order_id)
            if existing is not None:
                if existing.request_hash != request_hash:
                    raise OrderError(f"client_order_id={req.client_order_id!r} 已存在但 payload 不同")
                if existing.user_id != actor_user_id:
                    # round3 開放清單 #6：既有列命中只驗 request_hash 不夠——非 owner 猜到
                    # 別人的 client_order_id+完全相同 payload 不得在授權前拿到他人 OrderAck。
                    raise AuthorizationError("client_order_id 已被其他使用者的委託佔用")
                return self._ack_from_order(existing)  # 冪等：不重跑風控、不燒 token、不再送單

            # Fix Round 1（審查 Important #1）：offline fail-fast 搬到冪等查找 miss 之後——
            # 原本擺在函式最開頭、request_hash 計算與冪等查找之前，會讓「client 因逾時用同一
            # client_order_id 重送、agent 恰好離線」這種情境提早短路，永遠吃不到上面的冪等
            # shortcut（in-process 模式反而能命中回快取 ack，remote 模式卻直接拒絕，冪等
            # 保證在跨網路後被削弱）。搬到這裡之後：existing 命中（無論 gateway 狀態）一律
            # 走上面冪等分支；只有「查無既有列、確定要建新委託」時才檢查 gateway 是否就緒，
            # 且仍在任何 `session.commit()` 之前，維持「連 Order/配額列都不建」的原始保證
            # （行為矩陣「gateway.ready=False（place 進入時）」一列不變）。C2（HIGH，codex
            # 終審）：改查 `admission_ready`——pending_health/failstop/lease 過期三態下也
            # 要在這裡 fail-fast，不得漏到下面才被 `_send_gate` 擋（那時 Order/reservation/
            # ledger 已經建好，`_send_gate` 擋下後還要走例外分支釋放，不如在這裡就不建）。
            if self._remote_gateway is not None and not self._remote_gateway.admission_ready:
                raise OrderError("agent 未連線，無法下單")

            if self._risk_guard is not None:
                order = self._risk_guard.check_place(
                    session, req, actor_user_id=actor_user_id, mode=self.mode, broker=self.broker,
                    account=self.account, request_hash=request_hash, confirm_token=confirm_token,
                )
                # Task 8 已知落差（見 agent_commands.py 模組頂部說明）：check_place 內部已
                # 自行 commit，故下面的 ledger insert 不是「同一次 SQL commit」，而是緊接著、
                # 中間無任何 await/IO 的獨立小交易——與 spec D4「送單前持久化與決策段同一
                # 交易」的字面要求有落差，殘留窗口只在純同步 Python 賦值間（行程崩潰）。
            else:
                trading_day = brepo.trading_day_for(int(_now_ms()))
                order = brepo.create_order(
                    session, client_order_id=req.client_order_id, request_hash=request_hash,
                    user_id=actor_user_id, mode=self.mode, broker=self.broker, account=self.account,
                    symbol=req.symbol, action=req.action, qty=req.qty, price=req.price,
                    price_type=req.price_type, order_type=req.order_type, octype=req.octype,
                    trading_day=trading_day,
                )
                # 尚未 commit：若 remote_gateway 存在，ledger insert 併入下面同一次 commit
                # （這個分支因此是真正的「同一交易」，見上方 risk_guard 分支的落差說明）。

            order_id, client_order_id = order.id, order.client_order_id

            # Inc1 D4（G1）：agent 模式送前持久化——建立這筆 place 指令的 ledger 列，
            # `reservation_id` 用 client_order_id（與 RiskGuard.check_place 建立保留列的
            # 同一把鍵），scope（broker/account/mode）取自這個 slot 綁定的帳號並凍結。
            # ledger `user_id` 優先取這個 adapter 綁定的 slot owner（`self._agent_user_id`，
            # D1 per-user slot 的真正權威來源——UpCmdAck 的 CAS 帶的正是連線認證身分）；
            # 未設定時（測試用單一 adapter 服務多個 actor 的舊寫法、或尚未接 registry 的
            # 呼叫端）退回 `actor_user_id`，兩者在真正的 per-slot 部署下恆相等。
            cmd_id: str | None = None
            if self._remote_gateway is not None:
                ledger_user_id = (
                    self._agent_user_id if self._agent_user_id is not None else actor_user_id
                )
                cmd = agent_commands.new_command(
                    kind="place", user_id=ledger_user_id, broker=self.broker,
                    account=self.account, mode=self.mode, client_order_id=client_order_id,
                    reservation_id=client_order_id,
                    payload={"action": req.action, "price": str(req.price), "qty": req.qty,
                             "price_type": req.price_type, "order_type": req.order_type,
                             "octype": req.octype},
                    expiry_seconds=self._agent_command_expiry_seconds,
                )
                agent_commands.insert_command(session, cmd=cmd)
                # C9：在這個 session 仍存活的當下就把 `expires_at` 讀成純字串——`cmd` 是
                # ORM 物件，`session.commit()`（下一行）預設 `expire_on_commit=True` 會讓
                # 它的屬性全部過期，`_do_place()` 是之後才在 `_supervisor.run()` 裡執行
                # （這個 `with session:` 區塊早已結束/session 已關閉），屆時再讀
                # `cmd.expires_at` 會觸發對已關閉 session 的 lazy refresh 而炸
                # `DetachedInstanceError`。純字串沒有這個問題，可以安全跨到閉包裡用。
                cmd_id = cmd.cmd_id
                cmd_expires_at = cmd.expires_at.isoformat()
            session.commit()
            # callback-before-ack：Order 此刻已 commit（client_order_id→user_id/mode 就位，
            # ordno/broker_order_id 仍是 NULL 佔位），即使成交回報早於下面的 native 呼叫完成，
            # RawInboxWorker 之後仍能靠 ordno/broker_order_id 補齊後解析到這筆委託。

        async def _do_place():
            try:
                await self._send_gate(actor_user_id)
                if self._remote_gateway is not None:
                    # C9：`expires_at` 傳這筆指令在 ledger 建立當下凍結的值（上面已讀成
                    # 純字串 `cmd_expires_at`），不讓 gateway 自己另外重算。
                    return await self._remote_gateway.place(
                        req, cmd_id=cmd_id, expires_at=cmd_expires_at
                    )
                return await asyncio.to_thread(self._place_blocking, req)
            except RiskError:
                # send gate 擋下（如 kill switch）：確定沒送出，直接標 failed，不留在
                # pending 卡死（V3-2 修 BLOCKER#13 TOCTOU 的收尾）。round3 #4 收尾：確定
                # 沒送出 → 退還這筆保留的配額（reservation_id 就是 client_order_id，與
                # RiskGuard.check_place 建立保留列時用的同一把鍵——見 repository.reserve_quota
                # 的呼叫端冪等鍵慣例）；沒有 risk_guard 時本來就沒有保留列可退。
                with self._session_factory() as fail_session:
                    fail_order = fail_session.get(Order, order_id)
                    reservation = req.client_order_id if self._risk_guard is not None else None
                    agent_commands.apply_place_failure(
                        fail_session, fail_order, reservation_id=reservation, status="failed"
                    )
                    if self._remote_gateway is not None and cmd_id is not None:
                        # Task 8：這筆指令從未送達 agent（第二層 kill switch 檢查落空）——
                        # 不會有任何 ack 到來，route 本地終結 ledger（見 resolve_never_dispatched
                        # docstring）。
                        agent_commands.resolve_never_dispatched(
                            fail_session, cmd_id=cmd_id, message="kill switch 已啟動（第二層檢查）"
                        )
                    fail_session.commit()
                raise
            except AgentUnavailableError as exc:
                # Task 8：同 RiskError 分支——`_send_gate`/`AgentChannel.request` 自己的
                # ready 檢查落空，這筆指令從未送達 agent，往後也不會有任何 ack，route 必須
                # 本地終結（分類固定為 "failed"，同既有 `_classify_place_failure` 語意）。
                with self._session_factory() as fail_session:
                    fail_order = fail_session.get(Order, order_id)
                    reservation = req.client_order_id if self._risk_guard is not None else None
                    agent_commands.apply_place_failure(
                        fail_session, fail_order, reservation_id=reservation, status="failed"
                    )
                    if self._remote_gateway is not None and cmd_id is not None:
                        agent_commands.resolve_never_dispatched(
                            fail_session, cmd_id=cmd_id, message=str(exc)
                        )
                    fail_session.commit()
                self._alert_place_failed(
                    client_order_id=client_order_id, action=req.action, qty=req.qty,
                    classification="failed", exc=exc,
                )
                raise
            except AgentCommandTimeoutError as exc:
                # Task 8（D4 route 逾時路徑，修正 round 1）：這個例外型別實際對映
                # `agent_channel.py` 至少 4 個觸發點，不是原先誤寫的「兩種情境」——
                # ①`AgentChannel.request` 的 `await self._send(cmd)` 送出失敗（socket 已壞，
                # 可能已部分送出）；②`asyncio.wait_for(fut, timeout)` 真的等不到 ack；
                # ③`AgentChannel.detach()` 連線中斷時對所有 pending future `set_exception`
                # （斷線，不是逾時但同型別）；④ agent 已經回了一則 `error_kind="timeout"` 的
                # ack（agent 自己的子程序無回應，`_unwrap` 轉出）。`_unwrap`/呼叫端對這 4 種
                # 情境目前一律 raise 出同一個型別，無法從型別本身分辨。`mark_timeout_observed`
                # 的 CAS 才是真正分辨依據：True＝①/②/③（這個 cmd 從未收到任何 ack，可放心走
                # 既有 unknown fail-safe）；False＝④（ack 已經透過 `apply_command_ack` 在別的
                # 交易完整落地過），這裡必須完全不寫 Order/quota，讓已落庫的結果（不論
                # ok/error/unknown）保持原樣（S#15/R1-1：Order 不得被逾時路徑改回 unknown）。
                won = True
                if self._remote_gateway is not None and cmd_id is not None:
                    with self._session_factory() as fail_session:
                        won = agent_commands.mark_timeout_observed(fail_session, cmd_id=cmd_id)
                        if won:
                            fail_order = fail_session.get(Order, order_id)
                            agent_commands.apply_place_failure(
                                fail_session, fail_order, reservation_id=None, status="unknown"
                            )
                        fail_session.commit()
                else:
                    with self._session_factory() as fail_session:
                        fail_order = fail_session.get(Order, order_id)
                        agent_commands.apply_place_failure(
                            fail_session, fail_order, reservation_id=None, status="unknown"
                        )
                        fail_session.commit()
                self._alert_place_failed(
                    client_order_id=client_order_id, action=req.action, qty=req.qty,
                    classification="unknown", exc=exc,
                )
                raise
            except Exception as exc:
                if self._remote_gateway is not None and isinstance(exc, OrderError):
                    # Task 8：其餘 ack 衍生的例外（`TradeNotFoundError`/一般 `OrderError`，
                    # 對映 agent 明確拒絕的 error_kind：trade_not_found/exception/
                    # mode_mismatch/expired/scope_mismatch）——`apply_command_ack` 已經在
                    # `channel.request()` 的 future resolve **之前**完整 commit 過 Order/quota
                    # 效果（agent_ws.py 的 UpCmdAck handler 保證順序），route 不得重新分類、
                    # 不得重寫，原樣 re-raise 讓呼叫端得到「已失敗」的訊號即可。
                    raise
                # 本次精進：先分類這個例外是否為「券商明確拒絕」（確定沒送出）——
                # 是的話比照 RiskError 分支立即標 failed + 釋放配額，不必等 watchdog
                # reconcile；分類不出來（逾時/連線中斷/無法辨識）一律維持原本 unknown
                # fail-safe（round3 #4：不得擅自 release，否則若其實已送達會讓配額被
                # 誤退還、變相突破日限）。見 `_classify_place_failure` docstring。此分支
                # in-process 模式仍會走到（remote 模式的已知例外型別皆已被上面攔截）。
                classification = _classify_place_failure(exc)
                with self._session_factory() as fail_session:
                    fail_order = fail_session.get(Order, order_id)
                    status = "failed" if classification == "failed" else "unknown"
                    reservation = (
                        req.client_order_id
                        if (self._risk_guard is not None and status == "failed") else None
                    )
                    agent_commands.apply_place_failure(
                        fail_session, fail_order, reservation_id=reservation, status=status
                    )
                    fail_session.commit()
                self._alert_place_failed(
                    client_order_id=client_order_id, action=req.action, qty=req.qty,
                    classification=classification, exc=exc,
                )
                if classification == "failed":
                    raise OrderError(
                        f"送單遭券商明確拒絕，委託標記 failed 並已釋放配額："
                        f"{_redact_secrets(str(exc), secrets=self._secrets_to_redact)}"
                    ) from exc
                raise OrderError(
                    f"送單失敗，委託標記 unknown 待 reconcile："
                    f"{_redact_secrets(str(exc), secrets=self._secrets_to_redact)}"
                ) from exc

        ack_fields = await self._supervisor.run(_do_place)

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            reservation = req.client_order_id if self._risk_guard is not None else None
            # Task 8：agent 模式下 `apply_command_ack` 多半已經在 `channel.request()` resolve
            # 之前寫過這筆效果——這裡的呼叫因此常是安全的重複覆寫（`set_order_ack` 寫入相同
            # 值、`confirm_quota` 的 CAS 對已 confirmed 的列是 no-op），保留呼叫是為了兩件事
            # 仍然成立：in-process 模式（唯一寫入者，行為零改動）與這裡本身（route 讀
            # `ack_fields` 建構回傳值，不需要另外查 DB）。
            agent_commands.apply_place_ack(
                session, order, ordno=ack_fields["ordno"],
                broker_order_id=ack_fields["broker_order_id"], reservation_id=reservation,
            )
            session.commit()
        return OrderAck(
            client_order_id=client_order_id, broker_order_id=ack_fields["broker_order_id"],
            ordno=ack_fields["ordno"], status="submitted",
        )

    def _place_blocking(self, req: OrderRequest) -> dict:
        # 防禦（bug 2，MKT 顯式送 0.0）與組 native Order/擷取 ack 欄位已搬到 native.py
        # `ShioajiNativeClient.place()`（Task 1）；本函式純委派，供 `_do_place` 呼叫。
        return self._native.place(
            action=req.action, price=req.price, qty=req.qty,
            price_type=req.price_type, order_type=req.order_type, octype=req.octype,
        )

    @staticmethod
    def _ack_from_order(order: Order) -> OrderAck:
        return OrderAck(
            client_order_id=order.client_order_id, broker_order_id=order.broker_order_id or "",
            ordno=order.ordno, status=order.status,
        )

    # ---- cancel ----

    async def cancel(self, broker_order_id: str, *, actor_user_id: int) -> OrderAck:
        with self._session_factory() as session:
            order = brepo.find_order_by_broker_id(
                session, broker=self.broker, account=self.account, mode=self.mode,
                broker_order_id=broker_order_id,
            )
            if order is None:
                raise OrderError(f"找不到委託 broker_order_id={broker_order_id!r}")
            if self._risk_guard is not None:
                self._risk_guard.assert_owner(actor_user_id)
            # round3 #6：owner allowlist 過了不代表這張委託是這個 owner 的——多 owner
            # 情境下若只驗 assert_owner 就直接放行，owner A 能取消 owner B 的委託。這裡
            # 一律再以 (user_id,broker,mode,broker_order_id) 驗真正委託所有權（先前版本
            # 這行只在沒有 risk_guard 時的 elif 分支才會跑，risk_guard 存在時被跳過）。
            if order.user_id != actor_user_id:
                raise AuthorizationError("非委託所有人不得取消")
            order_id, ordno, client_order_id = order.id, order.ordno, order.client_order_id

            # N3（MEDIUM，codex 終審 round2）：cancel 的 ledger insert 前加嚴格 readiness
            # fail-fast——舊版完全沒查 admission_ready 就建 ledger 列，`_do_cancel` 只查寬鬆
            # 的 `ready`（連線存活＋已登入，不含健康狀態），pending_health/failstop/lease
            # 過期三態下仍會建出一筆從一開始就注定被拒絕的 AgentCommand（且送到 gateway 的
            # wire call 才在 `_do_cancel` 被擋下，比 place/update 的「不建任何 DB 決策列」
            # 慢了一拍）。這裡改與 place()/update() 的 offline fail-fast 同一位置慣例——
            # `admission_ready` 為 False 就直接拒絕、連 ledger 都不建。**注意**：這裡擋的是
            # 連線健康狀態，不是 kill switch——cancel 不受 kill switch 影響的既有語意
            # （spec 明文）完全不動，`_send_gate`（kill switch 檢查的唯一位置）本就不會被
            # cancel 呼叫。
            if self._remote_gateway is not None and not self._remote_gateway.admission_ready:
                raise OrderError("agent 未連線，無法取消")

            # Inc1 D4（G1）：agent 模式送前持久化——cancel 無 quota 效果，`reservation_id`
            # 留 None（D4 轉移表：cancel 一律「無」quota 效果）。cancel 決策段本身不像
            # place/update 那樣有 risk_guard 內部 commit 的落差，這裡是真正「同一交易」。
            cmd_id: str | None = None
            if self._remote_gateway is not None:
                ledger_user_id = (
                    self._agent_user_id if self._agent_user_id is not None else actor_user_id
                )
                cmd = agent_commands.new_command(
                    kind="cancel", user_id=ledger_user_id, broker=self.broker,
                    account=self.account, mode=self.mode, client_order_id=client_order_id,
                    ordno=ordno, payload={"ordno": ordno},
                    expiry_seconds=self._agent_command_expiry_seconds,
                )
                agent_commands.insert_command(session, cmd=cmd)
                cmd_id = cmd.cmd_id
                # C9：見 place() 同名變數說明——趁 session 還活著讀成純字串，避免
                # `session.commit()`（下一行，`expire_on_commit=True`）之後、`_do_cancel()`
                # 才讀 `cmd.expires_at` 觸發對已關閉 session 的 lazy refresh 而炸。
                cmd_expires_at = cmd.expires_at.isoformat()
                session.commit()

        async def _do_cancel() -> None:
            # kill switch 不擋取消單（spec 明文：取消單仍允許），仍檢查 session/gateway 就緒
            # （Task 6：remote 模式下用 gateway.ready 取代 `_api is None`，同 `_send_gate`
            # 的 remote/in-process 分流方式，但取消單本身不經過 `_classify_place_failure`/
            # release_quota 那條路——這裡刻意用 OrderError 而非 AgentUnavailableError）。
            # N3（MEDIUM，codex 終審 round2）：改查嚴格的 `admission_ready`（同上面 ledger
            # insert 前那道 fail-fast 用同一個判準）——舊版查寬鬆的 `ready`（連線存活＋已
            # 登入，不含健康狀態），pending_health/failstop/lease 過期三態下 `ready` 仍可能
            # 是 True，讓這第二層縱深防禦形同虛設。與上面 ledger insert 前的檢查同屬一次
            # cancel 呼叫內的縱深防禦（第一層擋大多數情境、不建 ledger；這裡是鎖內、native
            # 呼叫前的最後防線，擋「ledger 建立後、native 呼叫前才轉為不健康」的窗口）——
            # 依然不是 kill switch，取消不受 kill switch 影響的語意不變。
            # Task 8：cancel 的 D4 轉移表除了 ok 以外一律「不改 Order」——不論是 agent
            # 明確拒絕（ack 衍生例外，已由 applier 處理）、逾時、還是這裡「從未送達 agent」
            # 的 offline 例外，都不需要 route 自己做任何本地 Order/ledger 寫回，例外原樣
            # 傳播即可（cancel 本就沒有 quota 效果，也不像 place/update 有「從未送達」窗口
            # 需要解 ledger——即使 ledger 永遠 unresolved，也只是不會被重連補送機制撿走
            # 重播，語意仍安全：cancel 意圖本就該在下次重連時重新嘗試，見 spec D4）。
            if self._remote_gateway is not None:
                if not self._remote_gateway.admission_ready:
                    raise OrderError("agent 未連線")
                # C9：`expires_at` 傳 ledger 建立當下凍結的值（見 place() 同名參數說明）。
                await self._remote_gateway.cancel(
                    ordno, cmd_id=cmd_id, expires_at=cmd_expires_at
                )
                return
            if self._api is None:
                raise OrderError("下單 session 尚未就緒")
            await asyncio.to_thread(self._cancel_blocking, ordno)

        await self._supervisor.run(_do_cancel)

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            agent_commands.apply_cancel_ack(session, order)
            session.commit()
        return OrderAck(client_order_id=client_order_id, broker_order_id=broker_order_id, ordno=ordno, status="cancelled")

    def _find_trade_by_ordno(self, ordno: str | None):
        """cancel/update 前先刷新 `update_status()` + `list_trades()` 比對回真正的 `Trade`
        物件（bug 2/3 根因收尾，邏輯已搬到 native.py `ShioajiNativeClient._find_trade_by_ordno`
        /`_refresh_and_list_trades`——Task 1）。這裡保留同名薄委派：既有測試
        （`test_find_trade_by_ordno_matches_order_id_not_broker_ordno_field`）直呼這個方法名。"""
        return self._native._find_trade_by_ordno(ordno)

    def _cancel_blocking(self, ordno: str) -> None:
        # 找 Trade + 呼叫 native cancel_order 已搬到 native.py（Task 1），找不到對應 Trade 時
        # native 拋出的 OrderError 訊息與重構前逐字相同。
        self._native.cancel(ordno)

    # ---- update ----

    async def update(
        self,
        broker_order_id: str,
        *,
        actor_user_id: int,
        price=None,
        qty=None,
        confirm_token: str | None = None,
    ) -> OrderAck:
        with self._session_factory() as session:
            order = brepo.find_order_by_broker_id(
                session, broker=self.broker, account=self.account, mode=self.mode,
                broker_order_id=broker_order_id,
            )
            if order is None:
                raise OrderError(f"找不到委託 broker_order_id={broker_order_id!r}")
            # Inc1 D4/R6-1（Task 10）：update admission——這張委託尚未取得券商流水號
            # （`ordno`）就拒絕改單，且必須排在建立 reservation/ledger 之前（不進 DB 決策段、
            # 不留任何殘影）。理由：① `DownUpdate` 協定的 `ordno` 是非空必填欄位（見
            # agent_protocol.py），沒有 ordno 根本組不出合法的下行指令；② 沒有 ordno 代表這張
            # 單尚未被券商確認承接（place 還在 unknown/等 ack 的中間態），改單語意上不成立。
            # 與下面的 offline fail-fast 同一位置慣例（比照 place() 冪等 miss 之後、
            # check_place 之前的既有寫法）；in-process 模式同樣適用——沒有 ordno 就沒有對象
            # 可改，不限 remote_gateway 存在與否。
            if order.ordno is None:
                raise OrderError(
                    f"委託尚未取得券商流水號（ordno），無法改單：broker_order_id={broker_order_id!r}"
                )
            # Inc1 D9：offline fail-fast——`check_update` 若判定口數增加會建立 delta
            # QuotaReservation（DB 決策段的一部分），這筆保留在改單真的送不出去時還得靠
            # `_send_gate`/`_do_update` 的例外分支釋放；搬到這裡、`check_update` 之前，
            # 與 `place()` 的既有寫法（冪等查找 miss 之後、`check_place` 之前）同一位置語意——
            # agent 未連線時直接拒絕，連保留列都不建立，不進 DB 決策段（比照 spec D9「offline
            # 擋新單」）。`_send_gate` 內仍保留同一判定作為縱深防禦第二層（若在這個檢查通過
            # 後、native 呼叫前才斷線，那裡的 unavailable→failed＋退配額語意不變）。C2
            # （HIGH，codex 終審）：改查 `admission_ready`——pending_health/failstop/lease
            # 過期三態下也要在這裡 fail-fast，不建 delta reservation。
            if self._remote_gateway is not None and not self._remote_gateway.admission_ready:
                raise OrderError("agent 未連線，無法改單")
            new_price = price if price is not None else order.price
            new_qty = qty if qty is not None else order.qty
            request_hash = canonical_payload_hash(
                symbol=order.symbol, action=order.action, qty=new_qty, price=new_price,
                price_type=order.price_type, order_type=order.order_type, octype=order.octype,
                account=self.account, mode=self.mode,
            )
            if self._risk_guard is not None:
                self._risk_guard.check_update(
                    session, order, actor_user_id=actor_user_id, new_qty=new_qty, new_price=new_price,
                    request_hash=request_hash, confirm_token=confirm_token,
                )
            elif order.user_id != actor_user_id:
                raise AuthorizationError("非委託所有人不得改單")
            order_id, ordno, client_order_id = order.id, order.ordno, order.client_order_id
            action = order.action  # 供 T0.3 place_failed 告警用（session 關閉後不再讀 detached order）
            price_type = order.price_type  # 改單不能改變 price_type，送出前判斷 MKT 用既有值

            # round3 #4/#11 收尾：這次改單「若有」保留的 delta 配額（RiskGuard.check_update
            # 只在 new_qty 較原本增加時才會建立這列，見 repository.reservation_id_for_update），
            # 送出成功/失敗後在這裡 confirm/release；沒有 risk_guard 就沒有保留列，不猜測呼叫。
            # Task 8：搬到 `with` 區塊內（原本在區塊外），讓下面的 ledger insert 能引用
            # 同一個 reservation_id——兩者都只依賴 client_order_id/request_hash，純函式計算，
            # 搬動不改變既有結果。
            reservation_id = (
                brepo.reservation_id_for_update(client_order_id=client_order_id, request_hash=request_hash)
                if self._risk_guard is not None else None
            )

            # Inc1 D4（G1）：agent 模式送前持久化。R5-2/R6-1 update 單飛靠 DB partial unique
            # index（`uq_agent_cmd_update_singleflight`，Task 1 已建）——若這個 Order 已有其他
            # 未 resolved 的 update ledger 列，這裡的 insert 會撞唯一鍵，走 IntegrityError
            # 分支轉成「前一筆改單結果未定」（codex R6-3：先確認撞的正是這個 index 再轉友善
            # 訊息，其餘 IntegrityError 原樣拋出）。
            cmd_id: str | None = None
            if self._remote_gateway is not None:
                ledger_user_id = (
                    self._agent_user_id if self._agent_user_id is not None else actor_user_id
                )
                cmd = agent_commands.new_command(
                    kind="update", user_id=ledger_user_id, broker=self.broker,
                    account=self.account, mode=self.mode, client_order_id=client_order_id,
                    ordno=ordno, reservation_id=reservation_id,
                    payload={"price": (str(new_price) if new_price is not None else None),
                             "qty": new_qty, "price_type": price_type},
                    expiry_seconds=self._agent_command_expiry_seconds,
                )
                try:
                    agent_commands.insert_command(session, cmd=cmd)
                    session.commit()
                except IntegrityError:
                    session.rollback()
                    if brepo.has_unresolved_update_command(session, client_order_id=client_order_id):
                        # C8（MEDIUM，codex 終審）：撞單飛鍵——這次嘗試在 `check_update`
                        # 階段（若 delta>0）已經 reserve 並 commit 過一筆 delta
                        # QuotaReservation（見模組頂部「已知落差」說明：check_update 內部
                        # 自行 commit，早於這裡的 ledger insert 撞鍵，回滾救不回它）。撞鍵
                        # 代表這筆 ledger 從未真正落地送出，若不清理會永久孤兒佔用配額——
                        # 這是「正常請求可確定觸發」的路徑（兩個併發改單就會踩到），不是
                        # crash 才會發生的窄窗。只在這次的 reservation_id 未被贏家（現存
                        # unresolved 的 update ledger 列）引用時才釋放：
                        # `reservation_id_for_update` 是 client_order_id+request_hash 的
                        # 確定性推導，相同內容重送會得到同一個 reservation_id——這種情況
                        # release 會誤傷贏家仍在使用中的保留列，必須跳過。
                        if reservation_id is not None:
                            winner_reservation_id = brepo.unresolved_update_reservation_id(
                                session, client_order_id=client_order_id
                            )
                            if winner_reservation_id != reservation_id:
                                brepo.release_quota(session, reservation_id=reservation_id)
                                session.commit()
                        raise OrderError("前一筆改單結果未定，請稍候或確認前次改單狀態") from None
                    raise
                cmd_id = cmd.cmd_id
                # C9：見 place() 同名變數說明——趁 session 還活著（`with` 區塊尚未結束）讀成
                # 純字串，避免 `_do_update()`（`with` 區塊結束、session 已關閉之後才執行）
                # 再讀 `cmd.expires_at` 觸發對已關閉 session 的 lazy refresh 而炸。
                cmd_expires_at = cmd.expires_at.isoformat()

        async def _do_update() -> None:
            await self._send_gate(actor_user_id)
            if self._remote_gateway is not None:
                # C9：`expires_at` 傳 ledger 建立當下凍結的值（見 place() 同名參數說明）。
                await self._remote_gateway.update(
                    ordno, price=new_price, qty=new_qty, price_type=price_type, cmd_id=cmd_id,
                    expires_at=cmd_expires_at,
                )
                return
            await asyncio.to_thread(self._update_blocking, ordno, new_price, new_qty, price_type)

        try:
            await self._supervisor.run(_do_update)
        except RiskError:
            # send gate 擋下（如 kill switch 剛好在改單當下被打開）：確定沒送出，退還這次
            # 改單嘗試「若有」保留的 delta 配額（沒有保留列時 release_quota 是 no-op）；
            # 委託本身的狀態不變（改單失敗不代表委託本身壞了，不比照 place 標 failed）。
            with self._session_factory() as fail_session:
                if reservation_id is not None:
                    brepo.release_quota(fail_session, reservation_id=reservation_id)
                if self._remote_gateway is not None and cmd_id is not None:
                    agent_commands.resolve_never_dispatched(
                        fail_session, cmd_id=cmd_id, message="kill switch 已啟動（第二層檢查）"
                    )
                fail_session.commit()
            raise
        except AgentUnavailableError as exc:
            # Task 8：這筆改單指令從未送達 agent（`_send_gate`/`AgentChannel.request` 的
            # ready 檢查落空），往後也不會有任何 ack——比照 RiskError 分支釋放 delta，
            # 並本地終結 ledger（同 place 分支的理由）。
            with self._session_factory() as fail_session:
                if reservation_id is not None:
                    brepo.release_quota(fail_session, reservation_id=reservation_id)
                if self._remote_gateway is not None and cmd_id is not None:
                    agent_commands.resolve_never_dispatched(fail_session, cmd_id=cmd_id, message=str(exc))
                fail_session.commit()
            self._alert_place_failed(
                client_order_id=client_order_id, action=action, qty=new_qty,
                classification="failed", exc=exc,
            )
            raise
        except _TradeNotFoundError:
            # bug 2/3 收尾：根本沒有送出任何 native update_order 呼叫（連對應 Trade 都找
            # 不到——已成交/已刪/跨日等），不屬於下面 except Exception 分支「結果不明」的
            # unknown fail-safe 範疇，不誤標委託狀態（委託本身狀態不變，同 RiskError 分支
            # 既有原則）。in-process：native 直接拋出，這次改單嘗試「若有」保留的 delta
            # 配額確定沒被使用，一律釋放。agent 模式：這個型別在這裡一定是 ack 衍生
            # （`_unwrap` 對 error_kind="trade_not_found" 的對映）——`apply_command_ack`
            # 已經 release 過 delta，route 不重套。
            if self._remote_gateway is None and reservation_id is not None:
                with self._session_factory() as fail_session:
                    brepo.release_quota(fail_session, reservation_id=reservation_id)
                    fail_session.commit()
            raise
        except AgentCommandTimeoutError as exc:
            # Task 8（D4 route 逾時路徑，同 place 分支的理由）：`mark_timeout_observed` 只用
            # 來記錄／分辨這次逾時是否仍然有效——update 逾時的轉移表結果是「不改 Order、
            # delta 保留」，不論 CAS 輸贏都不需要任何本地寫入（True＝真的還沒收到 ack，
            # 維持既有『不改』；False＝ack 已經在別的交易落地過，同樣不得再寫，讓已落庫的
            # 結果保持原樣，S#15/R1-1）。
            if self._remote_gateway is not None and cmd_id is not None:
                with self._session_factory() as fail_session:
                    agent_commands.mark_timeout_observed(fail_session, cmd_id=cmd_id)
                    fail_session.commit()
            self._alert_place_failed(
                client_order_id=client_order_id, action=action, qty=new_qty,
                classification="unknown", exc=exc,
            )
            raise
        except Exception as exc:
            if self._remote_gateway is not None and isinstance(exc, OrderError):
                # Task 8：其餘 ack 衍生的明確拒絕（exception/mode_mismatch/expired/
                # scope_mismatch）——`apply_command_ack` 已經 release 過 delta（不標
                # failed），route 不重套、不重新分類，原樣 re-raise。
                raise
            # 本次精進：同 place 分支，先分類是否為「券商明確拒絕」。是的話這次改單
            # 嘗試確定沒生效——比照上面 RiskError 分支，立即釋放「若有」保留的 delta
            # 配額，不必等 watchdog reconcile；委託本身狀態不變（改單失敗不代表委託
            # 本身壞了，不比照 place 標 failed，同 RiskError 分支的既有原則）。分類不出來
            # 一律維持原本 unknown fail-safe（結果不明，不 release、不 confirm）。此分支
            # in-process 模式仍會走到（remote 模式的已知例外型別皆已被上面攔截）。
            classification = _classify_place_failure(exc)
            if classification == "failed":
                if reservation_id is not None:
                    with self._session_factory() as fail_session:
                        brepo.release_quota(fail_session, reservation_id=reservation_id)
                        fail_session.commit()
                # T0.3 告警（純疊加）；RiskError/_TradeNotFoundError/AgentUnavailableError 在
                # 上面各自的 except 分支就已返回，不會走到這裡。
                self._alert_place_failed(
                    client_order_id=client_order_id, action=action, qty=new_qty,
                    classification="failed", exc=exc,
                )
                raise OrderError(
                    f"改單遭券商明確拒絕，delta 配額已釋放（委託本身狀態不變）："
                    f"{_redact_secrets(str(exc), secrets=self._secrets_to_redact)}"
                ) from exc
            with self._session_factory() as fail_session:
                fail_order = fail_session.get(Order, order_id)
                brepo.mark_order_status(fail_session, fail_order, status="unknown")
                fail_session.commit()
            self._alert_place_failed(
                client_order_id=client_order_id, action=action, qty=new_qty,
                classification="unknown", exc=exc,
            )
            raise OrderError(
                f"改單失敗，委託標記 unknown 待 reconcile："
                f"{_redact_secrets(str(exc), secrets=self._secrets_to_redact)}"
            ) from exc

        with self._session_factory() as session:
            order = session.get(Order, order_id)
            agent_commands.apply_update_ack(
                session, order, new_price=new_price, new_qty=new_qty, reservation_id=reservation_id
            )
            session.commit()
        return OrderAck(client_order_id=client_order_id, broker_order_id=broker_order_id, ordno=ordno, status="submitted")

    def _update_blocking(self, ordno: str, price, qty: int, price_type: str | None = None) -> None:
        # 找 Trade + 呼叫 native update_order 已搬到 native.py（Task 1）；找不到對應 Trade 時
        # native 拋出公開的 base.TradeNotFoundError——與本檔的 `_TradeNotFoundError` 別名同一個
        # 型別，下面 `update()` 的 `except _TradeNotFoundError:` 分支繼續能特判命中。
        self._native.update(ordno, price=price, qty=qty, price_type=price_type)

    # ---- positions（純讀 DB，不呼叫 native API——BrokerPosition 是唯一真相來源；
    #      仍經 supervisor.run 走同一通道，避免與 connect/reconnect 交錯讀到半新半舊狀態） ----

    def positions_snapshot(self, *, actor_user_id: int) -> list[Position]:
        """無鎖同步部位快照（輪詢/唯讀路徑用）：只讀 BrokerPosition（committed rows，WAL 下
        是一致快照），**不搶 supervisor 鎖**、不呼叫 native，可在 threadpool（同步 def 路由）
        跑、完全離開 event loop。與 `positions()` 唯一差別是少了那把「避免與 connect/reconnect
        交錯讀到半新半舊」的鎖——但這是純讀 committed 資料、reconnect 不寫 BrokerPosition，
        故快照仍一致。所有權檢查（assert_owner）與 `positions()` 相同。"""
        with self._session_factory() as session:
            if self._risk_guard is not None:
                self._risk_guard.assert_owner(actor_user_id)
            rows = brepo.list_open_positions(
                session, user_id=actor_user_id, broker=self.broker, account=self.account,
                mode=self.mode, symbol=self.symbol,
            )
            return [
                Position(
                    symbol=r.symbol, direction=r.direction,
                    qty=brepo.remaining_qty(r), avg_price=brepo.avg_entry_price(r),
                )
                for r in rows
            ]

    async def positions(self, *, actor_user_id: int) -> list[Position]:
        # 保留原本「走 supervisor 序列化通道」的語意給非輪詢呼叫端；讀取邏輯與無鎖快照共用。
        return await self._supervisor.run(
            lambda: self.positions_snapshot(actor_user_id=actor_user_id)
        )

    # ---- callback（背景執行緒）：只落地 RawInbox，不直接處理業務邏輯（round3 BLOCKER#2） ----

    def _on_order_cb(self, stat, msg) -> None:
        """set_order_callback 的 handler，跑在 Solace/.NET 背景執行緒。round3 修正：只呼叫
        `commit_raw_callback`（獨立 Session、同步、返回前保證落地），不做
        `loop.call_soon_threadsafe`+`ensure_future` 排程協程晚點才 commit 的作法——那個
        作法在 loop 未就緒/排程後 crash/等 supervisor lock 時 payload 仍會遺失（round3
        BLOCKER#2）。callback 內不碰部位/DB 業務邏輯，那是 RawInboxWorker 之後才做的事。

        **Task 2 委派重構的刻意例外**：native.py 也有一份等價的
        `ShioajiNativeClient._on_order_cb`——那才是 `connect()` 時真正註冊給 Shioaji SDK
        的 callback（見 `_connect_blocking`），本方法在正式連線路徑上**不會被呼叫**。這裡
        刻意保留本檔自己的實作（改呼叫 `_persist_raw`，落地邏輯與 native 端等價），而不是
        單純 `self._native._on_order_cb(stat, msg)` 委派——既有硬化測試
        （`tests/test_broker_hardening.py::_bare_adapter`）用 `object.__new__(ShioajiAdapter)`
        繞過 `__init__` 建構最小 adapter（不會建到 `self._native`），若改成委派 native 會在
        該測試炸 `AttributeError`。本檔與 native 端兩份實作邏輯逐字相同，非重複維護風險。
        """
        kind = "deal_report" if str(stat).endswith("Deal") else "order_report"
        try:
            payload = self._json_safe(msg)
            self._persist_raw(kind, payload)
        except Exception:
            # 券商回報是真錢關鍵路徑：序列化/落地失敗絕不能靜默丟單，也絕不能把例外拋回
            # Solace/.NET callback thread（會殺掉整條回報通道）。退化保存一筆帶原始 repr 的
            # 紀錄供 reconcile/人工補救；連退化保存都失敗才記錄後放行（DB 全掛時已無法多做）。
            log.exception("成交/委託回報落地失敗，改以退化 payload 保存（kind=%s）", kind)
            try:
                self._persist_raw(kind, {"_unparsed": True, "repr": repr(msg)})
            except Exception:
                log.exception("退化 payload 也落地失敗，回報恐遺失（kind=%s）", kind)

    @staticmethod
    def _json_safe(msg) -> dict:
        """轉換邏輯已搬到 native.py `ShioajiNativeClient.json_safe`（Task 1，逐字相同）；這裡
        保留同名 staticmethod 純委派——不吃 `self`/`_native`，供既有測試直呼
        `ShioajiAdapter._json_safe(msg)`（不建構 instance）。"""
        return ShioajiNativeClient.json_safe(msg)

    # ---- Task 5 DealMapper / OrderReportMapper 實作（V3-4 嚴格驗證） ----
    #
    # 欄位名稱依 `.venv/.../shioaji/_core.pyi` 的 `FuturesDealEvent`/`FuturesOrderEvent`
    # TypedDict 實測校正（bug 1(b)）——先前假設的 key（deal_id/octype/order_id）在真實 SDK
    # 裡不存在，導致所有成交/委託回報 100% quarantine。真實結構：
    #   FuturesDealEvent：trade_id, seqno, ordno, exchange_seq, broker_id, account_id, action,
    #     code, full_code, price, quantity, subaccount, security_type, delivery_month,
    #     strike_price, option_right, market_type, combo, ts（epoch 秒 float，非 epoch-ms）。
    #     **沒有 octype 欄位**——正確值由 `RawInboxWorker._process_deal` 解析出對應 Order 後
    #     用 `order.octype` 覆蓋（見該檔說明），這裡只給一個滿足 `Fill.__post_init__` 型別
    #     驗證的占位值。
    #   FuturesOrderEvent：巢狀 {operation, order, status, contract}；委託關聯鍵在 `order`
    #     內——比照既有 `_ack_fields_from_trade` 的慣例（`order.id`→我方 Order.ordno、
    #     `order.seqno`→我方 Order.broker_order_id），這裡對齊同一組 key 才對得起來；
    #     實際的「這次是什麼操作」在 `operation.op_type`
    #     （New/Cancel/UpdatePrice/UpdateQty/Reject），**不是**一個現成的 Filled/Cancelled
    #     狀態字串——`status`（`EventOrderStatusDict`）只有 id/exchange_ts/modified_price/
    #     cancel_quantity/order_quantity/web_id 這類診斷欄位，沒有語意狀態值。Filled/
    #     PartFilled 狀態一律由成交回報（deal_report）驅動的 `PositionTracker.apply_fill`→
    #     `brepo.apply_order_fill` 更新，不經這個 mapper。

    def _map_deal_report(self, payload: dict, *, account: str | None = None) -> Fill:
        """`account` 是 RawInbox 列上蓋章的值（Inc1 D5，呼叫端見
        `RawInboxWorker._process_deal`）——只在非 None 時才驗證 `payload["account_id"]` 與它
        相符（R1-5 payload_mismatch），None（列上未蓋章的舊資料/未帶 scope 的直接呼叫）時跳過
        驗證，維持既有位元級行為（同 R2-2 的 user_id NULL 放行原則，不因為新增檢查而讓歷史/
        測試路徑退化）。"""
        try:
            fill_id = payload["trade_id"]
            if not fill_id:
                raise ValueError("trade_id 為空")
            action = payload["action"]
            qty = int(payload["quantity"])
            price = Decimal(str(payload["price"]))
            ts = int(round(float(payload["ts"]) * 1000))  # 真實 ts 是 epoch 秒(float)，Deal.ts 需 epoch-ms
            payload_account = payload["account_id"]
            ordno = payload.get("ordno")
            broker_order_id = payload.get("seqno") or ordno
            fee_raw = payload.get("fee")
            fee = Decimal(str(fee_raw)) if fee_raw not in (None, "") else None
            symbol = payload.get("code") or self.symbol
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            raise ValueError(f"deal_report payload 缺值或格式不合法: {exc}") from exc

        if account is not None and payload_account != account:
            # R1-5：deal_report payload 自帶的 account_id 與這列 raw_inbox 事件產生當下蓋章的
            # account 矛盾——可能是帳號冒用或上游資料損壞，一律 fail closed 且**不重試**
            # （mapper 一律以列 scope 為權威，不信任 payload 自帶值）。
            raise RawInboxDeadLetterError(
                "payload_mismatch",
                f"deal_report payload.account_id={payload_account!r} 與列上蓋章 "
                f"account={account!r} 不符，fail closed",
            )
        account = payload_account

        if fee is None and self.mode == "sim" and self._sim_fee_per_lot is not None:
            # A6：sim 模擬單成交 fee 常缺值/零，依設定的「每口」估算，按 qty 分批累計時自然正確
            # （每筆 fill 各自算 qty*sim_fee_per_lot，PositionTracker 累加 open_fee_total/close_fee_total
            # 時就是「已成交口數 * 每口 fee」的正確累計，不需要另外處理批次）。real 模式缺值一律
            # 保持 None（round3 HIGH：不得靜默記 0，那會讓正式 PnL 永久低估成本）。
            fee = self._sim_fee_per_lot * qty

        return Fill(
            broker=self.broker, fill_id=str(fill_id), ordno=ordno, broker_order_id=broker_order_id,
            symbol=symbol, action=action, price=price, qty=qty, fee=fee,
            # 成交回報無 octype——"Auto" 只是滿足型別驗證的占位值，真正生效前一律會被
            # RawInboxWorker._process_deal 用解析到的 Order.octype 覆蓋；解不到對應 Order
            # 時整筆在覆蓋之前就已經 raise ValueError quarantine，這個占位值不會被業務邏輯讀到。
            octype="Auto",
            ts=ts, account=account, mode=self.mode, user_id=None,
        )

    # 即時串流 FuturesOrderEvent 的 operation.op_type → 我方 Order.status 詞彙。
    _OP_TYPE_STATUS_MAP = {
        "New": "submitted", "UpdatePrice": "submitted", "UpdateQty": "submitted",
        "Cancel": "cancelled", "Reject": "failed",
    }

    # `_reconcile_blocking` 週期性補洞時自建的扁平 payload（來自 `list_trades()` 的
    # `OrderStatusInfo.status`，是 REST 式彙總狀態、非即時 callback 結構）沿用的舊詞彙——
    # 與即時 callback 共用同一個 kind="order_report" 佇列/同一個 mapper，兩種來源都要能解析。
    _TRADE_STATUS_MAP = {
        "Cancelled": "cancelled", "Failed": "failed", "PartFilled": "partfilled",
        "Filled": "filled", "PendingSubmit": "sending", "Submitted": "submitted",
    }

    def _map_order_report(self, payload: dict, *, account: str | None = None) -> OrderReport:
        """`account`（Inc1 D5/S3 拆除）是 RawInbox 列上蓋章的值（呼叫端見
        `RawInboxWorker._process_order_report`）——**不**回退讀 `self.account`：那是 mutable
        單例，事件產生後若帳號被換掉，處理當下讀到的會是新帳號，錯誤地覆蓋掉舊事件真正的
        歸屬（S3 要拆的縫）。直接呼叫（未傳 `account`，例如既有單元測試）維持 `None`，不強迫
        補齊。"""
        if "operation" in payload or "order" in payload:
            # 即時串流 callback（真實 FuturesOrderEvent 巢狀結構）。
            order_detail = payload.get("order") or {}
            operation = payload.get("operation") or {}
            # 比照既有 `_ack_fields_from_trade` 慣例：order.id → 我方 Order.ordno、
            # order.seqno → 我方 Order.broker_order_id（我方下單時就是這樣存的，見
            # ShioajiAdapter._ack_fields_from_trade），這裡對齊同一組 key 才能正確關聯。
            ordno = order_detail.get("id")
            broker_order_id = order_detail.get("seqno") or ordno
            op_type = operation.get("op_type")
            status = self._OP_TYPE_STATUS_MAP.get(str(op_type))
            if status is None:
                raise ValueError(f"未知委託回報操作型態 operation.op_type: {op_type!r}")
        else:
            # reconcile 週期性補洞的扁平 payload（見 `_reconcile_blocking`）。
            ordno = payload.get("order_id")
            broker_order_id = payload.get("seqno") or ordno
            status_raw = payload.get("status")
            status = self._TRADE_STATUS_MAP.get(str(status_raw))
            if status is None:
                raise ValueError(f"未知委託狀態: {status_raw!r}")
        if not ordno and not broker_order_id:
            raise ValueError("order_report 缺委託關聯鍵（order.id/order.seqno 或 order_id/seqno），無法關聯委託")
        return OrderReport(
            broker=self.broker, account=account, mode=self.mode,
            ordno=ordno, broker_order_id=broker_order_id, status=status,
        )


def _now_ms() -> float:
    import time

    return time.time() * 1000


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
