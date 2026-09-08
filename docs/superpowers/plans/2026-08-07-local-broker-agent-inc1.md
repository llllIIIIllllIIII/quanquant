# 本機 Broker Agent Increment 1（多人 simtrade）實作計畫

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
> **薄計畫慣例**（本專案交接文件明定）：本計畫給介面簽名＋測試清單＋棘手演算法，不貼整段實作碼；實作者依 spec `docs/superpowers/specs/2026-08-06-local-broker-agent-inc1-design.md`（v8，codex 7 輪＋D3 delta APPROVE）為唯一正典，計畫與 spec 衝突時**以 spec 為準並回報**。

**Goal:** 把 Inc0 單人 agent 通道升級為真多人：per-user registry/token/隔離＋三硬門檻（G1 command ledger、G2 fail-stop、G3 unknown 配額解除）。

**Architecture:** server 端 per-user `UserAgentSlot`（channel/adapter/session_state/supervisor 鎖/worker/watchdog 各一份）＋DB 三新表（agent_tokens/agent_commands/agent_account_bindings）＋raw_inbox 蓋章；agent 端 buffer schema v2（command_ledger＋outbox 擴欄）；協定硬升 v2。

**Tech Stack:** FastAPI + SQLModel（SQLite/Postgres 雙方言）+ websockets>=14 + pydantic v2 + pytest。

## Global Constraints（每個 task 隱含遵守）

- **mode 鎖 sim**：協定/CLI/server/child 四層 `Literal["sim"]` 不動；不開 real。
- **in-process 零變更**：`ORDER_CHANNEL=inprocess` 行為與現行完全一致；既有 770 測試全綠、不弱化不 skip。
- **雙方言可攜**：raw SQL 用 named params；新欄位走 `db/migrate.py` `_MIGRATIONS` nullable ADD COLUMN；partial index 用 `sqlite_where`＋`postgresql_where`（範式 `db/models.py:355`）。
- **無新依賴**；pyproject 不加 force-include；`Candle.ts` BigInteger 等既有約束不碰。
- UI 全繁體中文；commit 格式 `type: description`（attribution 停用）；每 task 結束 commit。
- 測試指令：`uv run pytest`（全套）；單檔 `uv run pytest tests/<file> -v`。
- 檔案行號基準＝main@43e61e0；行號漂移時以符號名為準。
- spec 測試清單編號（§6 之 1-40）在下方各 task 的「測試」欄以 `S#n` 引用；實作者需讓該編號情境有對應測試。

---

## 檔案地圖（誰負責什麼）

| 檔案 | 動作 | 責任 |
|---|---|---|
| `src/quanquant/db/models.py` | 改 | 新增 AgentToken/AgentCommand/AgentAccountBinding 模型 |
| `src/quanquant/db/migrate.py` | 改 | raw_inbox 4 新欄 |
| `src/quanquant/broker/agent_protocol.py` | 改 | 協定 v2 全部訊息 |
| `src/quanquant/broker/risk.py` | 改 | KillSwitchState 兩層；RiskGuard blocked(uid) |
| `src/quanquant/auth/agent_tokens.py` | 新 | token 簽發/驗證/rotation |
| `src/quanquant/broker/agent_commands.py` | 新 | ledger repository＋兩維 CAS＋applier＋resolver |
| `src/quanquant/broker/agent_registry.py` | 新 | AgentRegistry/UserAgentSlot |
| `src/quanquant/broker/agent_channel.py` | 改 | gateway 加 query_qty；request 走 ledger applier |
| `src/quanquant/broker/inbox_worker.py` | 改 | scoped staging API；per-user 批次；mapper 帳號來源 |
| `src/quanquant/broker/shioaji_adapter.py` | 改 | 三段切接 ledger；_send_gate 查 blocked(uid)；mapper 吃列 account |
| `src/quanquant/broker/watchdog.py` | 改 | per-slot agent watchdog＋G3 resolver |
| `src/quanquant/broker/repository.py` | 改 | unquarantine 按 reason；binding/backfill 查詢 |
| `src/quanquant/web/routers/agent_ws.py` | 改 | token 驗證換 DB；UpLogin guard v2；applier 接線；health/lease |
| `src/quanquant/web/routers/orders.py` | 改 | kill-switch scope 參數；token 管理段；per-user badge |
| `src/quanquant/web/routers/health.py` | 改 | agent 模式 healthz 語意 |
| `src/quanquant/web/app.py` | 改 | lifespan 建 registry；backfill fail-closed；lease/清理任務 |
| `src/quanquant/config.py` | 改 | 新設定 3 枚；移除 agent_ws_token |
| `src/quanquant/agent/buffer.py` | 改 | schema v2＋command_ledger＋failstop sentinel |
| `src/quanquant/agent/runner.py` | 改 | ledger 去重/scope/expiry；health sender；failstop latch |
| `src/quanquant/agent/native_runner.py` | 改 | query_qty op；failstop IPC |
| `src/quanquant/web/templates/…orders…` | 改 | 兩顆急停＋token 段＋per-user badge |

---

### Task 1: DB 模型與 migration（三新表＋raw_inbox 四欄）

**Files:** Modify `src/quanquant/db/models.py`、`src/quanquant/db/migrate.py`；Test `tests/test_agent_models.py`（新）、`tests/test_migrate.py`（加案例）。

**Interfaces（Produces）:**
```python
class AgentToken(SQLModel, table=True):   # __tablename__="agent_tokens"
    id: int | None (PK); user_id: int (index); token_hash: str (unique)
    created_at: datetime; expires_at: datetime
    revoked_at: datetime | None; last_used_at: datetime | None

class AgentCommand(SQLModel, table=True): # __tablename__="agent_commands"
    cmd_id: str (PK, uuid4 hex); user_id: int (index)
    kind: str  # 'place'|'cancel'|'update'
    broker: str; account: str; mode: str            # scope 凍結（spec D4/R1-2）
    client_order_id: str | None; ordno: str | None; reservation_id: str | None
    payload: str; created_at: datetime; sent_at: datetime | None
    transport_acked_at: datetime | None             # transport 維
    outcome: str | None       # 'ok'|'error'|'unknown'
    resolved_via: str | None  # 'ack'|'query_qty'|'report'|'manual'
    resolved_at: datetime | None                    # 業務維唯一判準（R4-4）
    timeout_observed_at: datetime | None; result: str | None; expires_at: datetime

class AgentAccountBinding(SQLModel, table=True):  # __tablename__="agent_account_bindings"
    id: int | None (PK); broker: str; account: str; user_id: int; bound_at: datetime
    # UniqueConstraint("broker","account")（R2-7：不分 mode）
```
- AgentCommand 加 partial unique index `uq_agent_cmd_update_singleflight`：`client_order_id` WHERE `kind='update' AND resolved_at IS NULL`（`sqlite_where`＋`postgresql_where`，範式 models.py:355；R5-2/R6-1）。
- CHECK（雙方言可攜字面）：`resolved_at IS NULL OR resolved_via IS NOT NULL`；`resolved_via != 'ack' OR transport_acked_at IS NOT NULL`（R4-5）。
- `_MIGRATIONS` 加四筆：`("raw_inbox","user_id","INTEGER")`、`("raw_inbox","account","VARCHAR")`、`("raw_inbox","mode","VARCHAR")`、`("raw_inbox","quarantine_reason","VARCHAR")`；`RawInbox` 模型同步加同名 nullable 欄。

**測試（S#12 部分）：** 建表後欄位存在；migration idempotent；單飛 index 兩方言都擋第二筆 unresolved update、resolved 後可再插；CHECK 擋非法組合；`test_migrate.py` 補 raw_inbox 四欄案例。

- [ ] 寫失敗測試 → 跑紅 → 實作模型/migration → 跑綠 → `uv run pytest`（基線不破）→ commit `feat: Inc1 DB 模型與 raw_inbox scope 欄位`

---

### Task 2: 協定 v2（agent_protocol.py 全面改版）

**Files:** Modify `src/quanquant/broker/agent_protocol.py`；Test `tests/test_agent_protocol.py`（既有檔擴充）。

**Interfaces（Produces；欄位即 wire 格式）:**
- `PROTOCOL_VERSION = 2`；`UpLogin.protocol: Literal[2]`、`UpLogin.health_epoch: int`（基準宣告，R3-2）。
- `UpReport` +`account: str`、`mode: Literal["sim"]`（必填，S#12）。
- `UpCmdAck` +`event_id: int`；`error_kind: Literal["trade_not_found","exception","timeout","mode_mismatch","expired","scope_mismatch"]`。
- 新 `UpQueryResult(type="query_result", cmd_id, result: dict)`（volatile：無 event_id；reconcile 快照與 query_qty 結果共用，R1-7）。
- 新 `UpCommandRejected(type="cmd_rejected", cmd_id, error_kind: Literal["failstop"])`（volatile，R2-5）。
- `UpHealth` +`status: Literal["ok","failstop"]`、`detail: str | None`、`health_epoch: int`。
- `DownPlace/DownCancel/DownUpdate` +`account: str`、`expires_at: str(ISO)`（mode 欄既有，維持 Literal["sim"]）。
- 新 `DownQueryQty(type="query_qty", cmd_id, ordno, mode: Literal["sim"])`。
- Union 型別 `DownlinkMessage`/`UplinkMessage` 同步更新。

**測試：** protocol=1 的 UpLogin validation error（S#12）；UpReport 缺 account/mode 拒收；新訊息 round-trip serialize/validate；error_kind 新值合法。

- [ ] 失敗測試 → 紅 → 改協定 → 綠 → 全套（`test_agent_ws`/`test_agent_runner` 等舊測試會紅——**同 task 內把舊測試的訊息建構更新到 v2 欄位**，不弱化斷言）→ commit `feat: agent 協定 v2`

---

### Task 3: KillSwitchState 兩層（D3 拍板形）

**Files:** Modify `src/quanquant/broker/risk.py`、`src/quanquant/broker/shioaji_adapter.py`（`_send_gate` L421-433）、`src/quanquant/web/routers/orders.py`（kill-switch route L232-260）、orders 模板 `kill_switch_control.html`；Test `tests/test_risk.py`、`tests/test_orders_routes.py` 擴充。

**Interfaces（Produces）:**
```python
class KillSwitchState:
    global_on: bool
    per_user: dict[int, bool]
    def blocked(self, user_id: int) -> bool  # global_on or per_user.get(uid, False)

RiskGuard.set_kill_switch(on: bool, *, scope: str, actor_user_id: int)  # scope: 'self'|'global'
RiskGuard.kill_switch_view(user_id: int) -> dict  # {global_on, self_on, blocked}
```
- admission（`check_place` risk.py:132-133、update L196-197）與 `_send_gate` 都改 `blocked(actor_user_id)`；`_send_gate` 簽名加 `user_id` 參數（in-process 呼叫端傳 adapter 綁定 owner）。
- route：`POST /orders/kill-switch` body 加 `scope`；`scope='self'` 只改 actor 自己（無目標 user 參數——server 端天然無法指定他人）；`global` 沿用 assert_owner；audit 記 scope＋actor。
- `order_kill_switch_initial` 映射 `global_on`。取消單不查 kill switch（沿用）。
- UI：兩顆開關「我的急停」「全站急停」＋最後翻閘者。

**測試（S#40）：** A self-on → A place 擋/B 不擋；global-on → 全擋；scope=self 無法影響他人；cancel 不受擋；in-process 單 owner 等價（既有 kill switch 測試全數仍綠、必要時只改建構）。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: 兩層 kill switch（per-user＋全站）`

---

### Task 4: agent token 簽發/驗證＋WS 換發（D2）

**Files:** Create `src/quanquant/auth/agent_tokens.py`；Modify `src/quanquant/config.py`（+`agent_token_ttl_days: int = 30`；**刪 `agent_ws_token`**）、`src/quanquant/web/routers/agent_ws.py`（L32-42 驗證段）、`src/quanquant/web/routers/orders.py`＋模板（token 管理段）；Test `tests/test_agent_tokens.py`（新）。

**Interfaces（Produces）:**
```python
issue_token(session, *, user_id: int, ttl_days: int) -> str        # 明文只回傳一次；同 user 舊列全 revoked_at=now
validate_token(session, *, raw: str) -> AgentToken | None          # sha256 查表；過期/撤銷回 None；命中更新 last_used_at
```
- WS 握手：`x-agent-token` → `validate_token` → 載 User 驗 `is_active` ＋ `RiskGuard.is_owner(user_id)` → 得 user_id（後續 task 7 綁 slot；本 task 先以 user_id 取代舊全域驗證、單 slot 仍沿用）。任一步失敗 close(1008)。
- route：`POST /orders/agent-token`（owner-only）→ 回明文一次＋顯示 expires_at/last_used_at；再按＝rotation。
- 移除 `AGENT_WS_TOKEN` 的所有讀取點（`config.py:92`、`agent_ws.py:37-42`、`app.py:216` 註解）；agent CLI 的 `QQ_AGENT_TOKEN` 輸入流程不變。

**測試（S#11）：** 簽發→握手成功＋last_used_at 更新；過期/撤銷/rotation 後舊 token → close(1008)；非 owner 有效 token → 拒；同 user 僅一枚有效。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套（舊 AGENT_WS_TOKEN 相關測試改寫至新機制，不刪覆蓋）→ commit `feat: per-user agent token（DB opaque, TTL+rotation）`

---

### Task 5: scoped staging API＋RawInbox 蓋章＋quarantine 分級（D5/R2-6）

**Files:** Modify `src/quanquant/broker/inbox_worker.py`（`commit_raw_callback` L59-68、`stage_raw_inbox` 呼叫、worker 批次）、`src/quanquant/broker/shioaji_adapter.py`（`_persist_raw` L236-239、`_map_order_report` L928、`_stage_reconcile_results` L373）、`src/quanquant/broker/repository.py`（`unquarantine_stale_raw_inbox` L356-372、quarantine 寫入點）；Test `tests/test_inbox_worker.py` 等擴充。

**Interfaces（Produces）:**
```python
commit_raw_callback(session_factory, *, kind, broker, payload,
                    user_id: int | None, account: str | None, mode: str | None) -> None
# 唯一 staging 入口（R1-6）；三個呼叫端全帶 scope：
#   in-process _persist_raw → (None, adapter.account, adapter._mode)
#   agent_ws UpReport → (連線 user_id, msg.account, msg.mode)
#   reconcile 落列 → (slot user_id, slot account, "sim")，且與 cursor 推進同一交易
quarantine_raw_inbox(..., reason: str)  # 'association_pending'|'scope_violation'|'payload_mismatch'|'user_mismatch'
```
**棘手點（照 spec 逐條）：**
- mapper：`_map_order_report(payload, *, account)` 改吃列上蓋章 account，移除 `self.account` 讀取（S3 拆除）；deal mapper 驗 `payload.account_id == row.account`，不符 → quarantine(`payload_mismatch`)。
- worker：解析出 Order 後，`row.user_id IS NOT NULL` 才強制 `order.user_id == row.user_id`（R2-2）；不符 → quarantine(`user_mismatch`)。NULL 列只由 in-process worker 處理。
- 永久 dead-letter：`scope_violation/payload_mismatch/user_mismatch` → `processed=True`＋`quarantine=True`＋reason（退出 guard 計數）＋OpsAlerter 一次（同 key 節流）；`association_pending` 才進 unquarantine/重試迴圈（`unquarantine_stale_raw_inbox` 加 reason filter）。
- 既有 quarantine 寫入點（`inbox_worker.py:154-166` ValueError/PositionMismatch）→ reason=`association_pending`。
- UpReport envelope 驗證：account 必屬連線 user 的 binding（binding 表 task 6 建；本 task 先留 hook 點、task 6 接上），不符 → quarantine(`scope_violation`)＋照常 commit-then-ack（I1/I4）＋告警。

**測試（S#14/17/23，18 的交易性）：** 換帳號後補送舊事件歸屬舊帳號；偽造 account/payload 不符/user 不符 → 對應 dead-letter 且不再重試不擋 guard；in-process NULL 列行為與現行位元級一致；reconcile 落列＋cursor 同交易（模擬中途失敗雙回滾）。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: RawInbox per-user scope 蓋章與 quarantine 分級`

---

### Task 6: agent_account_bindings＋backfill＋UpLogin guard v2（D10/R1-8/R2-7）

**Files:** Modify `src/quanquant/broker/repository.py`（+binding 函式）、`src/quanquant/web/routers/agent_ws.py`（UpLogin 分支 L80-104）、`src/quanquant/web/app.py`（agent 分支啟動 backfill）；Test `tests/test_agent_ws.py`、`tests/test_account_binding.py`（新）。

**Interfaces（Produces）:**
```python
bind_account(session, *, broker, account, user_id) -> bool        # 先綁先贏；他人已綁回 False
backfill_account_bindings(session_factory) -> None                 # distinct (broker,account)→user_id 掃全 mode
                                                                   # 衝突 raise BackfillConflictError → agent 模式拒啟（fail closed）
count_unprocessed_for_login(session, *, user_id, account) -> int   # 該 user 的 unprocessed 中 account 不同或 NULL（dead-letter processed=True 天然排除）
has_unresolved_risky_commands_other_account(session, *, user_id, account) -> bool  # kind in (place,update) AND resolved_at IS NULL AND account != 來登帳號
```
**UpLogin 接受順序（inbox_lock 內，全過才 mark_logged_in）：**
1. binding：`bind_account` False → 拒登 close(1008)「此帳號已綁定其他使用者」。
2. 歷史 ownership 雙查：該 account 存在他人 Order → 拒登（R1-8，不能只信新表）。
3. per-user 換帳號 guard：`count_unprocessed_for_login > 0` → 拒登（S2 per-user 化）。
4. 他帳號未 resolved 曝險指令 guard（R1-2）：True → 拒登「先用原帳號連線收斂」。
（接上 task 5 的 UpReport envelope binding 驗證 hook。）

**測試（S#9/10/20/28）：** B 綁 A 已綁帳號拒登；backfill 多 mode 合併、跨 user 衝突 fail-closed；同帳號重連不擋；同 user 換帳號有未處理列擋/NULL 擋；他帳號有未 resolved place/update 擋、只有 cancel 不擋。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: 帳號綁定唯一性與登入 guard v2`

---

### Task 7: AgentRegistry／UserAgentSlot／lifespan／healthz（D1/D9 骨架）

**Files:** Create `src/quanquant/broker/agent_registry.py`；Modify `src/quanquant/web/app.py`（`_start_agent_channel_subsystem` L197-271 改建 registry）、`src/quanquant/web/deps.py`（user-aware 解析）、`src/quanquant/web/routers/agent_ws.py`（依 token user 取 slot）、`orders.py`（`orders_agent_status` L310-325 per-user）、`health.py`（agent 模式語意）、orders 模板 badge；Test `tests/test_agent_registry.py`（新）＋既有路由測試擴充。

**Interfaces（Produces）:**
```python
class UserAgentSlot:
    user_id: int; channel: AgentChannel; gateway: AgentNativeGateway
    adapter: ShioajiAdapter; session_state: OrderSessionState
    supervisor: BrokerSupervisor; tasks: list[asyncio.Task]  # worker+watchdog（task 11/12 填內容）

class AgentRegistry:
    def get(self, user_id: int) -> UserAgentSlot | None
    def slots(self) -> Iterable[UserAgentSlot]
# lifespan：for uid in owner_ids: 建 slot（session_state.mark_disabled("agent 未連線")）
# app.state.agent_registry 取代 app.state.agent_channel；in-process 分支完全不動
```
- deps：`get_order_service(request, user)`——inprocess → 現行 `app.state.order_service`；agent → `registry.get(user.id)` 的 adapter（None/非 owner → 現行 403/停用語意）。
- offline fail-fast：place/cancel/update route 先查 slot `session_state.ready`，未 ready 回「你的 agent 未連線」（不進 DB 決策段）。
- healthz：agent 模式 wiring 完成即 ready（200）；個別 slot 狀態不進 healthz（in-process 判定不變）。
- per-slot RawInboxWorker：批次 `WHERE user_id = slot.user_id`、拿 slot.supervisor 鎖；confirm-token 清理任務補進 agent 分支（全域一個）。
- 換帳號 guard／`adapter.account` 寫入點全部落在該 slot 的 `inbox_lock` 內（沿用 Inc0 generation/detach 機制，per-slot 化）。

**測試（S#8 隔離骨架/13）：** 兩 slot 各自連線互不干擾（A offline → B 下單不影響；A 慢 reconcile → B place 不排隊——用可控 fake gateway）；agent 模式 healthz=200 恆成立；in-process 全部既有測試綠（deps fallback 驗證）。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: per-user AgentRegistry 與 lifespan/healthz 語意`

---

### Task 8: AgentCommand repository＋兩維 CAS applier（G1 server 核心）

**Files:** Create `src/quanquant/broker/agent_commands.py`；Modify `src/quanquant/broker/shioaji_adapter.py`（place L437-486/562、cancel L582-619、update L644-720 決策段插 ledger insert；寫回段抽純函式）、`src/quanquant/web/routers/agent_ws.py`（UpCmdAck handler 接 applier）、`src/quanquant/broker/agent_channel.py`（future 只在 applier commit 後 resolve）；Test `tests/test_agent_commands.py`（新）。

**Interfaces（Produces）:**
```python
insert_command(session, *, cmd: AgentCommand) -> None   # 與決策段同交易（spec D4「送單前持久化」）
apply_command_ack(session_factory, *, cmd_id: str, user_id: int, ack: UpCmdAck) -> AppliedOutcome
# 兩維 CAS（R4-2）：
#   transport CAS: UPDATE ... SET transport_acked_at=now WHERE cmd_id=:c AND user_id=:u AND transport_acked_at IS NULL
#   確定 outcome（ok/error）→ 同一交易 resolution CAS（WHERE resolved_at IS NULL）＋套效果
#   timeout → outcome='unknown' 不 resolved；該 cancel 的 Order 已終態 → 同交易 resolve(via=report)
#   遲到 ack 遇已 resolved → 只補 transport 欄位，不改 outcome、不重套效果
mark_timeout_observed(session, *, cmd_id) -> bool   # CAS WHERE transport_acked_at IS NULL AND timeout_observed_at IS NULL；False=ack 已勝出
apply_place_ack / apply_place_failure / apply_cancel_ack / apply_update_ack(session, order, ...)  # 純函式，in-process 寫回段與 applier 共用（R1-10）
```
**棘手演算法（照 spec D4 kind×outcome 轉移表逐格）：** place ok→補 ordno＋confirm、明確拒絕→failed＋release、timeout→unknown 保留、expired/scope_mismatch/failstop→failed＋release；cancel ok→受理（終態由回報）、其餘不動 Order；update ok→原子寫 price/qty＋confirm delta、明確拒絕→**只 release delta 不標 failed**、timeout→unknown 保留。route 逾時路徑：先 `mark_timeout_observed`，False 就直接讀已落庫結果、不標 unknown；late ack 把 unknown→submitted/failed（狀態機允許此轉移）。applier 成功補 ordno 後解該 user `association_pending` quarantine。`AgentChannel` future 在 applier commit 後才 set_result（handler 內呼叫順序保證）。

**測試（S#1/2/15/25/32）：** late ack 全鏈收斂（ordno/quota/quarantine/狀態）；重複 ack no-op；ack 先 vs timeout 先兩交錯；四交錯（ack-first/resolver-first/並發/斷線間）效果恰一次；cancel/update 轉移表逐格；in-process 寫回經共用純函式且行為位元級不變（雙模式契約測試，S#13 部分）。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: agent command ledger 兩維 CAS applier`

---

### Task 9: agent 端 buffer v2＋command_ledger＋執行去重（D11/G1 agent 側）

**Files:** Modify `src/quanquant/agent/buffer.py`（schema v2）、`src/quanquant/agent/runner.py`（`_execute_command` L374-387 改）；Test `tests/test_agent_buffer.py`、`tests/test_agent_runner.py` 擴充。

**Interfaces（Produces）:**
```python
# buffer schema v2：outbox(+account TEXT, +mode TEXT, +cmd_id TEXT NULL)；
#   partial unique index：cmd_id WHERE sent_at IS NULL（R2-9）
#   command_ledger(cmd_id TEXT PK, kind TEXT, result TEXT, executed_at TEXT)
#   meta['schema_version']='2'；版本不符：outbox 有未送列→raise RefuseStartError（附提示）；乾淨→重建
Buffer.record_execution(cmd_id, kind, result_json, ack_event)  # 同一 SQLite 交易：寫 ledger＋append ack（check-and-insert）
Buffer.lookup_command(cmd_id) -> str | None                    # ledger 命中回 result_json
```
**執行順序（spec D4 agent 端①-④，順序即正確性）：** ①ledger 命中→不重執行，確保 outbox 有未送 ack（無則以存檔 result 補 append）；②scope 核對（指令 account/mode vs 目前登入）不符→`scope_mismatch` ack；③expiry 過期→`expired` ack（**先查 ledger 再查過期**，S#4）；④執行 native→`record_execution` 同交易→泵送。callback 落 outbox 時蓋當下 account/mode（I7 來源端蓋章）。

**測試（S#3/4/16 agent 側/21 agent 側）：** 重送指令 ledger 命中不重執行、重回存檔 ack；過期拒執行、過期但 ledger 命中仍回存檔 ack；同 cmd 至多一筆未送 ack；schema v2 升級三情境（乾淨重建/髒拒啟/新檔直建）。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: agent buffer v2 與 command_ledger 去重`

---

### Task 10: 重連補送＋expiry 設定＋update 單飛 admission（D4 收尾）

**Files:** Modify `src/quanquant/web/routers/agent_ws.py`（UpLogin 接受後補送）、`src/quanquant/broker/shioaji_adapter.py`（update 決策段單飛/ordno 檢查）、`src/quanquant/broker/agent_commands.py`（replay 查詢）、`src/quanquant/config.py`（+`agent_command_expiry_seconds: int = 120`）；Test 擴充 task 8/9 檔。

**棘手點：**
- replay：`WHERE user_id=:u AND transport_acked_at IS NULL AND resolved_at IS NULL AND account=:bound` 按 created_at 序下行（outcome=unknown 天然被 transport 條件排除；R4-2）；補送完才觸發 login reconcile。
- 單飛：update 決策段（同交易）insert ledger 撞 `uq_agent_cmd_update_singleflight` → 精確辨認該 index（沿 `repository.py:118` 模式）→ 整筆 rollback → 回「前一筆改單結果未定」；其他 IntegrityError 上拋（R6-3）。admission 先拒 `ordno IS NULL` 的 Order（不建 reservation/ledger；R6-1）。
- 指令建立時 `expires_at = created_at + agent_command_expiry_seconds`。

**測試（S#3/5/16/33/36/38）：** 斷線→重連補送→ledger 去重收斂；server 不自主過期（unknown 保留、配額跨 trading_day 歸零由既有計數天然成立——測 trading_day 過濾）；換帳號後原帳號指令不下行；並發 update 恰一成功（SQLite＋Postgres 方言標記測試）；ordno-less update 被拒。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: 重連補送與 update 單飛`

---

### Task 11: DownQueryQty＋unknown/終態 resolver＋per-slot watchdog（G3）

**Files:** Modify `src/quanquant/broker/agent_channel.py`（`AgentNativeGateway.query_qty`＋UpQueryResult future 對接）、`src/quanquant/agent/native_runner.py`（+query_qty op，等價 `_query_order_qty_blocking`）、`src/quanquant/agent/runner.py`（query 指令走 volatile 回覆）、`src/quanquant/broker/watchdog.py`（`run_agent_watchdog` L124-139 擴為 per-slot：retry_quarantined(reason filter)＋agent 版 `_reconcile_unknown_quota`）、`src/quanquant/broker/agent_commands.py`（resolver 函式）；Test `tests/test_watchdog.py`、`tests/test_agent_commands.py` 擴充。

**棘手演算法（spec D8＋D4 resolver，逐條）：**
- watchdog 收尾前置：凡關聯指令 `resolved_at IS NULL`（含 created/sent/unknown）→ 不 confirm/不 release；只有三入口可收尾（applier／resolver／同交易驗證無未 resolved 指令的 watchdog）。
- update unknown-resolver：`query_qty(ordno)` → ==改後值 → resolve(ok, via=query_qty)＋原子寫 Order price/qty＋confirm delta；==改前值 → resolve(error, via=query_qty)＋release delta；其他 → 留待下輪；**查無（已結案）→ 交終態 resolver**。
- 終態 resolver（R5-3/R6-2）：Order 終態時同交易處理 `resolved_at IS NULL AND transport_acked_at IS NOT NULL AND outcome='unknown'` 的 update——可得最終 qty → 二分收斂；不可得 → resolve(unknown, via=report)＋**保守 confirm 不 release**。created/sent 不碰。掛載點：worker 把 Order 推進終態的同一交易＋watchdog 週期 state-based 掃描（cancel 的 unresolved 同規則 resolve via=report）。
- place unknown 無 broker ID：resolver 不碰、永不自動 release（人工終結程序見 task 14 文件）。

**測試（S#7/19 部分/22/29/34/37/39）：** ledger 未終結 watchdog 跳過→終結後收斂兩分支；place acked_unknown 永不 release；cancel unknown × Order filled → resolve(unknown, via=report)；U1 unknown→cancel→list_trades 查無→終態 resolver 保守 confirm＋guard 解除；終態先到＋update created/sent → resolver 不碰、後續明確拒絕 ack release。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: G3 unknown 配額自動解除（query_qty＋resolver）`

---

### Task 12: G2 fail-stop 全鏈（latch/sentinel/health sender/lease/probe）

**Files:** Modify `src/quanquant/agent/native_runner.py`（callback 落地失敗 → 專用 IPC frame，不混 RPC pipe——獨立 queue/pipe，R2-8）、`src/quanquant/agent/runner.py`（failstop latch＋health_epoch 持久化於 buffer meta＋單一序列化 health sender＋出隊重驗＋recovery lock 只包本機轉移＋storage probe）、`src/quanquant/agent/buffer.py`（sentinel 檔案 API，buffer 之外路徑）、`src/quanquant/web/routers/agent_ws.py`（UpHealth/UpCommandRejected handler、pending_health、epoch 基準）、`src/quanquant/broker/agent_registry.py`（lease 檢查 task）、`src/quanquant/config.py`（+`agent_health_lease_seconds: int = 90`）、`src/quanquant/notify/ops_alerter.py`（+failstop 事件）；Test `tests/test_agent_failstop.py`（新）。

**棘手演算法（spec D9 ①-⑧ 全數落地）：**
- child：callback 雙寫失敗 → 專用 channel 通知父＋父寫 sentinel 檔＋latch（durable：sentinel 存在＝latch，重啟仍在效）。
- 父：每次 native 呼叫前查 latch；latch 中 mutating 指令 → volatile `UpCommandRejected(failstop)`（不經 outbox）；server 收到走 applier CAS 落 acked_error（failstop 分類→failed＋release，spec 轉移表）。
- health：單一 sender task；frame 出隊前 recovery lock 下重驗 (epoch,status,latch) 失效即棄；`health_epoch` 存 buffer meta、latch 時 +1；recovery：lock 內 probe（寫→commit→讀回同一 buffer）→確認無更新失敗→清 latch/sentinel→取 snapshot，**lock 外** send ok(epoch)。
- server：UpLogin 宣告 epoch 基準、slot 進 `pending_health`；本連線（handler my_generation fencing，payload 無 generation）收到 `ok` 且 epoch ≥ 已見最大 → ready；`failstop` 或斷線或 lease 過期（超過 `agent_health_lease_seconds` 無 ok）→ not-ready；見過較大 epoch 後舊 ok 永不恢復（R5-1 三保證）。OpsAlerter failstop 事件（連線/斷線不告警）。

**測試（S#6/19/24/30/31/35）：** 全鏈（落地失敗→latch→拒指令→server not-ready→告警→probe 通過→恢復）；舊 ok（小 epoch）不恢復；UpLogin 後未收 ok 不 ready；lease 過期 not-ready；send 卡住時新 failstop 立即 latch＋出隊丟棄舊 ok；transient ready 期間下行 → agent latch 拒絕（安全鏈）；buffer 重建 epoch 歸零→重宣告不死鎖。

- [ ] 失敗測試 → 紅 → 實作 → 綠 → 全套 → commit `feat: G2 durable fail-stop 全鏈`

---

### Task 13: 跨 user 端到端整合測試＋全套掃尾

**Files:** Test `tests/test_agent_multiuser_e2e.py`（新）；必要的小修散檔。

**內容：** 以兩個 fake agent（可控 gateway/WS 測試替身，沿 Inc0 `test_agent_ws` 手法）跑完整情境：A/B 各自 token 握手→登入綁不同帳號→place→report→UI 資料隔離；A quarantine/offline/慢操作不影響 B（S#8 全項）；spec 測試清單 1-40 逐號盤點——缺的在此補齊；`uv run pytest` 全綠（770 基線＋新增全部）；`uv run ruff check` 乾淨（本範圍新增不引入新錯誤）。

- [ ] 逐號盤點 spec S#1-40 → 補缺測試 → 全套綠 → commit `test: Inc1 多人端到端與規格測試盤點`

---

### Task 14: 文件與營運程序

**Files:** Modify `docs/deployment.md`（agent 模式段：新設定 3 枚、AGENT_WS_TOKEN 移除、healthz 語意變更）、Create `docs/superpowers/reviews/2026-08-XX-inc1-manual-test-plan.md`（人工測試流程，交使用者實測用——含：多人 token 簽發、雙帳號隔離驗證、kill switch 兩層、斷線補送收斂、failstop 演練、歷史 quarantine 清理指令、place unknown 人工終結程序 SQL）；Modify `CLAUDE.md`（若指令/設定有新增值得一句話的）。

- [ ] 寫文件 → fresh-context read-back 核對（含 spec §8 營運項全數覆蓋）→ commit `docs: Inc1 部署說明與人工測試流程`

---

## Self-Review 紀錄

- **Spec 覆蓋**：D1→T7；D2→T4；D3→T3；D4→T1/T8/T9/T10；D5→T1/T5；D6→T7/T11；D7→T2；D8→T11；D9→T7/T12/T14；D10→T6；D11→T9；G1→T8/T9/T10；G2→T12；G3→T11；S1/S2/S3 縫→T5/T6/T7；spec 測試 S#1-40 分配見各 task、T13 盤點兜底。
- **型別一致性**：`AgentCommand` 欄位（T1）與 T8/T10/T11 的 CAS/查詢條件同名；`commit_raw_callback` 簽名（T5）為 T7 per-slot worker 與 agent_ws 所用；`KillSwitchState.blocked`（T3）為 T8 admission 與 `_send_gate` 所用；`UpQueryResult/UpCommandRejected`（T2）為 T11/T12 所用。
- **依賴順序**：T1→T2→(T3,T4 可並)→T5→T6→T7→T8→T9→T10→T11→T12→T13→T14；T3/T4 互不依賴其餘按序。
