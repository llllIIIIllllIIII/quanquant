# 本機 Broker Agent — Increment 1（多人 simtrade）— 設計文件

**日期**：2026-08-06
**狀態**：設計 v8 — codex 第 7 輪 APPROVE 後，**2026-08-07 使用者拍板完成**：D3 改兩層 kill switch（per-user＋全站總閘）、D7 硬升 v2、D9 healthz 語意變更通過、其餘照案 → D3 delta 經 codex 聚焦覆核 → writing-plans
**前置**：Inc0 已完結併入 main（merge 5282c13，樹=9f9e23e，770 pytest 綠，人工 sim 實測全過）
**相關文件**：`docs/superpowers/specs/2026-08-04-local-broker-agent-design.md`（總體設計）、`docs/superpowers/handoffs/2026-08-06-local-broker-agent-inc1-handoff.md`（交接）
**範圍鐵則**：仍鎖 sim（協定/CLI/server/child 四層不開 real）；in-process 單人路徑不能壞；本期只到設計 spec 收斂，不寫實作計畫、不動碼。

---

## 1. 背景與目標

Inc0 交付了單人骨幹：一條靜態 token 的 WS 通道、單一 `app.state.agent_channel`、全域 adapter/RiskGuard/watchdog。Inc1 要把它變成**真多人**：幾個可信使用者各自在自己電腦跑 `quanquant-agent`、用自己的永豐帳號 simtrade，中央網站 per-user 隔離風控/資料/連線狀態。

同時，codex 對 Inc0 的 ACCEPT-DEFER 附了三個明文條件（real-mode 前必做，Inc1 正面處理）：

- **G1 command ledger + late-ack 冪等收斂**：修「ack 遺失後 placeholder 委託永不收斂」。
- **G2 durable 落地 fail-stop**：agent buffer 寫入失敗必須 fail-stop＋健康上報，不得繼續宣稱 healthy。
- **G3 unknown 配額自動解除**：恢復 in-process `_reconcile_unknown_quota` 的等價能力（需下行查詢口數）。

以及 Inc0 審查時標記、多人化必拆的三個縫：

- **S1 RawInbox 無 account/user scope 欄位**（`db/models.py:222-237`，靠全域換帳號 guard 規避）。
- **S2 換帳號 guard 是全域 unprocessed count**（`agent_ws.py:80-104`＋`_count_unprocessed_raw_inbox` L156-169），多人下互相誤擋。
- **S3 `adapter.account` mutable 單例**（`shioaji_adapter.py:228-234`），order_report mapper 讀它（L928），多 agent 會互相污染帳號對映。

**非目標**：real/CA、代客法遵、憑證落地保存（Inc2）、打包安裝檔、部署（spec 收斂＋實作完＋實測後另議）。

---

## 2. 現況關鍵事實（盤點結論，附 檔案:行號）

- **wiring**：`app.py:_start_order_subsystem` L274-394 依 `order_channel` 分流；agent 分支（L197-271）建 BrokerSupervisor/RiskGuard/AgentChannel/AgentNativeGateway/ShioajiAdapter(remote_gateway)/RawInboxWorker，塞 `app.state.agent_channel/order_service/order_risk_guard/order_inbox_worker/order_session_factory`；只跑 `run_agent_watchdog`（僅 `_retry_quarantined`，`watchdog.py:124-139`）＋一次性孤兒掃描；**沒跑** confirm-token 清理（in-process 有，`app.py:391-393`——Inc0 縫，Inc1 補）。
- **三段切**：place＝DB 決策（`shioaji_adapter.py:437-486`）→ native（`_do_place` L487-546，remote_gateway 注入點 L490-491）→ 寫回（L550-562）；cancel/update 同型。`_NativeGatewayLike` Protocol（L143-156）：`ready/place/cancel/update/trades_snapshot`。
- **鎖**：`BrokerSupervisor` 一顆 `asyncio.Lock`（`supervisor.py:25-46`）；agent 模式下 adapter 全部操作、`RawInboxWorker.run`（`inbox_worker.py:98`）、watchdog 兩工作（`watchdog.py:143,157`）都拿同一顆；`_reconcile_inner` 鎖內 await WS（`shioaji_adapter.py:352-363`）。
- **協定**（`agent_protocol.py`）：下行 DownPlace/DownCancel/DownUpdate/DownReconcile/DownReportAck/DownHealth（未使用）；上行 UpLogin（`protocol: Literal[1]` L68、`mode: Literal["sim"]`）/UpReport/UpCmdAck（`error_kind` 四值 L83）/UpHealth。
- **認證**：`x-agent-token` header vs `settings.agent_ws_token` 靜態單一密鑰 `compare_digest`（`agent_ws.py:32-42`）。
- **ack 紅線**：UpReport 在 `inbox_lock` 內 commit 完才回 DownReportAck（`agent_ws.py:118-125`）；agent 端 outbox `sent_at IS NULL` 補送、收 report_ack 才 `mark_sent`（`buffer.py:49-65`）。
- **cmd 逾時**：`AgentChannel.request` 逾時 raise（`agent_channel.py:89-92`）→ `_classify_place_failure` 判 `unknown` 保留配額（`shioaji_adapter.py:115-116`）；**ack 遺失即永久遺失**（UpCmdAck 不落 outbox、無重送）。
- **RawInbox**：欄位 id/kind/broker/payload/received_at/processed/quarantine/error/processed_at，**本身無去重鍵**；去重在下游 Deal 唯一鍵 `(broker,mode,account,trading_day,fill_id)`（`db/models.py:288-289`）。
- **RiskGuard**：唯一 instance 級可變狀態是 `_kill_switch`（`risk.py:70-79`）；owner 白名單/商品白名單/上限皆建構時固定；配額計數 DB 已 per-user keyed（`repository.py:256-260,620-629`）。
- **OrderSessionState**：ready/disabled/last_error/last_connected_at/reconnect_attempts（`session_state.py:9-17`）；`/healthz`：disabled→200、ready→200、否則 503（`health.py:29-44`）。
- **OrderEventHub**：全域無資料 ping 廣播，設計上不跨用戶洩漏（`order_events.py:9-11,30-39`）。
- **帳號系統**：cookie session（itsdangerous，`auth/tokens.py:35-49`）；`User.token_version` bump 使所有 cookie 失效；**無 per-user API token 前例**；最接近的是 ConfirmToken（簽名字串＋DB 列一次性 claim，`risk.py:90-100`＋`repository.py:545-556`）。
- **migration 慣例**：`db/migrate.py` `_MIGRATIONS = list[(table, column, ddl_type)]`，nullable ADD COLUMN 雙方言可攜（L5-6,34）；CHECK 用 `col IS NOT NULL AND col IN (...)`（L12-14）；新表走 `create_all`，既有表補欄位走 `ensure_columns`。
- **`_reconcile_unknown_quota` in-process 演算法**（`watchdog.py:161-201`）：無 ordno 且無 broker_order_id → failed＋release；有 ordno → 查 update 保留列、`_query_order_qty_blocking(ordno)` 比對改前/改後口數決定 confirm/release/跳過。agent 模式 Inc0 整段停用（L127-130）。

---

## 3. 架構總覽（Inc1）

```
使用者 A 電腦                     中央網站（單 uvicorn 進程）
┌──────────────┐  WS(token_A)   ┌────────────────────────────────────────┐
│ quanquant-agent│◀────────────▶│ AgentRegistry: user_id → UserAgentSlot │
│  outbox+ledger │               │  Slot_A: AgentChannel + Gateway +      │
└──────────────┘               │   Adapter(account_A) + SessionState +   │
使用者 B 電腦                     │   Supervisor鎖_A + InboxWorker_A +      │
┌──────────────┐  WS(token_B)   │   Watchdog_A                            │
│ quanquant-agent│◀────────────▶│  Slot_B: （同構，完全獨立）              │
│  outbox+ledger │               │ 共用：RiskGuard(全域 kill switch)、      │
└──────────────┘               │  OrderEventHub、DB、agent_commands 表    │
                                └────────────────────────────────────────┘
```

per-user 隔離單位＝**UserAgentSlot**；跨 user 完全無共享可變 runtime 狀態（除全域 kill switch 與 DB）。in-process 模式不建 registry，維持 Inc0 原樣。

---

## 4. 決策清單（每條含選項與採用方案；待使用者拍板）

### D1 registry 生命週期 — 採用：啟動時按 owner 白名單 eager 建 slot

- 選項 a）**eager**：lifespan 對 `ORDER_OWNER_USER_IDS` 每個 uid 建一個 slot（含 per-user 背景任務），offline 只是 `session_state` 未 ready。
- 選項 b）lazy：首次 WS 連線才建 slot。
- **理由**：owner 集合小且固定（settings `lru_cache`，改名單本就要重啟）；eager 讓 routes/watchdog 永遠查得到狀態、不必處理「slot 尚不存在」分支；offline 語意從開機起就明確。lazy 省的記憶體可忽略。
- slot 內容：`AgentChannel`、`AgentNativeGateway`、`ShioajiAdapter(remote_gateway)`、`OrderSessionState`、`BrokerSupervisor`（per-user 鎖）、`RawInboxWorker` task、agent watchdog task。
- `app.state.agent_registry` 取代 `app.state.agent_channel`；deps 層 `get_order_service` 等改為 user-aware：inprocess → 沿用現有 `app.state` 單例（路徑零改動）；agent → `registry[user.id]`（不在 owner 名單 → 現有的 assert_owner 403 語意不變）。

### D2 agent token — 採用：DB opaque token（新表 `agent_tokens`，存 hash、預設 TTL 30 天、rotation＝發新廢舊）

- 選項 a）itsdangerous 簽名 token（無 DB）：短 TTL 會殺掉無人值守重連（agent 斷線自動重連是 durable 收斂的前提；token 過期＝重連失敗＝buffer 積壓到人工介入）；長 TTL 則只能靠 `token_version` bump 撤銷，會連 web session 一起殺。
- 選項 b）**DB opaque token**：`secrets.token_urlsafe(32)` 明文只顯示一次；DB 存 `sha256` hash。可即時單獨撤銷、TTL 可長（涵蓋無人值守）、`last_used_at` 可觀測。
- **理由**：b 的撤銷粒度與長 TTL 同時成立；ConfirmToken 已有「DB 列管 token」前例。Inc1 已解鎖新表。
- 表：`agent_tokens(id PK, user_id index, token_hash unique, created_at, expires_at, revoked_at nullable, last_used_at nullable)`。
- 語意：每 user 同時只有一枚有效 token（簽發新枚即 revoke 舊枚——rotation 語意最簡）；簽發入口在 orders 頁（owner-only，`assert_owner`）；TTL 預設 30 天（新設定 `agent_token_ttl_days`）。
- 握手：`x-agent-token` → sha256 → 查表（未過期、未撤銷）→ 載 User 驗 `is_active` ＋ owner 白名單 → 綁 `user_id` → 取 registry slot → detach/attach（Inc0 generation 機制不變）。任一步失敗 close(1008)。
- **`AGENT_WS_TOKEN` 靜態密鑰整個移除**（正式環境未部署、無存量 agent，不留 break-glass 後門）。

### D3 kill switch scope — ✅ 使用者拍板（2026-08-07）：兩層＝per-user 開關＋保留全站總閘

- 選項曾為 a）per-user／b）全站單一；原建議 b，**使用者拍板改採 a＋保留全站**。
- **形狀**：RiskGuard 維持單一共享實例（盤點證實其餘狀態為無狀態政策＋DB per-user 計數，不需拆實例）；kill switch 狀態改為 `KillSwitchState`＝`global_on: bool`＋`per_user: dict[user_id, bool]`（in-memory，`order_kill_switch_initial` 初始化全站閘）。判定：`blocked(uid) = global_on OR per_user.get(uid, False)`。
- **兩層 gate 語意不變（I3）**：admission（`check_place`/`check_update`）與 `_send_gate` 第二層都改查 `blocked(user_id)`；取消單仍不受 kill switch（沿用既有語意）。
- **權限與 UI**：per-user 開關＝owner 只能翻**自己的**（server 端強制，非前端隱藏）；全站總閘＝沿用 Tier0 語意、任一 owner 可翻（火警拉桿原則），翻閘者記 audit。UI：orders 頁兩顆開關「我的急停」＋「全站急停」，各自顯示狀態與最後翻閘者。
- **route**：`POST /orders/kill-switch` 加 `scope` 參數（`self`｜`global`）。
- in-process 模式：單一 owner 下 `blocked()` 行為與現行單一 bool 等價，既有測試語意不變。

### D4 command ledger 放置與粒度（G1）— 採用：server 新表 `agent_commands` ＋ agent 端 buffer 加 `command_ledger` 表；只記三種 mutating op

- **粒度**：place/cancel/update 三種 mutating 指令入 ledger；reconcile/query_qty 是唯讀冪等，不入（逾時下輪重試即可）。
- **server 表**：`agent_commands(cmd_id PK(uuid), user_id index, kind, broker, account, mode, client_order_id nullable, ordno nullable, reservation_id nullable, payload TEXT, created_at, sent_at nullable, **transport_acked_at nullable, outcome IN ('ok','error','unknown') nullable, resolved_via IN ('ack','query_qty','report','manual') nullable, resolved_at nullable**, timeout_observed_at nullable, result TEXT nullable, expires_at)`。**終結模型是兩維（codex R2-1/R3-1）**：`transport_acked_at`＝agent 已回覆（transport 維度）；`outcome`＋`resolved_via`＋`resolved_at`＝業務維度。**resolved 唯一由 `resolved_at` 定義（codex R4-4）**：一般情況 outcome=ok/error 時同交易 resolved；cancel 允許 `outcome=unknown 且 resolved(via=report)`＝「曝險已由回報終結、指令本身效果不可知」的誠實紀錄；其餘 unknown＝未 resolved，由 unknown-resolver 或人工收斂。文中 acked_ok/acked_error/acked_unknown 為「transport_acked＋outcome=ok/error/unknown」的簡寫。**scope（broker/account/mode）與 reservation_id 建立時凍結**：scope 取自該 user 當下綁定帳號（codex R1-2）；reservation_id 關聯該指令的配額保留列，使 D8 的收尾保護可機械判定（codex R1-4）。
- **送單前持久化**：ledger insert 與該指令的 DB 決策段**同一交易**（place＝create_order＋reserve_quota＋insert ledger 一次 commit；cancel/update 同理），commit 後才下行。
- **agent 端**：buffer 新表 `command_ledger(cmd_id PK, kind, result TEXT, executed_at)`。收到 Down 指令：①查 ledger，命中 → **不重執行**，重送存檔 ack（outbox 以 `cmd_id` 關聯，未送列由泵補送、不重複 append，見 D11）；②未命中 → **核對指令 scope（codex R1-2）**：指令的 `account/mode` 與目前登入 scope 不符 → 回 `error_kind="scope_mismatch"` 不執行；③檢查 `expires_at`，過期 → 回 `error_kind="expired"` 不執行；④執行 native → **同一 SQLite 交易**寫 `command_ledger` ＋ append ack 事件進 outbox → 泵送出。
- **ack 走 outbox at-least-once**：UpCmdAck 增加 `event_id`，與 UpReport 共用 outbox 補送機制與 DownReportAck 確認（見 D7 協定）。ack 遺失問題就此消滅——重連即補送。
- **server 冪等收斂（late-ack applier，單一交易；codex R1-1）**：`agent_ws` 的 UpCmdAck handler 為唯一效果套用入口，且 **command CAS＋Order 寫回＋quota transition 在同一 DB 交易**（消滅「CAS 已 commit、效果未 commit 即崩潰」窗口）：**兩維各自 CAS（codex R4-2）**：transport 維 `WHERE cmd_id=? AND user_id=<連線認證 user> AND transport_acked_at IS NULL`（帶 user_id 防跨 user 終結他人指令，codex R1-5）；業務維（效果套用）`WHERE resolved_at IS NULL`——**只有業務維 CAS 命中者可套 Order/quota 效果**。一般 ack：兩維同一交易完成（outcome=ok/error, via=ack）；timeout ack：只寫 transport 維＋outcome=unknown 不 resolved（該 cancel 的 Order 若已終態 → 同交易直接 resolve via=report）；**遲到 ack 遇已 resolved（如 report 先收斂）→ 只補記 transport_acked_at 與原始 result，不改寫 outcome、不重套效果**；命中才在同交易套用效果（place：`set_order_ack` 補 ordno/broker_order_id＋confirm_quota；失敗分類照 Inc0 `_classify_place_failure` 語意）；未命中＝重複 ack → 回 DownReportAck 不重套；user 不符 → 拒絕＋告警、不 ack。**timeout/失敗路徑同樣經 ledger CAS 決勝負**：route 逾時要把 Order 標 unknown 前，先 CAS 寫 `timeout_observed_at`（非終態）於同一列——CAS 失敗（ack 已勝出）就放棄標 unknown、直接讀已落庫結果；timeout 先勝出、ack 後到 → applier 仍把 Order 由 unknown 收斂為 submitted/failed（狀態機明文允許此轉移）。**route 的 in-memory future 只在 applier commit 後 resolve**，route 不再自行套用任何效果。in-process 模式不經 applier，共用抽出的純函式 `apply_place_ack/apply_place_failure`（單一實作、兩處呼叫）；**交易所有權明文（codex R1-10）**：ledger 只由 agent 模式 orchestration 層寫入，in-process 的 RiskGuard 自行 commit 行為零變更，實作期加雙模式契約測試。
- **kind × outcome 轉移表（codex R2-4；含 R2-1 的 acked_unknown 定義）**：`error_kind="timeout"` → `acked_unknown`（**非終結**）；「確定未執行」（expired/scope_mismatch/failstop）與明確拒絕 → `acked_error`。逐格效果：

| kind | outcome | command | Order 效果 | quota/reservation 效果 |
|---|---|---|---|---|
| place | 成功 ack（含 late） | acked_ok | 補 ordno/broker_order_id；unknown→submitted | confirm |
| place | 明確拒絕 | acked_error | failed | release |
| place | timeout | acked_unknown | unknown（保留） | 保留——**永不自動 release**（收斂靠可關聯 reconcile 或人工；配額跨 trading_day 自然歸零） |
| place | expired/scope_mismatch/failstop | acked_error | failed | release |
| cancel | 成功 ack（含 late） | acked_ok | 記取消已受理；終態仍由回報推進（既有原則）；Order 已終態則 no-op | 無 |
| cancel | 明確拒絕（如 trade_not_found） | acked_error | 不改（交由 reconcile/回報收斂）＋audit | 無 |
| cancel | timeout | acked_unknown | 不改 | 無 |
| cancel | expired/scope_mismatch/failstop | acked_error | 不改 | 無 |
| update | 成功 ack（含 late） | acked_ok | 原子寫新 price/qty | confirm delta reservation |
| update | 明確拒絕 | acked_error | **不得標 failed**（整張單仍有效） | release delta |
| update | timeout | acked_unknown | 不改 | delta 保留（D8 保護） |
| update | expired/scope_mismatch/failstop | acked_error | 不改 | release delta |

- **unknown-resolver（codex R3-1：outcome=unknown 不是死路）**：unknown 指令**不重播 native**（可能已執行），由收斂路徑解決——**update**：per-slot watchdog 以 `query_qty(ordno)` 比對，qty==改後值 → 同一交易標 resolved（outcome=ok, via=query_qty）＋原子寫 Order 新 price/qty＋confirm delta；qty==改前值 → resolved（outcome=error, via=query_qty）＋release delta；其他 → 維持 unknown 下輪再試（G3 主場景）；**查無（委託已結案、不在 list_trades）→ 交給終態 resolver（見下）**。**cancel**：無 quota 效果——resolver 採 **state-based 週期掃描**（unresolved cancel × Order 已終態 → 同交易 resolve，`outcome=unknown, via=report` 誠實紀錄，codex R4-2/R4-4），不依賴「進入終態」單次事件。**resolver 一律以 `WHERE resolved_at IS NULL` CAS 取勝者**，與 transport applier 互不覆寫。**終態 resolver（codex R5-3/R6-2）**：Order 進入終態時，同一交易只處理該 Order 中**已確立執行結果不明**的 update——適用集合嚴格為 `resolved_at IS NULL AND transport_acked_at IS NOT NULL AND outcome='unknown'`；**created/sent（尚無 transport 回覆）不碰**——繼續等 ack／重連重播，其明確未執行 ack（expired/scope_mismatch/failstop/明確拒絕）仍依轉移表 release delta（quota 的 confirmed 是不可逆終態，不得被 resolver 搶先 confirm）。適用者：終態資訊可得最終 qty → 依二分收斂；無法判斷 → resolve（outcome=unknown, via=report）＋ **delta 保守 confirm、不 release**（低估可能已執行的增量比高估危險）；agent 永不回連的 created/sent 殘留走 place 同款人工終結程序。cancel 因此可維持不受單飛限制、也不會把 unresolved update 變成永久孤兒。**place（無 broker ID）**：無可關聯鍵 → 維持 unknown、永不自動 release，走**明文人工終結程序**（ops 文件＋admin 操作：標 resolved(via=manual)＋依實況 confirm/release）。**UpLogin 換帳號 guard 只擋「未 resolved 且有曝險」的指令（place/update）**，cancel 的 unknown 不擋。
- **update 單飛規則（codex R4-3）**：DB 決策段（同一交易）檢查——同一 Order 存在任何未 resolved 的 update 指令 → **拒絕新 update**（回「前一筆改單結果未定」）；cancel 不受此限（取消優先）。防止多筆 unresolved update 使 query_qty 的二分判定（改前值/改後值）失效、舊 reservation 永久卡死。**單飛由 DB 強制（codex R5-2/R6-1）**：partial unique index `UNIQUE(client_order_id) WHERE kind='update' AND resolved_at IS NULL`——鍵取**非空且全域唯一**的 `Order.client_order_id`（`models.py:253`），不用 nullable 的 ordno（兩方言 unique index 皆允許多筆 NULL、互斥會失效）；並發輸家吃 constraint violation → 整筆決策交易（reservation/audit/ledger）rollback → 回「前一筆改單結果未定」——不倚賴 read-committed 下的先查再插。**update admission 同時拒絕 `ordno IS NULL` 的 Order**（DownUpdate 協定需非空 ordno；拒絕發生在建立 reservation/ledger 之前）。流水線改單（order version chain）留待未來需要再議。
- **late-ack 解 quarantine**：applier 成功補 ordno 後，將該 user 的 `quarantine=True AND processed=False AND quarantine_reason='association_pending'` 列解除 quarantine（既有 `unquarantine` 機制、bounded per-user；**永久 dead-letter 不解**，見 D5 quarantine 分級），讓 worker 用新 ordno 重試匹配。
- **重連補送（限同 scope；codex R1-2）**：UpLogin 接受後，server 掃 `agent_commands WHERE user_id=? AND transport_acked_at IS NULL AND resolved_at IS NULL AND account=<本次綁定帳號>` 按建立序重新下行（agent ledger 去重；codex R4-2——已 resolved（如 report 先終結的 cancel）或 outcome=unknown 者不重播，native 可能已執行/已終結）；**他帳號的未 transport-ack 指令永不下行到不同帳號的 session**。UpLogin guard 增列第二條件：同 user 換帳號時若「原帳號尚有未 resolved 且有曝險（place/update）的指令」→ 拒登（先用原帳號連線收斂，或走人工終結程序）。補送完成後才觸發 login reconcile（ordno 補寫先落、快照匹配後到；皆冪等）。
- **過期語意（重要取捨）**：**server 不自主過期**未 ack 的指令（agent 才知道有沒有執行過——server 單方面判死會把「已在券商成交的單」記成 failed＋錯誤退配額）。過期只由 agent 拒絕執行時回報（`expired` error ack → failed＋release）。agent 永不回連的殘留：unknown 委託與配額保留至當日結束——配額本就按 trading_day 計（`repository.py:256-260`），跨日自然歸零；委託列留待人工/下次連線收斂。`expires_at` 預設 `created_at + agent_command_expiry_seconds`（新設定，預設 120s）。
- **殘留風險（明列不解）**：agent 在「native 執行完成」與「ledger+outbox 落地」之間崩潰（毫秒級窗口），重啟後重執行同一 place 會重複下單。Shioaji place 非冪等、無 server 可驗的自然冪等鍵。緩解：窗口極小＋sim-only＋登入 reconcile 快照會把多出來的委託以孤兒回報面世（既有孤兒偵測只記錄不自動處理）。real-mode 前若要收斂，再議 custom_field 冪等鍵可行性。

### D5 RawInbox scope（S1/S2/S3 一次解）— 採用：上行 envelope 帶 immutable account/mode，server 落欄位，worker/guard/mapper 全改吃列上 scope

- **協定**：UpReport 增必填 `account`、`mode`（agent 在 **callback 落 outbox 當下**蓋章——outbox 加 `account/mode` 欄——換帳號後補送的舊事件仍帶舊帳號，immutability 在來源端保證）。
- **server 欄位**：`raw_inbox` 走 `_MIGRATIONS` 加三個 nullable 欄：`user_id INTEGER`、`account VARCHAR`、`mode VARCHAR`。agent 模式由 WS handler 蓋 `user_id`（連線認證身分）＋ message 的 account/mode；in-process 模式 `_persist_raw` 蓋 `account=self.account, mode=self._mode`，`user_id` 留 NULL（in-process 的 user 歸屬本就由 ordno 匹配 Order 決定，不變）。
- **S3 拆除**：`_map_order_report` 改吃**列上蓋章的 account**，不再讀 `adapter.account`（`shioaji_adapter.py:928` 的讀取點移除）；deal mapper 本就吃 payload 的 `account_id` 不受影響。adapter.account 本身仍存在，但已是 per-user slot 內的狀態（縫的「單例」性質消失），且唯一寫入點仍在該 user 的 `inbox_lock` 內。
- **S2 guard per-user 化**：UpLogin guard 改查「該 user 的 unprocessed 列中，`account` 與來登帳號不同**或為 NULL**」的數量，>0 才擋。同帳號重連永不誤擋；不同 user 完全互不影響；蓋章前的歷史列（NULL）維持保守擋下。
- **worker per-user 化**：agent 模式每 slot 一個 `RawInboxWorker`，批次查詢加 `WHERE user_id = slot.user_id`，拿**自己 slot 的 supervisor 鎖**；in-process 維持單一 worker、不加 user filter（行為不變）。agent 模式下 `user_id IS NULL` 的列（歷史遺留）無 worker 認領——面世方式見 D9 營運段。
- **逐訊息 scope 驗證（codex R1-5）**：UpReport 的 envelope `account` 必須屬於**該連線 user 的 binding**（查 `agent_account_bindings.user_id == 連線 user`——容許舊帳號重播、不容許冒用他人帳號），不符 → 落地為 quarantine 列（仍 commit 後 ack，維持 I1/I4 並止住無限重送）＋告警；deal payload 的 `account_id` 必須等於列上蓋章 `account`，不符 → quarantine（**mapper 一律以列 scope 為權威**）；worker 匹配到 Order 後**強制 `order.user_id == row.user_id`——僅在 `row.user_id IS NOT NULL` 時執行（codex R2-2）**：in-process 的 NULL 列沿用現行「由 Order 推導 user」行為零變更，且 NULL 列只允許 in-process worker 處理（agent per-slot worker 永不撿 NULL 列），不符 → quarantine＋告警，絕不寫入。
- **quarantine 分級（codex R2-6）**：`raw_inbox` 加 `quarantine_reason`——`association_pending`（可重試：如 ordno 尚未補寫）才允許 watchdog／late-ack 解除；`scope_violation`／`payload_mismatch`／`user_mismatch` 為**永久 dead-letter**：標 `processed=True`＋`quarantine=True`＋reason（退出所有 unprocessed 計數與換帳號 guard，防止永久擋登入與告警洪水；保留稽核證據），OpsAlerter 告警一次（同 key 節流）。
- **唯一 scoped staging API（codex R1-6）**：所有 RawInbox 寫入——WS handler、in-process `_persist_raw`、**remote reconcile 的快照落列路徑（`_stage_reconcile_results`）**——一律走同一個必帶 `user_id/account/mode` 參數的 staging 函式；remote reconcile 傳 slot 的 user/account/mode，in-process 明確傳 `user_id=None`。**staging 與 reconcile cursor 推進同一交易**，防「列無人認領但 cursor 已前進」的永久漏接。

### D6 鎖拆分 — 採用：BrokerSupervisor per-slot；watchdog per-slot；confirm-token 清理補回 agent 模式（全域一個）

- per-user supervisor 鎖 → 使用者之間零 head-of-line blocking；`_reconcile_inner` 鎖內 await WS 的行為保留，但爆炸半徑縮成單一 user 自己（與 Inc0 單人行為等價，可接受，明列）。
- agent watchdog per-slot：`_retry_quarantined`（只解自己 user 且 `quarantine_reason='association_pending'` 的列，codex R2-6）＋ `_reconcile_unknown_quota`（G3，見 D8）。
- confirm-token 清理是純 DB 全域工作，agent 模式 lifespan 補一個全域 task（Inc0 漏）。

### D7 協定版本 — 採用：硬 bump `PROTOCOL_VERSION = 2`，不做 v1 相容

- 變更集：UpReport +`account/mode`（必填）；UpCmdAck（**僅 mutating 指令**）+`event_id`（走 outbox）＋`error_kind` 增 `"expired"`/`"scope_mismatch"`；**新增 volatile `UpCommandRejected(cmd_id, error_kind="failstop")`（codex R2-5：無 event_id、不進 outbox、不回 DownReportAck——buffer 已壞時仍能拒絕；server 收到走 applier CAS 落 acked_error；訊息遺失靠重連重播→agent 再拒→收斂）**；**新增 `UpQueryResult`（read-only 結果專用：reconcile 快照與 query_qty——volatile、無 event_id、不進 outbox、只 resolve 同 user/generation 的 pending future、不回 DownReportAck；codex R1-7，避免「CAS 查無 command＝重複」把查詢結果丟掉）**；UpHealth +`status: Literal["ok","failstop"]`＋`detail`＋**`health_epoch`（單調遞增，codex R2-3；不帶 generation——由 server 連線 handler fencing，codex R3-2）**；UpLogin `protocol: Literal[2]`＋**`health_epoch` 基準宣告（codex R3-2）**；下行 DownPlace/DownCancel/DownUpdate +`account/mode/expires_at`（scope 凍結，codex R1-2）；新增 `DownQueryQty(cmd_id, ordno, mode)`（帶 mode 維持協定層鎖 sim）；DownReportAck 只 ack durable event（report 與 cmd_ack 兩種）。
- **理由**：正式環境未部署、存量 agent 只有開發者自己——雙版本相容是純負債。server 收到 protocol=1 → close(1008) 附「請更新 agent」reason；agent 端 pin 2。
- mode 仍是 `Literal["sim"]` 全協定不動。

### D8 unknown 配額自動解除（G3）— 採用：`DownQueryQty` 下行＋per-slot watchdog 恢復 in-process 等價演算法

- gateway 介面加 `query_qty(ordno) -> int`（`_NativeGatewayLike` 與 `AgentNativeGateway` 同步加）；child 子程序加同名 op（讀 `list_trades` 現快照，等價 `_query_order_qty_blocking`）。
- per-slot watchdog 週期跑 agent 版 `_reconcile_unknown_quota`：slot 未 ready 直接跳過本輪；**收尾保護全面化（codex R1-4）：凡與某 Order 或某 reservation 關聯的 mutating 指令（place/cancel/update）尚未 **resolved**（`resolved_at IS NULL`——含 created/sent 與 timeout 後的 outcome=unknown；timeout ack 不是業務終結，codex R2-1/R4-4），watchdog 對該 Order 與該 reservation 一律不 confirm/不 release——不限「無 ordno」分支，update 的 delta reservation 同樣受保護**（ledger 列存 `reservation_id`，關聯可機械判定，防止 query_qty 先看到改前口數就 release、稍後 update 成功 ack 卻無法 confirm 的少算）。收尾只允許三個入口：ledger applier（ack 到達同交易收尾）、**unknown-resolver**（D4：query_qty 原子收斂 update、回報終態收斂 cancel、人工終結 place，codex R3-1），或 watchdog 在**同一交易內先驗證「無未 resolved 的關聯指令」**再 confirm/release。ledger 已 resolved 或查無 ledger 列（異常）才走 in-process 原分支；**place 的 outcome=unknown 且無 broker ID → 永不自動 release**（人工終結程序，見 D4）。
- 與 D4 CAS 共用冪等保證：release/confirm 均經既有 reservation 狀態機，重複套用無效果。

### D9 offline 處理、UI、healthz — 採用：per-user badge＋token 管理段＋healthz 語意改「子系統就緒即 ready」

- **offline 擋新單**：place/cancel/update route 先查 slot `session_state.ready`，未 ready 直接回「你的 agent 未連線」錯誤（fail-fast，不進 DB 決策段不動配額）；`_send_gate` 第二層 unavailable→failed＋退配額語意保留（縱深不變）。
- **UI**：orders 頁 badge 改讀自己 slot 的狀態（`orders_agent_status` route per-user 化）；新增「Agent token」管理段（owner-only）：簽發（明文顯示一次）、rotation（發新廢舊）、顯示 last_used_at/expires_at。kill switch 控制改兩顆（D3 拍板）：「我的急停」（只能翻自己）＋「全站急停」（任一 owner 可翻），皆 owner-only＋audit。
- **healthz**：agent 模式下「有 user 的 agent offline」是常態（使用者關筆電），**不得**觸發 503。語意改為：wiring 完成即 ready（200），per-user 連線狀態只進 UI 與 `orders_agent_status`，不進 healthz 判定。in-process 模式 healthz 判定不變。此為語意變更，明列拍板。
- **G2 fail-stop 狀態機（codex R1-3，跨程序完整鏈）**：①SDK child 的 callback 落地失敗（含退化寫入亦失敗）→ 經 IPC 通知父程序＋寫**獨立 sentinel 檔**（不依賴已壞的 buffer）→ 父程序 latch `failstop`（durable，重啟仍在效，直到探針通過）；②latch 期間父程序**在每次 native 呼叫前檢查**，拒絕 place/cancel/update——failstop ack 走 best-effort **直送 WS**（不經可能已壞的 outbox），送不出就斷線交給 lease 判定；③server 端 **heartbeat lease**（新設定 `agent_health_lease_seconds`，預設 90）：超過 lease 未收到 `UpHealth(status="ok")` → slot 標 not-ready 擋新單，**WS 連線存活不等於健康**；④解除條件＝storage probe 通過（對同一 buffer 寫入→commit→讀回）才准回報 `status="ok"`；⑤**健康狀態單調性（codex R2-3/R3-2）**：agent 持久化 `health_epoch`，latch failstop 時 +1；**generation 不進 payload**——server 由連線 handler 以自己的 `my_generation` 對「該連線收到的所有訊息」fencing（沿用 Inc0 模型，agent 無需知道 server generation）；**UpLogin 宣告當前 `health_epoch` 作為本 session 基準**（buffer 重建歸零由重宣告吸收、不會永久拒收），server per-session 追蹤已見最大 epoch——failstop 立即生效，**epoch 較小的 ok 一律忽略**，recovery ok 必須引用當前 failstop epoch；⑥ **UpLogin 後 slot 進 `pending_health`**，由目前連線收到有效 ok 才 ready（廢除「登入即 ready」）；⑦ agent 端 recovery lock **只包本機原子轉移**（probe 通過→確認無更新失敗→清 latch/sentinel→取 (epoch,status) snapshot），**釋放 lock 後才 await WS send（codex R3-3）**——failstop latch／epoch++ 永不被網路 I/O 阻塞；⑧**health 出隊契約與 G2 安全語意界定（codex R4-1/R5-1）**：health frame 由**單一序列化 sender** 依序送出，每 frame 出隊前在 recovery lock 下重驗 (epoch,status,latch)、失效即丟棄（減少 stale frame；**已交付 wire 的 frame 無法撤回**）。因此 G2 語意明訂為三條可實現保證：(a) **權威安全點在 agent**——latch 後 native gate 立即拒絕一切 mutating 指令，server 短暫樂觀 ready **絕不會變成 native 執行**（下行指令到 agent 一律先過 latch 檢查）；(b) server ready 是**容許傳播延遲的樂觀值**——收到 failstop、斷線、lease 過期三者任一立即 not-ready；(c) **單調性**——server 一旦見過較大 epoch，較舊 ok 永不恢復 ready。「server 在本機失敗瞬間絕不短暫 ready」在非同步網路下不可實現，不以有限個 ok 假裝達成（不採兩-ok 規則）。per-user UI/route gate 全程反映 failstop；全域 /healthz 維持 200。failstop 期間拒新指令走 volatile `UpCommandRejected`（見 D7，codex R2-5）。
- **OpsAlerter**：新增事件＝agent failstop 上報（G2）、scope 驗證失敗 quarantine（R1-5）；連線/斷線不告警（常態雜訊）。quarantine/漂移告警照舊。
- **營運**：本機 quanquant.db 的 5 筆歷史 quarantine 列（user_id NULL）在 agent 模式無 worker 認領、且會觸發 guard 的 NULL 保守擋——文件寫明人工清理指令，不做自動遷移。

### D10 帳號↔使用者綁定唯一性（新發現，多人才出現的洞）— 採用：新表 `agent_account_bindings` 先綁先贏

- 問題：回報→委託匹配鍵是 `(broker,account,mode,ordno)`（`db/models.py:249-250`），**不含 user**。若 user A 與 user B 先後綁同一個永豐帳號，B 的回報會匹配到 A 的委託列，跨 user 資料互染。
- 方案：`agent_account_bindings(broker, account, user_id, bound_at；UNIQUE(broker, account))`——**綁定不分 mode（codex R2-7）**：I9 語意＝帳號整體屬一人，避免「同 user 的 sim/real 兩列撞 PK」與 mode 欄語意含糊；Inc1 只會有 sim 登入，但綁定與檢查涵蓋全部 mode。UpLogin 時 upsert——該 account 已綁其他 user → 拒登 close(1008)（訊息說明找管理員）；同 user 重複綁定 no-op。解綁走人工 DB/後續 admin UI（Inc1 不做 UI）。
- **既有資料 backfill（codex R1-8/R2-7）**：啟用 agent 模式時自既有 `Order` 資料 backfill binding（distinct `(broker, account) → user_id`，**掃全部 mode 合併 ownership**）；同一 account 歷史上屬於多個 user → **fail closed**（agent 模式拒啟、要求人工裁決），不能讓「先綁先贏」覆蓋既有 ownership；UpLogin 綁定交易除查 binding 表外**同時核對歷史 Order ownership**（該 account 存在他人的 Order → 拒登），不得只信新表。
- 這是 D5 蓋章之外必要的第二道防線：蓋章解決「列是誰的」，綁定唯一性解決「帳號只能是一個人的」。

### D11 agent 端 buffer schema 升級 — 採用：`meta` 表加 `schema_version`，不相容即重建（僅限空 outbox）

- outbox 加 `account/mode/cmd_id(nullable)` 欄＋新 `command_ledger` 表；**每個 cmd 至多一筆未送 ack**（append 前以 `cmd_id` 查未送列，有則不重複 append；codex R1-9）。`meta(key,value)` 已存在（`buffer.py:12-25`），寫入 `schema_version=2`。啟動時版本不符：outbox 有未送列 → 拒啟並提示（防丟事件）；乾淨 → 重建 schema。無存量部署，簡單粗暴即可。

---

## 5. 不變量表（Inc0 五條跨網路不變量 → Inc1 增補）

| # | 不變量 | Inc1 保證方式 |
|---|---|---|
| I1 | 零丟單（callback 落地才返回） | 不變：agent outbox 同步落地＋at-least-once＋Deal 去重。**新增**：落地失敗 → G2 fail-stop，禁止假 healthy。 |
| I2 | native 單一序列化 | 不變：agent 端單實例本地鎖。server 端序列化縮為 per-user supervisor 鎖（跨 user 本無共享 native）。 |
| I3 | kill switch 雙層 gate | 不變：RiskGuard admission＋`_send_gate` 下行前。開關為兩層（per-user＋全站總閘，D3）：`blocked(uid)=global OR per_user`。 |
| I4 | commit 後才 ack | 不變，且**擴展到 cmd_ack**：server applier CAS 落庫成功才回 DownReportAck。 |
| I5 | 配額/confirm token/audit 全留 server | 不變。 |
| I6（新） | **每筆 mutating 指令恰好收斂一次**：送前持久化（同交易）→ agent ledger 去重 → ack at-least-once → server「CAS＋效果」同一交易至多套用一次；timeout 與 ack 以同一 ledger 列 CAS 決勝負、晚到方不得覆蓋 → ∴ 效果恰好一次（executed-but-unrecorded 毫秒窗口除外，明列殘留） | D4 |
| I7（新） | **回報歸屬不可變**：account/mode 在事件產生端蓋章，任何重送/換帳號/重連不改變歸屬 | D5 |
| I8（新） | **跨 user 隔離**：任一 user 的 offline/quarantine/慢操作不影響其他 user 的登入、下單、回報處理 | D1/D5/D6/D10 |
| I9（新） | 一個 broker account 至多屬於一個 user（含歷史 Order ownership，backfill＋登入雙查） | D10 |
| I10（新） | **scope 逐訊息驗證**：mutating 指令 scope 建立時凍結、agent 執行前核對；上行 account 必屬該 user 的 binding；CAS 帶 authenticated user_id | D4/D5 |

---

## 6. 測試重點（設計驗收清單，供之後 writing-plans 展開）

1. late ack：place 逾時判 unknown → 重連補送 ack → ordno 補寫、quota confirm、相關 quarantine 解除、狀態收斂（G1 主場景）。
2. 重複 ack（同 cmd_id 兩次）→ 第二次 no-op（CAS 驗證）。
3. 重連補送指令 → agent ledger 命中不重執行、重回存檔 ack。
4. 過期指令 → agent 拒執行回 expired → server failed＋release；**過期但 ledger 命中 → 仍回存檔 ack（順序：先查 ledger 再查過期）**。
5. server 不自主過期：agent 永不回連 → 委託維持 unknown、配額跨 trading_day 自然歸零。
6. G2：buffer 寫入失敗 → agent 拒新指令（failstop ack）＋ UpHealth(failstop) → server mark_unhealthy＋OpsAlerter＋UI 紅badge；恢復後解除。
7. G3：unknown 委託有未終結 ledger 列 → watchdog 跳過；ledger 終結後 → query_qty 比對收斂（confirm/release 兩分支）。
8. 跨 user 隔離：A 有 quarantine/unprocessed 列 → B 登入不受擋；A offline → B 下單不受影響；A 的 reconcile 慢 → B 的 place 不排隊。
9. 換帳號 guard per-user：同帳號重連不擋；同 user 換帳號且有未處理列 → 擋；NULL 蓋章歷史列 → 擋。
10. D10：B 綁 A 已綁的帳號 → 拒登。
11. token：過期/撤銷/rotation 後舊 token → close(1008)；`last_used_at` 更新；非 owner user 拿有效 token → 拒。
12. 協定：protocol=1 → 拒；UpReport 缺 account/mode → validation error。
13. in-process 迴歸：既有 770 測試全綠、不改任何 in-process 行為（healthz/watchdog/寫回路徑）。
14. mapper 吃列上 account：換帳號後補送的舊事件回報歸屬舊帳號（I7）。
15. R1-1 交錯：ack 先落庫、route timeout 後到 → Order 不得改回 unknown；timeout 先標 → late ack 把 unknown 收斂為 submitted/failed。
16. R1-2 scope：換帳號後重播原帳號指令 → 不會下行；原帳號有未終結指令時換帳號 → 拒登；agent 收到 scope 不符指令 → `scope_mismatch` ack → server failed＋release。
17. R1-5 偽造 scope：UpReport 帶非本 user binding 的 account → quarantine＋告警；payload `account_id` ≠ row.account → quarantine；匹配 Order 後 `order.user_id` ≠ `row.user_id` → quarantine。
18. R1-6：remote reconcile 落列帶 user/account/mode，且與 cursor 推進同一交易（模擬中途失敗 → 列與 cursor 同時回滾）。
19. R1-3 G2 全鏈：child 落地失敗 → 父 latch failstop → 拒新指令（直送 failstop ack）→ lease 過期 slot not-ready → storage probe 通過才恢復 ready。
20. R1-8：既有 Order 的 account 被其他 user 嘗試綁定 → 拒登；同 account 歷史多 user → agent 模式 fail closed。
21. R1-4：unknown 的 update 指令未終結 → watchdog 不 release delta reservation；ack 到達 → applier confirm；expired ack → release。
22. R2-1：place timeout → command=acked_unknown → watchdog 永不自動 release（含跨日）；配額由 trading_day 歸零吸收。
23. R2-2：in-process 模式 `user_id IS NULL` 列處理行為與現行完全一致；agent per-slot worker 不撿 NULL 列。
24. R2-3：failstop 後遲到的舊 ok（epoch 較小）不恢復 ready；UpLogin 後未收本 generation ok 前 slot 維持 pending_health 不 ready。
25. R2-4：cancel late ack 不誤改 Order 終態；update 明確拒絕只 release delta、Order 不標 failed。
26. R2-5：failstop 拒絕走 volatile UpCommandRejected → server applier 落 acked_error＋依轉移表處置；訊息遺失 → 重連重播 → 再拒 → 收斂。
27. R2-6：scope_violation dead-letter（processed=True）不被 watchdog/late-ack 解除、不計入換帳號 guard、告警節流一次。
28. R2-7：同 account 同 user 的多 mode Order backfill 合併為一列；任一 mode 屬不同 user → agent 模式 fail closed。
29. R3-1：update outcome=unknown → query_qty 兩分支原子收斂（改後值→confirm＋寫 Order；改前值→release）；place unknown 不被 resolver 觸碰、永不自動 release；cancel unknown 不擋換帳號 guard。
30. R3-2：pending_health 由連線 fencing 判定（payload 無 generation）；buffer 重建 epoch 歸零 → UpLogin 重宣告基準 → 健康訊息不被永久拒收；跨 task 亂序的舊 ok（epoch 較小）不恢復 ready。
31. R3-3/R4-1/R5-1：WS send 卡住 → 新 failstop 立即 latch＋epoch++；出隊重驗丟棄排隊舊 ok；**transient 樂觀 ready 期間下行的指令被 agent latch 拒絕（安全鏈驗證）**；server 見過較大 epoch 後舊 ok 永不恢復 ready。
32. R4-2 四交錯：ack 先／report-resolver 先／兩者並發／之間斷線重連——效果恰套一次、outcome 不被改寫、已 resolved 不重播。
33. R4-3：U1 timeout（unresolved）→ U2 被拒；U1 經 query_qty resolved 後 U2 可送；cancel 不受單飛限制。
34. R4-4：cancel unknown × Order filled → resolve(outcome=unknown, via=report)；audit 不偽稱成敗；D8 以 resolved_at 判定、不再保護。
35. R5-1：failstop／斷線／lease 過期任一 → 立即 not-ready；恢復＝probe 通過後的有效 ok（單調 epoch）。
36. R5-2：兩並發 update 同一 Order → 恰一個成功、輸家整筆決策交易 rollback（constraint violation）＋「前一筆改單結果未定」；SQLite/Postgres 兩方言皆驗。
37. R5-3：U1 unknown → cancel 成功 → Order 從 list_trades 消失 → 終態 resolver 收斂 U1（無法判斷時保守 confirm 不 release）＋換帳號 guard 解除。
38. R6-1：`ordno IS NULL` 的 Order 改單被 admission 拒絕（不建 reservation/ledger）；兩並發 update 撞 client_order_id 單飛 index → 恰一成功、輸家 rollback。
39. R6-2：終態 report 先到、update 仍 created/sent → resolver 不碰；隨後 expired/failstop/明確拒絕 ack → release delta；只有 transport_acked＋outcome=unknown 者由終態 resolver 保守 confirm。
40. D3 兩層 kill switch：A 開自己的急停 → A 新單被擋、B 不受影響；全站閘開 → 全員被擋；`scope=self` 無法影響他人（server 端強制）；取消單不受兩層開關影響；in-process 單 owner 行為與現行等價。

---

## 7. 設定與 schema 變更清單

- 新表：`agent_tokens`、`agent_commands`（含 broker/account/mode/reservation_id/timeout_observed_at；transport_acked_at＋outcome(ok/error/unknown)＋resolved_via＋resolved_at 兩維終結模型；update 單飛 partial unique index `UNIQUE(client_order_id) WHERE kind='update' AND resolved_at IS NULL`）、`agent_account_bindings`（UNIQUE(broker,account) 不分 mode；啟用 agent 模式時自既有 Order backfill、衝突 fail closed）（`create_all` 自建）。
- 既有表加欄（`_MIGRATIONS`）：`raw_inbox` +`user_id INTEGER`/`account VARCHAR`/`mode VARCHAR`/`quarantine_reason VARCHAR`。
- agent 端 buffer：outbox +`account/mode/cmd_id`；新 `command_ledger`；failstop sentinel 檔（buffer 之外）；`meta.schema_version=2`。
- 新設定：`agent_token_ttl_days`（預設 30）、`agent_command_expiry_seconds`（預設 120）、`agent_health_lease_seconds`（預設 90）。
- 移除：`agent_ws_token` 設定與其驗證路徑。
- 協定：`PROTOCOL_VERSION = 2`（新訊息 `UpQueryResult`/`UpCommandRejected`；UpHealth 帶 `health_epoch`）。

## 8. 殘留風險與明確再議（不藏）

1. **executed-but-unrecorded 窗口**（D4）：native 完成→ledger 落地之間崩潰會重複下單。sim-only 接受；real 前再議 custom_field 冪等鍵。
2. **per-user 鎖內 await WS**（D6）：單 user 自己的 reconcile 仍會排隊自己的下單，與 Inc0 行為等價；若實測體感差，Inc2 再拆。
3. **healthz 語意變更**（D9）：agent 模式不再反映個別連線健康——監控面要知道這件事。
4. **歷史 NULL 列**（D9）：人工清理，不自動遷移。
5. **owner 名單改動需重啟**（D1）：沿用 settings 現狀，Inc1 不做熱載。
6. **實作期注意（codex R2-8）**：child 的 failstop IPC 不得混用現行 RPC pipe（rpc_id 不符會被當遲到 reply 丟棄、callback thread 併發寫）——用獨立單向 channel 或有鎖多工 dispatcher。
7. **實作期注意（codex R2-9）**：outbox「每 cmd 至多一筆未送 ack」用同一 SQLite 交易 check-and-insert＋未送 ack 的 `cmd_id` partial unique index 保證。
8. **實作期注意（codex R4-5）**：`agent_commands` 合法狀態組合加 CHECK（resolved → resolved_via/resolved_at 皆非空；via=ack → transport_acked_at 非空），SQLite/Postgres 雙方言一致驗證。
9. **實作期注意（codex R6-3）**：constraint violation 須精確辨認單飛 index 才轉「前一筆改單結果未定」，其他 IntegrityError 上拋（沿用 `repository.py:118` create_order 撞鍵精確化模式）；雙方言 partial index 沿用 `models.py:355` 的 `sqlite_where`＋`postgresql_where` 範式。

---

## 9. codex 覆核歷程

- **Round 1（2026-08-07）：REVISE**——5 BLOCKER（R1-1 live-ack×timeout 競態與 CAS/效果原子性、R1-2 指令 scope 未凍結會在換帳號後打錯帳號、R1-3 G2 缺跨程序 fail-stop 鏈、R1-4 update reservation 可被 watchdog 搶先釋放、R1-5 上行未逐訊息驗 user/account）＋2 HIGH（R1-6 reconcile staging 無 scope 且 cursor 先行、R1-8 D10 未 backfill 既有 ownership）＋1 MEDIUM（R1-7 read-only ack 語意未閉合）＋2 LOW（R1-9 outbox ack 關聯鍵、R1-10 in-process 交易所有權）。**v2 已全數修訂納入**，修訂處以 `codex R1-n` 標注。
- **Round 2（2026-08-07）：REVISE（明顯收斂）**——R1 十項中 9 項 RESOLVED、R1-3 PARTIAL（由 R2-3/R2-5 收尾）。新發現 3 BLOCKER（R2-1 timeout ack 被誤當終結→watchdog 誤放已送達 place、R2-2 NULL user 強制相等會打爆 in-process 回報、R2-3 健康狀態非單調→舊 ok 誤恢復）＋2 HIGH（R2-4 缺 kind×outcome 轉移表、R2-5 failstop ack 與 durable 契約矛盾）＋2 MEDIUM（R2-6 quarantine 未分級、R2-7 綁定鍵維度不一致）＋2 LOW 實作期（R2-8 IPC 通道、R2-9 outbox 約束）。**v3 已全數修訂納入**，修訂處標 `codex R2-n`。
- **Round 3（2026-08-07）：REVISE（再收斂）**——R2 九項中 7 項 RESOLVED、R2-1/R2-3 PARTIAL。新發現 2 BLOCKER（R3-1 acked_unknown 是不可重播、不可收尾的死路——與 G3 衝突；R3-2 UpHealth 的 generation 無 wire 來源——slot 會永卡 pending_health）＋1 HIGH（R3-3 recovery lock 包 WS send 會延遲新 failstop latch）。**v4 已全數修訂納入**：兩維終結模型（transport_acked × outcome/resolved）＋unknown-resolver（query_qty 收斂 update、回報收斂 cancel、人工終結 place）、連線 handler fencing＋UpLogin 宣告 epoch 基準、recovery lock 只包本機原子轉移。標 `codex R3-n`。
- **Round 4（2026-08-07）：REVISE（持續收斂）**——R3-2 RESOLVED、R3-1/R3-3 PARTIAL。新發現 3 BLOCKER（R4-1 舊 recovery-ok 的 wire ordering 可短暫假 healthy、R4-2 resolver 與 transport applier 缺獨立 CAS 維度會互相覆寫/重播已終結指令、R4-3 同 Order 多筆 unresolved update 使 query_qty 二分判定永久卡死）＋1 MEDIUM（R4-4 cancel via=report 無誠實 outcome 可表示）＋1 LOW 實作期（R4-5 狀態組合 CHECK）。**v5 已全數修訂納入**：兩維各自 CAS＋replay 加 resolved 條件、cancel state-based resolver、update 單飛規則、health 單一 sender＋出隊重驗＋恢復需連續兩 ok、resolved 唯一由 resolved_at 定義。標 `codex R4-n`。
- **Round 5（2026-08-07）：REVISE**——R4-2/R4-4 RESOLVED、R4-1/R4-3 PARTIAL。新發現 3 BLOCKER：R5-1 已上 wire 的舊 ok 無法撤回（兩-ok 規則不成立）→ **重定義 G2 為可實現語意**（權威安全點在 agent latch、server ready 是容許傳播延遲的樂觀值、epoch 單調）；R5-2 單飛缺 DB 線性化點 → partial unique index 強制；R5-3 cancel 使 unresolved update 失去 query_qty 關聯 → 終態 resolver（無法判斷時保守 confirm）。**v6 已全數修訂納入**，標 `codex R5-n`。
- **Round 6（2026-08-07）：REVISE（G2 閉合）**——R5-1 RESOLVED（含 R4-1/R3-3/R2-3/R1-3 全鏈 RESOLVED，G2 依重定義語意閉合）。剩 2 BLOCKER：R6-1 單飛 index 用 nullable ordno 互斥失效 → 改鍵 `client_order_id`＋admission 拒無 ordno 的 Order；R6-2 終態 resolver 會搶先明確未執行 ack 造成不可逆錯誤 confirm → 適用集合限縮為 transport_acked＋outcome=unknown。＋1 LOW 實作期（R6-3 IntegrityError 精確辨認）。**v7 已全數修訂納入**，標 `codex R6-n`。
- **Round 7（2026-08-07）：APPROVE**——R6-1/R6-2 覆核皆 RESOLVED、無新發現；G1、重定義後的 G2、G3 全部閉合，多人隔離與 in-process 零變更維持成立。設計收斂完成。
- **2026-08-07 使用者拍板**：D3 改兩層（per-user 開關＋保留全站總閘，v8 已更新）；D7 硬升 v2、D9 healthz 語意變更照案通過；其餘 8 條照預設方案（D2 TTL 30 天/單枚/移除舊密鑰、D4 配額保留至當日收盤、D8 保守 confirm、D10 一人一帳號先綁先贏等）。設計定案 → writing-plans。

---

## 10. 實作實現註記（2026-08-08）

> 版本說明：本檔的 c1657de commit 除新增本節外，亦一併入庫了 2026-08-07 使用者拍板當時已改寫、但尚未 commit 的 §1-9 內容（狀態列、D3 兩層 kill switch 整節、D9 句尾、I3 表格列）——那些改寫屬 v8 拍板修訂（見 §9 拍板紀錄），非本節所屬 Task 14 的產出。

Task 1-13 實作期間，審查（codex／人工）已裁定接受下列與本 spec 字面描述的既裁決偏離；本節
只記錄「實際落地是什麼、為何被接受」，**不改動上方 §1-9 的既有文字**。完整過程見
`.superpowers/sdd/progress.md`（Inc1 段）與各自的 `task-N-report.md`。

**(a) D9 offline fail-fast 落在 adapter 層，而非 route 層（Task 7 裁決）**

§4 D9 原描述「place/cancel/update route 先查 slot `session_state.ready`，未 ready 直接回
『你的 agent 未連線』錯誤」讀起來像是 route 層的前置檢查。實作改為在 **adapter 層**
（`ShioajiAdapter`）判斷離線：`gateway.ready` 與 `session_state.ready` 同步翻轉，adapter 的
place/cancel/update 決策段一律先讀 `gateway.ready`，未就緒才 fail-fast。理由：Inc0 既有的
冪等重放不變量（同一 `client_order_id` 重送不重建、離線時仍能查中既有 Order 並回既有結果）
是建立在「離線判斷本就在 adapter 決策段內」這個既有結構上——若搬到 route 層先擋，會在
「查得到既有冪等結果」與「offline 擋新單」之間造成優先序衝突，需要額外邏輯才能兩全。改在
adapter 層判斷，讓三條路徑（place/cancel/update）都證實了「不進 DB 決策段就不動配額＋明確
錯誤」這個 D9 原意仍然成立，且完全不動 Inc0 既有的冪等重放路徑。§4 D9 的措辭因此可理解為
「效果落在 adapter 層」而非逐字的 route 層前置檢查。

**(b) D4 place/update 的 ledger insert 與決策段是兩個緊鄰無 I/O 的小交易，而非嚴格同一次
commit（Task 8 裁決）**

§4 D4 描述「ledger insert 與該指令的 DB 決策段**同一交易**」。實作發現 `RiskGuard.
check_place`/`check_update`（Task 7 既有程式，Task 8 未改動範圍）內部會自行
`session.commit()`，因此當 risk_guard 存在時，`insert_command` 無法真的併入同一次 SQL
COMMIT，只能是緊接在決策段 commit 之後、中間不含任何 `await`/I/O 的**獨立小交易**。殘留的
crash 窗口（決策段已 commit、ledger insert 尚未 commit 就崩潰）與 §8 第 1 點「
executed-but-unrecorded 窗口」同量級（純同步 Python 語句間的行程崩潰機率），且有明確出口
可收斂：孤兒委託掃描（`_scan_orphan_orders_once`，開機時跑一次）會把「決策段已落地但無
ledger 列」的孤兒委託列出供人工核對；重連補送（`list_unresolved_for_replay`）查詢也涵蓋
這個窗口——已落 ledger 的指令會被正常收斂，真正遺失 ledger 列的極端情況維持既有的孤兒可見
但不動配額語意，不會超賣。此偏離已交 codex 終審確認，接受為 Inc1 落地形態。

**(c) `resolved_via` 增列 `'local'`（Task 8 裁決）**

§4 D4／`db/models.py::AgentCommand` docstring 原列舉 `resolved_via` 的四個值
（`'ack'|'query_qty'|'report'|'manual'`）未涵蓋「route 完全沒有經過任何與 agent 的
round-trip，就在本地判定這筆指令確定不會有任何 ack」的情境——例如第二層 `_send_gate`
攔下的 kill switch、或 `AgentUnavailableError`（agent 未連線）。實作新增第五個值
`'local'`（`broker/agent_commands.py::resolve_never_dispatched`），語意是「route 本地終結，
未下發即決」，與 `'manual'`（人工 ops 事後介入既有 unknown 委託）刻意區分、不共用同一個值，
維持稽核可讀性。`db/models.py` 的兩條 CHECK constraint（`ck_agent_commands_resolved_
requires_via`／`ck_agent_commands_ack_requires_transport_ack`）不限制 `resolved_via` 的
列舉值本身（只驗證「resolved 必須帶 via」與「via=ack 必須帶 transport_acked_at」兩個組合
規則），故此新增值不需要 migration、與既有 CHECK 相容。

**(d) 終態 resolver 雙掛載點（worker 同交易＋watchdog 掃描）已依 spec 落地（Task 11）**

§4 D4「unknown-resolver」段落描述的終態 resolver（Order 進入終態時收斂該 Order 名下
`resolved_at IS NULL AND transport_acked_at IS NOT NULL AND outcome='unknown'` 的 update／
state-based 掃描收斂 cancel）依 spec 字面落地在兩個掛載點：①`inbox_worker.
_process_order_report` 在 Order 被回報推進終態的**同一交易**內立即呼叫
`resolve_one_unresolved_update`/`resolve_one_unresolved_cancel`（即時掛載點）；②per-slot
watchdog（`watchdog._reconcile_unknown_quota_agent`）週期性用
`list_unresolved_unknown_updates`/`list_unresolved_cancels` 做 state-based 全量掃描（兜底
掛載點，涵蓋①錯過或尚未觸發的殘留）。兩個掛載點共用同一份 `resolve_update_via_query_qty`／
`resolve_unresolved_cancel_via_report`（`broker/agent_commands.py`），一律經 `_cas_resolve`
（`WHERE resolved_at IS NULL`）互斥，誰先贏誰生效，輸家 no-op——與 §4/§6 S#29/S#37 的測試
重點逐條對應，無偏離，此處僅記錄「確實依 spec 落地」供交接查核。

**(e) 兩維 CAS 在 ack applier 側也以原子 UPDATE 落地（Task 11 修復回合 2 裁決）**

§4 D4「server 冪等收斂（late-ack applier）」段落描述業務維 CAS 是收斂的唯一防線，但實作
第一輪（Task 11 修復回合 1）只在 resolver 側（watchdog／worker 掛載點）補上 `_cas_resolve`
原子 CAS，`apply_command_ack`（ack applier）側仍是「函式開頭讀一次 `resolved_at` 判斷是否
已終結」的非原子讀-判-寫——re-reviewer 用真實函式重現了反向競態：resolver 恰好在 applier
的「讀」與「（任何）寫」之間於別的交易 commit，applier 仍會覆寫掉 resolver 已落地的
outcome/resolved_via/Order/quota 效果（update 情境尤其嚴重，可能把 resolver 保守不動的
price/qty 覆寫成改單後的值）。修復回合 2 把 ack applier 的終結寫入也改走同一個
`_cas_resolve`（`WHERE resolved_at IS NULL` 的原子 `UPDATE`）：贏家才套用 Order/quota 效果，
輸家（`race_lost=True`）降級為「只補 transport_acked_at、不改 outcome、不重套效果」，與
「遲到 ack 遇已 resolved」的既有規則 5 語意一致。至此兩維 CAS 在 resolver 與 applier 兩側
都是原子 UPDATE，雙向 last-writer-wins 的競態閉合（re-reviewer 獨立驗證 route 層 re-apply
亦 safe by construction），完整落地 §5 不變量表 I6「同一交易至多套用一次」的字面保證。
