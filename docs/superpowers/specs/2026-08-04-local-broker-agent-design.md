# 本機 Broker Agent — 多人 simtrade（版本 B）— 設計文件

**日期**：2026-08-04
**狀態**：設計 v1，**開放決策已於 2026-08-04 全部拍板**（見末節）→ 選擇性 codex 覆核 → writing-plans；本期只到設計，未動實作
**相關記憶**：`project-order-integration`、`project-shioaji-realtime`、`project-account-system`、`project-market-halt-guard`
**依據**：2026-07-29 多 session concurrency spike（issue #203 直譯器凍結）＋ 2026-08-04 打包難度研究＋ 2026-08-04 下單子系統元件放置盤點

---

## 背景與目標

QuanQuant 下單子系統（`feat/shioaji-order-integration` 分支，Tier 0 真錢硬化已完成、649 綠、未部署）目前是**單一 uvicorn 進程內的全域單例**：`ShioajiAdapter` 直接吃 server 端 `.env` 的憑證、直接在 web 進程裡跑 Shioaji SDK。使用者要求：**正式環境先不部署,除非能讓可信任的人用自己的永豐帳號做 simtrade**。

**目標**：把 Shioaji 原始 I/O 搬到**使用者自己電腦上的 agent 程序**執行（本機登入 simtrade、送單、收回報），**憑證永不上伺服器**；中央網站只保留政策/風控/持久化/UI/狀態機，透過一條認證通道跟本機 agent 交換「下單指令（下行）」與「委託/成交回報（上行）」。

**動機（三合一）**：
1. **拆掉憑證託管牆**——這是使用者不肯部署的主因。憑證留在使用者機器＝session-only 推到極致（server 根本不存），法遵/金鑰金庫責任最低。
2. **免費隔離 Shioaji #203**——spike 發現單一 1.5.x 實例（simtrade + order callback，環境同我方）會凍死整個 Python 直譯器。本機 agent 把這個爆炸範圍關在使用者機器上，**永遠碰不到 web server 與看盤圖表**。
3. **通往真多人的同一程式路徑**——simtrade 先驗架構,日後真錢只換 `simulation=True→False`＋本機 `activate_ca`（憑證仍不上 server）。

**非目標（本階段）**：真錢/CA 上線、代客操作法遵、把 agent 打包成雙擊簽章安裝檔（打包研究：對技術型信任使用者走輕量法即可，簽章 exe 留路線圖）。

---

## 架構總覽

```
使用者電腦（本機 agent）                    中央網站（server，GCP VM）
┌─────────────────────────┐               ┌──────────────────────────────────┐
│ Shioaji SDK（sim 登入）  │               │ 政策/風控：RiskGuard、配額、       │
│ 憑證（session-only 記憶體）│  下行指令      │  confirm token、audit、kill switch │
│ native 序列化鎖          │ ◀──────────── │ 持久化：Order/Deal/RawInbox/       │
│ place/cancel/update      │               │  BrokerPosition（Postgres）        │
│ set_order_callback       │  上行回報      │ RawInboxWorker → position_tracker  │
│ list_trades（reconcile） │ ──────────▶   │ 狀態機 + OrderEventHub(SSE) + UI   │
│ 本機 durable buffer      │  認證 WS       │ per-user agent registry            │
└─────────────────────────┘               └──────────────────────────────────┘
      agent 主動連出，TLS（caddy 既有）
```

**一句話**：本機 agent ＝ spike 建議的 per-user sidecar，只是跑在使用者機器而非我方 VM。

---

## 現況要點（盤點結論，附 `檔案:行號`）

**天然分界縫已存在**：回報上行**完全走 `RawInbox` 表中介**，路徑上無任何 volatile `asyncio.Queue`（`inbox_worker.py:8-9` 明文排除）。這是搬機構最大利多——DB 中介點天然就是「server 端接收 agent 上行」可以插入的縫。

**元件放置盤點**（12 個 `broker/` 模組）：

| 分類 | 模組 | 依據 |
|---|---|---|
| **must-stay-server**（DB/政策/持久化） | `repository`、`position_tracker`、`risk`(RiskGuard)、`inbox_worker`(RawInboxWorker)、`session_state`、`lifecycle`(部分) | 全無 `import shioaji`；純 DB CAS/政策；`inbox_worker` 只假設「有 session_factory」不假設跟 SDK 同進程（`repository.py`、`position_tracker.py:78`、`risk.py:48-238`、`inbox_worker.py:96-280`） |
| **must-go-client**（native I/O） | `shioaji_adapter` 的 native 部分、`preflight`（本機憑證檢查） | `import shioaji as sj`＋`api.login/place_order/cancel_order/update_order/set_order_callback`（`shioaji_adapter.py:183-193, 494-504`）；`preflight` 檢查本機 CA/金鑰檔權限（`preflight.py:19-53`） |
| **split（要拆）** | `shioaji_adapter` 的 place/cancel/update/reconcile（DB 決策＋native＋寫回**融在同一鎖住的方法**）、`watchdog`、`types`/`base`/`redaction`（雙邊契約） | `place` L389-492 同一 async 方法內混 `session_factory()` 寫 `Order`/`QuotaReservation` 與 `supervisor.run(native)`；`watchdog` 直接持有 adapter 私有方法與鎖（`watchdog.py:124-183`） |

**回報上行（loop 邊界，刻意設計）**：券商 native 背景執行緒呼叫 `_on_order_cb(stat, msg)`（`shioaji_adapter.py:754-776`）→ **不經 asyncio loop**、直接同步 `commit_raw_callback(session_factory, ...)`（`inbox_worker.py:59-68`）落地 `RawInbox` 並 `commit()`，**返回前保證落地**（零丟單根基，round3 BLOCKER#2）。之後 `RawInboxWorker.run()`（協程）週期 `async with supervisor.lock` → `asyncio.to_thread(process_batch_once)` → `OrderEventHub.publish()` 推 SSE（`inbox_worker.py:96-108`）。

**下單下行（序列化點）**：`POST /orders`（`orders.py:310-340`）→ `adapter.place()` → DB 決策（`risk_guard.check_place` reserve quota + create_order + audit 同一 commit，`risk.py:117-169`）→ **序列化點** `await supervisor.run(_do_place)`（`shioaji_adapter.py:478`，全子系統唯一 `asyncio.Lock`，`supervisor.py:27`）→ 鎖內 `_send_gate()`（檢查 api 存在 + kill switch，L381-385）→ `asyncio.to_thread(_place_blocking)` → `api.place_order`（L494-504）→ 鎖外再開 session 寫 ack/confirm quota。

**全域 vs per-user**：**目前全部全域單一實例**（RiskGuard/BrokerSupervisor/ShioajiAdapter/OrderSessionState/RawInboxWorker 都在 `app.py:_start_order_subsystem` L172-300 建一次、塞 `app.state`）。DB **資料列**per-user keyed（`Order`/`Deal`/`BrokerPosition`/`QuotaReservation` 帶 `user_id`），但 runtime 物件不分鍵。`mode` 是啟動時讀單一全域設定（`orders.py:3` 明文不接受表單覆寫）。

---

## 放置設計（哪些留 server、哪些搬 agent）

**Server 端保留（幾乎不動）**：`repository`/`position_tracker`/`risk`/`inbox_worker`/`session_state`/狀態機/`OrderEventHub`/UI/路由。`commit_raw_callback` 的角色改由「WS 上行 handler」擔任：收到 agent 送上來的原始 payload → 呼叫既有 `stage_raw_inbox` 落地 → 下游 `RawInboxWorker` 以下**整條完全不變**。

**Agent 端（新）**：一支薄 Python 程序，內嵌現有 `ShioajiAdapter` 的 **native 部分**（連線/送單/取消/改單/list_trades/callback），抽掉所有 `session_factory` DB 呼叫。憑證 session-only。自帶 native 序列化鎖。

**要拆的三顆縫**（設計核心）：
1. **adapter 的 place/cancel/update**：拆成 server 端「DB 決策（risk/quota/create_order/audit）→ 產生已解析的 native 指令」＋ 下行給 agent 執行 native ＋ agent 回 ack → server 端「寫回 ack/confirm quota」。目前融在一顆鎖裡，要一刀切開。
2. **native 單一序列化**：`supervisor` 的 in-process `asyncio.Lock` **無法跨網路**。native 序列化保證**移到 agent 端**（它那顆單一 Shioaji 實例自己序列化）；server 端下行指令帶 `cmd_id` 與序，agent 端保證同一實例的 native 呼叫不交錯。server 端殘留的 DB-only 序列化（watchdog 的 unquarantine/unknown-quota reconcile，`watchdog.py:124-183`）**改用獨立 server 端鎖**，跟 native 序列化脫鉤。
3. **kill switch gate**：**決策留 server**（下行指令前 gate，非 owner→擋），agent 只執行收到的指令。比現況更安全——agent 端無法繞過政策。取消單仍不受 kill switch（沿用現行語意）。

---

## 關鍵不變量的跨網路保存（最需小心）

既有 Tier 0 硬化保證必須跨網路後仍成立：

| 不變量 | 現況 | 跨網路後怎麼保 |
|---|---|---|
| **零丟單**（T0.1，callback returns-before-persist） | callback thread 同步 commit RawInbox | **agent 端本機 durable buffer**（本機 SQLite/WAL）先落地 raw payload → 經 WS 送 → server commit RawInbox → **ack 回 agent** → agent 標記已送。at-least-once 交付 + RawInbox 既有去重（`trade_id`/`seqno`）吸收重送。**斷線期間 agent 續存本機、重連補送。** |
| **native 單一序列化**（round3 #11） | 全域 supervisor 鎖 | 移 agent 端（單實例本地鎖）；server 下行帶序 |
| **kill switch**（T0.3-B） | 鎖內 `_send_gate` | 決策移 server 下行前；agent 只執行 |
| **開機/斷線 reconcile + 孤兒**（T0.2） | 開機 `adapter.reconcile()` | server 對每個 online agent 發 reconcile 指令 → agent 跑 `list_trades` → 上行 → server 對帳。**agent 重連自動觸發 reconcile。** |
| **配額/confirm token/audit/mode 強制**（round3） | server 端 DB | 全留 server，完全不變 |

---

## 控制通道協定

- **Transport**：WebSocket，**agent 主動連出** server（穿越使用者 NAT/防火牆最省事），TLS 走 caddy 既有憑證。
- **認證**：per-user **agent token**，由現有帳號系統（`project-account-system`）在使用者登入後簽發（短期 + 可 rotation），綁 `user_id`。WS 握手驗 token → 在 registry 註冊該 user 的連線。
- **下行指令**（server→agent，JSON）：`{type: place|cancel|update|reconcile|health, cmd_id, mode:"sim", native:{action,price,qty,price_type,order_type,octype,account,order_id/client_order_id}}`。形狀對應 `_place_blocking(req)` 輸入。
- **上行事件**（agent→server，JSON）：`{type: report|ack|login|health, ...}`。`report`＝原始 `stat/msg` payload（→ RawInbox）；`ack`＝對應 `_ack_fields_from_trade`（`ordno`/`broker_order_id`）或 `_classify_place_failure` 失敗分類（`shioaji_adapter.py:65-91`）；`login`＝登入成功 + 券商回傳 `account`（server 端快取，取代 route 直接讀 `service.account`）；`health`＝liveness。
- **冪等**：`cmd_id` + 既有 `canonical_payload_hash`（`types.py:159-190`）/confirm token 機制，重送不重複下單。
- **Liveness/offline**：heartbeat；registry 標 online/offline；**agent offline 時 UI 顯示「agent 未連線」且擋新單**（沿用 `session_state` 健康分級 → `/healthz` 邏輯）。

---

## per-user agent registry（多人）

- 取代 `app.state` 單例：`registry: user_id → AgentConnection`（WS handle + 該 user 的 `session_state` + RiskGuard scope）。
- 路由/watchdog 由 `user_id` 查 registry，而非直接讀 `app.state.order_service`。
- `RiskGuard`/`OrderSessionState` 改 **per-user 實例**（DB 已 per-user keyed，runtime 物件跟著拆）。
- `lifespan`（`app.py:303-408`）不再啟動單一 adapter；改啟動 registry + WS 端點 + per-user watchdog 排程。

---

## 散佈（打包研究結論）

- **輕量法（難度低，推薦）**：發一個小 repo（`pyproject.toml` pin `shioaji>=1.5.3` + 一支 agent 腳本），使用者 `uv sync && uv run`。**simtrade 免 CA**（`shioaji_adapter.py:_connect_blocking` 的 `activate_ca` 只在 `mode=="real"`）；wheel 全平台覆蓋（mac x86_64/arm64、win_amd64、linux）＋無 sdist（免裝 Rust）。
- **備案 B2**：用官方本機 HTTP/CLI server（`shioaji-pro-app` 架構）當 agent、server 去打它——官方背書但多一層本機 hop、我方未實跑。
- **跳過**：PyInstaller/Nuitka 單一簽章 exe（macOS 公證是硬門檻），留路線圖第二步。

---

## 分階段增量

- **Increment 0 — 骨幹驗證（單一信任使用者）**：最小 agent（內嵌 native adapter）+ WS 通道 + server 端上行 handler，證明 **login(sim)→place→report→UI roundtrip + kill switch 下行**。registry 可先 hard-code 單 user。**目的：消掉打包研究的低信心待實測項**（真機打包、官方模式易用度、欄位名再驗）。
- **Increment 1 — 多人**：per-user registry + agent token 簽發 + offline 處理 + UI「我的 agent 連線狀態」。
- **Increment 2（可選）**：憑證本機加密保存（重啟免重輸，仍不上 server）/ 打包成安裝檔。

---

## 開放決策（已於 2026-08-04 全部拍板）

1. **v1 範圍** → ✅ **Increment 0 骨幹**（單 user、最省、先消低信心項）。
2. **通道方案** → ✅ **B1 自建 WS 協定 + 內嵌現有 adapter native 部分**（最多重用、每層我方掌控；#203 程序隔離用「shioaji 跑子程序」補，細節留 writing-plans；不關 B2 的門）。
3. **憑證重啟持久化** → ✅ **純 session-only**（責任最低，重啟重輸；憑證永不落地）。本機加密保存留 Increment 2 可選。
4. **agent token 簽發** → ✅ 沿用現有帳號系統，使用者登入後簽發 per-user 短期 token（可 rotation）。
5. **watchdog DB-only 序列化** → ✅ server 端移除 native 序列化鎖後，`_retry_quarantined`/`_reconcile_unknown_quota` 改用獨立 server 鎖，與 native 序列化脫鉤。

**下一步**：選擇性 codex 覆核設計 → writing-plans（Increment 0 實作計畫）。

---

## 風險與待實測（承接研究，低信心項）

- 查無公開「成功打包 shioaji 成單一 exe」案例（走輕量法可繞過，但 Increment 2 若做需真機驗）。
- 官方本機 HTTP/CLI 模式（B2）的實際易用度/認證/跨平台一致性未實跑。
- `_map_deal/order_report` 欄位名綁 1.5.3 `_core.pyi`，升版需重驗（升版風險不因搬機構改變）。
- #203 隔離有效性：仍需 agent 端長時 soak 確認凍結真的只發生在使用者機器、不外溢。
- 跨網路「零丟單」的 agent 端 durable buffer 是新增可靠性面——需針對「送出未 ack 就崩潰/斷線」寫測試。
