# Shioaji 期貨下單整合（單人自用 + 日誌/績效自動整合 + 模擬/正式分流）— 設計文件

**日期**：2026-07-23（2026-07-24 依 codex 外部審查全面修訂）
**狀態**：修訂後待 codex 覆核 → APPROVE 後待 writing-plans（本期只到「設計+計畫通過審查」，暫不進實作）
**相關記憶**：`project-order-integration`、`project-shioaji-realtime`、`project-account-system`
**審查依據**：`docs/superpowers/reviews/2026-07-24-shioaji-order-codex-review.md`（codex REVISE 全部發現 A–H 已納入本版）

---

## 背景與目標

QuanQuant 目前是純監控 + 手動交易日誌：Shioaji（永豐）streaming 已上線做**唯讀行情**（app 層單一 key、`api.login` 不帶 CA），無下單能力；`Trade` 日誌全靠人工輸入。

**目標**：加上**單人自用的期貨下單**（走 Shioaji），成交後**自動寫日誌並反映績效**，且**模擬(simtrade)/正式(real)完全分流、絕不混算**。下單子系統以 **broker 無關介面**設計，日後接元大 adapter。**這是真錢、不可逆的子系統**——正確性（部位守恆/冪等/授權/mode 完整性/確認/驗證/崩潰復原）是第一優先。

**券商決策**：Shioaji 先行（使用者將申請永豐期貨帳戶），元大後補。

---

## 範圍

**本 spec 做**：broker 無關 `OrderService` 介面 + `ShioajiAdapter`；持久化委託/成交/部位帳務 + durable fill 處理；server-side mode 強制；owner 授權；兩階段確認；驗證/風控/kill switch/audit；readiness/watchdog/shutdown；成交自動寫日誌（source 隔離）；`mode` 全鏈路分流；下單 UI（下單/委託/部位、place/cancel/update）。

**本 spec 不做**（見「未來」）：多使用者、per-user 憑證金庫、多券商帳戶 per-account scoping、Yuanta adapter、alert 自動策略下單、法遵執照、模擬/正式分表、多 uvicorn worker。

**明訂前提**：**單一 uvicorn worker**（Dockerfile 無 `--workers`）。所有「同一交易內做 quota reservation + 建單」的原子性，建立在此不變量上；多 worker 需回頭補 DB lock（記入「未來」）。

---

## 現況要點（調查結論，附 `檔案:行號`）

### Shioaji 下單 API（官方文件）
- 憑證：`api.activate_ca(ca_path=".pfx", ca_passwd, person_id)`；token 與 CA 兩套獨立憑證，real 下單兩者都要。
- 下單：`api.Contracts.Futures.TXF.TXF{YYYYMM}` + `api.Order(action, price, quantity, price_type=LMT/MKT, order_type=ROD/IOC/FOK, octype=New/Cover/Auto/DayTrade, account=api.futopt_account)` → `api.place_order`；`update_order`/`cancel_order`。
- 回報：`api.set_order_callback(cb)`（login `subscribe_trade=True`），`cb(stat, msg)`，`OrderState.FuturesOrder`/`FuturesDeal`。**成交可能先於委託回報，也可能先於 `place_order()` 回傳**（見難點 5）。
- `Shioaji(simulation=True)`：模擬，不需 CA，可全流程測試。
- 限額：per person_id **5 連線 / 每日 1000 login**；違規以 **IP+ID** 封鎖 → 需節流。

### 現有 codebase
- 行情 `sources/shioaji_stream.py`（`ShioajiStreamer`、`loop.call_soon_threadsafe` 跨執行緒範式、watchdog）；`web/app.py:127-182` `use_shioaji` 分支；`QuotePoller.publish()` fan-out bus。
- `Trade` `db/models.py:31-60`：一列=一次 round-trip（`entry_*`/`exit_*` 同列、`exit_time IS NULL`=未平倉）；無 status/source/mode/broker 欄位；`pnl` 系統算（`pnl_is_manual`）；`user_id`。時間為 naive local datetime。
- 倉儲 `journal/repository.py`：`create_trade`/`update_trade`/`list_trades`/`list_for_stats`（皆 `where(user_id)`）；無 close/找未平倉 helper。
- 損益 `journal/pnl.py`：`compute_pnl = move*size*point_value - fee`。
- 績效 `stats/metrics.py` + `web/routers/stats.py`（`_filtered`）+ `stats.html`：現成完整，只算已平倉。
- 遷移 `db/migrate.py`：**無 alembic**，`init_db()`=`create_all`+`ensure_columns`（nullable `ALTER ADD COLUMN`，含 DDL 型別字串，可含 `DEFAULT`）；新表 `create_all` 自動建。
- 認證 `web/deps.py:43-58` `get_current_user`；`web/routers/trades.py` per-user CRUD 樣板。**單一 uvicorn 進程、背景任務全站單例**。

---

## 關鍵設計難點（codex 審查後的正確版）

1. **Fill 流 → 部位帳務**：broker 給成交 fill 串流；不可直接映射到手動 `Trade` 表（會誤配手動日誌、口數不守恆、重啟遺失分批進度）。**改為獨立、持久化的 broker 部位帳務**（`BrokerPosition` lot 表），round-trip 完成才寫一筆 `source=shioaji` 的 `Trade` 供 journal 顯示。
2. **冪等**：委託/成交回報會重播；且**成交可能早於下單 ack**。需 durable inbox（`Deal.processed`）+「Deal insert + 部位更新 + Trade 寫入 + processed=true 同一交易」+ 未配對 fill quarantine 重試。**fill 去重鍵（fill_id）與委託關聯鍵（client_order_id/ordno）分離**。
3. **mode 完整性**：mode **只由 server-side session 決定**，絕不信任表單；否則 real session 可被騙送真單卻標 sim。
4. **授權**：單一券商帳戶掛 app singleton；app 有多使用者 → 必須 owner allowlist + 服務層再授權，第二個使用者一律 403。
5. **並發**：callback 由背景執行緒觸發、可能早於 ack；同步 DB 處理不可佔用 event loop。需 pending correlation 先落地 + enqueue-only + 單一有序 worker。
6. **模擬/正式絕不混算**：`mode` 第一級 scope，統計不跨 mode 聚合。

---

## 已定決策

- Shioaji 先行、adapter pattern（難的 90% 只做一次）。
- 全程 `simulation=True` 開發，最後接真 CA。
- **模擬/正式用 `mode` 欄位分流不分表**；手動日誌預設 `real`；`mode` 為第一級 scope。
- **broker 自動交易與手動日誌隔離**：Trade 加 `source`（manual/shioaji）；自動部位帳務走獨立 `BrokerPosition` 表。
- **mode/授權/確認一律 server-side**；驗證 fail closed；kill switch 即時。
- 單一券商帳戶 + owner allowlist（多帳戶延後）。
- **單一 uvicorn worker 不變量**（原子性前提）。

---

## 設計

### A. Broker 抽象層 `OrderService` 介面（新模組 `quanquant/broker/`）

```
# broker/types.py（純資料；mode/action/price_type/order_type/octype 皆 Literal 限定）
Mode = Literal["sim","real"]; Action = Literal["Buy","Sell"]
PriceType = Literal["LMT","MKT"]; OrderType = Literal["ROD","IOC","FOK"]
OcType = Literal["New","Cover","Auto"]

OrderRequest(client_order_id, symbol, action, qty:int>0, price:Decimal>0,
             price_type, order_type, octype, user_id)   # 注意：無 mode 欄位——mode 由 session 決定
OrderAck(client_order_id, broker_order_id, ordno, status)
Fill(broker, fill_id, ordno, symbol, action, price, qty, fee, octype, ts, account, mode, user_id)
Position(symbol, direction, qty, avg_price)
RiskDecision(allowed:bool, reason:str|None, needs_confirm:bool)

# broker/base.py
class OrderService(Protocol):
    mode: Mode                                         # server-side 真實 session mode
    async def place(req, *, actor_user_id, confirm_token=None) -> OrderAck
    async def cancel(broker_order_id, *, actor_user_id) -> OrderAck
    async def update(broker_order_id, *, actor_user_id, price=None, qty=None, confirm_token=None) -> OrderAck
    async def positions(*, actor_user_id) -> list[Position]
    def on_fill(handler) -> None
```

- **mode 不在 `OrderRequest`**：`place/update` 產生的 Order/Fill/Trade 一律蓋 `self.mode`；adapter 若收到任何攜帶 mode 的外部值且與 `self.mode` 不符 → 拒絕。
- 每個對外方法都收 `actor_user_id`，服務層先做 owner 授權（見 F）。

### B. 委託關聯與冪等（`client_order_id` / `fill_id` 分離）

- **`client_order_id`**：下單前由伺服器產生的唯一冪等鍵（UUID 或 request hash）。POST 下單以它去重：同 `client_order_id` 已存在且非終態 → 不重送，回既有狀態（防 ack/HTTP 失敗後使用者重送真單）。
- Order 狀態機：`pending → sending → submitted → (partfilled) → filled | cancelled | failed`；`sending` 中途失敗/ambiguous → 標 `unknown`，**先向券商 reconcile 再決定**，禁止盲送。
- **`fill_id`**：券商成交唯一識別，作 fill 去重鍵，與 `client_order_id`/`ordno` 分離。唯一性納入 `(broker, environment=mode, account, trading_day, fill_id)` 以保證跨日/重連/sim-real 全域唯一。

### C. 資料模型（新表，`create_all` 自動建；epoch-ms 用 `BigInteger`）

```
class Order(SQLModel, table):
    id, client_order_id(unique), user_id(index), mode(Literal sim/real), broker, account,
    symbol, action, qty, price(DecimalText), price_type, order_type, octype,
    broker_order_id(nullable), ordno(nullable),
    status(pending/sending/submitted/partfilled/filled/cancelled/failed/unknown),
    filled_qty, avg_fill_price(nullable), created_at, updated_at
    # UniqueConstraint(client_order_id)

class Deal(SQLModel, table):                  # durable fill inbox（去重 + 待處理佇列）
    id, broker, account, mode, trading_day, fill_id, ordno,
    order_id(FK nullable), user_id(nullable), symbol, action, price, qty, fee,
    octype, ts:BigInteger, processed(bool default False), quarantine(bool default False),
    error(nullable), created_at
    # UniqueConstraint(broker, mode, account, trading_day, fill_id)

class BrokerPosition(SQLModel, table):        # 持久化部位帳務（與手動日誌隔離）
    id, user_id(index), broker, account, mode, symbol, direction(long/short),
    open_qty, avg_entry(DecimalText), closed_qty, exit_notional(DecimalText),
    status(open/closed), opened_at, updated_at, trade_id(FK nullable)  # 結案時回填對應 Trade

class OrderAudit(SQLModel, table):            # append-only 稽核（不含秘密）
    id, ts:BigInteger, actor_user_id, mode, action(place/cancel/update/risk_reject/fill/reconnect),
    payload_hash, rule(nullable), result, detail(nullable)
```

- `Trade` 加 `source: str = "manual"`（手動）/ `"shioaji"`（自動）+ `mode: str = "real"`（見 E）。
- **IntegrityError 精確化**：insert Deal 撞唯一鍵才當重播；FK/NULL 等其他 IntegrityError 記錄並拋出，不靜默吞。

### D. Durable fill 處理 → 部位帳務 → 自動寫日誌（`broker/fill_worker.py` + `broker/position_tracker.py`）

**進料**：adapter 的 order callback（背景執行緒）→ `loop.call_soon_threadsafe(enqueue)`，**只 enqueue**（不在 callback/event loop 內做 DB）。單一有序 + backpressure 的 worker 逐筆處理，每筆**開新 Session**。

**每筆 fill 在一個 DB 交易內**：
1. insert `Deal`（撞唯一鍵=重播 → 跳過，冪等）。
2. 依 `ordno`/`client_order_id` 解析 order context 取 `user_id`；解不到 → `Deal.quarantine=True`、`processed=False`，記 reconcile，不 drop（可能 callback 早於 ack 或啟動重建前）。
3. 部位帳務（`BrokerPosition`，per user/broker/account/mode/symbol）：
   - **開倉**（`New`；`Auto` 依現有部位方向推斷、歧義 fail closed）：無同方向 open → 建；有 → 更新 `open_qty`/`avg_entry`（加權均價）。
   - **平倉**（`Cover`）：以**目標 direction** 找 open 部位；每次只消耗 `min(fill.qty, remaining)`；累計 `closed_qty`/`exit_notional`；**超額部分不吞**——依 octype 明確拒絕/隔離/或作反向開倉（記 audit）。剩餘 0 → `status=closed`。
   - 分批平倉進度全在 `BrokerPosition` 欄位（**持久化**），跨重啟續平不歸零。
4. round-trip 完成（部位 closed）→ `repo.create_trade(TradeCreate(..., source="shioaji", mode=self.mode, entry=avg_entry, exit=exit_notional/closed_qty, size, fee 累計), user_id)`，回填 `BrokerPosition.trade_id`；pnl 自動算。
5. `Deal.processed=True`（**與 1–4 同一交易提交**）。crash → 未 processed 的 Deal 由 worker 重試。

**`repo.find_open_trade`／部位查找一律限 `source=shioaji`**，永不觸及手動 `Trade`（A1）。
`fee`：real 取 broker 回報；sim 由 session 依設定 `order_sim_fee` 填（按口/按 fill 明確定義並測分批累計）。

### E. `mode` 分流（Trade + 全鏈路）

- `Trade` 加 `mode`（`Literal["sim","real"]`，預設 real）+ `source`（manual/shioaji）。
- 遷移 `db/migrate.py` `_MIGRATIONS`：`("trades", "mode", "VARCHAR DEFAULT 'real'")`、`("trades", "source", "VARCHAR DEFAULT 'manual'")`（用**實際表名 `trades`**、帶 `DEFAULT` 讓既有列自動補值）。
- `mode` 參數穿過 `list_trades`/`list_for_stats`(`_filtered`) + journal/stats filter；**第一級 scope（tab 模擬|正式，預設 real，絕不跨 mode 聚合）**；`stats/metrics.py` 零改。
- **手動新增日誌**：`/trades/new` modal、表單、create route 都帶「當前 tab 的 mode」（sim tab 手動新增 → 落 sim）。
- settings/schema/DB 三處限制 `Literal["sim","real"]`；非法值拒絕。

### F. 授權 + 風控 + 確認（server-side 全部）

- **Owner allowlist**：設定 `ORDER_OWNER_USER_IDS`（或角色）。`place/cancel/update/positions` 服務層先驗 actor 是 owner，否則 403。`cancel/update` 再以 `(user_id, broker, mode, broker_order_id)` 驗證委託所有權；`positions` 只回 owner scope。
- **RiskGuard（fail closed）**：qty>0、price>0、合法枚舉；per-user 單筆/單日口數與次數上限；商品白名單；**即時 kill switch**（server-side、緊貼券商呼叫前再查一次；取消單仍允許）。`update(qty=...)` **重跑全部風控**，以變更後總量原子保留 quota（同一交易內 reserve+建單，靠單-worker 不變量）。違規記 `OrderAudit(risk_reject)`。
- **兩階段確認（real）**：`place/update` 對 real 需 `confirm_token`——伺服器產生短效、一次性、綁 `(actor_user_id, payload_hash)` 的 token；缺/不符 → `needs_confirm`。adapter 依 `self.mode` 強制：real 無有效 token 不送。sim 不需 token。**修掉「confirmed 永不傳→real 永遠被擋，且可用 mode 竄改走 sim 繞過」**。

### G. Session 生命週期（`broker/shioaji_adapter.py` + lifespan）

- 下單 session 獨立於行情 streamer；`activate_ca`（real）或 `simulation=True`（sim）。
- **readiness gate**：lifespan `await connect()` 成功才 publish `app.state.order_service`；登入失敗 **fail closed**（service 不可用、反映 `/healthz`），不留 detached task 吞例外。
- **callback-before-ack**：`place` 送單**前**先落地 pending correlation（`client_order_id`→user/mode/ordno 佔位）；callback 先寫 Deal inbox，worker 再延遲解析關聯。
- **watchdog**：重連 + backoff + login 節流（避免撞 5 連線/1000 login）+ **重連後對帳**（拉券商委託/成交補回 inbox）。
- **shutdown**：先停 callback → drain inbox worker → 關 loop。
- `ORDER_MODE` 為 `Literal["sim","real"]`；拼錯/未知值**拒絕啟動**；real 啟動前做 CA/readiness preflight。

### H. UI（`web/routers/orders.py`，仿 trades 樣板）

- 下單面板（place，real 走 confirm token 兩步）、委託列表（`Order`）、部位（`positions`）、cancel/update（帶所有權驗證）。
- journal/stats 掛「模擬|正式」tab；journal 新增 modal 帶當前 mode。
- 所有敏感操作服務層授權；HTMX-first。

### I. 設定 / 部署安全

- `.env`（不進 git）：`SHIOAJI_TRADE_API_KEY`/`SECRET`、`SHIOAJI_CA_PATH`、`SHIOAJI_CA_PASSWD`、`SHIOAJI_PERSON_ID`、`ORDER_MODE`、`ORDER_OWNER_USER_IDS`、風控上限、`ORDER_KILL_SWITCH`、`ORDER_SIM_FEE`。
- 部署 task 補：`.pfx` **read-only mount + 0600 owner + gitignore + secret scan + log redaction + 啟動前權限檢查**；秘密不進 log/repr。
- 三步部署（pytest 全綠 → commit → push → deploy.sh）；開發全程 `ORDER_MODE=sim`。

---

## 不做（YAGNI）

多使用者 / per-user 憑證金庫 / 多券商帳戶 per-account scoping / Yuanta adapter / alert 自動下單 / 法遵執照 / 模擬正式分表 / 多 uvicorn worker（原子性靠單-worker 不變量）。

---

## 測試計畫（每個失敗模式都要有「會抓到 bug」的測試）

- **部位帳務**：跨重啟分批平倉續平不歸零；超額 Cover 不吞口數（守恆）；Auto reversal；雙向 open 各自配對；**手動 Trade 與 broker 列並存不互相污染**（`source` 隔離）；缺開倉的 Cover 進 quarantine 不亂配。逐筆斷言 entry/closed/remaining/均價守恆與 `Deal.processed`。
- **冪等/並發**：同 fill_id 重播不重寫；**Deal commit 後 handler 失敗 → 重播最終只有一次 journal effect**；兩 Session 競態；**callback-before-ack**（真 asyncio loop + worker thread + barrier）最終正確歸屬；未知回報進 quarantine 後可被重建處理。
- **mode/授權**：real-session + sim-form 被拒；缺/錯 confirm token 被擋；kill switch 即時擋單（取消仍可）；**第二位 user 對 place/cancel/positions 一律 403**；跨 user cancel 被拒。
- **驗證**：qty=0/負、price≤0、非法枚舉全被 fail closed 擋。
- **mode 分流隔離**：`list_for_stats`/`compute_stats` 在 sim 與 real 下互不相加。
- **遷移**：`mode`/`source` 加入、既有列預設 real/manual。
- **啟動安全**：`ORDER_MODE` typo → 拒絕啟動；real 缺 CA → 不啟動；秘密不出現在 log/repr。
- 首次 sim E2E 後把去識別 payload 固化成 regression fixture（多筆部分成交/成交先於委託/重播）。
- **不得改弱既有測試**（實作審查以 commit diff 確認 assertion 數量/語意）。`uv run pytest` 全綠。

---

## 風險與防線

- **真錢不可逆**：simtrade 優先、real 兩階段確認、風控 fail closed、即時 kill switch、real 預設關閉。
- **口數不守恆 / 誤配手動單**：持久化 `BrokerPosition` + `min(qty,remaining)` 消耗 + `source` 隔離 + 守恆測試。
- **重複/遺失日誌**：Deal 唯一鍵 + `processed` + 同交易提交 + quarantine 重試 + crash/replay 測試。
- **寫錯人**：order correlation 持久化、啟動重建、未知進 quarantine；Deal 存 user_id/mode/account。
- **mode 被騙**：mode 僅 server-side；adapter 拒絕不符；三處 Literal 限定。
- **越權下單**：owner allowlist + 服務層再授權 + 所有權驗證 + 403 測試。
- **並發**：readiness gate；enqueue-only + 單一有序 worker + 每筆新 Session；callback-before-ack barrier 測試；shutdown drain。
- **連線額度**：watchdog backoff + login 節流；行情+下單=2/5。
- **CA 安全**：read-only mount/0600/gitignore/secret scan/log redaction/啟動前檢查。
- **雙方言**：新表/欄位可攜；epoch-ms `BigInteger`。
- **單-worker 不變量**：原子性前提，spec/plan 明寫並以測試驗證；多 worker 屬未來。

---

## 未來（Phase 2+）

- 多使用者：per-user 加密憑證金庫（信封加密/KMS）、多帳戶 per-account 部位 scoping、永豐官方 HTTP server 模式或一帳戶一容器、per-IP 節流、**法遵（代客操作/全權委託執照）**、多 worker 需補 DB lock/reservation（取代單-worker 不變量）。
- Yuanta adapter：Phase 0 spike（pythonnet 3.0.5 + .NET8 載 DLL、#2595 型別 bug、記憶體），跑不動則 Windows 邊車；接同一 `OrderService` 介面。
