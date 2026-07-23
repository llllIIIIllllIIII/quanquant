# Shioaji 期貨下單整合（單人自用 + 日誌/績效自動整合 + 模擬/正式分流）— 設計文件

**日期**：2026-07-23
**狀態**：待審查 → 待 writing-plans
**相關記憶**：`project-order-integration`（本計畫決策全紀錄）、`project-shioaji-realtime`（現有 Shioaji 唯讀行情）、`project-account-system`（app 層認證/隔離）

---

## 背景與目標

QuanQuant 目前是純監控 + 手動交易日誌：Shioaji（永豐）streaming 已上線做**唯讀行情**（app 層單一 key、`api.login` 不帶 CA），完全沒有下單能力；`Trade` 日誌全靠人工事後輸入。

**目標**：為系統加上**單人自用的期貨下單**（走 Shioaji），且成交後**自動寫入交易日誌並反映到績效統計**，同時**模擬（simtrade）與正式（real）的日誌與績效完全分流、絕不混算**。下單子系統以 **broker 無關的介面**設計，讓日後元大（Yuanta）adapter 能接同一介面。

**券商決策**：Shioaji 先行（使用者將申請永豐期貨帳戶做真實自用），元大 `.NET/pythonnet` adapter 後補。理由見 `project-order-integration`：Shioaji 純 Python、已半整合、有 `simulation` 模式可零成本開發，整合摩擦遠低於元大。

---

## 範圍

**本 spec 做**：
- Broker 無關的 `OrderService` 介面 + `ShioajiAdapter`（`activate_ca` / `place_order` / `update_order` / `cancel_order` / 委託成交 callback）。
- 專屬下單 Shioaji session 的登入/CA/重連生命週期（單人＝單 session）。
- 成交回報 → **position-tracker** → 自動映射成 `TradeCreate` 寫日誌（含 `user_id` 綁定、seqno 去重、開/平倉配對）。
- `Trade` 加 `mode`（real/sim）欄位 + 全鏈路過濾，日誌與 `/stats` 以「模擬 | 正式」第一級 scope 分流。
- 下單 UI（下單面板 / 委託列表 / 部位）+ 風控（口數/單日上限、商品白名單、二次確認、**全域 kill switch**）。

**本 spec 不做（見末段「未來」）**：多使用者、per-user 憑證金庫、Yuanta adapter、alert 觸發的自動策略下單、法遵執照評估、模擬/正式分兩張表。

---

## 現況要點（調查結論）

### Shioaji 下單 API（官方文件）
- 憑證：下單前需 `api.activate_ca(ca_path="…Sinopac.pfx", ca_passwd, person_id)`；CA 綁身分（person_id）。**token（api_key/secret_key）與 CA 是兩套獨立憑證**，正式下單兩者都要。
- 下單：`contract = api.Contracts.Futures.TXF.TXF{YYYYMM}`；`order = api.Order(action="Buy/Sell", price, quantity, price_type="LMT/MKT", order_type="ROD/IOC/FOK", octype="New/Cover/Auto/DayTrade", account=api.futopt_account)`；`trade = api.place_order(contract, order)`。改單 `api.update_order`、刪單 `api.cancel_order`。
- 回報：`api.set_order_callback(cb)`（login 帶 `subscribe_trade=True`）；`cb(stat, msg)`，期貨事件 `OrderState.FuturesOrder`（委託）/`OrderState.FuturesDeal`（成交）。**成交可能先於委託回報到達**。
- **`api = sj.Shioaji(simulation=True)`：模擬模式，不需 activate CA，可跑完整下單/回報流程而不送真單** → 整個子系統先用它開發。
- 限額：以 person_id 計，**5 連線 / 每日 1000 login**；違規以 IP+ID 封鎖 → 需應用層節流。

### 現有 codebase（探勘結論，附 `檔案:行號`）
- 唯讀行情：`sources/shioaji_stream.py`（`ShioajiStreamer`，只 `api.login`、`shioaji_stream.py:10-12` 註解已標明「下單才需 CA」）；接線 `web/app.py:127-182`（`use_shioaji` 分支）；`QuotePoller.publish()` 為全站 fan-out bus。
- Trade 模型 `db/models.py:31-60`：**一列 = 一次 round-trip**（`entry_*`/`exit_*` 同列，`exit_time IS NULL` = 未平倉，`:41`）。無任何狀態欄位、無 source/mode/broker 欄位；`pnl` 由系統算（`pnl_is_manual` 旗標可覆寫）；`point_value` 預設 200；`user_id`（`:58`）。`entry_time`/`exit_time` 為 naive datetime（非 BigInteger）。
- 倉儲 `journal/repository.py`：`create_trade(session, TradeCreate, *, user_id)`（`:27`，自動 `_recompute_pnl`）；`update_trade`（`:60`，局部更新 + 平倉需 exit_time/price 同時有無）；`list_trades(..., status, date_from/to, tag)`（`:95`，一律 `where(user_id)`）；`list_for_stats`（`:124`，只取 closed）。**無 `close_trade`、無「依 symbol 找未平倉部位」helper**。
- 損益 `journal/pnl.py`：`compute_pnl = move*size*point_value - fee`（`:9`）；未實現用 `poller.last.price` 當 mark（`trades.py:20-21`）。
- 績效 `stats/metrics.py` + `web/routers/stats.py` + `stats.html`：**現成完整**（總損益/勝率/筆數/平均/獲利因子/最大回撤 + by_tag/by_symbol + CSV/XLSX），只算已平倉。`_filtered`（`stats.py:19`）走 `list_for_stats`。
- 遷移 `db/migrate.py`：**無 alembic**；`init_db()`（`db/engine.py:41`）= `create_all` + `ensure_columns`；`_MIGRATIONS`（`:11-16`）逐筆 nullable `ALTER TABLE ADD COLUMN`（SQLite/Postgres 皆可攜）。新表只要定義 SQLModel，`create_all` 自動建。
- 認證/隔離：`web/deps.py:43-58` `get_current_user`（FastAPI dependency，注入 `request.state.user`）；`web/routers/trades.py` 是**現成 per-user CRUD 樣板**（HTMX-first，`user.id` 穿進倉儲）。單一 uvicorn 進程、背景任務全站單例（`web/app.py:120` lifespan）。

---

## 關鍵設計難點（本 spec 的核心）

1. **Fill 流 → round-trip 一列**：Trade 是「一列一趟」，但 broker 給的是**成交 fill 串流**。需自建 position-tracker：`octype=New` 開倉 → 開一列；`octype=Cover` 平倉 → 找該部位的未平倉列補上出場。倉儲缺「依 symbol/mode 找未平倉部位」helper，需新增。部分成交/分批進出 → **聚合成加權均價**（維持一列一趟模型、stats 零改）。
2. **callback 無 HTTP 使用者情境**：委託/成交 callback 是背景執行緒、拿不到登入 user。下單當下必須把 `user_id` 綁在 order context，成交回來才知道寫進誰的日誌。
3. **回報會重播 → 冪等去重**：斷線重連時成交回報可能重收；不去重就會寫重複日誌、灌爆績效。以 broker `seqno`/`trade_id` 唯一鍵去重。
4. **模擬/正式絕不混算**：`mode` 需為第一級 scope，統計不得把 sim + real 加總。

---

## 已定決策

- **Shioaji 先行、元大後補**；下單子系統走 **adapter pattern**（broker 無關介面 + `ShioajiAdapter`），難的 90%（介面/狀態機/回報/風控/UI/日誌整合）只做一次。
- **全程先用 `simulation=True` 開發**，最後接真 CA 切正式。
- **模擬/正式用 `mode` 欄位 + 過濾，不分表**：遷移零成本（`_MIGRATIONS` 加一筆），stats 邏輯零改；分表要 fork repo/pnl/stats/模板四處。既有手動日誌預設 `real`。`mode` 當第一級 scope（分頁 tab）。
- **部位配對採聚合**：同一部位多筆 fill 聚合加權均價 → 一列 round-trip。
- **單人**：下單 session 為 lifespan 單例；下單憑證放 app 層 `.env`（單人可接受），多人版才做 per-user 金庫。
- **真錢防線**：simtrade 優先、二次確認、風控上限、全域 kill switch。

---

## 設計

### A. Broker 抽象層 — `OrderService` 介面（新模組 `quanquant/broker/`）

Broker 無關的 domain 型別與介面（`ShioajiAdapter` 實作，`YuantaAdapter` 日後同介面）：

```
# broker/types.py（dataclass / SQLModel-free 純資料）
OrderRequest(symbol, action(Buy/Sell), qty, price, price_type(LMT/MKT),
             order_type(ROD/IOC/FOK), octype(New/Cover/Auto), user_id, mode)
OrderAck(broker_order_id, seqno, status)          # place/cancel/update 的即時回覆
Fill(broker, seqno, symbol, action, price, qty, fee, octype, ts, order_ref)  # 成交
Position(symbol, direction, qty, avg_price)       # 部位查詢

# broker/base.py
class OrderService(Protocol):
    async def place(req: OrderRequest) -> OrderAck
    async def cancel(broker_order_id) -> OrderAck
    async def update(broker_order_id, *, price=None, qty=None) -> OrderAck
    async def positions(user_id) -> list[Position]
    def on_fill(handler: Callable[[Fill], None]) -> None   # 註冊成交回呼
```

`YuantaAdapter` 之所以能接同一介面：元大 `SendFutureOrder` / 即時回報 `RR_RealReport` 可映射到相同 `OrderRequest`/`Fill`（差異吸收在 adapter 內）。

### B. Shioaji 下單 session 生命週期（`broker/shioaji_adapter.py`）

- **獨立於唯讀行情 streamer**：下單用自己的 `Shioaji` 實例登入交易帳戶並 `activate_ca`（real）或 `simulation=True`（sim），與 `ShioajiStreamer` 解耦——交易帳戶可與行情帳戶不同，且下單故障不影響行情。
- 沿用 streamer 已驗證的**跨執行緒橋接**：`set_order_callback` 由 .NET/Solace 執行緒觸發 → `loop.call_soon_threadsafe(...)` 回 FastAPI event loop（同 `shioaji_stream.py` 模式）；watchdog 重連。
- 生命週期：`web/app.py` lifespan 起一個下單 session（單人＝單例），掛 `app.state.order_service`；`mode` 由設定決定（sim/real）。
- **連線數**：行情 session + 下單 session 若同一 person_id 佔 2/5 連線，仍在額度內。

### C. 委託資料模型 + 去重（新表，`create_all` 自動建）

```
# db/models.py 新增
class Order(SQLModel, table):        # 委託單
    id, user_id(index), mode(real/sim), broker("shioaji"),
    symbol, action, qty, price, price_type, order_type, octype,
    broker_order_id, status(pending/submitted/partfilled/filled/cancelled/failed),
    filled_qty, avg_fill_price, created_at, updated_at

class Deal(SQLModel, table):         # 成交（冪等去重來源）
    id, order_id(FK), broker, seqno, price, qty, fee, ts, processed(bool)
    __table_args__ = UniqueConstraint("broker", "seqno")   # 重播不重寫
```

- `ts` 若存 epoch-ms 用 `BigInteger`（照專案約束）；raw SQL 兩方言可攜。
- 去重靠 `Deal` 的 `unique(broker, seqno)`：重播的成交 insert 撞唯一鍵 → 跳過。

### D. Position-tracker → 自動寫日誌（`broker/position_tracker.py`）

成交 `Fill` 進來（已去重）後：
1. 依 `octype` 分流：`New` = 開倉、`Cover` = 平倉（`Auto` 依當前部位方向推斷）。
2. **開倉**：`repo.create_trade(TradeCreate(symbol, direction, entry_time=fill.ts, entry_price=fill.price, size=fill.qty, fee=fill.fee, mode=mode), user_id=fill.user_id)` → 產生未平倉列。
3. **平倉**：新增 `repo.find_open_trade(session, symbol, *, user_id, mode)` 找未平倉列 → `repo.update_trade` 補 `exit_time`/`exit_price`（+ 累加 fee）收單；`pnl` 自動重算。
4. **部分成交/分批**：同一部位累積 fill → 加權均價；部位歸零時該 round-trip 完成。多筆平倉分次收 → 依聚合均價更新出場。
5. `mode` 由下單 session 決定並蓋在 `TradeCreate` 上；`fee` real 取 broker 成交回報、sim 用設定估算。
6. 未實現損益 mark 仍走 `poller.last.price`（不變）。

**下游全自動**：`create_trade` 已自動算 pnl、`/stats` 已從 trades 讀 → 日誌與績效無痛反映，核心邏輯零改。

### E. `mode` 分流（Trade schema + 全鏈路過濾）

- `Trade` 加 `mode: str = "real"`（`db/models.py`）；`db/migrate.py` `_MIGRATIONS` 追加 `("trade", "mode", "VARCHAR")` 一筆 → 既有列自動補 `real`。
- `TradeCreate`/`TradeUpdate`（`journal/schemas.py`）加 `mode`。
- `mode` 參數穿過 `list_trades` / `list_for_stats`(`_filtered`) + `journal_page` / `stats_page` 的 filter 表單。
- **第一級 scope**：journal 與 stats 頁加「模擬 | 正式」切換（預設 `real`），**永不聚合跨 mode**。統計核心 `stats/metrics.py` **一行不改**——只是餵過濾後的清單。
- 手動新增日誌的表單也帶 `mode`（預設 real，可標 sim 供紙上交易紀錄）。

### F. 下單 UI + 風控（新 router `web/routers/orders.py`，仿 trades 樣板）

- 路由（HTMX-first、`Depends(get_current_user)`、`user.id` 穿進去）：下單面板（place/cancel/update）、委託列表（讀 `Order`）、部位（`positions()`）。
- journal / stats 模板加「模擬 | 正式」tab。
- **風控（下單前於 `OrderService.place` 檢查）**：per-user 單筆口數上限、單日口數/次數上限、商品白名單、**全域 kill switch**（一旗標停所有下單）；**real 單需二次確認**。違規/超限一律擋下並記 audit。
- audit：委託/成交/風控攔截寫入（可先用 `Order`/`Deal` + log，多人版再擴 audit_log 表）。

### G. 設定 / 部署

- `.env`（不進 git）：`SHIOAJI_TRADE_API_KEY`/`SECRET`、`SHIOAJI_CA_PATH`（.pfx）、`SHIOAJI_CA_PASSWD`、`SHIOAJI_PERSON_ID`、`ORDER_MODE=sim|real`、風控上限、`ORDER_KILL_SWITCH`。
- `.pfx` bind-mount 進 Docker（同 Caddy 單檔 bind-mount 慣例，勿進 git）。
- 部署照既有三步：`uv run pytest` 全綠 → commit → push → `./scripts/deploy.sh`。開發全程 `ORDER_MODE=sim`；`real` 待永豐帳戶下來。

---

## 不做（YAGNI / 本期範圍外）

- **多使用者**：per-user 加密憑證金庫（信封加密/KMS）、per-user N session、永豐官方 HTTP server 模式——延後至 Phase 2（見「未來」）。
- **Yuanta adapter**：介面已預留，實作後補（.NET DLL + pythonnet，需 Phase 0 spike，可能 Windows 邊車）。
- **alert 觸發自動下單**：風險最高，最後做。
- **法遵/執照評估**：代客操作/全權委託是多人才觸及的問題，單人自用不涉及。
- **模擬/正式分表**：以 `mode` 欄位取代。
- **改動既有唯讀行情 streamer**：下單 session 獨立，行情零改動。

---

## 測試計畫

- **單元（pytest）**：
  - `OrderService`/`ShioajiAdapter` 對 fake/mock（或 `simulation=True`）：place/cancel/update 回 `OrderAck`；`on_fill` 觸發。
  - position-tracker：`New` fill → 建未平倉列（帶 mode/user_id）；`Cover` fill → 收單、pnl 自動算；分批 fill → 加權均價；同 seqno 重播 → 不重寫（`Deal` 唯一鍵）；缺對應未平倉列的 Cover → 安全處理（記錄異常，不炸）。
  - `find_open_trade`：以 (user_id, symbol, mode) scope，跨 user/跨 mode 不誤配。
  - 遷移：`mode` 欄位加入、既有列預設 `real`。
  - **mode 分流**：`list_trades`/`list_for_stats`/`compute_stats` 在 `mode=real` 與 `mode=sim` 下**互不含對方**（同一組資料放 sim+real 各若干，斷言兩邊統計完全隔離、不相加）。
  - pnl 公式回歸（既有）不受影響。
- **端到端（simtrade 手動）**：`ORDER_MODE=sim` → 下單面板送單 → 收成交回報 → journal 自動出現 `mode=sim` 一列 → `/stats`「模擬」tab 反映、「正式」tab 不受影響。
- `uv run pytest` 全綠（既有 270+ 全數 + 新增）。

---

## 風險與防線

- **真錢不可逆**：simtrade 優先開發、real 單二次確認、風控上限、全域 kill switch；real 切換為顯式設定，預設 sim。
- **重複日誌（回報重播）**：`Deal` `unique(broker, seqno)` + `processed` 旗標；position-tracker 只處理未處理過的 seqno。
- **寫錯人的日誌**：`user_id` 綁在 order context 一路帶到 fill，寫入前斷言存在。
- **sim/real 混算**：`mode` 為必要 scope，stats 絕不跨 mode 聚合；以隔離測試把關。
- **Shioaji session/執行緒**：重用已驗證的 streamer 跨執行緒橋接 + watchdog 重連；下單 session 與行情 session 解耦，互不拖累。
- **連線額度（5/person_id）**：行情 + 下單佔 2；應用層節流避免 login/呼叫暴衝害 IP 被封。
- **CA 憑證安全**：`.pfx` 不進 git、bind-mount、`.env` 權限；多人版的 per-user 金庫延後但已在「未來」標記，勿把單人的 app 層明文憑證直接沿用到多人。
- **雙方言**：新表/新欄位可攜（nullable ALTER、SQLModel create_all）；epoch-ms 用 `BigInteger`。
- **部位配對錯亂**：聚合模型 + `find_open_trade` scope 嚴格；缺對應開倉的平倉走「記錄異常不自動收單」而非亂配。

---

## 未來（Phase 2+ 指引，非本 spec 範圍）

- **多使用者**：per-user 加密憑證金庫（信封加密 / GCP KMS，master key 不與 DB dump 同源）；per-user session 走**永豐 2026 官方 HTTP server 模式**（`backend='http'` + `Dockerfile-server` + 多帳戶 Dashboard），或「一帳戶一容器」隔離，勿在單 uvicorn 進程塞多個原生 `Shioaji()` 實例（Solace 全域狀態疑慮）；per-IP 節流；**先過法遵**（代客操作/全權委託恐需投顧投信執照）。in-memory 登入鎖定/簽章金鑰在多 worker 下需外移。
- **Yuanta adapter**：Phase 0 spike（`linux/amd64` Docker 內 pythonnet 3.0.5 載 `YuantaSparkAPI.dll` + `.pfx` UAT 登入 + 送單收報 + 量記憶體；注意 pythonnet #2595 型別曝露 bug）；跑不動則 Windows 邊車 gateway。接上 `OrderService` 同介面。
