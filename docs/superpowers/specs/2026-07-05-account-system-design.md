# 帳戶系統設計規格

**日期**：2026-07-05
**狀態**：已與使用者逐段確認
**背景**：目前全站只有 Caddy `basic_auth` 單一共用帳號（`quant`），FastAPI 應用本身沒有使用者概念。多人共用導致交易日誌、盈虧統計、指標設定、畫線、alerts 全部混在一起。本規格定義應用層帳戶系統，讓個人資料每人一份。

## 1. 範圍與決策摘要

| 決策點 | 定案 |
|---|---|
| 帳號來源 | 管理員建立（私人機制）；未來功能完整後才開放自行註冊，設計需保留擴充空間 |
| Per-user 資料 | trades（交易日誌＋盈虧）、指標設定＋畫線、alerts＋觸發紀錄 |
| 維持共享 | 行情資料（candles/quotes）、Market Pulse 全域開關 |
| 認證機制 | 應用層 session 登入（自建），移除 Caddy `basic_auth` |
| 管理介面 | CLI（bootstrap／備援）＋ Web 管理頁 `/admin/users` 兩者都要 |
| 舊資料歸屬 | 全部歸給第一個 admin（`bootstrap` 時認領） |
| Telegram 個人綁定 | 分兩階段：第一版通知仍發共用 chat（訊息前綴使用者名），第二階段做深度連結綁定 |

不在本次範圍：開放註冊、Email 驗證、忘記密碼自助流程（由 admin 重設代替）、API token、TG 個人綁定（第二階段）、前端頁面改版（見 §8）。

## 2. 資料模型

### 2.1 新表 `users`

| 欄位 | 型別 | 說明 |
|---|---|---|
| `id` | int PK | |
| `username` | str, unique, index | 登入帳號 |
| `display_name` | str | 顯示名稱（navbar、通知前綴） |
| `password_hash` | str | bcrypt（cost 12） |
| `role` | str | `"admin"` \| `"user"` |
| `is_active` | bool, default True | 停用即無法登入／使用，取代刪除 |
| `token_version` | int, default 0 | 改密碼／重設時 +1，使所有舊 cookie 失效 |
| `telegram_chat_id` | str, nullable | 第一版僅留欄位，第二階段使用 |
| `created_at` / `updated_at` | datetime | naive UTC，沿用專案慣例 |

### 2.2 新表 `user_chart_states`

結構同 `chart_states` 但加 `user_id`，unique 約束為 `(user_id, symbol, kind)`。

**為何開新表而不改舊表**：`chart_states` 建表時帶 `(symbol, kind)` unique 約束，SQLite 無法 ALTER 掉約束，硬改需整表重建且兩方言（SQLite／Postgres）要分開處理，風險高。舊 `chart_states` 降級為「系統層 KV store」，Market Pulse 全域開關（`kind="pulse"`）繼續住在那裡，語意更乾淨。

### 2.3 既有表加欄位

- `trades.user_id`：int, nullable, index — `ALTER TABLE ADD COLUMN`（兩方言可攜）
- `alerts.user_id`：int, nullable, index — 同上
- `alert_events` 不加欄位：經 `alert_id` join 即知擁有者

欄位在 schema 層 nullable（遷移需要），應用層一律視為必填。

### 2.4 遷移機制

專案無 alembic，維持 `init_db()` 的 `create_all` 建新表；另加一段啟動時輕量遷移：用 SQLAlchemy inspector 檢查欄位不存在才執行 `ALTER TABLE ADD COLUMN`（named-param、dialect-aware，遵守專案 raw-SQL 可攜約束）。

## 3. 認證與 session

### 3.1 登入／登出

- `GET /login`：獨立登入頁（不套主 layout），帳號＋密碼表單。
- `POST /login`：查 `users` → `bcrypt.checkpw` → 成功發 cookie，303 轉首頁；失敗留在登入頁顯示錯誤。
- `POST /logout`：清 cookie，303 轉 `/login`。
- Cookie：HttpOnly＋Secure＋SameSite=Lax，內容 `{user_id, token_version, issued_at}`，以 `itsdangerous` `URLSafeTimedSerializer` 簽章，有效期 30 天。無 server-side session 表。
- 簽章密鑰 `SESSION_SECRET` 放 `.env`（不進 git）；未設定時本機開發自動生成臨時密鑰並記 warning（重啟即全員登出，僅限開發）。
- 登入失敗保護：同一帳號連續失敗 5 次鎖 60 秒（in-memory，單機部署足夠）。

### 3.2 request 驗證

新增 dependency `get_current_user`：解析 cookie → 載入 user → 驗 `is_active` 與 `token_version`。失敗時：

- 一般頁面請求 → 303 轉 `/login`
- HTMX／API 請求 → 401 帶 `HX-Redirect: /login`

掛在所有現有 router 上（router-level dependency）；routes 維持 sync `def` 不動（勿改 async，見專案約束）。豁免：`/login`、`/healthz`、`/static/*`。SSE 端點同樣走 cookie 驗證。

另有 `require_admin` dependency：非 admin 回 403。

### 3.3 新增依賴

- `bcrypt`（密碼雜湊）
- `itsdangerous`（cookie 簽章）

### 3.4 Caddy

移除 `basic_auth` 區塊；`/healthz` 免驗證維持不變（GCP uptime check）。

## 4. 管理功能

### 4.1 CLI（新 console script `quanquant-user`）

| 指令 | 行為 |
|---|---|
| `bootstrap` | 建第一個 admin ＋ 認領舊資料（§6）；已有 admin 時拒絕重跑 |
| `create <username>` | 建一般帳號、設初始密碼 |
| `reset-password <username>` | 重設密碼並 bump `token_version` |
| `list` | 列出帳號、角色、狀態 |

### 4.2 Web 管理頁 `/admin/users`（僅 admin）

- 使用者列表：帳號、顯示名稱、角色、狀態、建立時間
- 建立帳號（admin 設初始密碼）、重設密碼、停用／啟用、變更角色
- 不做刪除帳號（資料歸屬會斷），以停用取代

### 4.3 個人設定

navbar 加使用者選單（顯示 `display_name`）→「修改密碼」（需驗舊密碼，成功後 bump `token_version`＋重發自己這台的 cookie）＋「登出」。

## 5. 資料隔離落地

- **trades／stats**：`journal/repository.py` 全部查詢加 `user_id` 過濾；建立時蓋上 `current_user.id`。統計（勝率、盈虧曲線、匯出）自然只算自己的。
- **指標＋畫線**：讀寫改走 `user_chart_states`，以 `(user_id, symbol, kind)` 定位。
- **alerts**：CRUD 與觸發紀錄列表以 `user_id` 過濾；操作他人的 alert 回 **404**（非 403，避免洩漏存在性）。
- **alert 引擎**（背景任務）：維持全域評估所有啟用中 alerts（引擎不分使用者），僅在發送時分流。
- **瀏覽器 SSE toast**：alert 觸發通知只推給擁有者的連線（SSE 連線建立時綁定 user）。行情與 Pulse SSE 為市場資料，照舊廣播。
- **Market Pulse**：全域開關維持系統級（存舊 `chart_states`），僅 admin 可切換。

## 6. 舊資料遷移（`bootstrap`）

`quanquant-user bootstrap` 做兩件事：

1. 建立第一個 admin 帳號。
2. 認領孤兒資料：
   - `UPDATE trades SET user_id = <admin> WHERE user_id IS NULL`
   - `UPDATE alerts SET user_id = <admin> WHERE user_id IS NULL`
   - 把 `chart_states` 中 `kind IN ('indicators','drawings')` 的列**複製**到 `user_chart_states`（歸 admin）；舊列保留不動，可隨時回退。

## 7. Telegram 通知

**第一版**：所有 alert 觸發仍發到現有共用 chat，訊息前綴擁有者名稱，例如 `[Henry] TXF 5m MA20 上穿`。Pulse 通知照舊。

**第二階段（另開規格，本次不實作）**：設定頁產生一次性 token → 深度連結 `t.me/<bot>?start=<token>` → bot getUpdates 背景任務收 `/start` 綁定 `users.telegram_chat_id` → alert 通知發個人 chat（未綁定 fallback 共用 chat）→ Pulse 改個人訂閱。

## 8. 未來前端方向（參考，不在本次範圍）

使用者提供了未來前端樣板（導覽列：Dashboard／Note／Strategy／Alert／Backtesting／API；商品下拉切換；主圖＋副圖佈局）。本次不做前端改版，但帳戶系統設計需相容：

1. **多商品**：`user_chart_states` key 含 `symbol`，trades／alerts 本有 `symbol` 欄位 — 天然相容。
2. **導覽列重構**：帳戶 UI 接觸點最小化且自包含（`/login` 獨立頁、navbar 使用者選單、`/admin/users` 獨立頁），未來改版只需保留使用者選單插槽，認證層不用重寫。
3. **API 分頁**：cookie session 供瀏覽器用；未來可在 `users` 表加 API token 欄位（`token_version` 機制可沿用），本次僅預留、不實作。

## 9. 測試計畫

沿用 pytest，部署前必須全綠：

- **單元**：密碼雜湊／驗證、cookie 簽章／過期／`token_version` 失效、登入失敗鎖定。
- **路由保護**：未登入訪問各頁 → 303/401；非 admin 訪問 `/admin/users` → 403；`/healthz` 免驗證。
- **隔離（核心）**：建 A、B 兩使用者，驗證 A 看不到、改不到 B 的 trades／alerts／畫線／指標設定（含 stats 統計只含自己）。
- **遷移**：`bootstrap` 後舊資料全歸 admin；重跑被拒；輕量遷移對既有 DB 冪等。
- **既有測試修補**：新增「已登入 client」pytest fixture，套用到受保護路由的既有測試。

## 10. 部署注意

依序執行：

1. VM 的 `.env` 加 `SESSION_SECRET`。
2. 照固定三步部署 app（`git push` 不會自動部署，需跑 `./scripts/deploy.sh`）。
3. 在 VM 執行 `quanquant-user bootstrap` 建第一個 admin 並認領舊資料。
4. 用 admin 帳號實際登入驗證。
5. 確認可登入後才移除 Caddyfile `basic_auth` 並 `docker compose up -d caddy`（此前雙層驗證並存，安全無虞）。
