# QuanQuant 雲端部署架構與規格

> 版本：v1.0（2026-06-12 上線）
> 對應文件：[architecture.md](./architecture.md)（系統架構書）、[requirements.md](./requirements.md)（需求書）
> 本文件記錄實際部署完成的雲端架構、規格選型依據、維運流程與擴展路線。

---

## 1. 部署總覽

| 項目 | 值 |
|---|---|
| 雲端平台 | GCP（專案 `quanquant`）|
| 區域/機房 | `asia-east1-b`（台灣彰化 — 距 TAIFEX 延遲最低）|
| 運算 | Compute Engine **e2-small**（2 vCPU shared / 2GB RAM）|
| 磁碟 | 30GB pd-balanced |
| 靜態 IP | `35.229.185.30`（`quanquant-ip`，保留制）|
| 對外網址 | https://quant.35-229-185-30.sslip.io |
| TLS | Caddy 自動簽發 Let's Encrypt（有效至 2026-09-09，自動續期）|
| 存取控管 | app 層 session 登入（帳戶系統）；`/login`／`/healthz` 免認證，其餘全站需登入 |
| OS | Ubuntu 24.04 LTS + Docker Engine 29 + Compose v5 |
| 程式碼遞送 | 私有 GitHub repo + VM 唯讀 deploy key |

## 2. 容器拓撲（docker-compose）

```
                    Internet
                       │ :80/:443（防火牆僅開這兩個 port + IAP SSH）
                 ┌─────▼─────┐
                 │  caddy:2  │  自動 HTTPS / basic_auth / 反向代理
                 └─────┬─────┘
                       │ :8000（不對外發布）
                 ┌─────▼─────────────────────────────┐
                 │  app（python:3.11-slim + uv）      │
                 │  uvicorn factory，內含：           │
                 │  QuotePoller(5s) → SSE + 1m 蠟燭   │
                 │  健康檢查 /healthz                 │
                 └─────┬─────────────────────────────┘
                       │ postgresql+psycopg
                 ┌─────▼──────────┐
                 │  postgres:16   │  volume: pgdata
                 └────────────────┘
```

- 三服務皆 `restart: unless-stopped`；VM 重開機後全自動恢復（已實測）。
- app/postgres 皆有 healthcheck；app `depends_on: service_healthy`。
- SSE 經 Caddy 串流正常（`text/event-stream` 自動不緩衝）。

## 3. 規格選型依據

實測工作負載（部署前量測）：

| 維度 | 量測值 |
|---|---|
| 記憶體 | 閒置 ~70MB / 峰值 ~150MB |
| CPU | 極低（I/O-bound：5 秒輪詢 + 查詢時聚合，無 pandas/numpy/ML）|
| 資料庫 | 16MB（71,699 根 K 棒），年增約 50MB |
| 上游流量 | TAIFEX MIS API ~1.2M 請求/年（免費公開 API）|

**結論：e2-small（2GB）**。e2-micro（1GB）也跑得動現況，但 Postgres 容器 + 後續迭代（警示引擎、多商品、開放使用者）需要餘裕；常駐 5 秒輪詢使 serverless（Cloud Run scale-to-zero / Vercel）不可行 — 詳見 architecture.md 決策 #16。

## 4. 資料庫：Postgres（雲端正式）

- 本機 SQLite → 雲端 Postgres 16，以 `scripts/migrate_sqlite_to_pg.py` 一次性搬遷：
  SQLAlchemy Core 逐表複製（Decimal-as-TEXT 逐位元組保真）→ 重設 serial sequences → 逐表 count/min/max/sum 驗證，全等才放行。
- 完整搬遷內容：2,786 根日K（2015→今）+ 68,913 根 1m K + 圖表指標/繪圖狀態。
- **關鍵修正**：`Candle.ts`（epoch 毫秒）改為 `BigInteger` — SQLModel 預設 `int` 在 Postgres 是 4-byte INTEGER，epoch-ms（~1.78e12）會溢位；SQLite 因動態寬度從未暴露此問題。
- K 棒 upsert 依 dialect 自動選 `ON CONFLICT`（原始碼已可攜，無需修改）。
- 本機開發仍用 SQLite（`DB_URL` 一行切換）。

## 5. 備份與監控

| 項目 | 設定 |
|---|---|
| 資料庫備份 | VM cron 每日 05:15 CST（夜盤收盤後）`pg_dump -Fc` → `gs://quanquant-backups-quanquant/pg/` |
| 保留政策 | GCS 生命週期 30 天自動刪除 |
| 還原驗證 | 已實測 `pg_restore --list` 可讀 |
| Uptime check | Cloud Monitoring 每 5 分鐘打 `/healthz`，content match `"status":"ok"` |
| 告警 | 連續失敗 → email（dev@orgstar.tech）|

已知限制：`/healthz` 永遠回 `ok`（不檢查報價新鮮度），因 TXF 有收盤時段，`last_quote_age_s` 告警需開收盤判斷 — 列為未來工作。

## 6. 日常更新流程

```
git commit → git push → ./scripts/deploy.sh
```

`deploy.sh` = SSH 進 VM → `git pull --ff-only` → `docker compose up -d --build`。
約 30–60 秒完成；Postgres/Caddy 與資料不受影響。已端到端驗證。

## 7. 成本（月，2026 價格）

| 項目 | US$ |
|---|---|
| e2-small（asia-east1, 730h）| ~14–16 |
| 30GB pd-balanced | ~3 |
| 靜態 IPv4 | ~3.7 |
| GCS 備份 + 流量 | <1 |
| **合計** | **~21–24** |

## 8. 擴展路線（迭代觸發點）

| 觸發 | 動作 |
|---|---|
| 換正式網域 | DNS A 記錄 `quant.orgstar.tech → 35.229.185.30` + VM `.env` 改 `DOMAIN=` 一行 + 重啟 caddy（憑證自動換發）|
| 開放大眾使用 | Caddyfile `basic_auth` 換 OAuth proxy（`forward_auth`，app 不動）+ per-user 資料隔離 |
| 多商品/多策略掃描 | poller 拆獨立容器（compose 加 service，DB 共享）|
| K 棒查詢變慢 | 衍生週期物化（materialized 表或 TimescaleDB）|
| DB 量大/高併發 | 遷 Cloud SQL（`DB_URL` 一行切換 + pg_dump/restore）|
| 規模升級 | e2-small → e2-medium/e2-standard-2（停機 ~1 分鐘改 machine-type）|

## 9. 機密與權限配置

- VM `~/quanquant/.env`：`POSTGRES_PASSWORD`、`FINMIND_TOKEN`、`DOMAIN`、`SESSION_SECRET`（chmod 600，不進 git）
- 認證：帳戶系統上線後改為 app 層 session 登入，`SESSION_SECRET` 簽章 cookie（`openssl rand -base64 32` 產生，輪替即讓所有人重新登入）；Caddy `basic_auth` 於 cutover 第 5 步移除（見下方「帳戶系統部署」）
- VM 服務帳戶 scopes：`storage-rw`（備份上傳，bucket 已授 `objectAdmin`）、`logging-write`、`monitoring-write`
- GitHub deploy key：唯讀，僅供 VM pull

## 10. 下單子系統部署（Shioaji，Task 10 收尾）

下單子系統（`ORDER_MODE=sim|real`）是選用模組：未設定對應環境變數時 `order_subsystem_preflight`
軟性停用，app 其餘功能（行情/日誌/交易紀錄）不受影響，`/healthz.order_subsystem` 會反映
`ready=false` 與停用原因。

### 10.1 環境變數

| 變數 | 說明 |
|---|---|
| `ORDER_MODE` | `sim`（預設，Shioaji simulation）或 `real`（正式下單）；拼錯直接拒絕啟動下單子系統（其餘系統仍正常） |
| `SHIOAJI_TRADE_API_KEY` / `SHIOAJI_TRADE_SECRET_KEY` | 下單專用 API 金鑰（與行情用的 `SHIOAJI_API_KEY` 分開，最小權限原則） |
| `ORDER_OWNER_USER_IDS` | 逗號分隔的 user id 白名單，唯一允許下單/取消/改單的帳號 |
| `ORDER_SYMBOL_WHITELIST` / `ORDER_MAX_QTY_PER_ORDER` / `ORDER_MAX_QTY_PER_DAY` / `ORDER_MAX_ORDERS_PER_DAY` | 風控：商品白名單、單筆/單日口數上限、單日委託次數上限 |
| `ORDER_KILL_SWITCH_INITIAL` | 啟動時 kill switch 預設狀態（`true` 時啟動即擋所有送單，取消單仍允許） |
| `SHIOAJI_CA_PATH` / `SHIOAJI_CA_PASSWD` / `SHIOAJI_PERSON_ID` | **僅 `real` 模式需要**：CA 憑證（`.pfx`）路徑、密碼、身分證字號 |

### 10.2 CA 憑證檔（`.pfx`）部署規則（round3 F8）

- **絕不進 git**：`.gitignore` 已含 `*.pfx`；`tests/test_deployment_safety.py::test_no_pfx_files_tracked_in_git` 每次測試都驗證 git 追蹤清單裡沒有任何 `.pfx`。
- **唯讀 bind-mount 進容器**：VM 上的 `.pfx` 放在 `~/quanquant/secrets/`（不進 git 的目錄），docker-compose 用 `:ro` 掛進容器，容器內程序無法修改宿主機檔案。
- **權限 0600 + owner 等於執行 process 的 UID**：`order_subsystem_preflight`（`src/quanquant/broker/preflight.py::_ca_file_permissions_ok`）啟動時強制檢查——權限不是恰好 `0600`，或檔案 owner 不是目前執行 process 的 UID，一律拒絕啟動下單子系統（軟性停用，不崩站，`/healthz` 反映原因）。VM 上部署前手動執行：
  ```bash
  chmod 600 ~/quanquant/secrets/sinopac.pfx
  chown <app容器內執行使用者對應的宿主 UID> ~/quanquant/secrets/sinopac.pfx
  ```

### 10.3 例外訊息 redaction（round3 F8，中央化）

`src/quanquant/broker/redaction.py::redact_secrets` 是唯一的秘密遮蔽實作，套用在**所有**
可能夾帶 `api_key`/`secret_key`/`ca_passwd`/`person_id` 的例外訊息落地/回顯路徑：
`ShioajiAdapter.place/update/health_probe`、watchdog 重連失敗（`OrderSessionState.last_error`，
會經**未認證的 `/healthz`** 直接回顯）、`shutdown_order_subsystem`、lifespan `_start_order_subsystem`
的 connect 失敗、以及 `web/routers/orders.py` 所有把例外文字回顯到 HTMX 表單錯誤訊息的路徑
——不是只有 place 錯誤那一處。見 `tests/test_deployment_safety.py` 的端到端 redaction 回歸
（故意讓 adapter/watchdog/lifespan 各自拋出含 api_key/ca_passwd/person_id 的例外，驗證外顯
訊息不含明文）。

### 10.4 部署前必做：Postgres schema smoke（round3 #20）

`tests/test_deployment_safety.py` 對 8 張新表（`orders`/`deals`/`raw_inbox`/
`broker_positions`/`order_audits`/`confirm_tokens`/`quota_reservations`/
`broker_reconcile_cursors`）在 PG 方言下逐項斷言 `CreateTable` 編譯出的 DDL（BigInteger→
BIGINT、CHECK constraint 全文、UniqueConstraint 名稱與欄位、`broker_positions` 的
partial unique index），但這只是**編譯層級**驗證，不保證真連線建表成功。**首次啟用下單
子系統前**（或這 8 張表的 schema 有任何變更後）必須額外跑一次真實 Postgres 驗證：

```bash
# 起一個一次性 Postgres 容器（跟正式環境同版本 postgres:16）
docker run -d --rm --name qq-pg-smoke \
  -e POSTGRES_USER=quanquant -e POSTGRES_PASSWORD=smokepw -e POSTGRES_DB=quanquant \
  -p 55432:5432 postgres:16
# 等待就緒後，用專案既有的 init_db()（create_all + ensure_columns，與正式啟動同一條路徑）
DB_URL="postgresql+psycopg://quanquant:smokepw@localhost:55432/quanquant" \
  uv run python -c "from quanquant.db.engine import init_db; init_db()"
# 人工或用一次性腳本驗證：非法 mode/direction/state 值被 CHECK 擋、
# 同 scope 兩筆 open BrokerPosition 被 partial unique index 擋、Deal.ts 大 epoch-ms 值不溢位
docker rm -f qq-pg-smoke
```

已於 Task 10 實作完成時人工執行過一次（見完成報告），全數通過（8 張表建表成功、
`ck_orders_mode`/`ck_broker_positions_direction`/`ck_broker_positions_total_opened_qty_positive`/
`uq_broker_positions_active_scope`/`ck_quota_reservations_state` 皆正確擋下非法列、
`Deal.ts` 以 epoch-ms 量級的值往返無溢位）。這不是常態 pytest（避免沒有 docker 的機器
整批測試變 flaky），**部署到會真正啟用 `ORDER_MODE=real`（或任何動到這 8 張表 schema）
的環境前，必須重新跑一次**。

### 10.5 secret scan（人工，非自動化）

```bash
git grep -nE "SHIOAJI_(TRADE_)?(API_KEY|SECRET_KEY)\s*=\s*['\"][A-Za-z0-9]" -- . ':!*.md' || echo "clean"
```
確認除 `.env`（已在 `.gitignore`）外沒有其他檔案硬編碼真實金鑰樣式。

## 帳戶系統部署（首次啟用）

依序執行：

1. VM 的 `.env` 加 `SESSION_SECRET`（`openssl rand -base64 32` 產生）。
2. 照固定三步部署 app：`git commit → git push → ./scripts/deploy.sh`。
   ⚠️ 本次部署會**同時移除 Caddy `basic_auth`**（本分支的 Caddyfile 已改純反向代理，改由 app 層登入把關）。部署完成到下一步 bootstrap 之間，全站要求登入但**尚無任何帳號** → 所有路由 redirect `/login` 且無法登入。這是 **fail-closed 的預期安全狀態，非故障**；因此第 3 步務必緊接著做。
3. 立即在 VM 建第一個 admin 並認領舊資料：
   `docker compose exec app quanquant-user bootstrap <帳號>`（互動輸入密碼）。
   此指令走 `docker compose exec`／SSH、不經瀏覽器，**不受上述登入鎖定影響**。
4. 用剛建立的 admin 帳號瀏覽器登入驗證。

日常帳號管理：Web `/admin/users`（admin），或 SSH 備援
`docker compose exec app quanquant-user create|reset-password|list`。
密碼重設會 bump token_version，所有裝置立即登出。
