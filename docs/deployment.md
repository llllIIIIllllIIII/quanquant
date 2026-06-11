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
| 存取控管 | Caddy `basic_auth` 共用帳密（`/healthz` 免認證供監控）|
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

- VM `~/quanquant/.env`：`POSTGRES_PASSWORD`、`FINMIND_TOKEN`、`DOMAIN`（chmod 600，不進 git）
- basic-auth 密碼：bcrypt hash 存於 Caddyfile；旋轉：`docker run --rm caddy:2 caddy hash-password` → commit → deploy
- VM 服務帳戶 scopes：`storage-rw`（備份上傳，bucket 已授 `objectAdmin`）、`logging-write`、`monitoring-write`
- GitHub deploy key：唯讀，僅供 VM pull
