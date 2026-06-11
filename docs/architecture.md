# QuanQuant 系統架構書

> 版本：v1.0（2026-06-11）
> 對應需求：[requirements.md](./requirements.md)。本文件描述現況架構、目標雲端架構（GCP 台灣機房）、遷移路徑與所有重大技術決策的紀錄。

---

## 1. 現況架構（本機）

```
                    ┌──────────────────────────── FastAPI (uvicorn) ────────────────────────────┐
                    │                                                                            │
 TAIFEX MIS API ◄───┤  QuotePoller（單一輪詢迴圈, 5s）                                            │
 (免費, 無認證)      │    ├─ pub/sub fan-out (asyncio.Queue)                                      │
                    │    ├─► SSE /quote/stream ──► 瀏覽器報價列（多分頁共用一個上游輪詢）          │
                    │    ├─► _persist_market_data：raw quote → quotes 表                          │
                    │    │                        CandleBuilder → 1m candles upsert               │
                    │    └─► CLI `quanquant`（terminal 顯示, 同一 poller 抽象）                    │
                    │                                                                            │
 FinMind API ◄──────┤  quanquant-backfill daily（HistoryProvider）──► 1d candles                  │
 (歷史日K, token)    │                                                                            │
                    │  HTTP routes：                                                              │
                    │    /            儀表板（HTMX + Alpine + KLineCharts, 無 npm build）          │
                    │    /journal /stats /trades…   交易日記與績效（HTMX）                         │
                    │    /api/candles /api/candles/latest   K 棒查詢（衍生週期即時聚合）           │
                    │    /api/chart/state           指標設定與繪圖持久化                           │
                    │    /healthz                   健康檢查                                      │
                    └────────────────────────────────┬───────────────────────────────────────────┘
                                                     │
                                              SQLite (WAL)
                                   quotes / candles / chart_states / trades
```

關鍵設計：
- **單一輪詢服務所有消費者**（瀏覽器分頁、CLI、持久化）— 不重複打上游。
- **正準 K 棒只存 1m + 1d**，其餘 10 種週期查詢時即時聚合（session-anchored 規則見需求書 §4.2）。
- **領域邏輯與框架分離**：`candles/`、`journal/`、`stats/`、`history/`、`poller.py` 不依賴 FastAPI，可純函式測試。
- 前端零 build：HTMX + Alpine + KLineCharts 皆 CDN 釘版。

## 2. 目標架構：容器化

```
docker-compose.yml
├── app:      python:3.11-slim + uv（multi-stage build）
│             CMD uvicorn quanquant.web.app:create_app --factory --host 0.0.0.0
│             （內含 poller + 持久化背景任務；單容器即完整服務）
├── postgres: postgres:16（volume 掛載；本機開發仍可用 SQLite）
└── caddy:    caddy:2（自動 HTTPS / 反向代理 :443 → app:8000）
```

Dockerfile 要點：
```dockerfile
FROM python:3.11-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev

FROM python:3.11-slim
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8000
CMD ["uvicorn", "quanquant.web.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
```

12-factor：所有設定走環境變數（pydantic-settings 已就緒）；機密不進 image。

## 3. 部署：GCP asia-east1（台灣彰化機房）

| 項目 | 選擇 | 理由 |
|---|---|---|
| 區域 | `asia-east1` | 距 TAIFEX 延遲最低（~5ms 級）|
| 運算 | **Compute Engine e2-small VM**（2 vCPU shared / 2GB）跑 docker-compose | 常駐輪詢需要 always-on；Cloud Run 需 min-instances=1 + CPU always allocated，反而更貴更複雜 |
| 資料庫 | 起步：VM 內 Postgres 容器；量大後：Cloud SQL（db-f1-micro 起）| 單人系統 VM 內即可；Cloud SQL 換 `DB_URL` 即遷移 |
| 網路 | 防火牆僅開 443/80（Caddy）+ IAP SSH；不開 8000 | 最小暴露面 |
| 網域/TLS | Cloud DNS 或任意註冊商 + Caddy 自動 Let's Encrypt | 零維護憑證 |
| 開機自啟 | `systemd` unit 跑 `docker compose up -d`（或 VM container-optimized OS）| 重開機自癒 |

部署步驟綱要：
1. 建 VM（e2-small, Ubuntu LTS, asia-east1-b），裝 docker + compose plugin。
2. `git clone` / `docker compose pull` → `.env`（FINMIND_TOKEN、DB_URL、TELEGRAM_*…）。
3. `docker compose up -d`；`quanquant-backfill daily` 初始化歷史。
4. Cloud Monitoring uptime check 指向 `https://<domain>/healthz`。

### 成本估算（月，2026 中價格級距）

| 項目 | 金額（US$）|
|---|---|
| e2-small VM（asia-east1, 730h）| ~13–16 |
| 標準持久磁碟 30GB | ~1.5 |
| 流量（個人使用）| <1 |
| **合計（VM 內 Postgres）** | **~15–18** |
| （改用 Cloud SQL db-f1-micro 另加）| +~9–15 |

新 GCP 帳號有 US$300 試用額度，足跑一年以上。

## 4. 資料庫遷移路徑（SQLite → Postgres）

| 步驟 | 內容 |
|---|---|
| 現況 | `DB_URL=sqlite:///./quanquant.db`（WAL）；單人本機綽綽有餘 |
| 已就緒的可攜性 | SQLModel-on-SQLAlchemy；K 棒 upsert 依 dialect 自動選 sqlite/postgresql 的 `ON CONFLICT`；Decimal-as-TEXT 兩邊行為一致 |
| 切換 | `DB_URL=postgresql+psycopg://user:pass@host/quanquant` + `uv add psycopg`；`init_db()` 建表 |
| 資料搬遷 | 量小：CSV 匯出入或一次性腳本；量大：pgloader |
| **Alembic 導入時機** | 本輪**不導入**（僅新增表，`create_all` 足夠）。觸發條件：第一次需要 ALTER 既有表、或正式遷 Postgres 時——屆時 `alembic init` + autogenerate 基線 |

## 5. 設定與機密

| 變數 | 用途 |
|---|---|
| `POLL_INTERVAL_SECONDS` / `SYMBOL` / `SOURCE` | 擷取行為 |
| `DB_URL` | 資料庫（SQLite ↔ Postgres 一行切換）|
| `FINMIND_TOKEN` | 歷史回補（免費註冊）|
| `QUOTE_RETENTION_DAYS` | 原始快照保留天數（預設 7）|
| `HOST` / `PORT` / `TV_SYMBOL` | Web 服務 |
| （Phase 4+）`TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | 通知 |

本機/VM 用 `.env`（不進 git）；GCP 進階選項：Secret Manager + 啟動時注入。

## 6. 備份

| 階段 | 方案 |
|---|---|
| SQLite | 方案 A：每日 cron `sqlite3 quanquant.db ".backup backup-%F.db"` 上傳 GCS；方案 B（推薦）：**litestream** 容器即時複寫到 GCS bucket |
| Postgres | 每日 `pg_dump` cron 上傳 GCS（Cloud SQL 則用內建自動備份）|
| 保留 | GCS 生命週期 30 天 |

## 7. 監控與健康檢查

- `GET /healthz` → `{"status":"ok","last_quote_age_s":N}`；盤中 `last_quote_age_s > 30` 視為異常（uptime check 可加 content match）。
- Cloud Monitoring uptime check（免費額度內）→ Email/SMS 告警。
- 日誌：stdout → `docker logs` / Cloud Logging agent。
- Phase 4 之後：警示引擎自身的失敗通知共用 Telegram notify()。

## 8. 服務拆分路線（規模升級時才做）

| 觸發 | 動作 |
|---|---|
| 多商品、多策略掃描 | poller/ingestor 拆獨立容器（compose 內加 service；DB 為共享狀態）|
| K 棒查詢變慢 | 衍生週期物化（materialized candles 表或 Timescale continuous aggregates）|
| 多使用者 | 加 auth 層（Caddy basic-auth → OAuth proxy）+ Postgres 必選 |

## 9. 決策紀錄（Decision Log）

| # | 日期 | 決策 | 理由／替代方案 |
|---|---|---|---|
| 1 | 2026-06-04 | 即時資料源用 **TAIFEX MIS API** | 免費、即時、無認證；FinMind 即時 snapshot 需付費等級（HTTP 400）|
| 2 | 2026-06-09 | UI 採 **FastAPI + HTMX/Alpine + Jinja2，無 npm build** | 單人工具，Python 為主；替代案 Streamlit（即時性差）、Next.js（過重）|
| 3 | 2026-06-09 | 本機先用 **SQLite（WAL）**，設計可攜 | 零維運；Postgres 留待雲端 |
| 4 | 2026-06-09 | 交易日記 P&L 自動計算、可手動覆寫；金額 **Decimal-as-TEXT** | 避免浮點漂移；SQLite 無原生 Decimal |
| 5 | 2026-06-09 | **TradingView 免費嵌入 widget 不可行**（已實測）| TWSE/TAIFEX/SGX 台灣行情皆「此商品僅在 TradingView 上可用」；Investing.com 嵌入被 Cloudflare 牆；Stooq 僅靜態日線圖 → 必須自繪 |
| 6 | 2026-06-09 | 自繪改用 **自家擷取資料 + Lightweight Charts** | 當時最快路徑；後被決策 #8 取代 |
| 7 | 2026-06-10 | 圖表即時更新用 **5 秒輪詢**（非 SSE 推 K 棒） | K 棒為衍生聚合、隨週期而異；輪詢簡單且與資料節奏一致；報價列仍走 SSE |
| 8 | 2026-06-11 | 圖表庫換 **KLineCharts v9.8.x（釘版）** | 需求＝繪圖工具+內建指標多參數多色+多窗格；LWC 兩者皆缺（自建工程大）；TradingView charting_library 需申請、個人專案難過審；**不升 v10**（API 改名，驗證後再升）|
| 9 | 2026-06-11 | K 棒正準儲存 **只有 1m + 1d**，其餘衍生 | 避免 12 份冗餘儲存與一致性問題；查詢成本可控（上限 20 萬列）|
| 10 | 2026-06-11 | 盤中分桶 **session-anchored**（日盤 08:45、夜盤 15:00 起算）| 對齊台灣看盤軟體慣例（10 分 K = 08:45–08:55）；純 epoch 對齊會產生 08:40 起的桶 |
| 11 | 2026-06-11 | **日K = 日盤 OHLC**，夜盤不併入 | 台灣主流慣例；夜盤合併版本日後可作為選項 |
| 12 | 2026-06-11 | K 棒時間戳 **epoch 毫秒 UTC**；刪除舊 `+8h` hack | KLineCharts 原生 ms + setTimezone；時區數學集中於 bucketing.py |
| 13 | 2026-06-11 | 歷史回補本期**只做日K（FinMind 免費）**；近月=結算日（第三個週三）含當天 | 分鐘級需 Shioaji（永豐帳戶）或 FinMind 付費——`HistoryProvider` 介面已預留，零重工 |
| 14 | 2026-06-11 | 原始 quotes 保留 **7 天**後清除 | candles 已為正準；quotes 僅供除錯與重建（5M 列/年不值得久存）|
| 15 | 2026-06-11 | **Alembic 延後導入** | 本輪僅新增表；觸發條件見 §4 |
| 16 | 2026-06-11 | 雲端目標 **GCP asia-east1 e2-small VM + docker-compose** | 延遲最低；常駐輪詢不適合 serverless；替代案 VPS（少 GCP 生態）、PaaS（always-on 設定繁）|
| 17 | 2026-06-11 | 指標顯示端用 KLineCharts 內建計算；**警示端（Phase 4）伺服器重算** | 顯示零後端成本；警示需在無瀏覽器時運作，公式以需求書 §5.1 為準確保一致 |
| 18 | 2026-06-11 | 繪圖以 (timestamp, price) 錨定、**per symbol 跨週期共用**，存 DB 而非 localStorage | 換瀏覽器/上雲不丟失；單人系統一張表即可 |
| 19 | 2026-06-11 | **讀取效能架構**：K 棒讀取走原生 SQL→float（`FastCandle` tuple，跳過 ORM/Decimal 解析）；candle 路由用 sync `def`（threadpool，不卡 event loop）；伺服器端 TTL 快取（歷史頁 600s／首頁 2s／日K序列 30s）；前端 per-TF 客戶端快取（切換瞬時，只補尾端）+ 背景分頁暫停輪詢 | 實測：日K頁 40ms→1.6ms、週線 61ms→1.6ms、5 秒輪詢 17ms→1.5ms；寫入路徑保持 Decimal 精確不變。快取代價：另程序回補後最多 TTL 內短暫過期（可接受） |
| 20 | 2026-06-11 | **KLineCharts v9 渲染防護**（chart.js 四道防線，勿移除）：指標線樣式必須給完整物件（>5 條時內建預設用罄）；指標窗格逐影格建立（同 tick 連建會靜默卡死渲染迴圈）；所有非同步資料路徑帶週期守衛（過期回應丟棄，防跨週期棒污染布局）；資料套用/縮放後排程看門狗（畫布空白偵測 → `resize()` 自癒，已實證有效） | 連續 9 次重載 + 高壓快速切換（200ms 連打）全數通過；KLineCharts 卡死無例外無 console 錯誤，只能以行為防護 |
| 21 | 2026-06-12 | **分鐘級歷史改用 FinMind Sponsor `TaiwanFuturesTick` 聚合**（取代「等 Shioaji」）：逐日抓 tick（~19MB/日）即時聚合 1 分 K，近月按 15:00 分界參考當日/次日，量為 tick 加總（優於 live 差分） | 使用者升級 Sponsor 後解鎖；歷史可回溯 2011；Shioaji 降為備援選項。Sponsor 另解鎖三大法人/大額交易人 OI 等資料集，列為警示/儀表板未來資料源 |
