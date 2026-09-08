# QuanQuant

台指期（TXF）即時監控系統：FastAPI + HTMX/KLineCharts，TAIFEX MIS API 每 5 秒輪詢，1m/1d K 棒為正準儲存。詳細需求與架構：`docs/requirements.md`、`docs/architecture.md`。

## 常用指令

```bash
uv sync --extra dev          # 安裝（含測試工具）
uv run pytest                # 測試（79+，必須全綠才可部署）
uv run quanquant-web         # 本機開發 → http://127.0.0.1:8000
uv run quanquant-backfill daily|minute   # 歷史回補（FinMind）
uv run quanquant-user bootstrap|create|reset-password|list   # 帳戶管理（首次部署先 bootstrap）
```

## 部署（重要：沒有自動 CI/CD）

正式環境在 GCP VM（asia-east1，docker-compose：app + postgres + caddy），對外網址 https://quant.35-229-185-30.sslip.io 。**push 不會自動部署**，更新流程固定三步：

```bash
git commit → git push → ./scripts/deploy.sh
```

`deploy.sh` 會 SSH 進 VM 做 `git pull --ff-only && docker compose up -d --build`（需本機 gcloud 已登入）。部署前務必 `uv run pytest` 全綠。完整部署架構與維運（備份/監控/換網域）見 `docs/deployment.md`。

## 關鍵約束（勿回退）

- `Candle.ts` 必須是 `BigInteger`（epoch-ms 會溢位 Postgres 4-byte INTEGER）
- 資料庫雙方言：本機 SQLite / 雲端 Postgres — 新增 raw SQL 必須兩邊可攜（named params、dialect-aware upsert，參考 `candles/repo.py`）
- pyproject 的 hatch wheel 設定**不可加 force-include**（與 packages 重複收錄會炸 Docker build）
- KLineCharts 釘版 v9.8.12，升版前先驗證（v10 改 API 名）；`chart.js` 的四道渲染防線勿移除
- KLineCharts locale 必須用繁體 `zh-TW`（`chart.js` 以 `registerLocale` 註冊，勿改回內建 `zh-CN`，否則十字游標標籤時間/開/高/低/收/成交量會變回簡體）；全站中文一律繁體台灣
- 本機 `quanquant.db` 含珍貴回補歷史，驗證清理時勿刪
- candle 讀取路徑走 raw-SQL→float（FastCandle），routes 為 sync `def` — 勿改 async/ORM
- `.env` 與 `*.dump` 不進 git

## 前端協作邊界（fork＋PR 貢獻者與其 AI 助手必讀）

本節只約束非維護者的貢獻（fork 後開 PR 回 main）。維護者不受此節限制：PR 作者是 repo owner、或維護者掛上 `maintainer-change` label 時，pr-guard 只提示不擋。流程細節見 `CONTRIBUTING.md`，CI 會機械執行下列規則。

- **可以動**：`src/quanquant/web/templates/**`、`src/quanquant/web/static/**`、`docs/**/*.md`。
- **永遠不可動**（CI 直接擋，即使有 label）：`.github/**`、`CLAUDE.md`／`AGENTS.md`（前者是後者的 symlink）、`CONTRIBUTING.md`、`scripts/**`、`pyproject.toml`、`uv.lock`、`tests/**`、docker／Caddy 設定檔。
- **白名單以外的 `src/quanquant/**`**（routers、models、broker、agent、auth、candles 等）預設不動。畫面需要新資料、新欄位、新 endpoint 時，**停下來開 issue** 描述需求，由維護者先在後端提供，再由貢獻者接畫面。不要自己加 router、改 context 變數、改 model；PR 動到這些檔案會被 CI 標紅，只有維護者加上 `backend-change` label 才放行。
- **動了必須在 PR 說明的敏感前端檔案**：`static/chart.js`（四道渲染防線）、`static/chart-guards.js`、`partials/kill_switch_control.html`、`partials/cooldown_control.html`、`partials/agent_connection_control.html`、`partials/agent_token_control.html`、`login.html`、`admin_*.html`，以及任何帶 `hx-confirm` 的表單。這些承載風控與權限語意，「順手美化」拿掉一個屬性就是事故；`hx-confirm` 要放 form 層。
- **不引入**新的 CSS／JS 框架、CDN 或外部字型：CSP 是 `default-src 'self'`，外部資源會被擋且靜默失敗。樣式沿用 Pico CSS＋`app.css`＋`tokens.css`；K 線沿用釘版的 KLineCharts。
- **不改測試**來讓 CI 變綠、不加 skip；CI 會比對 PR 前後的測試收集數量，減少即失敗。
- **一律繁體中文（台灣用語）**：CI 會掃簡體字。
- **PR 要小**：一個 PR 一件事，附改動前後截圖。送出前本機跑 `uv run pytest`、`uv run python scripts/ci/check_paths.py upstream/main HEAD`、`uv run python scripts/ci/check_simplified.py`。
