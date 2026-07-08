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
