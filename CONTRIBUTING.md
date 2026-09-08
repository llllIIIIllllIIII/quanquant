# 貢獻指南（前端協作）

本專案由維護者單人負責後端、資料與部署；歡迎以 **fork＋PR** 的方式協助前端畫面。
本文說明流程與邊界。若你使用 Claude Code 之類的 AI 助手，它會自動讀取根目錄
`CLAUDE.md`，其中「前端協作邊界」一節是硬規則，CI 會機械執行。

## 1. 本機環境

```bash
uv sync --extra dev                 # 安裝（需先裝 uv）
cp .env.example .env                # 預設 SOURCE=taifex，走 TAIFEX 公開輪詢，不需任何券商憑證
uv run quanquant-user bootstrap     # 建第一個登入帳號
uv run quanquant-web                # http://127.0.0.1:8000
```

- 本機沒有歷史 K 線時圖是空的。可向維護者索取去敏的 SQLite 副本放到專案根目錄，
  或 `uv run quanquant-backfill daily`（需在 `.env` 填 FinMind token，免費申請）。
- 盤中開著 `quanquant-web` 幾分鐘就會有 1 分 K 可看。

## 2. 流程

1. 在 GitHub 上 fork，clone 你的 fork，並加上 upstream：
   ```bash
   git remote add upstream git@github.com:llllIIIIllllIIII/quanquant.git
   ```
2. 每次開工先同步，從最新的 upstream/main 開分支：
   ```bash
   git fetch upstream && git checkout -b ui/<主題> upstream/main
   ```
3. 改動只落在白名單目錄：`src/quanquant/web/templates/`、`src/quanquant/web/static/`、`docs/*.md`。
   畫面需要新資料或新 endpoint 時，**先開 issue** 說明需求，由維護者在後端提供後你再接畫面。
4. 送 PR 前本機自檢：
   ```bash
   uv run pytest
   uv run python scripts/ci/check_paths.py upstream/main HEAD
   uv run python scripts/ci/check_simplified.py
   ```
5. 開 PR（base 選 `main`），照 PR 模板填寫：做了什麼、改動前後截圖、有沒有動到敏感檔案。
6. CI 會自動跑：測試全套、路徑守門、測試數量比對、簡體字掃描。CI 紅的 PR 不會被 review。
7. 維護者 review 後 merge，部署到 staging 讓你驗收，再上正式站。部署權限不下放。

## 3. PR 大小與切分

- 一個 PR 只做一件事。超過約 300 行或超過 5 個檔案就拆成多個 PR。
- 同一個頁面同一時間只有一個人在改。開工前在 issue 認領，避免衝突。
- 前端 PR 一律附改動前後截圖，這是最有效的 review 方式。

## 4. 你不會拿到的東西（刻意設計）

正式與 staging 環境的 SSH、gcloud 權限、券商 API 金鑰、FinMind token、資料庫 dump。
這些都不是做前端需要的，也請不要在 PR 或 issue 裡貼任何憑證。

## 5. 常見退回原因

- 動到白名單外的檔案而沒有事先開 issue。
- 引入 CDN、新 CSS／JS 框架或外部字型。CSP 是 `default-src 'self'`，會被靜默擋掉。
- 出現簡體字。全站一律繁體中文、台灣用語。
- 沒有截圖。
- 改了 `hx-confirm`、kill switch、冷靜期、登入或 token 相關畫面的語意卻沒在 PR 說明。
- 為了讓 CI 變綠而改測試或加 skip。
