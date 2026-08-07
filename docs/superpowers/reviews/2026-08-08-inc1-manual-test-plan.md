# 本機 Broker Agent Increment 1（多人 simtrade）— 人工測試流程

**適用範圍**：`feat/agent-multiuser-inc1` 分支，Task 1-13 全數完成（1089 pytest 綠、ruff
全庫歸零）之後，交使用者實測用的 checklist。比照 Inc0 Task 16 的驗收記錄風格
（`docs/superpowers/reviews/2026-08-06-local-broker-agent-inc0-sim-verification.md`），
但範圍擴大到多人隔離／command ledger／fail-stop／unknown 收斂等 Inc1 新增機制。

**前置事實來源**：`docs/superpowers/specs/2026-08-06-local-broker-agent-inc1-design.md`
（設計 spec，§4 決策清單／§6 測試重點／§10 實作實現註記）、`src/quanquant/config.py`
（設定鍵實碼）、`src/quanquant/db/models.py`（schema 實碼）。所有 sqlite3 指令已對照
`init_db()` 實際跑出的 migrated schema 逐條核對過（詳見本文件末尾「驗證方式」）。

**全域提醒**：
- Increment 1 仍鎖 `ORDER_MODE=sim`（協定/CLI/server/child 四層不開 `real`），本文件全程
  只用 simtrade，無金錢風險。
- 本機 `quanquant.db` 含珍貴回補歷史（K 棒資料），**任何步驟都不得刪改 `candles`/`trades`
  等既有表**；本文件唯一涉及清理的是下單子系統相關的 `raw_inbox` 少量歷史列（見情境 7），
  且以「標記為已終結」為優先做法，非必要不用 `DELETE`。
- 每個情境的 sqlite3 指令預設對本機 `./quanquant.db` 下（相對於專案根目錄執行
  `sqlite3 quanquant.db "..."`）；若在別的路徑執行，請自行改成絕對路徑。
- Agent 端憑證一律 session-only（getpass/env 讀入記憶體，不落地、不進 argv/log）；本文件
  範例指令若含 token/API Key，執行完畢後請自行從終端機歷史紀錄清除。

---

## 總覽（先勾選，詳細步驟見下方各節）

| # | 情境 | 結果（PASS/FAIL/備註） |
|---|---|---|
| 1 | 單人 in-process 迴歸不壞 | |
| 2 | token 簽發與 rotation | |
| 3 | 雙使用者隔離（或替代方案） | |
| 4 | kill switch 兩層 | |
| 5 | 斷線收斂（unknown → 補送 → submitted/filled） | |
| 6 | fail-stop 演練（G2） | |
| 7 | 歷史 quarantine 清理 | |
| 8 | place unknown 人工終結程序 | |

---

## 0. 前置準備

1. 確認分支與依賴：
   ```bash
   cd /Users/henrychang/Desktop/MyProjects/QuanQuant
   git status                 # 確認在 feat/agent-multiuser-inc1，且無未預期改動
   uv sync --extra dev
   uv run pytest -q           # 全套自動化測試作為起跑點基線，應為 1089 passed
   ```
2. 確認至少有一個既有 QuanQuant 帳號（`uv run quanquant-user list`）；情境 3/4 完整版需要
   **兩個**帳號與**兩組**永豐 simtrade API Key/Secret，沒有第二組時見各情境的替代方案。
3. `.env`（本機開發用 `.env.local` 亦可）需要的鍵：
   ```
   ORDER_CHANNEL=agent
   ORDER_MODE=sim
   ORDER_OWNER_USER_IDS=<你的 user id，逗號分隔多個>
   SESSION_SECRET=<任意隨機字串，本機測試可用 dev 值>
   ```
   agent 模式下**不需要**在伺服器端設定 `SHIOAJI_TRADE_API_KEY`/`SHIOAJI_TRADE_SECRET_KEY`
   /`SHIOAJI_CA_*`——永豐憑證只活在 agent 端（每次啟動 `quanquant-agent` 互動輸入或走
   `QQ_AGENT_API_KEY`/`QQ_AGENT_SECRET_KEY` 環境變數）。
4. 啟動服務：
   ```bash
   uv run quanquant-web        # http://127.0.0.1:8000
   ```
   **首次在這個 DB 檔上啟動這個分支**時，`init_db()` 會自動：對 `raw_inbox` 補
   `user_id`/`account`/`mode`/`quarantine_reason` 四個 nullable 欄位（`ensure_columns`）；
   建立 `agent_tokens`/`agent_commands`/`agent_account_bindings` 三張新表（`create_all`）；
   對既有 `orders` 資料做帳號↔使用者綁定 backfill。若終端機印出「帳號綁定 backfill 衝突，
   agent 下單子系統拒啟」，代表本機 DB 的 `orders` 表裡同一個永豐帳號歷史上曾被判定屬於
   不同 user——這種情況下請先聯絡開發者裁決，不要自行改 DB。

---

## 1. 單人 in-process 迴歸不壞

**目的**：確認 Inc1 的多人改動完全不影響 `ORDER_CHANNEL=inprocess`（Increment 0 之前既有的
單機直連）路徑。

**操作步驟**：
```bash
# .env 暫時改回（或直接不設，inprocess 是預設值）
ORDER_CHANNEL=inprocess

uv run quanquant-web
```
瀏覽器開 http://127.0.0.1:8000/orders，確認頁面正常渲染（下單面板/委託列表/部位表皆
出現，且**不出現**「Agent Token」管理段與 agent 連線 badge——這兩個 UI 元素只在
`ORDER_CHANNEL=agent` 時渲染）。

**預期結果**：
- `curl -s http://127.0.0.1:8000/healthz | python3 -m json.tool` 回 `"status":"ok"`，
  `order_subsystem.status` 為 `"ready"`（已設 owner/CA 或 sim key 時）或 `"disabled"`
  （未設定憑證時，非故障）。
- `/orders` 頁面 200，無 500、無 traceback。
- 若已有 sim 憑證，可正常下 1 口 MKT/IOC 測試單，行為與 Inc0 完全一致（本情境非必須，
  重點是「頁面/健康檢查不壞」）。

**檢查方式**：
```bash
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/healthz
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/orders   # 需先登入 cookie，未登入應是 303 導向 /login，非 500
```

---

## 2. token 簽發與 rotation

**目的**：驗證 D2 per-user DB opaque token 的簽發、握手、rotation 後舊 token 失效。

**操作步驟**：
1. `.env` 切回 `ORDER_CHANNEL=agent`，`ORDER_OWNER_USER_IDS` 含你的 user id，重啟
   `uv run quanquant-web`。
2. 瀏覽器登入你的帳號，開 `/orders`，找到「Agent Token」段，按「產生 Agent Token」。
   複製顯示的明文 token（只顯示這一次，記下來，例如存進 shell 變數）：
   ```bash
   export QQ_AGENT_TOKEN="<剛複製的明文 token>"
   ```
3. 另開一個終端機啟動 agent（用 env 變數可跳過互動輸入）：
   ```bash
   export QQ_AGENT_API_KEY="<你的永豐 simtrade API Key>"
   export QQ_AGENT_SECRET_KEY="<你的永豐 simtrade Secret Key>"
   uv run quanquant-agent
   ```
   終端機應印出「agent 啟動（simtrade）→ ws://127.0.0.1:8000/ws/agent；Ctrl-C 結束」且**不**
   立即斷線退出。
4. 回瀏覽器 `/orders` 頁（可手動重新整理，或等 SSE 自動刷新），確認 badge 變成
   「🟢 agent 已連線」。
5. **rotation**：回 orders 頁，按「Agent Token」段的「重新產生（原 token 立即作廢）」，
   複製新 token 存成 `QQ_AGENT_TOKEN_NEW`。**注意**：rotation 只讓 DB 的舊 token 立即失效，
   **不會**強制踢掉目前已經用舊 token 握手成功、正在跑的那個 agent 連線（該連線的驗證只發生
   在一開始握手當下）——要驗證舊 token 真的被拒，需要讓舊 agent **重新連線**：
   ```bash
   # 回到步驟 3 的終端機，Ctrl-C 停掉目前的 agent（用的是舊 token）
   # 用舊 token 再啟動一次，預期立即被拒
   QQ_AGENT_TOKEN="$QQ_AGENT_TOKEN" QQ_AGENT_API_KEY="..." QQ_AGENT_SECRET_KEY="..." uv run quanquant-agent
   ```
6. 用新 token 啟動 agent，確認能重新連上：
   ```bash
   QQ_AGENT_TOKEN="$QQ_AGENT_TOKEN_NEW" QQ_AGENT_API_KEY="..." QQ_AGENT_SECRET_KEY="..." uv run quanquant-agent
   ```

**預期結果**：
- 步驟 4：badge 綠燈。
- 步驟 5 用舊 token 重連：agent 端會不斷 backoff 重連並失敗（WS 在握手階段被 server
  `close(code=1008)`）；orders 頁 badge 應維持/回到「🔴 agent 未連線」。
- 步驟 6：新 token 握手成功，badge 恢復綠燈。

**檢查方式**：
```bash
sqlite3 quanquant.db "SELECT id, user_id, substr(token_hash,1,12) AS hash_prefix, \
  created_at, expires_at, revoked_at, last_used_at FROM agent_tokens ORDER BY id DESC LIMIT 5;"
```
預期看到（至少）兩列：較舊那列 `revoked_at` 非空（被 rotation 撤銷）、較新那列
`revoked_at` 為空且 `last_used_at` 在你用新 token 連線後有更新。

---

## 3. 雙使用者隔離

**目的**：驗證兩個使用者的連線狀態/資料完全互不影響（I8 不變量）。

**完整版（需要兩個 QuanQuant 帳號＋兩組永豐 simtrade API Key/Secret）**：
1. `.env` 的 `ORDER_OWNER_USER_IDS` 同時列兩個 uid，例如 `1,2`，重啟服務。
2. 開兩個瀏覽器 session（例如一般視窗 + 無痕視窗），分別用兩個帳號登入，各自在 `/orders`
   頁簽發自己的 agent token。
3. 開兩個終端機，各自用不同的 `--buffer` 路徑（避免兩個 agent 共用同一個本機檔）與各自的
   token/sim key 啟動：
   ```bash
   # 使用者 A
   QQ_AGENT_TOKEN=<A的token> QQ_AGENT_API_KEY=<A的key> QQ_AGENT_SECRET_KEY=<A的secret> \
     uv run quanquant-agent --buffer ~/.quanquant-agent-A/outbox.db

   # 使用者 B（另一個終端機）
   QQ_AGENT_TOKEN=<B的token> QQ_AGENT_API_KEY=<B的key> QQ_AGENT_SECRET_KEY=<B的secret> \
     uv run quanquant-agent --buffer ~/.quanquant-agent-B/outbox.db
   ```
4. 兩邊各自的 orders 頁 badge 應各自轉綠。A 下一筆委託，B 的委託列表/部位表**不應**出現
   這筆委託（`/orders/list`／`/orders/positions` 皆以 `user_id` 過濾）。

**預期結果**：A、B 的委託/部位/agent 連線狀態完全獨立；A 斷線不影響 B 的 badge；A 的
quarantine 積壓（若有）不影響 B 登入。

**檢查方式**：
```bash
sqlite3 quanquant.db "SELECT id, client_order_id, user_id, symbol, action, qty, status \
  FROM orders ORDER BY id DESC LIMIT 10;"
sqlite3 quanquant.db "SELECT broker, account, user_id, bound_at FROM agent_account_bindings;"
```
確認 A、B 各自的委託列 `user_id` 分別對到各自帳號的 id，且 `agent_account_bindings` 每個
永豐帳號只對到一個 user_id（同一帳號出現兩個不同 user_id 就是隔離破功，需立即回報）。

**替代方案（沒有第二組 sim key 時）**：只驗證「欄位真的有蓋章」而非「兩人互不干擾」的
即時效果——單人下單後，檢查以下三個 scope 欄位都確實被填上你的 user id（不是 NULL）：
```bash
sqlite3 quanquant.db "SELECT id, client_order_id, user_id, account, mode, status \
  FROM orders ORDER BY id DESC LIMIT 3;"
sqlite3 quanquant.db "SELECT id, kind, user_id, account, mode, quarantine_reason \
  FROM raw_inbox ORDER BY id DESC LIMIT 5;"
sqlite3 quanquant.db "SELECT cmd_id, user_id, kind, account, mode FROM agent_commands \
  ORDER BY created_at DESC LIMIT 5;"
```
`orders.user_id`、`raw_inbox.user_id`（agent 模式新產生的列）、`agent_commands.user_id`
皆應為你的 user id，非 NULL——代表 D5/D4 的 scope 蓋章機制確實在跑，只是缺第二人無法用行為
證明互不干擾（此部分已由自動化測試 `test_agent_multiuser_e2e.py` 覆蓋，見
`.superpowers/sdd/task-13-report.md`）。

---

## 4. kill switch 兩層

**目的**：驗證 D3「我的急停」只擋自己、「全站急停」擋全部人、取消單不受兩層開關影響。

**操作步驟（單人也可做前三項；第四項需情境 3 的雙人環境）**：
1. 在 orders 頁按「啟動我的急停」。嘗試下一筆新單。
2. 確認取消既有掛單（若有）仍然成功。
3. 按「解除我的急停」，確認新單恢復正常。
4. 按「啟動全站急停（拒絕所有人新單）」。若有第二個使用者（情境 3 環境），確認**對方**
   的新單也被擋；解除後兩人皆恢復。

**預期結果**：
- 步驟 1：下單表單回錯誤訊息（阻擋新單），badge 顯示「🔴 我的急停已啟動」。
- 步驟 2：取消動作正常完成（HTTP 200，委託狀態轉 cancelled），不受任何一層 kill switch
  影響。
- 步驟 4：全站急停開啟時，**任何**使用者（不限翻閘者本人）下單皆被擋；「我的急停」與
  「全站急停」是 OR 關係（`blocked(uid) = global_on OR per_user.get(uid)`）。

**檢查方式**：
```bash
sqlite3 quanquant.db "SELECT id, ts, actor_user_id, mode, action, result, detail \
  FROM order_audits WHERE action IN ('risk_reject') ORDER BY id DESC LIMIT 10;"
```
確認被擋的下單嘗試有留稽核紀錄；也可觀察 Telegram（若已設定
`OPS_TELEGRAM_BOT_TOKEN`/`OPS_TELEGRAM_CHAT_ID`）是否收到 kill switch 告警（`scope` 欄位
應分別顯示「個人（我的）」/「全站總閘」）。

---

## 5. 斷線收斂（unknown → 補送 → submitted/filled）

**目的**：驗證 G1 command ledger 的核心場景——下單指令的 ack 因 agent 斷線而遺失，重連後
自動補送，最終收斂為正確終態，配額不多不少。

**操作步驟**：
1. 確保 agent 已連線（badge 綠燈）。準備好可以隨時 Ctrl-C 該 agent 終端機的姿勢。
2. 在下單面板送出一筆委託（如 Buy 1 口 TXF MKT/IOC），**送出的同時立刻切到 agent 的
   終端機按 Ctrl-C**。不需要精準卡秒數——WS 連線一旦斷開，server 端等待這筆指令 ack 的
   future 會被立即標記為「連線中斷，指令結果未知」，效果與真的等滿逾時完全等價（逾時判斷
   用的是 `agent_command_timeout_seconds`，預設 10 秒；`agent_command_expiry_seconds`
   預設 120 秒是給 agent 端判斷指令是否過期用的，兩者不同）。若手腳太慢、委託在 Ctrl-C
   前就已經顯示 submitted/filled，代表 ack 已經先落地，換一筆新單再試一次即可。
3. 觀察 orders 頁：這筆委託應停在某個未終結狀態（`pending`/`sending`，UI 上可能顯示為
   處理中；下方 sqlite3 查詢會看到 `status` 仍非終態、`agent_commands` 該列
   `outcome IS NULL`）。
4. 重啟 agent（用步驟 2 之前記下的同一組 token/sim key）：
   ```bash
   QQ_AGENT_TOKEN="$QQ_AGENT_TOKEN" QQ_AGENT_API_KEY="..." QQ_AGENT_SECRET_KEY="..." \
     uv run quanquant-agent --buffer <與剛才同一個 --buffer 路徑>
   ```
5. 觀察委託是否在數秒內自動收斂為 `submitted` 或 `filled`（sim 模式下 MKT/IOC 通常很快
   成交）。

**已知殘留風險（非本次要抓的 bug，見 spec §8 第 1 點）**：若 Ctrl-C 發生在「agent 子程序
已經把委託送到永豐、但還來不及把結果寫回本機 ledger」的極短窗口內，重連補送會讓 agent
**重新執行**這筆 place（因為本機 ledger 查無這個 cmd_id），可能在券商端造成同一委託被
下兩次（sim 環境無金錢風險，但你的部位口數可能因此比預期多）。若觀察到這個情況，屬已知
設計限制，非本次驗收需要回報的異常；直接視需要取消多出來的委託/反向沖銷部位即可。

**預期結果**：委託最終狀態為 `submitted`/`filled`（不會卡死在中繼態），配額「對平」——即
沒有重複佔用、也沒有莫名釋放：一筆委託只對應一份計入當日已用配額的紀錄。

**檢查方式**：
```bash
# 1) 找出剛才那筆委託（依 client_order_id 或用時間排序取最新一筆）
sqlite3 quanquant.db "SELECT id, client_order_id, status, ordno, broker_order_id, qty \
  FROM orders ORDER BY id DESC LIMIT 3;"

# 2) 對應的 agent_commands 兩維終結欄位——重連補送成功後，outcome 應為 'ok'，
#    resolved_via 應為 'ack'（收到補送的 late ack），resolved_at 非空
sqlite3 quanquant.db "SELECT cmd_id, kind, transport_acked_at, outcome, resolved_via, \
  resolved_at, sent_at FROM agent_commands WHERE client_order_id='<上面查到的client_order_id>';"

# 3) 配額對平——這筆委託對應的 reservation 應為 'confirmed'（成功送出，永久計入當日配額），
#    不應停在 'reserved'（表示還沒收斂）或錯誤地變成 'released'（表示被誤判失敗退配額）
sqlite3 quanquant.db "SELECT reservation_id, user_id, mode, trading_day, qty, state \
  FROM quota_reservations WHERE reservation_id='<上面查到的client_order_id>';"
```

---

## 6. fail-stop 演練（G2）

**目的**：驗證 agent 本機 durable buffer 寫入失敗時的 fail-stop 狀態機——拒絕新指令、UI
紅燈、（若有設定）Telegram 告警，修復儲存問題後**手動重啟 agent** 才會恢復（2026-08-08
設計降級：G2 恢復不再是 session 進行中自動探測，只在 agent 程序啟動時做一次 storage
probe，通過才清除 fail-stop；latch 之後若不重啟 agent，會一路保持 fail-stop 到操作者
手動介入為止）。

**做法**：把 agent 的本機 buffer 目錄整個改成唯讀，讓 agent 端「主寫入（SQLite）」與
「退化寫入（純檔案 append）」**兩條路徑都失敗**（只鎖 `outbox.db` 檔本身不夠——那樣
退化寫入會成功接住，不會觸發 fail-stop；必須連目錄本身的可寫入權限一起收回）。

**操作步驟**：
1. 確認 agent 已連線且健康（badge 綠燈），且該 agent 用的是預設 buffer 路徑
   `~/.quanquant-agent/outbox.db`（或你自訂的 `--buffer` 路徑，下面指令請自行代換）。
2. **另開一個終端機**，把 buffer 目錄整個改唯讀：
   ```bash
   chmod -R -w ~/.quanquant-agent
   ```
3. 在下單面板送出一筆新單（觸發成交回報，讓 agent 的 callback 嘗試寫入已唯讀的 buffer）。
4. 觀察 orders 頁 badge：預期在數秒到最多約 15 秒內（下一次心跳週期）轉為紅燈，顯示固定
   訊息「agent 儲存故障，交易已停止」。**注意**：因為本測試把整個目錄都鎖唯讀（比純粹只有
   SQLite 主檔損毀的情境更嚴苛——連 sentinel latch 檔 `outbox.db.failstop` 都寫不進去），
   agent 端終端機可能會印出一行 `Task exception was never retrieved` 之類的例外訊息——這是
   已知的次要邊界情況（唯讀範圍波及了原本設計成獨立路徑的 sentinel 檔），核心的「拒絕新
   指令＋最終仍會回報 failstop」結論不受影響，健康訊息會靠週期性心跳（約 15 秒一次）補上。
5. 再送一筆新單，確認被拒（agent 端直接拒絕，不會嘗試呼叫永豐 API）。
6. 修復儲存問題後**重啟 agent**（Ctrl-C 再啟動）：
   ```bash
   chmod -R u+w ~/.quanquant-agent
   ```
   復原權限後，回到跑 `quanquant-agent` 的終端機，Ctrl-C 停掉這個 agent 程序，用同一組
   token/sim key 重新啟動（見情境 5 步驟 4 的啟動指令）。
7. 觀察 agent 啟動時的終端機輸出，應出現一行「已從 failstop 恢復（啟動時儲存探測通過）」
   的 log；回頭確認 orders 頁 badge 恢復綠燈，新單恢復正常。

**預期結果**：
- 步驟 4：badge 轉紅，文案為「agent 儲存故障，交易已停止」（不得洩漏 agent 本機的原始
  例外字串/檔案路徑——那些只會進 agent 本機 log 與 server log，不進 UI）。
- 若設定了 `OPS_TELEGRAM_BOT_TOKEN`/`OPS_TELEGRAM_CHAT_ID`：應收到一則「agent 進入
  fail-stop（拒絕新單）」告警；恢復後應再收到一則「agent 解除 fail-stop」告警。
- 步驟 5：新單被拒，且**不應**在永豐 sim 後台看到這筆委託真的被送出（agent 落地失敗時是
  「先發現寫不進去、再決定要不要送」，不是「送出去才發現寫不進去」）。
- 步驟 6-7（2026-08-08 設計降級）：badge **不會**在原地自動恢復——G2 恢復只在 agent 程序
  啟動時做一次 storage probe，session 進行中 latch 後永不自動解除。必須先修好底層儲存
  問題、**手動重啟 agent 程序**，新一輪啟動時的探測通過後才會清除 fail-stop、badge 才會
  轉綠；重啟後只需要沿用同一組 token/sim key 即可，不需要重新在網頁端做任何額外操作。

**檢查方式**：
```bash
# 恢復期間 orders 頁 agent-status 這支 route 直接查也可以（HTML 片段，非 JSON）：
curl -s http://127.0.0.1:8000/orders/agent-status --cookie "<你的 session cookie>"

# /healthz 在 agent 模式下不反映個別使用者的 failstop（D9 語意變更），這裡預期仍是 200，
# 不是驗證 fail-stop 有沒有生效的地方：
curl -s http://127.0.0.1:8000/healthz | python3 -m json.tool
```
（`--cookie` 需要先在瀏覽器登入後從開發者工具複製 session cookie 值，或直接用瀏覽器頁面
肉眼確認 badge 顏色即可，不一定要跑這條 curl。）

**進階（可選）精確版**：若想避免上面「整個目錄唯讀」造成的心跳延遲與例外雜訊，可改用更
精確的做法——只鎖住會被寫入的具體檔案、保留目錄本身可寫（讓 sentinel 檔仍能即時寫入）：
```bash
# 先確保 outbox.db-wal / outbox.db-shm 存在（agent 正常運作一段時間後就會有）；
# 若尚未出現過 .degraded.jsonl，先手動建一個空檔佔位
touch ~/.quanquant-agent/outbox.db.degraded.jsonl
chmod 444 ~/.quanquant-agent/outbox.db ~/.quanquant-agent/outbox.db-wal \
  ~/.quanquant-agent/outbox.db-shm ~/.quanquant-agent/outbox.db.degraded.jsonl
# 復原：
chmod 644 ~/.quanquant-agent/outbox.db ~/.quanquant-agent/outbox.db-wal \
  ~/.quanquant-agent/outbox.db-shm ~/.quanquant-agent/outbox.db.degraded.jsonl
```
這個版本下，sentinel 寫入會成功、badge 轉紅應在 1-2 秒內發生，且不會有上述例外雜訊。
同樣地，`chmod` 復原權限後仍需照上面步驟 6 手動重啟 agent 才會恢復（不會原地自動恢復）。

---

## 7. 歷史 quarantine 清理

**背景**：本機 `quanquant.db` 的 `raw_inbox` 表裡，有幾筆 Increment 0 時代（`raw_inbox`
尚未有 `user_id`/`account`/`mode`/`quarantine_reason` 四個 scope 欄位以前）留下的
quarantine 列——本文件撰寫當下（2026-08-07）查得 **7 筆**（2026-07-26～2026-07-28 各一批，
非 5 筆，`docs/superpowers/reviews/2026-08-06-local-broker-agent-inc0-sim-verification.md`
記錄的「5 筆」是更早一次盤點的結果，之後又累積了幾筆——執行本節前請重新 SELECT 確認實際
筆數，以你自己機器上的查詢結果為準）。

**目前程式碼下的實際影響（已對照 `src/quanquant/broker/repository.py` 實碼核實）**：
`count_unprocessed_for_login`（登入換帳號 guard）與 `unquarantine_stale_raw_inbox`
（per-slot watchdog 的自動解隔離重試）兩者都是以 `RawInbox.user_id == <具體某個 user 的
id>` 精確比對來 scope，SQL 對 `user_id IS NULL` 的舊列**不會**命中（NULL 不等於任何具體
整數值）——也就是說，這批歷史列**不會**阻擋任何使用者登入，也不會被任何 per-slot watchdog
誤認領。清理這批列**不是 Inc1 啟用多人的阻塞項**，純粹是保持 `raw_inbox` 只留合法運作
歷史、避免未來人工排查時混淆的衛生工作。

**操作步驟**：
```bash
# 1. 先 SELECT 確認範圍——只鎖定「quarantine=1 且 user_id 是 NULL」的舊列
#    （agent 模式下新產生的 quarantine 列一定會有 user_id，不會被這個條件誤傷）
sqlite3 quanquant.db "SELECT id, kind, quarantine, processed, quarantine_reason, \
  user_id, account, error, received_at FROM raw_inbox WHERE quarantine=1 AND user_id IS NULL;"

# 2.（建議做法，保留稽核痕跡不刪列）標記為已終結的 dead-letter，退出所有重試/計數迴圈：
sqlite3 quanquant.db "UPDATE raw_inbox SET quarantine_reason='manual_cleanup_pre_inc1', \
  processed=1 WHERE quarantine=1 AND user_id IS NULL;"

# 3. 驗證：這批列現在應該 processed=1，且 quarantine_reason 不再是 NULL
sqlite3 quanquant.db "SELECT id, processed, quarantine_reason FROM raw_inbox \
  WHERE quarantine=1 AND user_id IS NULL;"
```
`quarantine_reason` 填的字串（`manual_cleanup_pre_inc1`）不需要是系統保留的三個 dead-letter
值（`scope_violation`/`payload_mismatch`/`user_mismatch`）之一——只要不是 `NULL` 或
`'association_pending'`，就會被 `unquarantine_stale_raw_inbox` 的重試迴圈排除在外，效果
等同永久 dead-letter；用一個好辨識的自訂字串即可。這是**手動更新既有列**，不會觸發
`quarantine_raw_inbox()`（新產生 dead-letter 列時才呼叫）內建的告警邏輯，不會誤發 Telegram。

**若確定不再需要保留這些列**（純粹想清空、不在意稽核痕跡），可以直接刪除，但**僅限**
`quarantine=1 AND user_id IS NULL` 這個精確條件、且務必先跑過上面的 SELECT 核對筆數：
```bash
# 危險操作，僅在明確需要且已核對過筆數/id 時才執行；本文件不建議優先使用這個版本
sqlite3 quanquant.db "DELETE FROM raw_inbox WHERE quarantine=1 AND user_id IS NULL;"
```

**預期結果**：清理後，`raw_inbox` 裡不再有「已處理但仍卡在 quarantine 且無主」的歷史雜訊；
`SELECT COUNT(*) FROM raw_inbox WHERE quarantine=1 AND processed=0;` 應回 0（假設本機沒有
其他當下正在走 `association_pending` 重試流程的新鮮列）。

---

## 8. place unknown 人工終結程序

**背景**：D4 明訂 place 指令一旦落入 `outcome='unknown'`（送出後逾時/斷線，且沒有任何
`ordno`/`broker_order_id` 可關聯回券商），**永不自動 release** 配額保留——因為系統無法
分辨「真的沒送出去」與「送出去了只是沒收到回覆」，寧可保守卡住配額也不誤判成失敗而讓
使用者以為當日額度還有空間。這類委託需要人工核對永豐 sim 後台的實際成交/委託記錄後，
手動終結。

**操作步驟**：

1. 找出候選（`kind='place' AND outcome='unknown' AND resolved_at IS NULL`）：
   ```bash
   sqlite3 quanquant.db "SELECT cmd_id, user_id, client_order_id, reservation_id, \
     payload, created_at FROM agent_commands \
     WHERE kind='place' AND outcome='unknown' AND resolved_at IS NULL;"
   ```
2. 對每一筆候選，查對應的 `Order` 目前狀態：
   ```bash
   sqlite3 quanquant.db "SELECT id, client_order_id, status, ordno, broker_order_id, \
     symbol, action, qty, price FROM orders WHERE client_order_id='<上面查到的 client_order_id>';"
   ```
3. **到永豐 simtrade 後台（或用其他管道）人工核對**：這筆委託（依 `payload` 內容比對
   symbol/action/qty/price/下單時間）究竟有沒有真的在券商端成立。這一步無法用 SQL 完成，
   是本程序唯一需要人工判斷的地方。
4. 依核對結果，二選一收斂：

   **(a) 確認未在券商端成立** → 標記失敗、釋放配額：
   ```bash
   sqlite3 quanquant.db "UPDATE agent_commands SET outcome='error', resolved_via='manual', \
     resolved_at=datetime('now'), \
     result='{\"message\":\"人工核對永豐後台未成立，判定失敗並釋放配額\"}' \
     WHERE cmd_id='<cmd_id>' AND resolved_at IS NULL;"

   sqlite3 quanquant.db "UPDATE orders SET status='failed', updated_at=datetime('now') \
     WHERE client_order_id='<client_order_id>';"

   sqlite3 quanquant.db "UPDATE quota_reservations SET state='released', \
     updated_at=datetime('now') WHERE reservation_id='<reservation_id，即 client_order_id>' \
     AND state='reserved';"
   ```

   **(b) 確認已在券商端成立**（查到真實 ordno/委託編號）→ 補記並確認配額：
   ```bash
   sqlite3 quanquant.db "UPDATE agent_commands SET outcome='ok', resolved_via='manual', \
     resolved_at=datetime('now'), \
     result='{\"message\":\"人工核對永豐後台已成立，補記並確認配額\"}' \
     WHERE cmd_id='<cmd_id>' AND resolved_at IS NULL;"

   sqlite3 quanquant.db "UPDATE orders SET status='submitted', ordno='<實際查到的 ordno>', \
     broker_order_id='<實際查到的委託編號>', updated_at=datetime('now') \
     WHERE client_order_id='<client_order_id>';"

   sqlite3 quanquant.db "UPDATE quota_reservations SET state='confirmed', \
     updated_at=datetime('now') WHERE reservation_id='<reservation_id>' AND state='reserved';"
   ```
5. 驗證收斂結果：
   ```bash
   sqlite3 quanquant.db "SELECT cmd_id, outcome, resolved_via, resolved_at FROM agent_commands \
     WHERE cmd_id='<cmd_id>';"
   sqlite3 quanquant.db "SELECT client_order_id, status, ordno FROM orders \
     WHERE client_order_id='<client_order_id>';"
   sqlite3 quanquant.db "SELECT reservation_id, state FROM quota_reservations \
     WHERE reservation_id='<reservation_id>';"
   ```

**預期結果**：`agent_commands` 該列 `resolved_at` 非空、`resolved_via='manual'`；`orders`
與 `quota_reservations` 的狀態與人工核對的實況一致（未成立→failed+released；已成立→
submitted+confirmed，且往後成交回報能靠補上的 ordno 正常匹配到這張委託）。

**注意事項**：
- 上面的 `UPDATE agent_commands` 語句都帶了 `AND resolved_at IS NULL` 這個 CAS 條件——
  若同一時間系統自己（late ack 或 query_qty resolver）也收斂了這筆指令，你的手動 UPDATE
  會影響 0 列（`sqlite3` 不會報錯，但也不會真的改到東西），這是**預期行為**，代表系統已經
  自動處理過了，不需要再手動介入；執行完後務必用步驟 5 的 SELECT 確認實際欄位值，不要只
  看指令有沒有報錯。
- `agent_commands.payload` 欄位是 JSON TEXT（下單當下的原始內容），可用
  `sqlite3 quanquant.db "SELECT payload FROM agent_commands WHERE cmd_id='<cmd_id>';" | python3 -m json.tool`
  美化輸出方便核對 symbol/action/qty/price。

---

## 附錄：常用 sqlite3 查詢速查表

```bash
# 目前所有下單子系統相關表
sqlite3 quanquant.db ".tables"

# 某使用者最近的委託
sqlite3 quanquant.db "SELECT id, client_order_id, status, symbol, action, qty, price, \
  ordno, created_at FROM orders WHERE user_id=<uid> ORDER BY id DESC LIMIT 20;"

# 未收斂的 agent_commands（不分 kind）——G1 健康度總覽
sqlite3 quanquant.db "SELECT cmd_id, user_id, kind, outcome, transport_acked_at, \
  resolved_at, created_at FROM agent_commands WHERE resolved_at IS NULL \
  ORDER BY created_at;"

# 當日配額使用量（某 user/mode/trading_day）
sqlite3 quanquant.db "SELECT SUM(qty) FROM quota_reservations \
  WHERE user_id=<uid> AND mode='sim' AND trading_day='<YYYY-MM-DD>' \
  AND state IN ('reserved','confirmed');"

# agent 本機 buffer（在該 agent 所在電腦執行，非 server 端 quanquant.db）
sqlite3 ~/.quanquant-agent/outbox.db "SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL;"
sqlite3 ~/.quanquant-agent/outbox.db "SELECT cmd_id, kind, executed_at FROM command_ledger \
  ORDER BY executed_at DESC LIMIT 10;"
sqlite3 ~/.quanquant-agent/outbox.db "SELECT key, value FROM meta;"
```

---

## 驗證方式（本文件的自我查核記錄）

本文件所有 `sqlite3` 指令，皆已於撰寫當下對照下列步驟核實過**語法與欄位名**（非只憑讀
`models.py` 猜測）：

1. 複製本機 `quanquant.db` 到暫存路徑，用 `DB_URL="sqlite:////<絕對路徑>"
   uv run python -c "from quanquant.db.engine import init_db; init_db()"` 跑一次本分支的
   `init_db()`，得到與「首次在這個分支啟動」完全等價的 migrated schema（`raw_inbox` 四個
   新欄位、`agent_commands`/`agent_tokens`/`agent_account_bindings` 三張新表，含全部
   CHECK/UNIQUE constraint）。
2. 對本文件每一條會寫入資料的 SQL（`UPDATE`/`DELETE`），用 `sqlite3 <db> "EXPLAIN ..."`
   確認語法可編譯（不對真實資料執行）；對只讀的 `SELECT` 直接在暫存 DB 上執行確認欄位名
   正確、回傳形狀符合預期。
3. 情境 7 的實際筆數（7 筆，非設計 spec 殘留風險段落寫的「5 筆」）是直接對本機
   `quanquant.db` 下 `SELECT` 查出來的當下事實，非沿用舊文件數字。
4. 情境 6 的「整個目錄唯讀會連帶讓 sentinel 寫入失敗、需靠心跳週期才能送出健康訊息」這個
   細節，是實際呼叫 `quanquant.agent.buffer.DurableBuffer`／
   `quanquant.agent.native_runner._wrap_on_raw` 在暫存唯讀目錄上跑過一次（模擬 agent 已
   啟動、之後才把目錄改唯讀），觀察到主寫入與退化寫入皆確實失敗（`PermissionError`）、
   `has_sentinel()` 為 `False` 之後才寫進本文件；「進階精確版」（鎖檔案不鎖目錄）的路徑
   拆解則是逐行讀 `runner.py`/`native_runner.py` 原始碼推導出來，未逐條實機模擬 Shioaji
   callback（那需要真的下單觸發，留給使用者本人在情境 6 執行時驗證）。
