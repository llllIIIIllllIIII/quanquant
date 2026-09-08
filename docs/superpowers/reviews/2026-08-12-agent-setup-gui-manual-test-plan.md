# Agent 設定精靈（GUI Onboarding）— 端到端人工測試計畫

**建立**：2026-08-12（Task 17，M4 收尾）
**適用範圍**：`feat/agent-setup-gui` 分支，Task 1-16 全數完成之後，交非技術使用者／維運者
實測用的 checklist。對應設計 spec `docs/superpowers/specs/2026-08-12-agent-setup-gui-design.md`
§10「驗收情境」1-19 條，逐條展開成可執行步驟，並標示每條是否已有自動化 pytest 覆蓋。
體例比照 `docs/superpowers/reviews/2026-08-08-inc1-manual-test-plan.md`。

## 0. 目的／前置條件／環境

**目的**：驗證 spec §1 五個目標（G1 零終端機輸入、G2 token 全自動、G3 憑證 GUI 遮罩＋
opt-in keychain、G4 狀態儀表板、G5 headless 完整保留）在真實 macOS／Windows／Linux 機器、
真實瀏覽器、真實 OS keychain 下確實成立，而不只是 pytest 綠燈。

**依賴**：本文件假設 Task 1-16 已全部完成並合併在同一個工作樹（`uv run pytest` 全綠）。
測試前先確認：

```bash
cd /Users/henrychang/Desktop/MyProjects/QuanQuant
git status                 # 確認在 feat/agent-setup-gui，且無未預期改動
uv sync --extra dev
uv run pytest -q           # 全套自動化測試作為起跑點基線
```

**環境需求（D5 驗收矩陣）**：
- **至少一台 macOS**（測試 `.command` 捷徑、macOS Keychain opt-in 落地）。
- **至少一台 Windows**（測試 `.lnk` 捷徑、Windows 憑證管理員 opt-in 落地）。
- 情境 8 需要**一台 Linux（無 Secret Service，如未安裝/未啟動 GNOME Keyring／KWallet
  的環境，例如乾淨的 headless 容器/伺服器）**驗證 keyring 不可用時的行為。
- 一個可連線的 QuanQuant 站台，`ORDER_CHANNEL=agent`；本機測試可用
  `http://127.0.0.1:8000`（`--site` 的 loopback host 例外允許 `http`），正式站用
  `https://quant.35-229-185-30.sslip.io`（`--site` 非 loopback host 一律要求 `https`）。
- 至少一個既有 QuanQuant 帳號＋一組永豐 simtrade API Key/Secret；情境 10 完整版需要
  **兩個**帳號。
- Agent 端憑證全程只在記憶體／OS keychain，不落地明文檔案；測試過程中若在終端機貼過
  token/API Key，事後請自行清除終端機歷史紀錄。

## 1. 已知實作落差（人工測試前必讀）

逐檔精讀 Task 1-16 實際程式碼（非只讀 spec）後，發現三處 spec 描述的行為**尚未串接到
GUI**，只在函式庫層有 pytest 覆蓋。測試時請勿誤認為故障，也不要臆造「應該有」的 UI：

1. **`/status` 頁「重新授權」「清除已存登入 token」「清除已存永豐 API 憑證」三顆按鈕
   目前是 `disabled`（灰階、瀏覽器無法點擊）**——`keyring_store.rotate_token_secret()`／
   `keyring_store.clear_secret()` 兩支函式已實作且有 pytest（`test_agent_keyring_store.py`），
   但目前沒有任何應用程式碼路徑呼叫它們（`status_routes.py` 對應的 POST handler 一律回
   固定樁接文案「憑證儲存模組（Task 11）尚未完成」）。**scenario 17 的「GUI 重試按鈕流程」
   目前無法實際點擊測試**，只能驗證按鈕確實呈現 disabled＋說明文案（而非誤判成壞掉）。
2. **「刪除 profile」按鈕可以點擊，但點下去不會真的刪除任何東西**——`status_routes.py`
   的 `delete_profile` 只做「暫存箱是否已淨空」的防呆檢查，之後一律回同一句樁接文案
   「profile registry（Task 12）與憑證清除（Task 11）尚未完成，刪除 profile 功能暫時
   停用」，未呼叫 `profile_registry.remove_profile()`／`keyring_store.clear_profile()`。
   整個 `/status` 頁「清除已存憑證／刪除 profile」區塊目前**唯一有實質作用的操作**是上方
   的防呆判斷本身（buffer 未淨空時會擋下並顯示筆數）。
3. **`keyring_store.check_secure_backend()`（啟動時 secure-backend 能力檢查）從未被任何
   GUI 路徑呼叫**——opt-in 寫入失敗（含不安全 backend 這種情況）目前只落一行
   `log.warning(...)`，**不會**在 UI 顯示 spec §5.2 描述的「此系統無安全儲存區，無法
   記住」明示訊息。使用者只會看到「這次沒有被記住、下次啟動一樣要重新輸入」的間接結果。
4. **桌面捷徑產生器沒有 CLI／GUI 包裝**——`generate_macos_shortcut()`／
   `generate_windows_shortcut()` 是純 Python 函式（`src/quanquant/agent/shortcut_gen.py`），
   刻意未掛 `quanquant` CLI 子命令、未匯出成套件公開介面（Task 15 brief 明訂 YAGNI）。
   維運者目前必須用一段 `uv run python -c "..."` 手動呼叫，見情境 15。

以上四點皆非本 task（文件整理）範圍內可修的程式行為，已如實記入下方對應情境；若使用者
希望補齊，需另開 task 接線 `status_routes.py`／`check_secure_backend()`／捷徑 CLI。

---

## 2. 總覽（先勾選，詳細步驟見下方各節）

| # | 情境 | 結果（PASS/FAIL/備註） |
|---|---|---|
| 1 | 全新使用者端到端 | |
| 2 | 二次啟動免精靈 | |
| 3 | 只記裝置授權 | |
| 4 | user_code 釣魚辨識 | |
| 5 | token rotation 不中斷連線 | |
| 6 | fail-stop GUI 存活 | |
| 7 | headless 迴歸 | |
| 8 | keyring 不可用平台 | |
| 9 | consumed 復原＋並行 poll | |
| 10 | 同機雙 profile | |
| 11 | keyring 寫入失敗 | |
| 12 | 秘密洩漏掃描 | |
| 13 | slow_down 封鎖 | |
| 14 | 信任鏈（真實 IP） | |
| 15 | 捷徑實跑（macOS/Windows） | |
| 16 | PoP（code_verifier） | |
| 17 | reauth 儲存失敗 | |
| 18 | reauth 先刪後寫／WS 被拒引導 | |
| 19 | registry 併發 | |

---

## 3. 逐情境展開

### 情境 1：全新使用者端到端

**前置條件**：
- 維運者已依情境 15 的步驟，在使用者的電腦上建立好桌面捷徑（雙擊即啟動，`--site`
  指向目標站台）。
- 使用者已有 QuanQuant 帳號、可登入該站台網頁版；已備妥一組永豐 simtrade API Key/Secret。
- 站台 `ORDER_CHANNEL=agent`。

**操作步驟**：
1. 使用者雙擊桌面上的捷徑圖示（例如「QuanQuant Agent.command」／「QuanQuant Agent.lnk」）
   —— 全程**不**需要開啟任何終端機視窗。
2. 系統自動開啟預設瀏覽器，顯示「設定精靈 — 步驟 1／3：連線授權」頁，文案「按下方按鈕
   開始裝置授權：QuanQuant Agent 會產生一組代碼，請在核准頁輸入以完成連線。」，按
   「開始授權」。
3. 頁面轉為顯示一組 8 碼大字代碼（格式 `XXXX-XXXX`）與「開核准頁」連結，文案「請在
   瀏覽器另開分頁（或使用手機）開啟核准頁，輸入以下代碼完成授權：」／「等待核准
   中……（本頁每 2 秒自動更新）」。
4. 點「開核准頁」連結（或另開分頁手動輸入網址），在核准頁登入 QuanQuant 帳號（未登入
   會先導去既有登入頁）。
5. 核准頁（`/agent/authorize`）顯示「裝置代碼」輸入框（無任何預填），手動輸入步驟 3
   看到的代碼，按「查詢」。
6. 核對畫面顯示「此裝置請求連線你的下單 agent（sim 模式）。請仔細核對下面的裝置代碼是否
   與 agent 端畫面顯示的完全一致——**代碼不一致就不要核准**。」＋顯示代碼與「請求建立
   時間」，按「核准」。
7. 回到步驟 3 的精靈分頁（最多等 2 秒自動刷新），應自動前進到「步驟 2／3：永豐憑證」：
   兩個遮罩密碼欄（Shioaji API Key／Shioaji Secret Key）＋兩個「記住」checkbox（記住
   裝置授權／記住永豐 API 憑證）＋「如何取得永豐 API 憑證？」連結。勾選兩個 checkbox，
   輸入永豐 sim API Key/Secret，按「下一步」。
8. 精靈轉到「步驟 3／3：確認啟動」，顯示伺服器網址、模式（sim）、商品（TXF）、帳號
   （核准頁登入的使用者名稱），按「啟動」。
9. 瀏覽器導向 `/status` 狀態頁。
10. 到既有 `/orders` 頁（未變動）下一筆測試單（Buy 1 口 TXF MKT/IOC，sim）。

**預期結果**：
- 全程 0 次終端機輸入。
- `/status` 顯示連線狀態「已連線（connected）」綠色 badge、模式「sim（模擬單）」、
  暫存箱待送出 0 筆，頁面每 1 秒自動更新。
- `/orders` 測試單成功送出並在數秒內收到成交回報（sim 環境）。

**自動化覆蓋**：純人工（跨 Task 1-16 全鏈路，無單一測試覆蓋整條端到端路徑；各步驟片段
分別由情境 2/3/4/9/10/15 的自動化測試覆蓋）。

---

### 情境 2：二次啟動免精靈

**前置條件**：情境 1 已完成過一次（registry 已有一筆 profile、keyring 已存 token 與
永豐憑證且皆未過期）。

**操作步驟**：
1. 於 `/status` 按「停止 Agent」（或直接關閉終端機/工作管理員結束該 agent 程序）。
2. 使用者再次雙擊同一個桌面捷徑。
3. 觀察瀏覽器開啟後落在哪個頁面。

**預期結果**：不經過 `/setup` 精靈任何一步，瀏覽器直接開在 `/status` 狀態頁，連線 badge
在數秒內轉綠「已連線」。

**自動化覆蓋**：`tests/test_agent_gui_startup_flow.py::test_single_profile_with_complete_unexpired_credentials_goes_direct`
（決策樹本身）＋`tests/test_agent_profile_registry.py`／`tests/test_agent_keyring_store.py`
（底層機制）＋人工（真的雙擊捷徑觀察）。

---

### 情境 3：只記裝置授權

**前置條件**：完成一次精靈時，只勾「記住裝置授權」、不勾「記住永豐 API 憑證」。

**操作步驟**：
1. 停止 agent，再次雙擊捷徑。
2. 觀察畫面落在哪個步驟。

**預期結果**：不經過步驟①（token 已從 keyring 取得），直接落在「步驟 2／3：永豐憑證」，
需要重新輸入永豐 API Key/Secret；帳號顯示與第一次相同（沿用同一 profile）。

**自動化覆蓋**：`tests/test_agent_gui_startup_flow.py::test_single_profile_missing_broker_creds_restarts_at_step2`
＋人工（GUI 畫面確認精靈從步驟②開始）。

---

### 情境 4：user_code 釣魚辨識

**前置條件**：無（獨立測試，不需要真的啟動 agent）。

**操作步驟**：
1. 登入 QuanQuant 帳號，開啟 `/agent/authorize`。
2. 在「裝置代碼」欄位輸入一組格式正確但不存在的代碼（例如 `ZZZZ-ZZZZ`），按「查詢」。
3. 觀察錯誤訊息用字。
4.（可選）真的啟動一次精靈拿到真實代碼，在核准頁故意輸入「多一碼／少一碼」的變體，
   驗證同樣被拒。

**預期結果**：
- 一律顯示泛用錯誤「找不到此代碼或已過期」，不透露代碼是否曾存在、屬於誰或目前狀態。
- 核准頁輸入框無任何預填（網址不帶 `?code=`），使用者必須手動抄錄比對代碼——這是刻意
  保留的防釣魚摩擦，人工測試重點是判斷這段警語（「代碼不一致就不要核准」）是否讓一般
  使用者能理解並照做。

**自動化覆蓋**：`tests/test_agent_authorize_page.py::test_lookup_unknown_code_shows_generic_error`
＋人工（UI 可讀性判斷）。

---

### 情境 5：token rotation 不中斷連線

**前置條件**：agent 已連線成功（`/status` badge 綠）。

**操作步驟**：
1. 另開瀏覽器分頁登入同帳號，到 `/orders` 頁「Agent Token」段按「重新產生（原 token
   立即作廢）」（既有手動備援功能，spec §7 明訂不動）。
2. 立刻回頭觀察 `/status` 頁，**不需要做任何動作**，確認 badge 仍是「已連線」。
3. 停止該 agent 程序（`/status` 按「停止 Agent」），用同一個 profile 重新啟動（雙擊
   捷徑，見情境 18 的重啟步驟）。
4. 觀察重新啟動的結果。

**預期結果**：
- 步驟 2：既有 WS 連線**不中斷**（rotation 只在下一次握手才驗證，屬正式化不變量）。
- 步驟 3-4：agent 用（keyring 記住的）舊 token 重新握手必然失敗；因為這是「registry
  命中→direct 快速連線」路徑（`launch_direct` 帶 `stop_on_token_reject=True`），觀察到
  握手被拒後會自動導回精靈步驟①，並顯示「先前記住的授權已失效（token 可能已被撤銷），
  請重新授權」，需要重新走一次裝置授權（見情境 18）。

**自動化覆蓋**：既有 `tests/test_agent_ws.py::test_rotation_invalidates_old_token_new_token_still_works`
（不變量本身）＋人工（GUI 重新授權流程，實測需搭配情境 18 的重啟步驟才會觀察到 UI
引導）。

---

### 情境 6：fail-stop GUI 存活

**前置條件**：agent 已連線（`/status` badge 綠）。做法沿用既有 Inc1 fail-stop 演練
（`2026-08-08-inc1-manual-test-plan.md` 情境 6）的手法，改在新的 `/status` 頁觀察。

**操作步驟**：
1. 找到目前 profile 的實際 buffer 目錄——GUI 路徑下預設是
   `~/.quanquant-agent/<origin_hash 16 hex>/<profile_hash 16 hex>/outbox.db`（16 進位
   雜湊目錄名，非人類可讀；可從啟動時的終端機/log 或 `~/.quanquant-agent/profiles.json`
   的 `buffer_path` 欄位對照確認）。
2. 另開終端機，把該 profile 目錄整個改唯讀（只鎖 `outbox.db` 本身不夠，退化寫入路徑
   會接住不觸發 fail-stop）：
   ```bash
   chmod -R -w ~/.quanquant-agent/<origin_hash>/<profile_hash>
   ```
3. 在 `/orders` 頁送出一筆新單，觸發回報寫入。
4. 觀察 `/status` 頁（每 1 秒自動重整）。
5. 再送一筆新單，確認被拒。
6. 修復權限並重啟：
   ```bash
   chmod -R u+w ~/.quanquant-agent/<origin_hash>/<profile_hash>
   ```
   回到 `/status` 按「停止 Agent」，再重新雙擊捷徑啟動。
7. 觀察恢復情況。

**預期結果**：
- 步驟 4：`/status` 頁在數秒到約 15 秒內（下一次心跳週期）出現紅色大警示區塊，標題
  「⚠ FAIL-STOP：Agent 已停止下單／改單／刪單」，內文含 sentinel 記錄的簡短原因＋固定
  指引文案「系統偵測到本機資料寫入異常，Agent 已自動停止一切下單／改單／刪單操作
  （唯讀查詢仍可運作），避免委託遺失或重複執行……」。**GUI 本身不會當掉或無回應**。
- 步驟 5：新單被拒，不會真的送到永豐 sim 後台。
- 步驟 6-7：G2 恢復已於 2026-08-08 降級為手動重啟——badge **不會**原地自動轉綠，必須
  「停止 Agent」後重新啟動整個 agent 程序，新一輪啟動時的探測通過才會清除 fail-stop。

**自動化覆蓋**：`tests/test_agent_status_page.py::test_status_page_shows_failstop_warning_when_latched`
＋人工（真實唯讀目錄，含手動重啟收斂）。

---

### 情境 7：headless 迴歸

**前置條件**：無，獨立於 GUI。

**操作步驟**：
1. ```bash
   export QQ_AGENT_TOKEN="..."
   export QQ_AGENT_API_KEY="..."
   export QQ_AGENT_SECRET_KEY="..."
   uv run quanquant-agent --no-gui
   ```
   （env 三件套齊全時，即使不加 `--no-gui` 也會依七層優先序自動落到 headless；加
   `--no-gui` 是最直接、最不受 tty/site 影響的驗證方式。）
2. 觀察終端機輸出。
3. ```bash
   uv run pytest tests/test_agent_startup_resolution.py tests/test_agent_cli.py \
     tests/test_agent_integration.py -q
   ```

**預期結果**：
- 終端機印出「agent 啟動（simtrade）→ ws://.../ws/agent；Ctrl-C 結束（憑證僅存記憶體）」，
  與既有行為逐位一致；**不**開啟瀏覽器、**不**探測 keyring/registry。
- 三個測試檔全綠，無 GUI 相關迴歸。

**自動化覆蓋**：`tests/test_agent_startup_resolution.py`（全部，七層優先序＋
`--no-gui`/`--gui`/`--reset` 互斥）＋既有 `tests/test_agent_cli.py`／
`tests/test_agent_integration.py`。

---

### 情境 8：keyring 不可用平台

**前置條件**：一台沒有安全 keychain backend 的環境（例如未安裝／未啟動 Secret Service
的 Linux，如乾淨的 headless 容器）。

**操作步驟**：
1. 在該環境下走一次情境 1 的精靈流程，勾選兩個「記住」checkbox。
2. 觀察啟動結果與畫面上是否有任何提示。
3. 檢查磁碟：`~/.quanquant-agent/profiles.json` 內容是否只含非秘密 metadata
   （`profile_id`/`username`/`buffer_path`/`created_at`）；系統上是否存在任何存放
   `api_key`/`secret_key`/`token` 明文的檔案。

**預期結果**：
- 精靈仍可完成、agent 仍可連線成功並完成單次連線下單（不受影響）。
- **見§1 已知落差第 3 點**：`check_secure_backend()` 未被 GUI 呼叫，opt-in 寫入在無安全
  backend 時會失敗，但只落 log warning，**UI 不會顯示明示訊息**——使用者只會觀察到
  「下次重啟仍要重新走一次精靈」這個間接結果，需要人工到 agent 端 log 才能看到具體原因。
- 絕不會在本機留下任何明文憑證檔案（無論寫入成功與否，寫入的目標永遠是系統 keyring
  API，不會退化成檔案）。

**自動化覆蓋**：`tests/test_agent_keyring_store.py::test_check_secure_backend_rejects_fail_backend`
＋人工（真實 Linux 無 Secret Service 環境；並人工確認§1 落差第 3 點在此情境下的實際
表現）。

---

### 情境 9：consumed 復原＋並行 poll

**前置條件**：需要維運者用 curl／Python 手動操作 device-code API（一般使用者不會遇到
這個情境；`consumed` 只在「回應在半路遺失」或「兩個輪詢併發搶兌」時觸發）。

**操作步驟**（維運者在能連到目標站台的機器上執行；`<SITE>` 換成實測站台網址）：
1. 發起一輪 device flow：
   ```bash
   VERIFIER=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
   CHALLENGE=$(python3 -c "import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "$VERIFIER")
   curl -s -X POST <SITE>/api/agent/device-code -H 'Content-Type: application/json' \
     -d "{\"code_challenge\":\"$CHALLENGE\"}"
   ```
   記下回傳的 `device_code`／`user_code`。
2. 到 `/agent/authorize` 手動輸入 `user_code` 並核准（同情境 1 步驟 4-6）。
3. **模擬回應遺失**：核准完成後，第一次輪詢直接不理會回應內容（視為遺失），改連續打
   兩次：
   ```bash
   curl -s -X POST <SITE>/api/agent/device-token -H 'Content-Type: application/json' \
     -d "{\"device_code\":\"<device_code>\",\"code_verifier\":\"$VERIFIER\"}"
   # 立刻再打一次（模擬「已經 consume 過，第二次重複輪詢」）
   curl -s -X POST <SITE>/api/agent/device-token -H 'Content-Type: application/json' \
     -d "{\"device_code\":\"<device_code>\",\"code_verifier\":\"$VERIFIER\"}"
   ```
4. 若用真正的 GUI 精靈觸發（步驟 1-2 改用精靈跑），觀察精靈是否在收到 `consumed` 後
   自動重開一輪（畫面重新出現新的 `user_code`），最多自動重開 3 次
   （`MAX_CONSUMED_RESTARTS = 3`，見 `device_flow_client.py`）；超過則顯示錯誤「授權多次
   交付失敗，請按『開始授權』重試或檢查伺服器狀態。」。
5. 並行 poll：兩個終端機同時對同一個 `device_code`/`code_verifier` 送出輪詢請求，觀察
   只有一方拿到 `approved`（含 token），另一方視時間點拿到 `pending`/`consumed`。

**預期結果**：
- 第一次輪詢拿到 `approved`（含 token 明文）；第二次重複輪詢應回 `consumed`（唯讀、
  冪等，重複打仍是 `consumed`，DB 沒有任何 revoke 動作）。
- 若走 GUI 精靈：收到 `consumed` 會自動重開新一輪（新的 user_code），不需要使用者手動
  介入；新一輪授權完成後，舊枚 token 依既有 rotation 不變量自動撤銷。
- 並行 poll：兩個併發輪詢中只有一方成功搶占並拿到 token，另一方不會拿到重複的 token。

**自動化覆蓋**：`tests/test_device_flow_poll.py::test_poll_after_consumed_is_readonly_idempotent`／
`test_concurrent_claim_only_one_winner_thread_level_race`＋
`tests/test_device_flow_client.py::test_poll_until_done_auto_restarts_on_consumed`。

---

### 情境 10：同機雙 profile

**前置條件**：兩個 QuanQuant 帳號、兩組永豐 simtrade API Key/Secret（同機測試也可以是
同一台電腦跑兩個 agent 程序）。

**操作步驟**：
1. 維運者用捷徑產生器（情境 15）替兩個帳號各建一個捷徑，`--profile` 分別帶各自的
   server user id（例如 `--profile 1`／`--profile 2`）。
2. 雙擊第一個捷徑，走完精靈（帳號 A）；再雙擊第二個捷徑，走完精靈（帳號 B）。
3. 確認兩個 agent 程序都在跑，`/status` 各自顯示各自帳號、各自 buffer pending 數。
4. 對帳號 A 的 profile 目錄，嘗試再啟動第三個程序（同一個捷徑再雙擊一次，或手動
   `uv run quanquant-agent --gui --site <SITE> --profile 1`）。
5. 不帶 `--profile` 直接啟動（`uv run quanquant-agent --gui --site <SITE>`），觀察畫面。
6. 在核准頁刻意用「另一個」QuanQuant 帳號登入完成核准（模擬使用者核准頁登入錯帳號），
   觀察精靈完成後綁定到哪個 profile。
7. 用兩個不同 port／scheme 的 `--site`（例如 `https://a.example` 與
   `https://a.example:8443`）分別走一次精靈，檢查 buffer 目錄。

**預期結果**：
- 步驟 3：兩個 agent 的 buffer／keyring／連線狀態互不干擾。
- 步驟 4：第三個程序啟動失敗（`AgentAlreadyRunningError`／精靈顯示「此帳號的 Agent
  已在執行中」），不會有兩個程序同時操作同一組帳號。
- 步驟 5：因為 registry 有 2 筆且未指定 `--profile`，落在「選擇帳號」頁（`/profiles`），
  列出兩個帳號，各自一顆「使用此帳號」按鈕，固定一個「新增帳號」連結。
- 步驟 6：完成後綁定到核准頁**實際登入**的帳號（`profile_id` 以 server user id 為準，
  非精靈原本預期的帳號）；registry 以 `(site_origin, profile_id)` 唯一鍵 upsert，不會
  建立重複的同 ID profile；若走的是 `--profile` 捷徑但核准頁登入了另一帳號，步驟③會
  額外顯示提示：「捷徑指向的帳號已變更，請重新產生捷徑或修改 --profile」。
- 步驟 7：不同 port/scheme 的兩個 site，buffer 目錄雜湊不同（`origin_dir` 用完整
  canonical origin 算雜湊，同 host 異 port 不共用 outbox）。

**自動化覆蓋**：`tests/test_agent_profile_registry.py`（全部）＋
`tests/test_agent_gui_startup_flow.py::test_multiple_profiles_without_hint_goes_to_profile_select`／
`test_reconcile_reuses_existing_profile_when_approved_id_already_registered`／
`test_reconcile_creates_isolated_new_profile_when_approved_id_unknown`＋人工（真兩個程序
＋真兩個捷徑）。

---

### 情境 11：keyring 寫入失敗

**前置條件**：需要能讓 OS keychain 寫入中途失敗的手段（例如 macOS：在寫入永豐憑證那
瞬間鎖定 Keychain／撤銷該 App 的存取權限；或改用有限額度/唯讀模式的測試 keyring
backend）。屬於進階場景，多數情況建議以 pytest 為主要驗收依據。

**操作步驟**：
1. 在情境 1 的精靈步驟②，勾選兩個「記住」checkbox，輸入永豐憑證後按「下一步」。
2. 在按下步驟③「啟動」的瞬間，人為讓其中一筆 keyring 寫入失敗（例如永豐憑證那筆——
   若使用可控測試環境，可暫時撤銷 keychain 存取權限只針對第二次呼叫生效）。
3. 觀察啟動結果與 agent 端 log。
4.（可選）重複一次，改讓「快照復原」本身也失敗。

**預期結果**：
- 兩個 opt-in 各自獨立提交：第一筆（裝置授權 token）成功即保留，第二筆（永豐憑證）
  失敗只復原它自己的快照，不影響第一筆。
- Agent 本次啟動**不受影響**（`setup_step3_launch` 目前把 keyring 寫入失敗只記
  `log.warning`，不阻擋本次已經啟動的 runner）——這代表「寫入失敗」對使用者當下體感
  幾乎無感，只有下次啟動時才會發現該筆沒被記住。
- 若「快照復原」本身也失敗，應得到顯式錯誤字串「儲存失敗且原值可能遺失——請至『清除
  已存憑證』檢查後重新設定」（此文案目前只會出現在 agent log／pytest 斷言中，見§1
  已知落差第 1/2 點，UI 上沒有對應的即時橫幅可看）。

**自動化覆蓋**：`tests/test_agent_keyring_store.py::test_broker_credentials_partial_write_failure_keeps_successful_field_and_restores_only_failed_snapshot`
（主要驗收依據；人工重現需要能控制 OS keychain 行為的進階環境，非必要不強求）。

---

### 情境 12：秘密洩漏掃描

**前置條件**：無。

**操作步驟**：
1. 確認 Task 9 範圍的自動化掃描已涵蓋：
   ```bash
   uv run pytest tests/test_agent_setup_wizard.py::test_step2_validation_error_does_not_echo_credentials \
     tests/test_agent_setup_wizard.py::test_wizard_flow_logs_do_not_leak_secrets -v
   ```
2. **人工覆核 Task 9 範圍之外的表面**：走一次情境 1（含步驟②故意留白/打錯憑證觸發
   422），用瀏覽器開發者工具檢查以下回應是否含 `api_key`/`secret_key`/token 明文：
   - `POST /setup/step2` 422 錯誤回應 body。
   - `POST /status/reauth`／`POST /status/clear-credential`／`POST /status/delete-profile`
     的回應 body（Task 10/11 新增路徑，Task 9 的 caplog 測試只涵蓋它自己新增的程式碼
     路徑，不含這幾支）。
3. 檢查 `~/.quanquant-agent/profiles.json` 內容確認只含非秘密 metadata。
4. 檢查所有 `/setup`、`/status`、`/profiles` 系列回應標頭是否都有 `Cache-Control: no-store`。

**預期結果**：
- 422 錯誤回應絕不回帶輸入的憑證值。
- 所有 GUI 回應（含 Task 10/11 新增的 `/status/*` 端點）皆 `Cache-Control: no-store`。
- `profiles.json` 全程不含任何 `api_key`/`secret_key`/`token` 字樣。
- 應用程式 log（`agent` 進程的 stdout/stderr）不含三項秘密明文。

**自動化覆蓋**：`tests/test_agent_setup_wizard.py::test_step2_validation_error_does_not_echo_credentials`
＋`test_wizard_flow_logs_do_not_leak_secrets`（Task 9 Step 5b，application log 掃描，
已是正式 pytest，非文件備忘）＋人工（覆核 Task 10/11 新增路徑，見上方步驟 2）。

---

### 情境 13：slow_down 封鎖

**前置條件**：維運者操作，需要能連到目標站台。

**操作步驟**：
1. 依情境 9 步驟 1-2 建立一輪 device flow（發起＋核准，或不核准也可以，slow_down 只看
   輪詢頻率）。
2. 連續 5 次以低於 `interval`（預設 5 秒）的間隔打 `/api/agent/device-token`（例如每 1
   秒打一次）：
   ```bash
   for i in 1 2 3 4 5; do
     curl -s -X POST <SITE>/api/agent/device-token -H 'Content-Type: application/json' \
       -d "{\"device_code\":\"<device_code>\",\"code_verifier\":\"$VERIFIER\"}"
     sleep 1
   done
   ```
3. 第 5 次之後立刻再打一次，觀察回應。
4. 等待封鎖時間（60 秒）過後，重新以正常間隔輪詢，觀察是否恢復。
5.（可選，驗證重啟不繞過）在封鎖期間重啟 server 進程，確認封鎖狀態仍生效（DB 持久化，
   非記憶體狀態）。

**預期結果**：
- 前 5 次過快輪詢逐次累加 `current_interval`（上限 30 秒）與 `consecutive_violations`，
  第 5 次觸發後 `blocked_until = now + 60s`，`consecutive_violations` 歸零。
- 封鎖期間任何輪詢一律回 `slow_down`（不消耗嘗試次數、不 claim）。
- 60 秒後恢復正常輪詢間隔即可繼續；恢復正常間隔後 `consecutive_violations` 歸零。
- 重啟 server 不能繞過封鎖（狀態存 DB `agent_device_codes` 表）。

**自動化覆蓋**：`tests/test_device_flow_poll.py::test_slow_down_escalates_interval_and_blocks_after_five_violations`。

---

### 情境 14：信任鏈（真實 IP）

**前置條件**：正式部署環境（GCP VM，docker-compose：app + postgres + caddy）；本機
開發環境無 proxy，不適用本情境（直接 socket peer IP）。

**操作步驟**：
1. 確認部署設定：
   ```bash
   grep -A2 FORWARDED_ALLOW_IPS docker-compose.yml
   ```
   應看到固定 CIDR/IP 字面值（不是 Docker 服務別名），且與 Caddy 容器在 compose ipam
   指定的固定 IP 一致。
2. 從**外部**對正式站發送帶偽造 `X-Forwarded-For` 的請求：
   ```bash
   curl -s -X POST https://quant.35-229-185-30.sslip.io/api/agent/device-code \
     -H 'X-Forwarded-For: 1.2.3.4' -H 'Content-Type: application/json' \
     -d '{"code_challenge":"deadbeef"}'
   ```
3. 檢查 server 記錄到的 `request_ip`（維運者查 DB，需 SSH 進 VM）：
   ```bash
   docker compose exec postgres psql -U <user> -d <db> -c \
     "SELECT request_ip, created_at FROM agent_device_codes ORDER BY id DESC LIMIT 1;"
   ```
4. 用兩個不同來源 IP（例如自己的網路＋手機熱點，或兩台不同機器）各發一次上面的請求，
   比較兩筆 `request_ip` 是否不同、且都不是 Caddy 容器 IP。

**預期結果**：
- 步驟 3：`request_ip` 是發送請求的**真實來源 IP**，不是偽造的 `1.2.3.4`，也不是 Caddy
  容器 IP——證明 Caddy 忽略外來 XFF＋uvicorn 只信任 Caddy 固定 IP 兩段信任鏈都生效。
- 步驟 4：兩筆來源記到各自不同的真實 IP，限流計數不合流（不會被誤判成同一個發送者
  互相拖累）。

**自動化覆蓋**：`tests/test_deployment_trust_chain.py`（全部：
`test_client_ip_reflects_direct_peer_when_no_proxy_trusted`／
`test_two_different_source_ips_are_recorded_distinctly_through_caddy_ip`／
`test_untrusted_proxy_ip_is_not_honored`／`test_settings_forwarded_allow_ips_defaults_to_loopback`／
`test_docker_compose_pins_caddy_ip_and_app_forwarded_allow_ips`）；上方步驟 2-4 是對已部署
環境的**真實網路**驗收，pytest 只驗證 app 層邏輯與 compose 設定檔內容，不能取代真實部署
的網路層驗證。

---

### 情境 15：捷徑實跑（macOS/Windows 驗收矩陣）

**前置條件**：目標電腦已裝好 `uv` 且 `PATH` 找得到（`shutil.which("uv")` 能命中）、repo
已 clone 到一個絕對路徑。**目前沒有 CLI／GUI 包裝**（見§1 已知落差第 4 點），維運者需要
手動執行一段 Python 呼叫產生器函式。

**操作步驟（macOS）**：
1. 維運者在目標 repo 目錄下執行：
   ```bash
   uv run python3 -c "
   from pathlib import Path
   from quanquant.agent.shortcut_gen import generate_macos_shortcut
   generate_macos_shortcut(
       repo_path=Path('/絕對路徑/QuanQuant'),
       site='https://quant.35-229-185-30.sslip.io',
       profile=None,
       out_path=Path.home() / 'Desktop' / 'QuanQuant Agent.command',
   )
   "
   ```
2. 到 Finder 桌面找到「QuanQuant Agent.command」，確認檔案屬性可執行（`ls -l` 應看到
   `-rwx------`，即 `0700`）。
3. **從 Finder 雙擊**這個檔案（不要用終端機直接跑，要驗證的是真的雙擊路徑）。
4. 觀察是否正確 `cd` 到 repo 目錄、成功找到 `uv`、瀏覽器自動開啟 GUI。
5.（`--profile` miss 情境）手動編輯剛才產生的 `.command`，把 `--profile` 加一個不存在
   的 id（例如 `--profile 999`），再雙擊一次。

**操作步驟（Windows）**：
1. ```powershell
   uv run python -c "
   from pathlib import Path
   from quanquant.agent.shortcut_gen import generate_windows_shortcut
   generate_windows_shortcut(
       repo_path=Path(r'C:\絕對路徑\QuanQuant'),
       site='https://quant.35-229-185-30.sslip.io',
       profile=None,
       out_path=Path.home() / 'Desktop' / 'QuanQuant Agent.lnk',
   )
   "
   ```
2. 到桌面右鍵「QuanQuant Agent.lnk」→內容，確認「目標」是絕對 `uv.exe` 路徑＋
   `run quanquant-agent --gui --site "..."`，「起始位置」（Start in）是 repo 絕對路徑。
3. **從檔案總管雙擊**這個捷徑。
4. 觀察是否正確切到 repo 目錄、成功找到 `uv.exe`、瀏覽器自動開啟 GUI。

**預期結果**：
- macOS：雙擊後終端機視窗短暫出現（`.command` 本質是 shell script），瀏覽器自動開啟
  `/bootstrap` 導向的精靈或狀態頁；`--profile` miss 情境會落在精靈步驟①，並在完成後
  顯示「捷徑指向的帳號已變更，請重新產生捷徑或修改 --profile」提示。
- Windows：雙擊後同樣能成功啟動並開瀏覽器，行為與 macOS 對等。
- 兩平台皆不需要使用者自行輸入任何指令。

**自動化覆蓋**：`tests/test_agent_shortcut_gen.py`（內容/權限/shell 跳脫覆蓋，含 macOS
命令注入與 Windows PowerShell 單引號 breakout 的敵意字串測試）＋人工（真實雙擊，含
`--profile` miss 情境）。

---

### 情境 16：PoP（code_verifier）

**前置條件**：維運者操作。

**操作步驟**：
1. 依情境 9 步驟 1 發起一輪 device flow，取得 `device_code`，但**不要**核准。
2. 用**錯誤的** `code_verifier`（隨便打一個字串）輪詢：
   ```bash
   curl -s -X POST <SITE>/api/agent/device-token -H 'Content-Type: application/json' \
     -d "{\"device_code\":\"<device_code>\",\"code_verifier\":\"wrong-verifier\"}"
   ```
3. 用**缺少** `code_verifier` 欄位的請求輪詢（body 不帶這個欄位）。
4. 用正確的 `code_verifier`（步驟 1 產生時記下的那組）核准後輪詢，確認能正常拿到 token。
5. 在步驟 2/3 之後，檢查 DB 該筆 `agent_device_codes` 的 `last_polled_at`／
   `current_interval`／`consecutive_violations` 是否被更動過（驗證「零副作用」）。

**預期結果**：
- 步驟 2/3：一律回 404（不透露是「代碼不存在」還是「verifier 錯」的差異），且**不會**
  搶占這筆 device_code，也**不會**污染節流計數（verifier 驗證在輪詢入口最先執行，失敗
  即刻返回，不觸碰 slow_down 相關欄位）。
- 步驟 4：正確 verifier 才能成功兌換 token。
- 步驟 5：DB 該筆的節流欄位與步驟 2/3 之前一致（未被更動）。

**自動化覆蓋**：`tests/test_device_flow_poll.py::test_poll_wrong_verifier_returns_invalid_and_zero_side_effects`。

---

### 情境 17：reauth 儲存失敗

**見§1 已知落差第 1 點**：`/status` 頁「重新授權」按鈕目前是 `disabled`（灰階、瀏覽器
無法點擊），`keyring_store.rotate_token_secret()` 沒有任何應用程式碼路徑會呼叫它——
**本情境目前無法透過真實 GUI 按鈕流程重現**，只能驗證函式庫層行為與 UI 呈現的誠實度。

**前置條件**：無。

**操作步驟**：
1. 走一次情境 1，勾選「記住裝置授權」完成連線。
2. 到 `/status` 頁「重新授權」區塊，用滑鼠確認按鈕呈現灰階、無法點擊，且旁邊顯示提示
   文案「憑證儲存功能（Task 11）尚未完成，『重新授權』暫時停用；如需重新授權，請先
   『停止 Agent』再重新啟動，啟動時會重新引導完成裝置授權。」——確認這段文案清楚、
   不誤導使用者以為按鈕壞掉。
3. 執行函式庫層 pytest 驗證 `rotate_token_secret` 本身的 fail-closed 行為：
   ```bash
   uv run pytest tests/test_agent_keyring_store.py::test_rotate_token_delete_failure_is_fail_closed -v
   ```

**預期結果**：
- 步驟 2：按鈕確實無法點擊，說明文案清楚可讀，不會讓使用者誤以為系統故障。
- 步驟 3：pytest 綠燈，確認函式庫層「刪除失敗即 fail closed、不繼續寫入新值」邏輯
  正確，即使目前 GUI 尚未接線也已為未來接線打好基礎。

**自動化覆蓋**：`tests/test_agent_keyring_store.py::test_rotate_token_delete_failure_is_fail_closed`
＋人工（僅能驗證 disabled 按鈕與提示文案的呈現，無法測試真正的「重試按鈕」流程——
見§1 已知落差第 1 點）。

---

### 情境 18：reauth 先刪後寫／WS 被拒引導

本情境分兩部分：前半（先刪後寫的 keyring 行為）與情境 17 同樣受§1 已知落差第 1 點限制，
只能走函式庫層 pytest；後半（WS 握手被拒→導回精靈）**已經完整串接**，可以真實 GUI
重現。

**前置條件**：情境 5 已完成到「舊 token 已在 server 端被撤銷」的狀態（或直接用下方步驟
1-2 重新製造）。

**操作步驟**：
1. 走一次情境 1，勾選「記住裝置授權」，完成連線並停止 agent（`/status` 按「停止
   Agent」）——此時 keyring 存著一組 token，registry 也有這個 profile。
2. 另開瀏覽器登入同帳號，到 `/orders` 頁按「重新產生 Agent Token」（撤銷 keyring 裡
   那一枚）。
3. 重新雙擊同一個桌面捷徑（或執行同樣的 `--profile` 指令）啟動 agent。
4. 觀察啟動過程與最終畫面。
5.（函式庫層驗證，§1 落差適用）：
   ```bash
   uv run pytest tests/test_agent_keyring_store.py::test_rotate_token_deletes_old_before_writing_new \
     tests/test_agent_ws_client.py::test_receive_raises_token_rejected_on_close_code_1008 -v
   ```

**預期結果**：
- 步驟 3-4：registry 命中該 profile→「direct」快速連線路徑；agent 用 keyring 裡的舊
  token 嘗試握手，server 端因為 token 已被撤銷以 close code 1008 拒絕；GUI **不信任**
  keyring 的 `expires_at` metadata，觀察到 `probe_direct_connect()` 回傳 `"rejected"`
  後，自動導回精靈步驟①，顯示「先前記住的授權已失效（token 可能已被撤銷），請重新
  授權」——需要使用者重新走一次裝置授權（同情境 1 步驟 3-6）。
- 步驟 5：pytest 綠燈，確認 `rotate_token_secret` 的「先刪後寫」順序（刪除失敗才 fail
  closed，不繼續寫入新值）；WS client 正確把 close code 1008 分類成
  `TokenRejectedError`。

**自動化覆蓋**：`tests/test_agent_keyring_store.py::test_rotate_token_deletes_old_before_writing_new`
＋`tests/test_agent_ws_client.py::test_receive_raises_token_rejected_on_close_code_1008`（WS
1008 分類）＋人工（GUI 端到端：`probe_direct_connect` 觀察到 `rejected` 後真的導回精靈
步驟①——這部分已完整串接，可如實重現）。

---

### 情境 19：registry 併發

**前置條件**：兩個（或以上）profile 對應的 agent 程序（可沿用情境 10 建立的兩個帳號）。

**操作步驟**：
1. 同時（盡量同一秒內）雙擊兩個不同帳號的捷徑，讓兩個 `upsert_profile()` 幾乎同時
   寫入 `~/.quanquant-agent/profiles.json`。
2. 兩者都完成精靈後，檢查 `profiles.json`：
   ```bash
   cat ~/.quanquant-agent/profiles.json | python3 -m json.tool
   ```
3. 確認兩筆 profile 都完整存在，檔案本身是合法 JSON（無截斷/雜訊）。
4.（進階，模擬更激烈的併發）用多個終端機同時對同一個 site_origin 觸發
   `upsert_profile`/`remove_profile`（需要能直接呼叫 Python API 的環境）：
   ```bash
   uv run pytest tests/test_agent_profile_registry.py::test_concurrent_upserts_from_multiple_threads_do_not_corrupt_registry -v
   ```

**預期結果**：
- `profiles.json` 全程維持合法 JSON、兩筆 profile 資料互不覆蓋遺失（全域跨程序鎖
  `profiles.lock` ＋ `fsync`/`os.replace` 原子替換）。
- pytest 併發測試綠燈。

**自動化覆蓋**：`tests/test_agent_profile_registry.py::test_concurrent_upserts_from_multiple_threads_do_not_corrupt_registry`。

---

## 4. 需求覆蓋自查表（spec §1-§8 → 對應本計畫情境／Task）

逐條列 spec 章節內容，標註對應到本文件哪個情境／哪個 Task，供撰寫完後核對「無遺漏」。

| spec 章節 | 內容 | 對應本文件情境 | 對應 Task |
|---|---|---|---|
| §1 G1 | 零終端機輸入啟動 | 情境 1（步驟 1）／情境 15 | 8/9/13/15 |
| §1 G2 | token 全自動（device-code） | 情境 1（步驟 3-6）／情境 9／情境 16 | 3/4/5/9 |
| §1 G3 | 永豐憑證 GUI 遮罩＋opt-in keychain | 情境 1（步驟 7）／情境 3／情境 8／情境 11 | 9/11 |
| §1 G4 | 狀態儀表板＋停止按鈕 | 情境 1（步驟 9）／情境 6 | 7/10 |
| §1 G5 | headless 完整保留 | 情境 7 | 14（既有測試安全網） |
| §2 D1 | device-code 授權流程 | 情境 1／情境 9／情境 16 | 3/4 |
| §2 D2 | 憑證落地兩個獨立 opt-in | 情境 3／情境 11 | 11 |
| §2 D3 | 新依賴 `keyring` | 情境 8 | 11（pyproject） |
| §2 D4 | 排程落點（Inc1 之後獨立 increment） | （排程決策，非人工測試情境） | 本計畫本身 |
| §2 D5 | 啟動交付物（捷徑） | 情境 15 | 15 |
| §2 D6 | token 不熱換，reauth 需重啟 | 情境 5／情境 17／情境 18 | 10/11/13（WS 握手被拒引導屬 D6 直接後果） |
| §3 URL 規則／不變量 | `--site` canonical／GUI 禁 `--server`／rotation 不中斷 | 情境 5／情境 7（間接） | 14（`--site` canonical／GUI 禁 `--server`）／4／5 |
| §4.1 協定全部子項 | 發起/PoP/輪詢五步/claim 交易/slow_down/consumed | 情境 9／情境 13／情境 16 | 3/4 |
| §4.2 新表 | `agent_device_codes` | 情境 9／情境 13／情境 14 | 2 |
| §4.3 濫用防護＋信任鏈 | 限流／`request_ip`／XFF | 情境 13／情境 14 | 3/5/16 |
| §5.1 輸入路徑洩漏面 | 422 不回帶／no-store／log 掃描 | 情境 12 | 9（Step 5b，正式 pytest） |
| §5.2 keychain | backend 檢查／兩 opt-in／先刪後寫／清除粒度 | 情境 8／情境 11／情境 17／情境 18 | 11／13（keyring entry 被外部刪除→回精靈重建） |
| §5.3 registry／GUI 決策樹／profile 選擇／fallback／CLI／buffer 隔離 | 全域鎖／決策樹／profile 選擇頁／fallback 規則／七層優先序／origin hash 路徑 | 情境 2／情境 3／情境 10／情境 19 | 12（registry 機制）／13（決策樹＋profile 選擇頁＋fallback 規則）／14（CLI 優先序／canonical site） |
| §5.4 token 到期 | 倒數／opt-in 分流重新授權 | 情境 5／情境 17／情境 18 | 10／13（direct-connect 探測失敗時的 reauth 導向） |
| §6.1 生命週期 | 單一協調器 | 情境 1（整體）／情境 6 | 8／13（`run_gui()` 串入決策樹） |
| §6.2 本機安全邊界 | bootstrap exchange／Host/Origin | 情境 1（步驟 2，自動、無感） | 8 |
| §6.3 頁面 | `/setup`／`/status`／profile 選擇頁 | 情境 1／情境 6／情境 10 | 9/10/13（profile 選擇頁） |
| §6.4 Runner 快照 | immutable snapshot | 情境 5／情境 18（`"rejected"` 連線態） | 7／13（新增 `"rejected"` 連線態） |
| §7 server 變更清單 | 全部六項 | 情境 1／情境 9／情境 13／情境 14／情境 16 | 1/2/4/5/6/16 |
| §8 威脅模型 | 十項對策 | 情境 4（釣魚）／情境 9（device_code 竊取）／情境 12（洩漏）／情境 13（限流）／情境 14（信任鏈）／情境 16（PoP） | 2/4/5/6/8/9/11/13/16 |

---

## 5. 驗證方式（本文件的自我查核記錄）

本文件撰寫過程中，所有引用的 pytest 檔名／測試函式名，皆已用下列方式對照過真實原始碼
（非只憑 spec 或 brief 猜測）：

1. `grep -n "^def test_\|def test_"` 逐檔核對本文件引用到的每一支測試函式**確實存在**
   於對應檔案（`tests/test_agent_*.py`／`tests/test_device_flow_*.py`／
   `tests/test_deployment_trust_chain.py`），並修正了 brief 對照表中兩處不完整命名
   （`test_concurrent_claim_only_one_winner` 補全為
   `test_concurrent_claim_only_one_winner_thread_level_race`；
   `test_broker_credentials_partial_write_failure_...` 補全為完整函式名）。
2. 逐一讀過 `src/quanquant/agent/gui/status_routes.py`、`setup_routes.py`、
   `coordinator.py`、`startup_flow.py`、`keyring_store.py`、`profile_registry.py`、
   `device_flow_client.py`、`shortcut_gen.py`、`startup.py`、`main.py` 與對應的 5 個
   Jinja2 模板、`web/routers/agent_authorize.py`、`web/routers/agent_device.py` 全文，
   確認本文件描述的按鈕文案、頁面流程、錯誤訊息字串、CLI 參數名稱皆逐字對應原始碼，
   非憑空想像。
3. 發現並記入§1「已知實作落差」：`/status` 頁「重新授權」／「清除已存憑證」按鈕實際
   為 `disabled`（用 `grep -n "disabled" status.html` 與讀 `status_routes.py` 全文
   確認）、`delete_profile` 未真正呼叫 registry/keyring 清除函式、
   `check_secure_backend()` 未被任何 GUI 路徑呼叫（`grep -rn "check_secure_backend"
   src/` 只出現在定義處與 docstring）、捷徑產生器無 CLI 包裝（`grep -rn "shortcut"
   pyproject.toml scripts/` 無結果）——這四點若未發現而逕自照 spec 原文寫「情境 17
   可以點擊重試按鈕重現」，會是臆造未實作功能，違反本 task 的核心約束。
4. `git log --oneline -- src/quanquant/agent/gui/status_routes.py` 確認該檔自 Task 10
   建立、Task 10 reviewer 修復後即未再被任何後續 task（11-16）修改，佐證上述落差非
   筆誤而是真實現況。
5. 情境 14 的 `docker-compose.yml` 內容（`FORWARDED_ALLOW_IPS: 172.28.0.10`）與情境 9/13/16
   引用的 rate-limit／TTL 數值（10 次/分 burst 5、per-IP 10 筆、全站 500 筆、token TTL
   30 天、poll interval 預設 5 秒、device code TTL 600 秒）皆直接讀
   `src/quanquant/config.py`／`docker-compose.yml` 現值，非沿用 spec 草稿數字。
