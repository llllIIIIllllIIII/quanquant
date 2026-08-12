# 設計 Spec — Agent 設定精靈（GUI Onboarding）

**建立**：2026-08-12（v6 定稿；v2-v6 為 codex 第 1-5 輪修訂）
**狀態**：**定案**（codex 第 6 輪 APPROVE；D1-D6 使用者 2026-08-12 拍板照推薦案）→ 實作計畫：`docs/superpowers/plans/2026-08-12-agent-setup-gui.md`
**關聯**：`2026-08-04-local-broker-agent-design.md`（通道協定/不變量表）、`docs/superpowers/reviews/2026-08-08-inc1-manual-test-plan.md`（現況痛點來源）
**排程**：獨立 increment，排在 Inc1（feat/agent-multiuser-inc1）人工實測收尾**之後**；不併入 Inc1。

---

## 0. 問題陳述

現況要非技術使用者完成四段操作才能啟動 agent（manual-test-plan §0/§2 實測）：

1. 登入網站 → `/orders` → 按「產生 Agent Token」→ 手動複製 token 明文
2. 開終端機 `export QQ_AGENT_TOKEN="..."`
3. `export QQ_AGENT_API_KEY=... QQ_AGENT_SECRET_KEY=...`（或啟動後被 getpass 逐一問）
4. `uv run quanquant-agent`

痛點本質：**網站產 token → 終端機貼變數 → 命令列啟動，三段斷裂**；且憑證 session-only（不落地）＝每次啟動重貼一次。終端機那一段對非技術使用者是斷崖。

## 1. 目標與非目標

**目標**
- G1：使用者從啟動 agent 到連線成功，全程不需要打開終端機輸入任何 `export`／不需要複製貼上 token。日常啟動＝點擊維運者預先建立的桌面捷徑（見 §2 D5 啟動交付物）。
- G2：token 取得全自動（device-code 授權流程，密碼不經手 agent）。
- G3：永豐 API_KEY/API_SECRET 用 GUI 遮罩欄位輸入；opt-in 存 OS keychain 後續免重輸。
- G4：連線後同一頁轉為狀態儀表板（連線/健康/fail-stop 可視化——現況 fail-stop 只有終端機看得到）＋「停止 Agent」按鈕（無終端機也能乾淨關閉）。
- G5：既有技術路徑（env vars + headless 直跑）完整保留、行為逐位一致，CI 與進階使用者不受影響。

**非目標**
- 取得永豐 API_KEY/API_SECRET 本身（永豐官網流程）——只提供教學連結。
- 打包成雙擊桌面 app（PyInstaller 等發行工程）——另議；本 increment 的安裝契約見 D5（維運者代裝 uv + repo ＋建捷徑，一次性）。
- real mode（mode 四層鎖 sim 不動）。
- token 熱換（不重啟程序換 token）——第一版明確不做，見 §5.4。
- server 部署側 `.env`（ORDER_CHANNEL/ORDER_OWNER_USER_IDS 等）——那是維運者職責，不是終端使用者步驟。

## 2. 決策記錄（2026-08-11 討論＋codex 第 1-5 輪裁決；**2026-08-12 使用者拍板定案，照推薦案**）

| # | 決策 | 暫定結論 |
|---|---|---|
| D1 | token 取得方式 | **device-code 授權流程**（使用者在真正的網站登入＋核准；agent 不經手網站密碼）。備案（不採）：精靈內輸入 QuanQuant 帳密由 agent 代簽。 |
| D2 | 憑證落地政策 | 拆成**兩個獨立 opt-in**：①「記住 QuanQuant 裝置授權」（token）②「記住永豐 API 憑證」（API_KEY/SECRET）。都存 OS keychain、都明示儲存位置、預設皆不勾。**所有 keyring 寫入（含重新授權）都必須尊重 opt-in 狀態**。永不落地明文檔案／永不進 server DB／不進 argv/log 之鐵律不變（本機 profile registry 只存非秘密 metadata，見 §5.3）。 |
| D3 | 新依賴 | `keyring`（僅為 D2 的 keychain 存取；含安全 backend 能力檢查，見 §5.2）。 |
| D4 | 排程落點 | Inc1 之後的獨立 increment（Inc1 待人工實測；審計期間凍結 merge）。 |
| D5 | 啟動交付物（第 3 輪 BLOCKER 補強：可執行性契約） | 不做 app 打包，但**安裝契約明文化**：維運者一次性代裝 uv + repo，並用 **M4 捷徑產生器**建立桌面捷徑。捷徑必須自帶可執行環境：**cd 到絕對 repo 路徑＋以絕對路徑呼叫 uv**（產生器以跨平台 `shutil.which("uv")` 當場偵測寫死）。macOS `.command` 內容形如 `cd "/abs/QuanQuant" && exec "/abs/uv" run quanquant-agent --gui --site https://<正式站origin>`，產生後 **`chmod 0700`**（無執行位雙擊不了）；Windows `.lnk` 的 target 用絕對 uv.exe＋同參數、`Start in` 設 repo 目錄。皆非秘密；多 profile 同機時另加 `--profile <id>`。**不可依賴 `--server` 預設值**。日常「雙擊」＝點捷徑；關閉走 GUI「停止 Agent」。 |
| D6 | token 熱換（第 1 輪裁決） | 第一版不做。重新授權的套用一律需重啟 agent；寫不寫 keyring 依 opt-in 分流（§5.4）。 |

## 3. 架構總覽

```
┌─ 使用者本機 ─────────────────────────────┐      ┌─ Server（GCP）──────────────┐
│ quanquant-agent 程序（單一 async 協調器）  │      │ FastAPI                      │
│  ├─ AgentRunner（既有，run_forever）       │◄─WS──┤  /ws/agent（不動，v1 協定）  │
│  ├─ 本機設定/狀態 HTTP server（新）        │      │  /api/agent/device-code（新）│
│  │   bind 127.0.0.1:隨機port               │─HTTPS┤  /api/agent/device-token（新）│
│  │   /setup 三步精靈 → /status 儀表板      │      │  /agent/authorize 核准頁（新）│
│  ├─ profile registry（非秘密 metadata）    │      │  agent_tokens（公開語意不動， │
│  └─ keyring 存取（新，opt-in）             │      │   內部抽交易 primitive，§4.1）│
│ 使用者瀏覽器：本機精靈頁＋正式站核准頁      │      │  agent_device_codes 表（新）  │
└──────────────────────────────────────────┘      └─────────────────────────────┘
```

原則：**WS 協定 v1 的 wire frame 完全不動**；`agent_tokens` 的公開簽發/rotation 語意不動（單 user 單枚有效、DB 只存 hash、明文只出現一次），但內部重構出不自行 commit 的 rotation primitive 供 device flow 組交易（§4.1）。

**URL 規則（第 3 輪 MAJOR 收斂）**：
- `--site`＝canonical origin，格式**限 `https://host[:port]`**（loopback host 例外允許 `http`，供本機開發）；**禁止** path/query/userinfo/fragment；預設 port 正規化（443/80 省略）後才比較與存入 registry。
- GUI 模式必填 `--site`；WS URL **一律導出**：`wss://<host[:port]>/ws/agent`（http site → `ws://`）。
- **GUI 模式禁止 `--server`**：同時指定 `--site` 與 `--server` → 啟動錯誤（不做部分比對，杜絕同 host 異 port/scheme/path 漏洞）。
- argparse 的 `--server` 預設改 **`None`**；headless 分支維持現行解析優先序 **`--server` > env `QQ_AGENT_SERVER` > `ws://127.0.0.1:8000/ws/agent`**（行為逐位一致，G5），預設值不再污染 GUI 判斷。

**正式化不變量**（寫進整合測試）：token revoke **不中斷**既有 WS 連線（token 只在握手驗）；舊 token 重連必失敗；新 token 重啟後必成功；同 user 新連線成功時舊連線由既有 generation replacement 取代。

## 4. Device-code 授權流程（G2）

參考 OAuth 2.0 Device Authorization Grant（RFC 8628）精神，簡化為本專案規模：

### 4.1 協定

1. **agent 發起**：`POST /api/agent/device-code`（無需認證——第 1 輪裁決：不加 email 弱綁定，靠限流＋核准端防線；body 附 `code_challenge`）→ 回 `{device_code, user_code, verification_path, interval, expires_in}`。
   - `device_code`：`secrets.token_urlsafe(32)`，agent 持有的輪詢憑據；server 只存 sha256 hash（比照 `agent_tokens.py` 慣例）。
   - **PoP 綁定（第 4 輪裁決，PKCE 式）**：`code_verifier` 為 agent 記憶體隨機值（`token_urlsafe(32)`），發起只傳 `code_challenge = sha256(code_verifier)`；輪詢時才附 `code_verifier`，server 驗 hash 相符才受理 claim——自側漏管道（日誌/錯誤訊息）竊得 device_code 者無 verifier 即無法兌換。
   - `user_code`：8 碼人類可讀（`XXXX-XXXX`，大寫、剔除 0/O/1/I 混淆字元），顯示在精靈頁。
   - `verification_path`：固定相對路徑 `/agent/authorize`；**agent 以內建常數 `/agent/authorize` 拼接 canonical `--site` 開啟**——server 回傳值僅供 exact-match 核對（不一致→中止並警示），**絕不對回傳值做 URL resolve**（防 `//evil.example` 這類 network-path reference 導向外站；第 4/5 輪裁決）。不帶 code——使用者必須在核准頁**手動輸入**本機精靈顯示的 user_code——刻意保留的防釣魚摩擦（無 `?code=` 預填）。
   - `expires_in`：600 秒；`interval`：5 秒起。
2. **使用者核准**：agent 自動開瀏覽器至由 `--site`＋`verification_path` 解析的核准頁。使用者在**正式站**登入（既有 session 認證）→ 手動輸入 user_code → 頁面顯示請求建立時間＋「此裝置請求連線你的下單 agent（sim 模式）」→ 按「核准」或「拒絕」。
   - 資格沿用 owner-only guard（與 `POST /orders/agent-token` 相同）。
   - **核准/拒絕 POST 必帶一次性 synchronizer CSRF token**（既有 session cookie 是 SameSite=Lax，安全敏感操作不可只靠 cookie 行為）。
   - 核准以 conditional UPDATE 執行：`SET status='approved', user_id=:uid WHERE status='pending' AND expires_at > now`——過期/已處理列不可核准。**此時不簽 token**。
3. **agent 輪詢**：`POST /api/agent/device-token`（body 帶 device_code＋`code_verifier`）→ 回 `pending` | `slow_down` | `approved`（含 token 明文＋metadata）| `expired` | `denied` | `consumed`。
   - **agent 端輪詢紀律**：同一 device flow 任一時刻**只允許一個 in-flight 輪詢**（timeout 後才重送）；以第一個成功回應為準，遲到的重複回應丟棄。
   - **輪詢入口順序（第 5 輪裁決：verifier 先行、失敗零副作用）**：①以 hash 查列 ②constant-time 驗 `code_verifier`——**失敗回 generic 錯誤且不改任何狀態**（無 verifier 者不能污染 interval/封鎖、不能探知列狀態）③唯讀 terminal 狀態（`expired`/`denied`/`consumed`，恆同冪等）④查 `blocked_until`／節流判定與更新 ⑤`pending`／`approved` claim。
   - **consume-on-poll 的交易設計（第 1 輪 BLOCKER，第 2/3 輪核對維持）**：既有 `issue_token()` 會自行 commit、撞 rotation partial unique index 時 rollback 後內部重試——不能直接嵌在外層搶占交易裡。因此：從 `agent_tokens.py` 抽出**不自行 commit** 的 rotation primitive（revoke 舊枚＋插入新 hash，不 commit），新函式 `claim_and_issue_device_token()` 負責完整交易——每次嘗試依序（verifier 已於輪詢入口驗畢）：①conditional UPDATE 搶占 `consumed_at`（`WHERE consumed_at IS NULL AND status='approved'`）②呼叫 primitive stage rotation ③單次 commit。**任何 rollback 後必須從步驟①重新開始**（搶占與簽發同生共死）。併發輪詢只有一方搶占成功。`agent_tokens.py` 公開介面與語意不變。
   - **approved 回應內容**：token 明文之外附 `profile_id`（server user id 或不可變 opaque ID）、顯示用 `username`、`token_expires_at`——供 profile registry／keyring 定位（§5.3/§5.2）與狀態頁到期倒數。明文與 metadata 直接回傳、server 端不落地明文。
   - **`consumed` 終態（第 3 輪 BLOCKER 重設計：純資訊、不 revoke）**：commit 成功但回應在途遺失時，device code 已 consumed、明文不可復原。後續輪詢收到 `consumed`（**唯讀、冪等**，重複輪詢恆同）→ agent 自動重開新一輪 device flow。**server 不做 consumed-retry 即時 revoke**——v3 的即時 revoke 存在並行競態（P1 已 commit、回應在途，P2 重複輪詢會誤殺 P1 剛交付的合法 token）。孤兒 token 的收斂依既有**單枚有效 rotation 不變量**：新一輪授權簽發時自動 revoke 前枚；完全放棄則由 TTL 到期收斂。風險評估：回應遺失代表無人持有該明文，孤兒枚不可被使用；竊碼搶兌路徑另由 PoP verifier 封鎖（§8）。UI 文案：「授權交付中斷，請重新核准；先前的授權會在新授權完成時自動作廢」。
   - **slow_down**：`agent_device_codes` 以 `last_polled_at`/`current_interval`/`consecutive_violations`/`blocked_until` 四欄位、**單一 conditional UPDATE** 原子執行：低於 interval 的輪詢 → `current_interval += 5s`（上限 30s）、`consecutive_violations += 1`，達 5 次 → `blocked_until = now + 60s` 且歸零計數；**間隔合規的輪詢 → `consecutive_violations` 歸零**（interval 維持現值）。狀態全存 DB＝程序重啟不能繞過（含跨重啟測試）。

### 4.2 server 端新表 `agent_device_codes`

| 欄位 | 說明 |
|---|---|
| `id` | PK |
| `device_code_hash` | sha256，unique index |
| `code_challenge` | sha256(code_verifier)，發起時寫入；claim 前驗證（§4.1 PoP） |
| `user_code` | 8 碼，unique index（活動期間） |
| `user_id` | nullable FK，核准時綁定 |
| `request_ip` | 發起來源 IP（正規化）＋index——per-IP active pending 計數跨重啟有效（§4.3） |
| `status` | `pending`/`approved`/`denied`（CHECK 約束） |
| `created_at` / `expires_at` / `consumed_at` | 生命週期控制 |
| `last_polled_at` / `current_interval` / `consecutive_violations` / `blocked_until` | slow_down 原子節流與封鎖（§4.1） |

- **建表走 `db/models.py` SQLModel 定義（`create_all` 建立）**——`_MIGRATIONS` 只支援既有表的 nullable ADD COLUMN，不用於新表。未來替此表補欄位才進 `_MIGRATIONS`。雙方言可攜照 repo 慣例。
- 清理：過期列由簽發路徑順手 DELETE（`expires_at < now - 1 day`），不加背景任務。

### 4.3 濫用防護與真實 IP 信任鏈（第 3 輪 BLOCKER 補強）

- **信任鏈兩段都要設定**：
  1. **Caddy 段**：Caddy `reverse_proxy` 預設**忽略**客戶端送入的 `X-Forwarded-*` 並以真實 peer IP 覆寫（明載依賴此預設行為）。
  2. **uvicorn 段（v3 遺漏）**：app 與 Caddy 是**不同容器**，uvicorn 預設只信任 `127.0.0.1` 的轉發標頭——不設定就會把所有請求記成 Caddy 容器 IP（per-IP 限流全體合流＝互相 DoS）。部署必須設 `forwarded_allow_ips`＝**IP/CIDR 字面值**（docker-compose ipam 固定 Caddy IP 或專用 caddy→app 子網）；**不得填 Docker 服務別名**——uvicorn 只做 IP/CIDR 字面比對、不解析 DNS 名（uvicorn 亦讀 `FORWARDED_ALLOW_IPS` env）。
  - **驗收**：偽造 XFF 整合測試（外部帶假 XFF，server 看到真實 IP）＋**雙來源測試**（兩個不同來源 IP 經 Caddy 打入，server 記到的 client IP 必須不同、不得同為 Caddy IP）。
  - 本機開發（無 proxy）：不設定，直接 socket peer IP。app 端不得解析任意 `X-Forwarded-For`。
- **發起限流**：每 IP 10 次/分鐘（burst 5）；同 IP 最多 10 筆 active（未過期）pending；全域最多 500 筆 active pending。超限回 429。active 計數**只算未過期列**。
- **實作位置**：現部署為單一 uvicorn worker——`app.state` token bucket（每 IP 頻率）＋`asyncio.Lock` 序列化發起；**active pending 計數一律查 DB `request_ip` 欄位**（同鎖內 count→insert，跨重啟有效）。若未來多 worker 需改 DB 層頻率計數（記入部署備忘）。
- **輪詢節流**：DB 原子 slow_down＋封鎖（§4.1），不靠記憶體狀態。
- user_code 人工輸入＋比對是防釣魚的人工防線：核准頁與精靈頁都以大字顯示，文案明示「代碼不一致就不要核准」。

## 5. 憑證處理（G3；不變量保存）

### 5.1 輸入路徑

精靈步驟 2 表單（`type=password` 遮罩、`autocomplete="off"`）POST 到本機 server → 只進程序記憶體 → 組 `ChildHandle(credentials={...})` 走**既有路徑不變**（`main.py` → `runner.py` → `native_runner.py` child → SDK login）；`redact_secrets` 防線、不進 argv/log 全部保持。

**GUI 端洩漏面防堵**：秘密欄位手動解析、驗證失敗的 422/error body **絕不回帶輸入值**；所有 setup/status 回應 `Cache-Control: no-store`；狀態頁 `last_error` 只保存 redact 後字串；驗收含「掃 access/application log 不含三項秘密」的自動化測試。

### 5.2 keychain 儲存（兩個獨立 opt-in）

- 套件：`keyring`（D3）。**啟動時做 secure-backend 能力檢查**：明確拒絕 fail/plaintext/未鎖定/檔案型 backend——判定無安全 backend 時 **fail closed**：完全不寫入、UI 明示「此系統無安全儲存區，無法記住」，絕不退化成明文檔案。macOS Keychain 與 Windows 憑證管理員列入驗收矩陣；Linux 無 Secret Service＝不記住。
- **定位**：keyring entry 以 canonical site origin（§3）＋`profile_id` 定位；profile_id 由 profile registry（§5.3）或 device flow approved 回應取得。
- **儲存粒度**：永豐 API_KEY/SECRET 存成**單一版本化 JSON secret**（一筆、原子寫入，避免半套）；device token（含 `token_expires_at`、`username` metadata）另存一筆。checkbox ①只寫 token 筆、②只寫永豐筆。
- **寫入失敗處理（第 3 輪收斂：逐筆自治）**：兩個 opt-in **各自獨立提交、各自顯示結果、互不回滾**。每筆寫入前先讀取既有值快照；該筆寫入失敗時**只復原該筆自身的快照**（另一筆成功就保留成功）。**快照復原本身也失敗** → 顯式錯誤狀態：「儲存失敗且原值可能遺失——請至『清除已存憑證』檢查後重新設定」，**不得**無條件宣稱原值已保留。禁止盲目 delete。**token 筆例外（第 4/5 輪裁決：先刪後寫）**：重新授權（rotation 成功）後舊枚已被 server 撤銷——更新順序固定：①**先刪除舊 token 筆**（刪除失敗→顯式錯誤「無法安全更新授權，請至『清除已存憑證』手動清除後重試」，**不繼續寫入**、fail closed）②寫入新筆；寫入失敗時不復原舊快照、新 token 明文保留於程序記憶體＋UI「重試儲存」＋「儲存成功前請勿關閉」警示；使用者仍關閉 → 因舊筆已刪，下次啟動 token 缺失、自然回到重新授權（收斂成立）。快照復原僅適用於舊值仍有效的筆（永豐憑證）。
- 清除粒度（第 3 輪收斂）：狀態頁分項清除**單一 secret** 時 registry entry 保留；「**刪除整個 profile**」才移除 registry entry＋該 profile 全部 keyring 筆；buffer 有未送資料時拒絕刪 profile 並警示。
- token 沒存或已過期 → 重跑 device flow（成本＝按一次核准，可接受）。

### 5.3 profile registry、啟動決策樹與多實例隔離

**profile registry**：本機檔 `~/.quanquant-agent/profiles.json`，**只存非秘密 metadata**：`{site_origin: [{profile_id, username, buffer_path, created_at}]}`。用途：解「啟動時要讀哪組 keyring key」的雞生蛋——keyring 定位需要 profile_id，而 profile_id 在首次 device flow 才從 server 取得，registry 就是把它記下來的地方。秘密仍只在 keyring。**registry 併發保護（第 5 輪裁決）**：`profiles.json` 讀寫以**全域跨程序鎖**（獨立 registry lock 檔）保護——鎖內重新載入→修改→fsync→`os.replace` 原子替換，杜絕多 profile 並行 upsert/刪除互蓋或留下破損檔。

**CLI 優先序（第 3 輪 MAJOR 收斂；由上而下，先命中先生效）**：

```
1. --no-gui 與 --gui/--reset 同時指定 → argparse 互斥錯誤
2. --no-gui → headless（env/getpass 現行為；不探測 keyring/registry）
3. --reset（蘊含 GUI，需 --site）→ 強制重跑精靈（keyring 舊值保留至新值成功寫入——僅適用仍有效的秘密如永豐筆；token 筆一律依 §5.2 先刪後寫）
4. --gui（需 --site）→ GUI shell 照下方決策樹；GUI 模式不採用 env 三件套（單一來源原則）
5. （皆未指定）env 三件套齊全 → headless 直跑（現行為逐位一致，G5）
6. （皆未指定）tty 且有 --site → 等同 --gui
7. 其餘 → headless getpass 現行為
```

**GUI 決策樹**：查 registry[site_origin]：

```
├─ 0 筆 → /setup 精靈（完成後寫入 registry）
├─ 1 筆（或 --profile 命中）→ 讀該 profile 的 keyring
│    ├─ token＋永豐筆齊且未過期 → 直接連線＋開 /status
│    │     └─ WS 握手被拒（token 實際失效）→ 不信 metadata，引導精靈步驟①重新授權
│    └─ 缺哪筆 → 精靈從對應步驟開始（token 缺→步驟①；只缺永豐→步驟②）
└─ >1 筆且未指定 --profile → GUI profile 選擇頁 → 同上
```

**profile fallback 規則（第 3 輪 MAJOR 收斂）**：
- `--profile` miss（registry 無此 id）→ GUI 明示「找不到此帳號設定」→ 走精靈；完成後 UI 明示「捷徑指向的帳號已變更，請重新產生捷徑或修改 `--profile`」——避免捷徑永遠 miss。
- 授權完成回傳的 `profile_id` 與 registry/`--profile` 預期**不符**（例如使用者在核准頁登入了另一個帳號）→ **切換至該 profile_id**：registry 以 `(site_origin, profile_id)` 為**唯一鍵 upsert**——已存在則沿用該 profile 既有 buffer/keyring（永豐筆在則確認後沿用、缺則補問）；不存在則隔離新建（新 keyring entries、新 buffer、重問永豐憑證）。任何情況都不沿用**其他** profile 的永豐憑證或 buffer；不得建立重複的同 ID profile（第 4 輪裁決）。
- registry 指向的 keyring entry 已被外部刪除 → 回精靈重建（不 crash）；重建拿到同 profile_id 則沿用原 buffer 路徑。

**per-profile buffer 與單實例（第 4 輪 BLOCKER 收斂：以完整 origin 隔離）**：GUI 路徑 buffer 預設 `~/.quanquant-agent/{origin_dir}/{profile_dir}/outbox.db`，其中 `origin_dir = sha256(canonical_origin) 前 16 hex`、`profile_dir = sha256(profile_id) 前 16 hex`——路徑元件一律固定長度 hex（filesystem-safe：IPv6 host、含特殊字元的 opaque ID 都不會破壞跨平台路徑或目錄隔離；人類可讀對照存 registry metadata），同 host 異 scheme/port **不會**共用 outbox（杜絕跨 origin 回報重播）。lock 檔綁定**解析後的 buffer 路徑**取得**單實例 process lock**，同 profile 禁止第二個 agent 程序。**GUI 模式禁用 `--buffer` 與 `QQ_AGENT_BUFFER` 覆寫**（指定即啟動錯誤）；headless 維持現行預設與覆寫行為（`~/.quanquant-agent/outbox.db`）不變（G5）。

**舊 buffer 遷移政策**：舊預設 buffer 若有未送資料，**不自動搬移、不另開新 profile**——GUI 大字警示，要求先以原設定（headless）跑到收斂或人工處理後再走精靈。

### 5.4 token 到期與重新授權（D6）

- 狀態頁顯示 `token_expires_at` 倒數（<3 天變黃，附「重新授權」）。env 手動 token 無 expiry 資訊 → 顯示「到期時間未知」，不推測 30 天。
- 「重新授權」依 opt-in 分流：
  - **已勾「記住裝置授權」**：立即重跑 device flow → 新 token 寫入 keyring（寫入失敗走 §5.2 token 筆例外：記憶體保留＋重試＋勿關閉警示）→ UI 明示「重新啟動 Agent 後套用」。
  - **未勾（session-only）**：不預先取 token（取了既不能熱換、重啟又不會保留——白簽一枚）。UI 改為指引：「停止 Agent 後重新啟動，啟動時會重新引導授權」，並附「改為記住此授權」的 opt-in 選項（勾了才走上一條路徑）。
- 第一版不做熱換（`WebsocketsTransport._token` 建構時固定，不引入 mutable token）；既有 WS 連線在 revoke 後持續有效直到斷線，符合 §3 不變量。

## 6. 本機 GUI（G1/G4）

### 6.1 程序生命週期（單一 async 協調器）

- 入口改為單一 async coordinator：①先預綁 `127.0.0.1:0` socket 取得實際 port ②以 `uvicorn.Server.serve(sockets=[...])` 啟動 GUI server ③自動開瀏覽器 ④精靈完成、憑證齊備後**才**建構 ChildHandle/transport/AgentRunner 並啟動 `run_forever` task。
- Runner fatal（含 fail-stop）時 **GUI 保持存活**顯示錯誤與指引；「停止 Agent」與 OS signal 的關閉順序：stop runner → 關 WS → 驗證 child 終止 → 最後停 uvicorn。
- 技術：FastAPI + uvicorn（同 package 既有依賴，零新增）＋極簡 HTML/HTMX。GUI access log 關閉。

### 6.2 本機安全邊界（一次性 bootstrap exchange）

- `setup_secret`（`token_urlsafe`）只出現在自動開啟的 bootstrap URL **一次**：首個請求驗證後立即失效、種下 session cookie（`HttpOnly`、`SameSite=Strict`）並 303 到乾淨 URL——URL 歷史/Referer 不殘留可用 secret。
- 所有本機 API：驗 session cookie＋**exact `Host: 127.0.0.1:<實際port>`**（擋 DNS rebinding）＋same-origin `Origin`/Fetch Metadata 檢查；全 POST。
- 回應標頭：`Referrer-Policy: no-referrer`、`Cache-Control: no-store`、CSP `default-src 'self'`、`frame-ancestors 'none'`。

### 6.3 頁面

- profile 選擇頁（僅同 site 多 profile 且未指定 `--profile` 時出現）：列 username 清單＋「新增帳號」。
- `/setup` 三步精靈：①連線授權（顯示 user_code 大字、開核准頁按鈕、輪詢進度；`consumed` 自動重開流程）②永豐憑證（遮罩欄位＋兩個獨立「記住」checkbox＋永豐申請教學連結）③確認啟動（顯示 site、mode=sim、symbol、username）。
- `/status`：連線 badge（連線中/重連中/離線）、mode=sim 標示、buffer 健康（pending 筆數）、**fail-stop 大紅警示**（含指引文案）、token 到期倒數（§5.4）、「重新授權」、「清除已存憑證／刪除 profile」、「停止 Agent」。

### 6.4 Runner 狀態快照

- snapshot 為 **immutable dataclass、整份替換**，由主 event loop 單一擁有——GUI 與 runner 同 loop，讀取不需 threading lock；child 執行緒不得直接寫，只能 `loop.call_soon_threadsafe` 回主 loop 更新。
- buffer pending 計數走 `asyncio.to_thread(...)`（SQLite 同步 I/O 不壓 loop——repo 既有 event-loop 鐵律）。
- 狀態頁每 1 秒 HTMX 輪詢（本機、無負載疑慮），不需 SSE。

## 7. Server 端變更清單（全部）

| 項目 | 內容 |
|---|---|
| 新 endpoint ×2 | `POST /api/agent/device-code`（發起＋限流）、`POST /api/agent/device-token`（輪詢＋`claim_and_issue_device_token` 交易＋`consumed` 唯讀終態） |
| 新頁面 ×1 | `/agent/authorize` 核准頁（登入後、owner-only、手動輸入 user_code＋CSRF token＋核准/拒絕） |
| 新表 ×1 | `agent_device_codes`（§4.2，SQLModel `create_all`） |
| 內部重構 ×1 | `agent_tokens.py` 抽出不自行 commit 的 rotation primitive（**公開介面與語意不變**，既有測試不動全綠為驗收） |
| 部署設定 ×1 | app 容器 uvicorn 設 `forwarded_allow_ips`＝固定 Caddy IP／專用子網 **CIDR 字面值**（compose ipam 固定；不得用服務別名；§4.3 信任鏈） |
| 不動 | WS `/ws/agent` 握手與 v1 wire frame、`/orders` 頁既有「產生 Agent Token」按鈕（保留為手動備援路徑）、Caddyfile（依賴其預設 XFF 行為＋偽造 XFF 驗收測試） |

## 8. 威脅模型（安全審查重點）

| 威脅 | 對策 |
|---|---|
| 其他本機程式打精靈 API 竊憑證 | bind 127.0.0.1＋一次性 bootstrap secret→HttpOnly/SameSite=Strict session cookie＋exact Host＋Origin 檢查（§6.2） |
| 瀏覽器跨站頁面 CSRF/DNS rebinding 打 localhost | 同上；CSP＋frame-ancestors 'none'＋no-referrer/no-store |
| device_code 被猜/竊取搶兌 | 128-bit entropy、server 只存 hash、600s TTL、consume-once（`consumed` 唯讀終態）＋**PoP `code_verifier`**（竊得 device_code 無 verifier 不能兌換） |
| 釣魚：誘導使用者核准攻擊者的 device_code | 核准頁**手動輸入** user_code（無預填）＋請求時間顯示＋警語；發起限流壓低嘗試量 |
| 核准端 CSRF | 核准/拒絕 POST 帶一次性 synchronizer CSRF token（session cookie SameSite=Lax 不足恃） |
| 限流繞過／合流 | Caddy 預設忽略外來 XFF＋uvicorn `forwarded_allow_ips` 只信 Caddy（§4.3 兩段信任鏈＋雙來源驗收測試）；輪詢節流/封鎖存 DB（重啟不歸零） |
| 回應遺失產生孤兒 active token | 無人持有明文＝不可被使用；由單枚有效 rotation（下一輪授權）或 TTL 收斂；不做即時 revoke（避免誤殺競態） |
| server DB 外洩 | token/device_code 都只存 sha256 hash |
| 本機磁碟外洩 | 秘密只在 OS keychain（安全 backend 檢查，無則 fail closed）；profile registry 只存非秘密 metadata |
| 憑證進日誌/回應 | 既有 `redact_secrets` 不變；GUI 422 不回帶輸入、log 掃描測試、access log 關閉、no-store |

## 9. codex 覆核決議記錄

**第 1 輪（REVISE）**：consume-on-poll 交易原子性、啟動交付物、consumed 終態、新表建法、bootstrap exchange、核准頁 CSRF＋手動輸碼、限流原子化、兩 opt-in、approved metadata、keyring 能力檢查、生命週期協調器、不熱換、per-profile buffer、GUI 洩漏面、不變量正式化。開放問題六題採 codex 建議答案。

**第 2 輪（REVISE）**：捷徑 `--site`、profile registry、`--gui/--reset` 分拆、slow_down 欄位、keyring 快照復原、重新授權尊重 opt-in、consumed 文案、Caddy XFF 明載。

**第 3 輪（REVISE：3 BLOCKER＋4 MAJOR＋6 PARTIAL）→ 本版 v4 落點**：
- BLOCKER：consumed-retry 即時 revoke 有並行誤殺競態 → **改為純資訊唯讀終態、不 revoke**，孤兒枚由 rotation 不變量/TTL 收斂；移除 `issued_token_id`；加 agent 端單一 in-flight 輪詢紀律（§4.1/§4.2/§8）
- BLOCKER：uvicorn 不信任跨容器 Caddy 的轉發標頭 → 信任鏈兩段設定（`forwarded_allow_ips`）＋雙來源驗收測試（§4.3/§7）
- BLOCKER：捷徑可執行性 → cd 絕對 repo 路徑＋絕對 uv 路徑＋Windows `Start in`（D5）
- MAJOR：URL 規則 → `--site` canonical 定義（禁 path/query/userinfo/fragment、port 正規化）、GUI 禁 `--server`、parser 預設 None（§3）
- MAJOR：CLI 優先序 → 七層明定＋互斥錯誤（§5.3）
- MAJOR：profile fallback → 不符即新 profile 隔離建立、miss 後提示更新捷徑、分項清除保留 registry／刪 profile 才移除（§5.3/§5.2）
- MAJOR：keyring 驗收 → 逐筆自治（成功筆保留）＋復原失敗顯式錯誤狀態（§5.2）

**第 4 輪（REVISE：1 BLOCKER＋6 MAJOR＋1 MINOR＋5 PARTIAL）→ 本版 v5 落點**：
- BLOCKER：buffer 只以 host 隔離不足（同 host 異 scheme/port 共用 outbox＝跨 origin 重播）→ 完整 canonical origin hash 目錄＋lock 綁 buffer 路徑＋GUI 禁 `--buffer`/`QQ_AGENT_BUFFER`（§5.3）
- MAJOR：reauth 後 keyring 寫入失敗會復原已撤銷舊 token → token 筆不復原、記憶體保留新明文＋重試＋勿關閉警示（§5.2/§5.4）
- MAJOR：device_code 竊取搶兌 → PKCE 式 PoP `code_challenge`/`code_verifier`（§4.1/§4.2/§8）
- MAJOR：`forwarded_allow_ips` 不得用 Docker alias → IP/CIDR 字面值＋compose ipam 固定（§4.3/§7）
- MAJOR：相對 verification_url 無法開啟 → `verification_path`＋agent 以 `--site` 解析、不開 server 回傳絕對 URL（§4.1）
- MAJOR：profile_id 已存在時的碰撞 → registry `(site_origin, profile_id)` 唯一鍵、切換沿用而非重複新建（§5.3）
- MAJOR：headless server 優先序未明定 → `--server` > `QQ_AGENT_SERVER` > 本機預設（§3）
- MINOR：`.command` chmod 0700＋`shutil.which` 跨平台偵測（D5）

**第 5 輪（REVISE：4 MAJOR＋2 MINOR＋1 PARTIAL；無 BLOCKER）→ 本版 v6 落點**：
- MAJOR：reauth 收斂不成立（舊撤銷筆殘留 keychain）→ token 筆「先刪後寫」＋刪除失敗 fail closed＋WS 握手被拒即引導 reauth（§5.2/§5.3）
- MAJOR：無 verifier 者可污染節流/封鎖 → 輪詢入口順序改為 verifier constant-time 先行、失敗零副作用（§4.1）
- MAJOR：registry 併發 read-modify-write 互蓋 → 全域跨程序鎖＋fsync＋原子替換（§5.3）
- MAJOR：per-IP pending 計數缺欄位 → `request_ip` 欄位＋index、鎖內查 DB（§4.2/§4.3）
- MINOR：路徑元件 filesystem-safe → origin/profile 目錄一律固定長度 hash（§5.3）
- MINOR：network-path reference → 內建常數拼接、回傳值僅 exact-match（§4.1）

**第 6 輪（APPROVE）**：第 5 輪 6 項全數 RESOLVED；僅餘兩條 MINOR 文字修整（M4 情境範圍 1-19、`--reset` 舊值保留句排除 token 筆），已收入本版。

**目前無未決開放問題**；待使用者最終拍板 D1-D6 後進 writing-plans。

## 10. 驗收情境（人工測試觀點，寫 plan 時展開）

1. 全新使用者：點桌面捷徑（含 `--site`）→ 精靈 → 網站手動輸入 user_code 核准 → 輸入永豐憑證（勾兩個記住）→ 連線成功 badge 綠 → 下測試單成功。全程零終端機輸入。
2. 二次啟動：registry 命中＋keyring 憑證齊 → 直接連線＋開 `/status`，免精靈。
3. 只勾「記住裝置授權」不勾「記住永豐憑證」：二次啟動 token 自 keyring 取得、精靈只補問永豐憑證（步驟②起）。
4. user_code 不一致（模擬釣魚）：核准頁需手動輸碼，輸入不存在的碼會失敗；使用者可辨識並拒絕。
5. token rotation：網站手動再產 token → 既有 WS 連線**不中斷**（不變量）→ agent 斷線重連失敗 → 狀態頁引導重新授權（依 opt-in 分流）→ 重啟後恢復。
6. fail-stop：buffer 目錄唯讀 → GUI 保持存活、大紅警示＋指引。
7. headless 迴歸：env 三件套＋`--no-gui` → 與現行行為逐位一致（既有整合測試不動全綠；不探測 keyring/registry）。
8. keyring 不可用平台／不安全 backend：明示不能記住、仍可完成單次連線；絕無明文落地。
9. `consumed` 復原：模擬輪詢回應遺失 → 重試收到 `consumed`（重複輪詢恆同、DB 無 revoke 動作）→ 自動重開 device flow → 新授權完成後驗 DB：舊枚由 rotation 撤銷；並行 poll 測試：兩併發輪詢只有一方拿到 token。
10. 同機雙 profile：兩捷徑各帶 `--profile` → buffer/keyring/port 互不干擾；同 profile 起第二個程序被 process lock 擋下；未帶 `--profile` 時出 profile 選擇頁；核准頁登入不同帳號 → 切換/新建對應 profile（registry 唯一鍵、不重複建同 ID）、不沿用他 profile 憑證；同 host 異 port 的兩個 site → buffer 目錄不同（origin hash 隔離）。
11. keyring 寫入失敗：模擬第二筆寫入失敗 → 第一筆成功保留、第二筆復原自身快照；快照復原也失敗 → 顯式錯誤狀態（不宣稱原值保留）。
12. 秘密洩漏掃描：自動化測試掃 GUI 回應/422/log 不含三項秘密；registry 檔內容驗證無秘密。
13. slow_down 封鎖：連續提早輪詢 5 次 → 封鎖 60 秒（重啟 agent 不能繞過）；恢復正常間隔後 violations 歸零。
14. 信任鏈：外部帶偽造 XFF → server 記真實 IP；兩個不同來源 IP 經 Caddy → server 記到各自真實 IP（限流不合流）；`forwarded_allow_ips` 以 IP/CIDR 字面值設定並實測。
15. 捷徑實跑（macOS/Windows 驗收矩陣）：從 Finder/檔案總管雙擊 → 正確 cd、找到 uv、`.command` 具執行位、GUI 開啟；`--profile` miss → 精靈＋更新捷徑提示。
16. PoP：正確 `code_verifier` 才能兌換；錯誤/缺 verifier 的輪詢被拒且不搶占（竊碼搶兌模擬）。
17. reauth 儲存失敗：重新授權後模擬 keyring 寫入失敗 → 不復原舊 token、UI 重試＋勿關閉警示；仍關閉後重啟 → 回到重新授權收斂。
18. reauth 先刪後寫：模擬刪除舊 token 筆失敗 → fail closed 顯式錯誤、不寫入；模擬 keyring token 未過期但 server 已撤銷 → WS 握手被拒 → 引導重新授權（不信 metadata）。
19. registry 併發：兩 profile 程序同時 upsert/刪除 `profiles.json` → 無互蓋、無破損檔（全域鎖＋原子替換）。

## 11. 里程碑切分（供 writing-plans 參考）

1. **M1 server 側**：`agent_tokens.py` primitive 重構（既有測試全綠）→ `agent_device_codes` 表＋兩 endpoint（交易＋PoP 入口驗證零副作用＋`consumed` 唯讀終態＋slow_down/封鎖＋`request_ip` 計數）＋核准頁（CSRF＋手動輸碼）＋限流＋信任鏈部署設定與偽造 XFF/雙來源測試——可獨立測試（curl 模擬 agent）。
2. **M2 agent 本機 GUI**：async 協調器＋精靈＋狀態頁＋bootstrap exchange 安全邊界＋runner 狀態快照＋`--site` URL 導出與 CLI 優先序——用 M1 打真 server。
3. **M3 keyring＋profile registry**：backend 能力檢查＋兩 opt-in 逐筆自治儲存/復原/清除（token 先刪後寫）＋registry 全域鎖與原子替換＋profile 生命週期（fallback 規則）＋per-profile buffer/lock（hash 路徑）。
4. **M4 端到端**：捷徑產生器（絕對路徑契約＋執行位/`Start in`，寫入 `--site`/`--profile`）＋驗收情境 1-19。
