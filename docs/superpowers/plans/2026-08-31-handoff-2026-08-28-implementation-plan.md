# 2026-08-28 交接包實作計畫（2026-08-31 擬）

依據：`2026-08-28/` 五份文件（README＋change-requests 001–009＋sim-account D1–D5＋risk-control F0–F6＋附錄 feedback）。
查證：以 fresh-context subagent 對 branch `feat/order-report-latency` 逐項比對原始碼（2026-08-31），結論見 §1。

---

## §1 文件假設 vs 現況的落差（先讀，影響整份計畫）

### 已過期／需回饋測試端的重大項

1. **F1「交易冷靜期」已經實作並存在於本分支**（commit 1339032，2026-08-22）——風控文件把它當全新構想評估。
   現況：`db/models.py:661-687`（Cooldown 表）、`broker/risk.py:176-183`（check_place 閘門，**已經只擋 New/Auto、放行 Cover**）、
   `POST /orders/cooldown`（orders.py:410-448）、admin 解除頁 `/admin/cooling-off`、`partials/cooldown_control.html`。
   per-user、不分 sim/real、本人啟動後**完全無法自行解除**（僅到期或 admin）。
   → F1 的實作範圍縮小為 **delta**：clean-slate 啟動條件、5 分鐘反悔窗、固定時長選項（現為自訂 datetime-local，上限 90 天）、
   啟動彈窗揭露（持倉/掛單/禁止與允許清單）、期滿通知。其餘文件內容可視為已滿足或需依現況重寫。

2. **Kill switch 授權敘述已過期**：文件寫「owner-only」，實際 b9f2511（2026-08-27）已收緊為 **admin-only**
   （orders.py:304-309 先查 `user.role != "admin"`）。
   → 與 F0 的產品邊界**直接矛盾**：F0 定案「本人可隨時啟動與解除的自我煞車、per-user、只擋 real」，
   但現行 kill switch 是 admin-only 全域開關（收緊正是為了防非 admin 誤按全站停止）。
   **建議解法（需使用者／測試端確認）**：拆成兩個東西——
   (a) 現行 admin 全域「緊急停止下單」維持 admin-only（技術/營運煞車，擋全部新單），落點 /risk 頁；
   (b) F0 新建 **per-user「自我停止」**（本人隨時啟停、只擋開倉、只擋 real、含掛單揭露＋批次撤銷），與冷靜期同屬自我約束系。
   不要把 admin 開關改回人人可按。

3. **「斷開 Agent」鈕位置**：文件與記憶說在帳戶頁，實際在下單頁 admin 區塊（orders.html:17）。006 精簡化下單頁時一併決定去留（建議隨緊急停止移入 /risk 或 admin 區）。

### 已查證屬實的關鍵事實（計畫直接建立在這些之上）

- orders.html:81-82 mode 分頁兩態同色（P0-1 bug 仍在）；kill_switch_control 仍 include 於 orders.html:16；報價列/橘標/精簡條均未做。
- `/quote`、`/quote/stream` 完全不吃 symbol 參數（dashboard.py:68+）——002 唯一後端工作。
- `data-scheme` 只在 dashboard.html:396 設；`User.chart_color_scheme` 存在（models.py:189）——001 的前提成立。
- `Deal` 表（含 fee/fill_id，models.py:292-321）與 `Trade.source`（manual/shioaji）都在——006 成交頁、008 來源規則零 schema 變更。
- `Order`/`BrokerPosition` 無契約月欄位——共同前置 #2 屬實，F2/F6 硬前置。
- `check_place` 日筆數/日口數配額不分 octype（risk.py:188-198）——T2「保護性平倉被自家風控擋」屬實，需修。
- SSE 只有無 payload 的 `orders-changed`（orders.py:478-492）——007 需新增帶內容的 user-scoped 事件。
- `unrealized_pnl` 純函式在（pnl.py:24）；`_POINT_VALUES` 缺 TMF（position_tracker.py:39）。
- 績效頁已有 mode/symbol/tag/date 篩選（stats.py:47-51）——008 只差輪次與來源規則。
- sim 保證金/權益/輪次完全不存在；`templating.py` 無 symbol_label（num/dt 等 filter 齊全）。

---

## §2 實作批次（依 README 建議順序微調）

> 基線：本分支 1443 測試綠。每批完成必須全綠＋（涉及 UI 的）staging sim 實測後才進下一批。
> `2026-08-28/` 資料夾目前 **untracked**，第一批開工前先 commit 進 repo（需求文件要有版本）。

### 批次 1 — UI 第一輪：001–005（彼此獨立、後端風險近零）

| 項 | 工作 | 後端變更 |
|---|---|---|
| 001 | `base.html` 設 `data-scheme`（全站化）；方向改 radio+fieldset 分段開關（name/value 不變）；`.dir`、`.pnl` 改 `--rise/--fall`（損益隨偏好，2026-08-28 改訂）；保留 chart.js:453 即時切換路徑 | 無 |
| 002 | `/quote`、`/quote/stream` 加 symbol 參數（先「收下並驗證」，未知 symbol 回「無報價」）；下單頁 `.quote-compact` sticky 報價列，B 版：商品選擇器併入報價列（`form="order-form"` 關聯）；market-banner 帶入 | quote 端點 symbol-scoped |
| 003 | `templating.py` 加 `SYMBOL_LABELS`＋`symbol_label` filter；報價列顯示「台指近」（不含契約月，使用者定案）；下拉用 `<optgroup>`；儀表板標題後綴隨 session 變（全/日/夜） | 無 |
| 004 | chart.js sessions 顯示字串：全日盤/日盤/夜盤（code 不變） | 無 |
| 005 | 新增 `/risk` 風險控管頁＋主導覽入口；kill switch 控制項搬入並更名「緊急停止下單」（**維持 admin-only**，非 admin 只見狀態——依 §1-2 調整文件原案）；**冷靜期控制一併搬入 /risk**（自我約束系集中）；下單頁：停止時紅色狀態橫幅＋送出停用，正常時零占位 | 新 GET /risk（沿用既有依賴） |

驗收：各項文件內驗收條件逐條打勾＋pytest 全綠＋staging 目視。

### 批次 2 — 006＋007＋009 一起上線（頁面重構＋回饋橫幅＋橘標）

README 明訂 006 與 007 不可拆（委託表移走後回饋全靠橫幅）。009 純呈現層順帶。

1. **007 後端（本批唯一的實質後端工作）**：`/orders/stream` 新增帶 payload 的事件型別
   （委託回報／逐筆成交回報，源頭 RawInboxWorker 落地時；**user-scoped 不廣播**；一筆 Deal 一事件＝滑價一價一橫幅天然成立）。
2. **007 前端**：banner-stack（右上堆疊、成功中性/失敗 `--down` 實心＋圖示/成交隨方向 `--rise/--fall`）、
   aria-live、reduced-motion 降級、停留時長失敗>成功；改單/取消成敗走同一套（收掉 P0-4/5/6）。
3. **007 sim 確認視窗**：預設跳、可勾「不再顯示」存 `User` 欄位（跨裝置）；real 兩階段確認不受影響；預留 F5 鎖定。
4. **006 頁面重構**：主導覽「交易」大類——下單（精簡）/委託/成交/未平倉。
   - 下單頁只留：報價列、表單、position strip（symbol-scoped；無倉整段不顯示；sim 保證金「—」占位）、agent 狀態、緊急停止橫幅。
   - 委託頁承接全部既有功能＋**順修 P0-1（mode tab）、P1-8（補時間/類型欄）、P1-9（狀態中文化）、P1-10（num filter/市價）、P1-11（table-wrap）**。
   - 成交頁：讀 `Deal` 表（資料已在，零 schema 變更）。
   - 未平倉頁：部位表＋浮損欄（重用 `unrealized_pnl`，勿另寫數學）。
5. **009**：sim 執行模式橘黃「模擬單」徽章（`#f0b90b` 系）、real 無標；real 確認框補完整委託內容＋明示「正式單」（併 P1-15）；sim 確認視窗明示「模擬單」。

### 批次 3 — 008 已平倉頁＋交易績效頁（工作量小）

- 已平倉頁（新）：筆數＋區間總損益＋逐筆明細；預設今日（trading_day 含夜盤跨日）；快捷區間；mode/商品篩選；來源標記＋連交易日記。
- 交易績效頁：既有 stats 搬進「交易」大類＋更名；篩選已存在，只補**來源規則**（預設只計 `source="shioaji"`，可切含手動）。
- **輪次篩選延後到批次 4**（輪次模型屬 sim 資金帳，先做頁面骨架、輪次選單留插槽）。
- 順手：委託頁或 /risk 頁顯示 quarantine 計數（「偵測到 N 筆非本系統委託的回報」，inbox_worker.py:182-194 已隔離）。

### 批次 4 — sim 虛擬保證金（D1–D5，分兩期）

- **第一期（顯示層）**：新增輪次表 `(user, started_at, initial_capital, ended_at?)`＋每口保證金設定值（含 TMF 點值補進 `_POINT_VALUES`）；
  首次強制設定初始資金（50/100/300 萬/自訂）；帳戶設定頁「模擬帳戶」區（餘額/權益/占用/可動用/重置）；
  006 精簡條 sim 占位換真數字；重置需 clean slate（**與 F1 delta 的 clean-slate 判定共用同一實作**，D4 明訂）；戰績按輪篩選（回填批次 3 的插槽）。
  帳本設計照規格：不存跑動餘額、一切衍生計算；無入出金（D3）；爆倉不強平（D5）。
- **第二期（檢核層）**：開倉檢核掛 RiskGuard 鏈（僅 sim）；平倉不檢核；Auto 從嚴當開倉；拒單訊息含缺口金額；可動用 ≤0 時下單頁預先顯示。
- DB 變更注意：雙方言可攜（named params、dialect-aware upsert，照 candles/repo.py 慣例）。

### 批次 5 — 風控功能（依 risk 文件順序，但 F0 需先過 §1-2 的產品決策）

1. **全域原則先落地（小改、高價值）**：`check_place` 改為 octype-aware——
   平倉（Cover 且口數 ≤ 該方向持倉）不佔日筆數/日口數配額、不受緊急停止/冷靜期攔截；
   同時加「平倉口數 ≤ 持倉」驗證（防假 Cover）。這是 F0/F6 共用地基，README 點名回饋項。
2. **F0**：依 §1-2 拆案後實作 per-user 自我停止（只擋開倉、只擋 real、隨時啟停）＋
   啟動彈窗揭露開倉掛單＋一鍵批次撤銷（結果逐筆回報：成功/失敗/已成交/狀態不明，R1–R5 全數落規格）＋
   解除意識化摩擦（顯示啟動時長/原因/次數，無時間鎖）＋稽核軌跡＋1 小時 ≥3 次建議升級冷靜期。
3. **F3 損益提醒**：規則引擎（已實現＋未實現合計、trading_day 界定）＋去抖動＋訊息拆列＋SSE toast＋Telegram 雙發；
   觸發後帶「一鍵進入冷靜期」（有部位時改帶 F0/部位處理入口）。
4. **F1 delta**：clean-slate 啟動條件＋啟動彈窗揭露＋5 分鐘反悔窗＋固定時長選項＋期滿通知（畫面＋TG）。
5. **F5 操作摩擦**：偏好開關化（sim 二次確認鎖定、停用市價單、首筆風險提示、停損單向鎖定——後者依賴 F6，先做前三）。
6. **契約月欄位（共同前置 #2，正式交付）**：`Order`/`BrokerPosition` 加契約月並納入部位 scope；
   migration 雙方言可攜；這是 F2/F6 的硬前置，**每次換月都會踩**，不等多商品。
7. **F4**：等 Shioaji margin 查證回饋後定欄位/頻率/降級（見 §3）；sim 分母已由批次 4 補齊，可重新評估。
8. **F6 條件單／F2 一鍵清倉**：另開規格（引擎選型查證後）；F2 最後、且必須在契約月之後。

---

## §3 需工程查證／回饋測試端的清單

| 項 | 內容 | 怎麼做 |
|---|---|---|
| 回饋 1 | **F0 與 admin-only 的矛盾**（§1-2）：建議拆「admin 全域緊急停止」＋「per-user 自我停止」兩功能 | 回饋測試端＋使用者拍板後才動 F0 |
| 回饋 2 | **冷靜期已存在**，F1 請按 §1-1 的 delta 清單修訂文件 | 回饋測試端 |
| 查證 1 | Shioaji margin API：欄位（總保證金/權益數/可用保證金）、頻率、離線降級（F4＋006 real 保證金共用） | scripts/shioaji_api_test.py 延伸實測 |
| 查證 2 | Shioaji 是否推送場外委託回報到本 API session（quarantine 提示實用性） | 同上，手機 App 下一單實測 |
| 查證 3 | Shioaji 期貨條件單能力（F6 引擎選型，已授權工程決策；T1 可靠性揭露為不可退讓驗收） | API 文件＋實測 |
| 查證 4 | KLineCharts v9.8.12 overlay 拖曳能力（F6 T6） | 本機 spike |

---

## §4 執行注意

- **分支策略**：本分支 `feat/order-report-latency` 尚未 push、回報延遲三修待 staging 實測；冷靜期（1339032）同樣待實測。
  建議：先把現分支 push 備份＋完成待驗收實測，再從其上開新 feature branch 逐批實作（批次 1 一個 branch、批次 2 一個…），避免又堆一座未驗收的山。
- 每批完成：pytest 全綠 → staging（`.env.staging` 流程）sim 實測 → 測試端確認驗收條件 → 才進下一批。
- 「本機參考實作」僅供效果驗證，不照抄；行號以最新碼為準（本計畫行號已於 2026-08-31 重新查證）。
- 所有 DB 變更雙方言可攜；`Candle.ts` BigInteger 等既有約束不回退。
