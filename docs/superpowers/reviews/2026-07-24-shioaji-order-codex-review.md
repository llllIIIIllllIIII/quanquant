# Shioaji 下單整合 — Codex 外部審查發現與裁決（改寫清單）

**日期**：2026-07-24
**審查者**：codex 外部 GPT（GPT-5 / Codex；指定 gpt-5.6 被 ChatGPT 帳號拒用，改用預設最強）
**對象**：`specs/2026-07-23-shioaji-order-integration-design.md` + `plans/2026-07-23-shioaji-order-integration.md`
**總判定**：**REVISE，CONFIDENCE 99%**
**主對話裁決**：以下發現**全部接受**；僅兩點做範圍化處理（見末段）。本檔為 spec/plan 全面改寫的權威輸入。

---

## A. 部位配對正確性（Critical）
- **A1** `find_open_trade` 會誤抓同 user/symbol/mode 的**手動日誌**（Trade 無 source/broker scope）→ 自動 New 併入手動列、Cover 關掉手動交易。**解**：自動交易與手動日誌隔離——新增持久化 `broker_position`（或 lot）表（user_id, broker, account, symbol, mode, direction, open_qty, avg_price, closed_qty, exit_notional, status），自動成交只動此表；round-trip 完成才寫對應 `Trade`（journal 顯示用）並標來源（Trade 加 `source`：手動預設 "manual"、broker 自動 "shioaji"）；`find_open_trade` 僅限 broker 來源。
- **A2** 分批平倉進度只在記憶體 `_partial`，重啟即遺失、交易不結案。**解**：closed_qty/exit 均價/剩餘口數**持久化**（落 DB 或由 Deal 重建），支援跨重啟續平。
- **A3** `acc_qty >= size` 時整筆超額 Cover 都納入出場均價再丟棄 excess，口數憑空消失。**解**：每次只消耗 `min(fill.qty, remaining)`；超額依 octype 明確拒絕/隔離/反向開倉，部位守恆。
- **A4** New 找不分 direction 的最舊 open → 雙向並存時誤配、另建第三列；Auto 只看該列可能誤判開平。**解**：New 以目標 direction 查找；Auto 依券商部位或完整雙向集合判斷，歧義 **fail closed**。
- **A5** 缺對應開倉的 Cover 已寫 Deal、只記 warning、無 pending/reconcile，重播被 unique 擋 → 永不自癒。**解**：未配對 fill 保留 `processed=false` 進 reconcile/quarantine 重試。
- **A6**（Medium）Spec 要 sim 用設定估算 fee，但 plan 的 `order_sim_fee` 從未傳入，callback 無 fee 時以零處理。**解**：sim session 伺服器填設定 fee，明確定義按 fill/order/口計費並驗證部分成交累計。

## B. 冪等去重（Critical）
- **B1** `record_deal()` 先 commit、Trade 更新另一交易、`processed` 從未設 true → commit 後 crash 就永久遺失。**解**：Deal insert + 部位聚合 + Trade 更新 + `processed=true` **同一交易**（或 durable inbox worker 持續重試 `processed=false`）。
- **B2** 同一 `seqno` 同時當委託 context key 與逐 fill 去重鍵 → 部分成交共用委託 seqno 時第二筆合法 fill 被當重播，或成交 seqno 每筆不同又查不到 context。**解**：**委託關聯鍵與 fill 去重鍵分離**（fill/deal id 去重；order id/ordno 關聯）。
- **B3** 未證明 seqno 跨 sim/real/帳戶/交易日/重連永久唯一。**解**：唯一識別含 environment/account/trading-day/fill-id，做跨日/重連/sim-real 測試。
- **B4** POST 下單無 client idempotency key → ack/DB/HTTP 失敗時使用者重送會再送真單。**解**：唯一 client order id / request hash + pending→sending→submitted 狀態機；ambiguous 先向券商 reconcile，禁止盲送。
- **B5**（Medium）catch-all `IntegrityError` 一律當重播回 None，吞掉 FK/NULL。**解**：只在確認命中指定 unique constraint 才當 duplicate，其餘記錄並拋出。

## C. 模擬／正式分流（Critical）
- **C1** 券商 session mode 由 adapter `_mode` 決定，但 Order/Fill/Trade mode 信任**可竄改的表單值** → real session 送 `mode=sim` 用真 API 送單、跳過 real 確認、卻標 sim 灌模擬績效。**解**：下單 mode **只取 server-side session `_mode`**；移除表單對執行 mode 的控制；adapter 拒絕不一致 request；Order/Fill/Trade 一律以 `_mode` 覆蓋。
- **C2**（Medium）無 `real|sim` enum/驗證/DB check → 第三種 mode 可寫入成隱形資料。**解**：settings/domain schema/DB 三處限制 `Literal["sim","real"]`，非法拒絕。

## D. user_id 綁定與授權（Critical/High）
- **D1** 單一 Shioaji 真實帳戶掛 app singleton，orders router 只要一般登入 → 第二個帳號可用同組券商憑證下真單（user_id 只改日誌歸屬）。**解**：server-side owner allowlist/角色；place/cancel/update/positions 服務層再授權，第二位 user 一律 403。
- **D2** user/mode context 僅記憶體，重啟/重連 replay 查不到就 drop。**解**：order→user/mode 關聯持久化、啟動重建；未知回報進 durable quarantine 不 drop。
- **D3** cancel route 注入 user 卻沒用、service 無 user_id → 任一登入者可取消別人委託。**解**：cancel/update 傳 actor user_id，先以 `(user_id, broker, mode, broker_order_id)` 驗證所有權。
- **D4** `positions(user_id)` 忽略 user_id → 所有登入者看到共用帳戶部位。**解**：限 configured owner 讀取。
- **D5** Deal 不存 user_id/mode，靠 nullable order_id，正常 fixture 無 id → 建 `order_id=None` 的 Deal，無法對帳/audit。**解**：Deal 持久化 user_id/mode/account/broker_order_id/fill_id；無法關聯者隔離。

## E. 跨執行緒並發與生命週期（Critical/High）
- **E1** broker 可在 `_place_blocking()` return 前從背景執行緒發成交，主 loop 還沒寫 `_context` → callback 直接 drop（快速成交即觸發）。**解**：送單前建 durable client correlation/pending context；callback 先寫 durable inbox 再延遲解析；加 barrier 測試重現 callback-before-ack。
- **E2** lifespan 只 `create_task(connect())` 未等就緒即公開 service → HTTP 可先 place 因 `_api is None` 失敗；connect 例外只留 detached task。**解**：連線成功才 publish service（readiness gate）；登入失敗 fail closed 反映 health。
- **E3** Spec 承諾 watchdog 重連，plan 只有一次 connect、無 callback 停止/logout/drain/loop 關閉。**解**：補 reconnect/backoff/login 節流 + 重連後對帳 + shutdown（先停 callback→drain inbox→關 loop），作為明確 Task。
- **E4**（Medium）同步 `handle_fill` 排到 event loop、含多次同步 commit → 阻塞 HTTP 與後續 callback；handler 例外無 supervised retry。**解**：`call_soon_threadsafe` 只 enqueue，單一有序 + backpressure 的 worker 處理，每筆 worker 內開新 Session。

## F. 風控與安全（Critical/High）
- **F1** RiskGuard 有 `confirmed` 但 adapter 永不傳、無確認流程 → 正常 real 單永遠被擋；反可用 mode 竄改走 sim 繞過。**解**：server-side 兩階段確認（短效、一次性、綁 user + order payload hash 的 token）；adapter 依真實 session mode 強制驗證。
- **F2** RiskGuard 只在 place，`update(qty=...)` 無口數/日限/白名單/real 再確認 → 先下小單再增量超限。**解**：update 重跑所有風控，以變更後總量原子保留 quota，real 重大變更重新確認。
- **F3** qty=0/負、price≤0、非法枚舉都可進 API；負 qty 降低 daily sum；callback 非預期字串被當 Auto/short。**解**：domain boundary 嚴格 enum + 數值驗證，RiskGuard fail closed，測零/負/非法枚舉。
- **F4**（Medium）kill switch 是啟動快照。**解**：改可即時讀取的 server-side switch，緊貼券商呼叫前再檢查（取消單仍允許）。
- **F5**（Medium）Spec 要風控攔截寫 audit，plan 只 raise、不建 Order，Self-Review 卻宣稱有。**解**：append-only audit（actor/mode/payload hash/規則/結果/時間，不含秘密）。
- **F6**（Medium）`order_mode` 任意字串，拼錯非 sim/real → 建 `simulation=False` 卻跳過 CA。**解**：`Literal["sim","real"]`，未知拒絕啟動；real 需完整 CA/readiness preflight。
- **F7**（Medium）quota 檢查與建單分不同交易、無 lock。**解**：quota reservation 與建單同一交易；**本專案明訂單一 uvicorn worker**，以「明訂並在測試驗證 single-worker invariant」滿足（不強加 DB lock），須在 spec/plan 明寫此前提。
- **F8**（Medium）只宣告 .pfx/.env 不進 git。**解**：補部署 task——read-only mount、0600 owner、gitignore、secret scan、log redaction、啟動前權限檢查。

## G. 測試品質（High）
為每個失敗模式補「會真正抓到 bug」的測試：跨重啟分批平倉、超額 Cover、Auto reversal、雙向 open、手動與 broker 列並存、回報亂序、callback-before-ack（真 asyncio loop + worker thread + barrier）、Deal commit 後 handler failure/replay、兩 Session 競態、real-session+sim-form 被拒、確認 token、kill switch、跨 user cancel/positions 403、零/負/非法枚舉。首次 sim E2E 後把去識別 payload 固化成 regression fixture。**不得改弱既有測試**（實作審查以 commit diff 確認 assertion 數量/語意未削弱）。

## H. spec↔plan 一致性（High/Medium）
watchdog（補實作或移除 Self-Review ✅）、mode 資料流改 server-side、手動新增日誌帶當前 tab mode（modal+表單+create route）、update_order 路由/UI（本期補上具所有權+風控+確認）、migration DDL 用實際表名 `trades` + `DEFAULT 'real'`、Task6→Task7 RiskGuard 相依（前移或明確 `risk_guard=None` 回填 step）。

---

## 範圍化處理（未照單全收的兩點，附理由）
1. **多 worker 原子性**（F7）：本專案部署明訂單一 uvicorn worker（Dockerfile 無 `--workers`）。以「明訂 single-worker invariant + 測試驗證」滿足，不強加 DB row lock。若未來上多 worker，需回頭補 lock/reservation——記入 spec「未來」。
2. **真正的多帳戶部位 scoping**（D1/D4 的多帳戶延伸）：屬 Phase 2。本期只做**單一券商帳戶 + owner allowlist**（擋掉第二個 app 使用者），不實作多帳戶 per-account scoping。
