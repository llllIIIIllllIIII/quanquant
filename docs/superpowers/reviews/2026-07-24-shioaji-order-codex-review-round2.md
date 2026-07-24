# Shioaji 下單整合 — Codex 覆核 Round 2（delta）發現與待修清單

**日期**：2026-07-24
**審查者**：codex GPT-5（Codex）
**對象**：修訂後 `specs/…-design.md`（233 行）+ `plans/…-shioaji-order-integration.md`（4683 行），對照 round1 `reviews/…-codex-review.md`
**總判定**：**REVISE，CONFIDENCE 99%**（方向大幅改善，但仍有數個足以擋真錢上線的缺陷）

## 回歸核對（round1 A–H）
- **RESOLVED**：A1 A2 A3 A4 A5 B1 B5 C1 E2 F5 F6（手動 Trade 隔離、持久化部位、min(qty,remaining)、方向配對、quarantine、同交易 fill effect、精確 IntegrityError、OrderRequest 無 mode、readiness gate、audit、ORDER_MODE Literal 拒啟）。
- **PARTIAL**：A6 B2 B3 B4 C2 D1 D2 D3 D4 D5 E1 E3 E4 F1 F2 F3 F4 F8 G H。
- **NOT-RESOLVED**：F7（quota check 與寫入仍分交易；「單 uvicorn worker」不等於跨 thread/await 的 DB 交易被序列化）。

## Round 2 新發現（20 條）

### BLOCKER（擋真錢上線）
1. **正式改單永遠鎖死**（plan 3150,4164-4171）：update route 簽 `broker_order_id`、RiskGuard 驗 `client_order_id`，hash 不符 → real update 必失敗。**修**：單一 canonical update payload helper，簽發與驗證共用；真 RiskGuard 做 real update round-trip 測試。
2. **durable inbox 前有 volatile queue 可丟成交**（plan 2061-2086,3630-3659）：QueueFull/worker exception/callback 後 crash 可永久漏 fill，watchdog 又不拉券商資料。**修**：callback 先寫 durable raw-inbox/spool 再排 worker；watchdog 做 broker cursor reconciliation。
3. **委託關聯未 scope → 成交可能寫錯 user**（plan 1092-1097,2107-2113）：`ordno`/`broker_order_id` 用裸單鍵 `.first()`。**修**：以 `(broker,account,mode,ordno/broker_order_id[,trading_day])` 查詢 + 索引/唯一 + 衝突 fixture。
4. **兩個並行 update 可突破日限**（plan 2601-2627,3054-3057,3167-3176）：兩請求在 broker await 前各讀舊 quota 皆通過。**修**：持久化 reservation/CAS；broker 失敗/unknown 由 reconcile 釋放；勿跨網路持有長 DB transaction。

### HIGH
5. **place token 未綁全部可執行欄位**（plan 3109-3111,4111-4116）：hash 未含 `price_type/order_type` → 確認 LMT/ROD 後可篡改 MKT/IOC/FOK。canonical hash 含所有執行欄位+mode/account，逐欄 mutation test。
6. **冪等重送先燒掉 token 無法回既有 Order**（plan 2516-2525,2837-2851）：`check_place` 在查 existing 前消費 token。先查 existing、驗 actor/mode/request hash，再決定是否消費 token。
7. **client idempotency key 對一般 HTTP retry 不穩**（plan 4032-4044）：無 hidden client id 的表單每次 POST 生新 UUID。首次渲染即生並存 key；Order 存 request hash，同 key 不同 payload 拒絕。
8. **非法 callback 被當合法空單/Auto/short**（plan 601-621,2685-2699,3394-3418）：Fill 只驗 mode，mapper 缺值用空字串/0/Auto。raw callback 先 durable quarantine；Fill 驗 action/octype/qty/price/fill_id/ts/account，不猜測。
9. **`seqno` fallback 重新合併 fill key 與 order key**（plan 2687-2688,3406-3407）：部分成交共用 seqno 時第二筆被 dedup。只收真實 deal id；缺則 quarantine，不用 ordno/seqno 冒充。
10. **部分平倉後加碼改寫歷史入口成本 → PnL 錯**（plan 1693-1759）：開2@100、平1、再開1@200 → 用剩餘算 avg=150 卻以 size=3/entry=150 結算。**持久化 `total_opened_qty/entry_notional` 或改 lot ledger**。
11. **單一 Shioaji client 無鎖被多 to_thread + watchdog 同時操作**（plan 2480-2503…）：place/update/cancel/positions/close/connect 無共同 session lock，重連與送單競態。單一 broker-operation supervisor；重連取同鎖、停 mutation、reconcile 完才 ready。
12. **Order 成交狀態永不由 fill 更新**（plan 2671-2672,2105-2134）：FuturesOrder callback 被忽略、fill transaction 不更新 `filled_qty/avg_fill_price/status` → UI 持續顯示 submitted。同 fill transaction 單調更新 Order aggregate。
13. **kill switch 送單 TOCTOU**（plan 3112-3136）：風控通過後、native 呼叫前可被打開。所有 broker mutation 共用 send gate，線性化點最後檢查。
14. **單-worker 不變量在背景 thread 下不成立**（plan 2079,2142-2178,3656-3658）：live worker 在 to_thread 寫部位，watchdog 同時跑 `retry_quarantined`，兩路改同一 BrokerPosition。所有 fill/retry 走同一序列化 command worker，或加 DB/CAS 防線。

### MEDIUM
15. 超額 Cover 的 fee 被計兩次（plan 1712-1738）：依 consumed/excess 比例拆 fee，斷言合計等於原 fill fee。
16. token nonce 集合無界成長且過早消費（plan 2811-2856）：TTL 清理的 DB/JTI row，最後可失敗檢查後才原子 claim。
17. shutdown timeout 後仍可能有 DB thread 背景執行（plan 2074-2096,3711-3727）：stop/sentinel + 停 ingress + 等 worker thread；超時資料 spool 並保持 unhealthy。
18. TDD 敏感度不足甚至把錯誤行為定為正確（plan 1975-2020…）：QueueFull 測試期待丟 fill、callback 測試沒強制先後、confirmation 用不驗 token 的 fake。改零遺失、強制 barrier、真 RiskGuard+adapter。
19. Task 10 使 Task 8 preflight 測試失敗（plan 3294-3300,4542-4568）：Task 8 用不存在 `/x.pfx` 期待 enabled，Task 10 後要求檔存在且 0600。Task 10 同步改該測試用 `tmp_path` 真檔。
20. （LOW/hypothesis）Postgres 可攜性只有宣稱沒驗證：加 PG dialect `CreateTable` compile smoke + 部署前 PG schema smoke。

## 待補的 PARTIAL 重點（round1 未竟）
- A6 sim fee 多筆部分成交累計 + 超額 Cover 重複計 fee 未測。
- C2 既有 `Trade` 與 `OrderAudit.mode` 無 DB CHECK。
- D5 Deal 仍缺 `broker_order_id`。
- F2 update 缺白名單/次數限制、quota 分交易、real token 目前不可成功。
- F8 未驗檔案 owner UID；例外原文可能把上游秘密帶進 log。

## 主對話裁決
全部接受。核心是三塊架構級補強：**(1) 部位帳務改 lot ledger（或持久化 total_opened_qty/entry_notional）修 PnL；(2) callback→durable raw-inbox→序列化 command worker（唯一 broker-operation supervisor + send gate），修丟單/競態/TOCTOU/單-worker 假設；(3) canonical payload hash + 先查 existing 再 claim token + CAS quota reservation，修改單鎖死/token 篡改/並行超限。** 這是實質再架構，非小修。
