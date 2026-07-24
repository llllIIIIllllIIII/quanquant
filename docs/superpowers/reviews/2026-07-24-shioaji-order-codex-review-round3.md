# Shioaji 下單整合 — Codex 覆核 Round 3（delta）發現與待修清單

**日期**：2026-07-24
**審查者**：codex（gpt-5.6 被 ChatGPT 帳號拒用，fallback 預設最強）
**對象**：spec v3（277 行，含「修訂 v3」節）+ plan v3（5321 行/10 Task），對照 round2 清單
**總判定**：**REVISE，CONFIDENCE 99%**（架構已收斂，剩局部正確性 + 少數新 bug）

## 收斂進度（round2 → round3）
- **BLOCKER**：#3 RESOLVED（複合 scope 鍵，不再寫錯 user）；#1 PARTIAL（route real update token 仍不一致）；#2 NOT-RESOLVED（callback→DB commit 間仍有 volatile crash window）；#4 NOT-RESOLVED（quota release 非原子/無 reservation 身分）。
- **HIGH RESOLVED**：#5 canonical hash 九欄、#7 client_order_id 穩定、#8 Fill 嚴格驗證、#9 fill_id 只認真 deal_id、#10 lot ledger 累計不回推、#13 send-gate 線性化點、#14（窄義）序列化通道、#15 fee 拆分守恆、D5 Deal.broker_order_id。
- **MEDIUM RESOLVED**：#19 Task10 同步修 Task8 preflight 測試。

## 仍 OPEN（續跑要修）

### 擋真錢的核心殘留
1. **#1 real update token 在 route 仍鎖死**（P3:3702-3718,4922-4929）：route 簽 update token 用固定 `fake_req`、未合併既有 Order 的未改欄位；只改 qty 時 route 用 price=1、adapter 用既有價 → hash 必不符。**修**：route 先以複合 scope 讀既有 Order，合併新舊欄位後呼叫同一 canonical helper；補 route→真 RiskGuard→adapter 完整 real update 測試。
2. **#2 callback 非真 durable**（P3:3382-3401,4394-4405）：只 `call_soon_threadsafe`+`ensure_future`；loop 未就緒/排程後 crash/等 supervisor lock 時 payload 仍會遺失；watchdog 只 `list_trades()` snapshot、無持久 cursor、把所有列當 deal_report。**修**：callback thread 返回前完成獨立 Session 的 raw spool commit（或同步 disk spool）；reconcile 用持久 cursor/watermark、分辨委託/成交、週期補洞、重啟續接。
3. **#4 quota 未閉環**（P3:1777-1827,3325-3357,4394-4405）：正向 CAS 有，但 update 在鎖外算 delta；`release_quota` 非原子 read-modify-write、無 per-order 身分；update send-gate/native 失敗無 release/unknown 收尾；reconcile 不處理 reservation。**修**：per-order/per-update reservation 列 + 狀態機（reserved/confirmed/released）、`UPDATE...WHERE state='reserved'` 一次性 confirm/release、unknown 由 reconcile 決議、網路呼叫期間不持長交易。

### HIGH（新發現或殘留）
4. **real 缺 fee 被靜默記 0**（P3:3044-3050,2186-2190）：`fill.fee or Decimal(0)` → 正式 PnL 永久低估成本。**危險**。**修**：real fee 缺值 quarantine/reconcile 或存 `fee_unknown` 禁 finalize，不得 zero-coerce。
5. **canonical Decimal 正規化自相矛盾**（P3:602-606,751-773）：`format(Decimal("18000.00"),"f")` 仍是 `"18000.00"`≠`"18000"`，計畫自己的 formatting-noise 測試會失敗；同義 payload 可能因格式差異得不同 hash/token。**修**：明確 canonicalization（零值處理 + `normalize()` + 固定非科學記號 + 固定 schema canonical JSON）。
6. **#11 connect/close 未納序列化通道**（P3:3142-3165,3299-3303）：place/update/cancel 拿 supervisor lock，但 connect/close 直接 to_thread；watchdog reconnect 可與 place 交錯替換 `_api`。**修**：封裝 supervisor command API，connect/reconnect/close/native mutation 全走同一 command executor。
7. **#12 Order 狀態非單調**（P3:2740-2805）：晚到/重播的 `Submitted` 可把 `filled` 回退、`cancelled` 覆蓋成交。**修**：定義狀態偏序 + 條件 UPDATE/CAS 單調 + 亂序測試。
8. **#6 existing lookup 命中只驗 request_hash、未先驗 owner/user_id**（P3:3200-3205）：非 owner 猜到 client id+payload 可在授權前拿到他人 OrderAck。**修**：existing 分支先 owner + `existing.user_id==actor` 驗證。
9. **callback 雙鍵不交叉驗證**（P3:2743-2757）：同時帶 ordno 與 broker_order_id 時只查 ordno，不驗第二鍵指向同一 Order。**修**：兩鍵各自 scoped lookup 確認同一 order，矛盾即 quarantine。
10. **watchdog health 只看 `_api is not None`**（P3:4340-4356）：底層連線死但 object 還在則永不重連。**修**：序列化 broker health probe，失敗標 unhealthy→停 mutation→重連→reconcile 才 ready。
11. **F2 update order-count 未限、broker failure 無 reservation 收尾**（P3:3956-4009）。

### MEDIUM
12. **#17 shutdown 無真正 ingress sentinel**（P3:2685-2693,4481-4492）：timeout 只記 log、不終止已起的 to_thread、不保持 unhealthy。**修**：原子關 ingress→解 callback→等所有 ingest tasks→drain worker，用可 join 的 executor，timeout 回失敗標 unhealthy。
13. **C2 mode CHECK 對 NULL 無效 + fresh DB Trade 無 CHECK**（P3:114-120）：nullable ALTER 的 `CHECK(mode IN(...))` 對 NULL 為 UNKNOWN；fresh DB `create_all` 後 ensure_columns 跳過 ALTER → Trade 無 CHECK。**修**：`Trade.__table_args__` 加 CheckConstraint；ALTER 改 `CHECK(mode IS NOT NULL AND mode IN(...))`；補 fresh + migrated 兩套測試。
14. **#16 confirm token 無清理 + TTL 線性化點模糊**（P3:1744-1772）：無 expired/consumed cleanup（無界成長移到 DB）；claim 後排隊等 supervisor 可超 TTL。**修**：定期清理 + TTL 線性化點定義（若「送單時有效」則 claim 移入 supervisor native call 前）。
15. **BrokerPosition 無 active-scope 唯一性/數量 invariant CHECK**（P3:1280-1305）：重播/競態可造成兩個同 scope+direction open row，`.first()` 任選致帳本分裂。**修**：active-position 唯一鍵 + `total_opened_qty>0`、`0<=closed_qty<=total_opened_qty` CHECK + version/CAS。
16. **#18/測試敏感度**（P3:989,1158,4739-4740）：有 no-op `pass` 測試與 `assert ... or True` 恆真 assertion（且 Self-Review 誤稱無 placeholder）；supervisor 測試只測 lock、callback 測試用 sleep happy path、real update 只直測 RiskGuard 未穿 route。**修**：刪恆真/no-op；barrier/fault injection 跑實 callback/worker/watchdog/adapter + 完整 route real-update。
17. **#9 UI 缺 update control**（P3:4988-5005）：只有取消鈕、無改價/改量表單觸發 `PUT /orders/{id}`。**修**：補 update modal 帶現值 + real 確認 round-trip。
18. **#20/PG smoke 只 assert `CREATE TABLE`**（P3:5141-5151）：未驗 BigInteger/CHECK/UniqueConstraint/DEFAULT；`Field(default=)` 是 Python default 未必產 DB DEFAULT。**修**：逐項斷言編譯 DDL + 必要欄位 `server_default` + 真 PG schema smoke。
19. A6 缺「多筆部分成交都缺 fee」端到端回歸；#15 缺完整 reversal round-trip 守恆測試。

### 無法從純計畫驗證
- 「有無改弱既有測試」需實作後以 commit diff + `uv run pytest` 確認；目前只知新測試有 no-op/恆真項。plan 明載未執行（P3:5319-5320）。

## 主對話裁決
全部接受。殘留已從「架構級」收斂到「局部正確性 + 少數新 bug」；架構形狀（序列化 supervisor / lot ledger / durable inbox / canonical hash / CAS）codex 已認可。核心續修：(1) route real-update token 合併；(2) callback 真同步落地 + 持久 reconcile cursor；(3) quota reservation 狀態機（per-order id、一次性、unknown reconcile）；(4) real 缺 fee 不得記 0；(5) Decimal canonicalization；(6) connect/close 納通道 + health probe + 單調狀態；(7) 清恆真測試。多屬實作期 TDD 自然會抓的細節。
