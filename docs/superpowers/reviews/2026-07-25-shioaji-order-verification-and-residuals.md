# Shioaji 下單子系統 — 獨立驗收結論 + 真錢上線前殘留

**日期**：2026-07-25
**驗收者**：fresh-context 獨立 agent（opus，找碴模式，未參與實作）
**標的**：branch `feat/shioaji-order-integration`，10 個實作 commit（`f6a6110`..`2201872`）
**總判定**：**PASS-WITH-WARNINGS，CONFIDENCE 88%**
**測試**：`536 passed`（exit 0，獨立重跑一致）；baseline 271 未被弱化（既有測試檔 diff 只增不刪 assertion）。

## 已獨立確認（round3 待修清單關鍵項全 PASS）
real update 不鎖死（route 合併既有 Order + 同一 canonical hash + 真 adapter/RiskGuard round-trip）、durable raw-inbox 零丟單（callback 返回前同步 commit）、quota 原子（reservation 狀態機 + 真多執行緒不超賣）、mode 僅 server-side（OrderRequest 無 mode 欄位）、owner 授權 403（含跨 owner）、real 缺 fee fail-closed 不記 0、canonical Decimal 格式無關同 hash、Order 狀態單調、秘密不進 log/`/healthz`。

## 測試敏感度抽查（驗收者故意改壞 → 對應測試確實 FAIL，已全數還原）
quota 上限放大／canonical hash 拿掉 octype／owner 檢查短路 True／real 缺 fee 不 raise／狀態偏序 return True —— 五者對應測試皆 FAIL，證明測試非形同虛設。無 `assert ... or True`／no-op／無 assert 之虛設測試殘留。

## ⚠️ 真錢上線前應收的殘留（皆 LOW、方向保守、不危及真錢；simtrade 開發不受影響）

1. **update 路徑的 quota unknown 未閉環**（`broker/watchdog.py::_reconcile_unknown_quota_blocking`、`broker/shioaji_adapter.py::update` except 分支）：real 加量改單若 native 逾時（unknown），該 delta `QuotaReservation` 永遠停 `reserved`；watchdog 的 unknown 對帳只釋放「無任何券商 id」的委託，update 因委託早有 broker_order_id 被跳過，order_report pipeline 又只更新 status 不碰 quota。**淨效果保守**（高估用量→**不會超賣/突破日限**），但當日配額會被逾時改單靜默侵蝕。`test_update_marks_order_unknown_without_touching_reservation` 目前把「永遠 reserved」固化為預期。**修法**：watchdog 對 update-unknown 依券商真實狀態 confirm/release 該 reservation（一次性）。

2. **callback 雙鍵交叉驗證只做一半**（`broker/inbox_worker.py::_process_order_report`）：deal_report 路徑**有**（兩鍵指不同 Order → quarantine，fail-closed）；order_report 路徑**沒有**（ordno 命中即用、broker_order_id 只 fallback，不驗第二鍵）。風險低（僅改 status、有單調守衛）。**修法**：order_report 亦雙鍵各自 scoped 查詢確認同一 Order，矛盾即 quarantine。

## 本機無法獨立驗證（上線前需實機補驗）
- **真 shioaji SDK 欄位名**：adapter/mapper 全用防禦性 `getattr/payload.get`，欄位不符時 deal_report 映射 `raise ValueError→quarantine`（**fail-closed，非靜默吃錯**）。需以真套件核對 `order.id/seqno/status`、deal 的 `deal_id/quantity/price/fee` 等鍵名（adapter/watchdog docstring 已標「待實機驗證」）。
- **真 Postgres**：測試套件對 PG 只做 compiled-DDL 逐項斷言（CHECK/UniqueConstraint/BIGINT/partial index）；Task 10 實作期用 Docker PG16 手跑過一次 schema smoke（見 `docs/deployment.md §10.4`），未納入套件（避 CI flaky）。**部署前依該程序重跑真 PG smoke**。並發僅 SQLite 檔案 DB 多執行緒驗證，PG MVCC 行為靠論證。
- **真瀏覽器 UI/HTMX**：只經 TestClient 斷言 server 端 HTML，未經真瀏覽器互動。

## 結論
架構收斂、round3 必修核心全數修好且有具敏感度的測試把關；殘留兩項均 LOW 且保守。可安全用 `ORDER_MODE=sim`（simtrade）繼續開發驗證；**接真 CA 上線前**應收上述 2 殘留 + 完成 3 項實機補驗（SDK 欄位、真 PG smoke、瀏覽器 UI）。
