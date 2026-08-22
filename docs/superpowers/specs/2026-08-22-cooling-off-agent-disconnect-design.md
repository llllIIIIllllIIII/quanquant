# Spec — 冷靜期（自我禁制）＋下單頁「斷開 Agent」（2026-08-22）

下單頁新增兩個 server 端風控（跑在 web server，非 agent .app）：
1. **斷開 Agent 連線**：比 kill switch 更強——真的斷 WS ＋停用到重新啟動。
2. **冷靜期（自我禁制）**：使用者自訂到期時間，期間只能平倉不能開新倉、agent 一併斷線、自己解不掉、只有 admin 能提前解除。

## 已拍板決策（2026-08-22）
- **D1 冷靜期擋單範圍**：只擋「開新倉」（octype=開倉），**允許平倉（octype=平倉）與取消掛單**。
- **D2 斷線鈕語意**：斷該使用者現有 agent WS ＋停用其 agent slot，**直到使用者重新啟動 App（重新授權/連線）才會再連**；自動重連被擋。
- **D3 冷靜期 × 連線**：進入冷靜期**一併斷開 agent 連線**（沿用 D2 的停用機制）。
- **D4 Admin 頁 MVP**：列出目前處於冷靜期的使用者＋每人「解除」鈕（admin-only，沿用 /admin 守門）。
- **D5 冷靜期到期自動解除**；期間使用者**無法自行縮短/取消**；admin 可提前解除。
- **D6 時長**：until 必須 > now；上限 90 天（防手殘）；無下限（5 分鐘也可）。
- **D7 持久化**：冷靜期狀態存 DB（重啟不失效——與現有 in-memory KillSwitchState 不同）。
- **D8 範圍**：只做 sim（agent 通道鎖 sim）；不做通知（admin 靠頁面查看）。
- **D9（2026-08-22 補）手動斷線鈕恢復方式**：改為**下單頁自助「允許 Agent 重連」按鈕**，取代原 D2「到 App 重啟才恢復」。理由：server 端無法區分「使用者重啟 App 的新連線」與「同一還在跑的 agent 自動重連」（同一 agent token、同一 WS 路徑），要做到「真・到重啟」須改 agent 端＋重 build/重發 .app，與「本功能純 server 端、不重 build」相牴觸。手動斷線＝自願暫停（非鎖定），自助恢復合理；**冷靜期（真正的自我禁制）仍 admin-only 解除、不受此影響**。使用者已確認（2026-08-22）。
- **D10（實作細節）**：`cooldowns` 表不用 partial-unique index（broker_positions 的 `status='open'` 是事件翻轉欄位，冷靜期到期是**時間**判定、無欄位可翻，partial-unique on `lifted_at IS NULL` 會把「到期未解除」的舊列卡住新列）。改：plain index on `user_id`＋app 層「已在冷靜期則拒絕新建」（同時擋自我縮短/重設）；`active = lifted_at IS NULL AND until_ts > now`；admin lift 清掉該 user 全部 unlifted 列。時間一律用真 UTC epoch-ms（`datetime.now(timezone.utc)`），until 由 datetime-local 以固定 +08:00（台灣無 DST）解析，兩邊同框可比。
- **D11（斷線機制）**：手動斷線與冷靜期共用 server 端 gate＝(in-memory 手動封鎖集合) OR (DB active cooldown)。WS `_authenticate` 後命中即 `close(1008)` 擋重連；即時踢現有連線靠 `AgentChannel.attach(..., closer=websocket.close)` 存 closer、route 端 `await channel.force_close()`。

## 資料模型（新表，兩方言可攜；比照 candles/repo.py named params＋BigInteger）
`cooling_off`：`id`、`user_id`(FK)、`until_ts`(BigInteger epoch-ms)、`created_at`(BigInteger)、`lifted_by`(int, nullable)、`lifted_at`(BigInteger, nullable)。
- 一個 user 同時最多一筆「有效」（`lifted_at IS NULL`）——partial unique index（比照 broker_positions 慣例）。

## 判定
- `active_cooling_off(user_id, now_ms)` = 存在 `lifted_at IS NULL` 且 `until_ts > now_ms` 的列。
- `check_place`：若 active 且該單為**開新倉** → `RiskError("冷靜期中，僅允許平倉")`（在既有 kill-switch 檢查附近，rollback＋risk_reject audit 沿用）。平倉/取消/改單放行。
- 到期以查詢時 `until_ts <= now` 判定失效（不需背景 job）。

## Agent 停用/斷線（D2/D3，最棘手，需 fail-closed）
- server per-user「agent 停用」旗標：停用時 (a) 主動關閉現有 agent WS；(b) 擋該 user 的 agent 重連（或 slot 標 not-ready 擋單）。
- 解除：斷線鈕＝使用者重啟 App（新連線/授權）即恢復；冷靜期＝到期或 admin 解除後恢復。
- 落點：`broker/agent_registry`（UserAgentSlot）＋ `web/routers/agent_ws`（認證/連線）＋ `web/routers/orders`（按鈕）。

## UI
- **下單頁**：新增「斷開 Agent 連線」按鈕（POST＋CSRF）；「冷靜期」設定區（date+time 輸入＋啟用鈕＋二次確認，因不可逆）。冷靜期中顯示到期時間＋「需聯繫管理員解除」。
- **Admin**：新頁 `/admin/cooling-off`（admin 守門）：表列有效冷靜期（user／until／建立時間）＋「解除」鈕。

## 驗收
- 單元測試：冷靜期擋開倉 / 放行平倉 / 放行取消；到期自動失效；使用者無法自解；admin 可解；斷線鈕停用到重連；非 owner/非 admin 一律 403。
- 部署前對真實 Postgres 跑一次新表 smoke（比照 `docs/deployment.md` §10.4）。
- staging 實測（henry sim）。

## 落點分支/部署
- 分支 `feat/order-report-latency`（延續本次 staging 線）；server 端功能 → 重部署 staging **server**（scp/rebuild app 容器），**不需重 build .app**。
