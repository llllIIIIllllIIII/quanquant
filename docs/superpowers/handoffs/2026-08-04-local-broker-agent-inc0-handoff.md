# 交接計劃 — 本機 Broker Agent（版本 B）多人 simtrade，Increment 0

**建立**：2026-08-04
**分支**：`feat/shioaji-order-integration`（未 push、未部署）
**給下一個 session 的一句話**：設計已完成、5 個決策已全拍板，下一步是**為 Increment 0 寫實作計畫（writing-plans）**，先不 codex 覆核。勿重新討論架構方向。

---

## 0. 先讀這些（依序）

1. **本檔**（狀態 + 決策 + 下一步 + 地雷）。
2. **設計 spec**：`docs/superpowers/specs/2026-08-04-local-broker-agent-design.md`（放置分界、5 條 Tier0 不變量跨網路保存、WS 通道協定、per-user registry、分階段）。
3. **記憶** `project-order-integration`（自動載入；含此功能完整脈絡、Tier0 歷程、simtrade 實測欄位定案）。

---

## 1. 這功能是什麼、為什麼

把 Shioaji 原始 I/O 搬到**使用者自己電腦的 agent 程序**（本機登入 simtrade、送單、收回報），**憑證永不上伺服器**；中央網站只留政策/風控/持久化/UI/狀態機，透過**認證 WebSocket** 交換「下行指令」與「上行回報」。

**三個動機**：(1) 拆掉憑證託管牆＝使用者不肯部署的主因；(2) 免費把 Shioaji issue #203（單實例凍死整個 Python 直譯器）隔離在使用者機器、碰不到 web server/圖表；(3) 通往真多人的同一程式路徑。

---

## 2. 已拍板的 5 個決策（勿重議）

| # | 決策 | 定案 |
|---|---|---|
| 1 | v1 範圍 | **Increment 0 骨幹**：單一信任使用者，證 login(sim)→place→report→UI roundtrip + kill switch 下行。registry 可先 hard-code 單 user。目的＝消掉低信心待實測項 |
| 2 | 通道方案 | **B1：自建 WS + 內嵌現有 adapter 的 native 部分**。B2（官方本機 HTTP server）因需重寫 mapper/進黑盒/我方未實跑而**否決當起點**；#203 程序隔離改用「shioaji 跑子程序」補（細節留計畫），不關 B2 的門 |
| 3 | 憑證重啟持久化 | **純 session-only**（重啟重輸、永不落地）。本機加密保存留 Increment 2 |
| 4 | agent token 簽發 | 沿用現有帳號系統（`project-account-system`），使用者登入後簽發 per-user 短期 token（可 rotation） |
| 5 | watchdog DB-only 序列化 | server 端移除 native 序列化鎖後，`_retry_quarantined`/`_reconcile_unknown_quota` 改用獨立 server 鎖，與 native 序列化脫鉤 |

---

## 3. 下一步任務：寫 Increment 0 實作計畫

用 writing-plans（或本專案慣例的 plan 文件，寫進 `docs/superpowers/plans/`），把 Increment 0 拆成可 TDD 的 Task 序列。**不進實作、不 codex 覆核**（codex 留 Increment 1 規劃時再用——那時最硬的分散式正確性才登場）。

**Increment 0 範圍（骨幹驗證）**：
- 一支薄 Python agent（新 repo/子目錄），內嵌現有 `ShioajiAdapter` 的 **native 部分**（connect/place/cancel/update/list_trades/set_order_callback + `_map_*` mapper + `_classify_place_failure`），**抽掉所有 `session_factory` DB 呼叫**。憑證 session-only。自帶 native 序列化鎖。
- server 端：WS 端點 + 單 user 的上行 handler（收原始 payload → 呼叫既有 `stage_raw_inbox`）+ 下行指令 dispatch。registry 可先 hard-code。
- 證明：login(sim) → 下一張市價單(MKT/IOC) → 成交回報上行 → RawInbox → RawInboxWorker → UI/SSE 顯示；kill switch 下行擋新單。

**設計裡已解的關鍵洞見（勿重新推導）**：
- **天然分界縫**：回報上行**走 `RawInbox` 表中介、無 volatile queue**（`inbox_worker.py:8-9`）→ agent 只送原始 payload，server `commit_raw_callback`（`inbox_worker.py:59-68`）接手落地，**`RawInboxWorker` 以下整條不動**。
- **唯一硬拆**：`shioaji_adapter.py` 的 `place/cancel/update` 把「DB 決策（`risk_guard.check_place`）+ native（`supervisor.run`）+ 寫回 ack」融在**同一顆鎖**裡（place = L389-492）→ 切成「server 決策 → 下行 native 指令 → agent 執行 → 回 ack → server 寫回」三段。
- **零丟單跨網路**（最需小心）：agent 端**本機 durable buffer** 先落地 raw payload → 送 → server commit RawInbox → **ack 回 agent** → 標記已送；at-least-once + RawInbox 既有去重（`trade_id`/`seqno`）吸收重送；斷線續存、重連補送。要為「送出未 ack 就崩潰/斷線」寫測試。
- **現況全是全域單例**（RiskGuard/Supervisor/adapter/session_state/worker 在 `web/app.py:_start_order_subsystem` L172-300 建一次塞 `app.state`）；DB 資料列已 per-user keyed。Inc0 可先不動 registry。

---

## 4. 必守約束 / 地雷（每個 session 都要記得）

- **禁部署**：使用者明令「正式環境先不部署，除非能讓可信任的人用自己的永豐帳號做 simtrade」。Tier0（T0.1–T0.3）已 commit 在分支但**未 push、未部署**，勿擅自 push/deploy。
- **push/PR/deploy 只在使用者明確要求時做**（關鍵決策先問）。
- **繁體中文回覆**（zh-TW）。
- **GateGuard hook**：每 session 第一次 Bash、建新檔、每檔首次 edit 前，會擋下並要求陳述事實（匯入者/受影響 API/資料 schema/使用者逐字指示）→ 照列點補上、重試同一操作即可，非故障。
- **graphify**：查 repo 架構問題先 `cd <repo> && graphify query "<問題>"` 再精讀指定檔行，**勿整棵樹 grep/glob**；派 subagent 探索也要交代這條。
- **指揮官不下場**：>3 檔探索/掃 repo/查網頁一律派 subagent，主對話只收結論。
- **驗收不自驗**：宣稱完成前，證據要來自測試輸出/實跑/fresh-context subagent。
- **本機 `quanquant.db` 含珍貴回補歷史**：只讀 SELECT，**勿刪改**。
- **雙方言**：本機 SQLite / 雲端 Postgres，新 raw SQL 兩邊可攜。
- **成本**：2026-08-04 這條線累計已 ~$1250+，注意節流（少讀原始碼進主對話、善用既有 spec/memory）。

---

## 5. 現況技術事實（已定案，可信）

- **測試**：Tier0 完成時 **649 pytest 全綠**（分支 `feat/shioaji-order-integration`）。
- **simtrade 免 CA**：`shioaji_adapter.py:_connect_blocking`（L183-193）的 `activate_ca` 只在 `mode=="real"` 呼叫；官方文件一致。
- **shioaji 1.5.3 wheel 全平台覆蓋**（mac x86_64/arm64、win_amd64、linux x86_64/aarch64，abi3 cp37+，專案要 py3.11+）、**無 sdist**（免裝 Rust）；原生收斂成單一 `_core.abi3.so`。
- **mapper 欄位名已 2026-07-28 實測定案**（deal_report 扁平 `{trade_id,seqno,ordno,exchange_seq,action,code,price,quantity,ts(epoch秒×1000),...}`；order_report 巢狀 `{operation:{op_type},order:{id,seqno,ordno,order_type,price_type,oc_type},status}`；我方存的 `ordno`=`order.id` 非 `order.ordno`）；**綁 1.5.3 `_core.pyi`，升版需重驗**。
- **交易時段**：台指期日盤 08:45–13:45、夜盤 15:00–次日05:00（此外無 tick、sim 不撮合）；sim 要驗成交下市價單。

---

## 6. 延後項（非本次，但別忘）

- **T0.0** 真環境實錄欄位驗證（只能使用者跑 real/CA session）。
- **T0.4** 保證金/曝險/日內停損（需使用者給數字：保證金門檻、日內最大虧損、淨口數上限）。
- **Increment 1**：per-user registry + token 簽發 + offline 處理 + UI（最硬的分散式正確性在這，屆時做 codex 覆核）。
- **Increment 2（可選）**：憑證本機加密保存 / 打包成安裝檔。
