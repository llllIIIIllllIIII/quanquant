# 交接計劃 — 本機 Broker Agent Increment 1（多人）

**建立**：2026-08-06
**給下一個 session 的一句話**：Inc0 骨幹已完結併入 main（merge commit 5282c13，樹=9f9e23e，770 pytest 綠，人工 sim 實測全過）；Inc1=真多人（per-user registry+token 簽發+offline 處理+UI）＋三個硬門檻。**下一步是 Inc1 設計討論與 spec**——這次最硬的分散式正確性登場，設計期就要 codex 覆核（Inc0 時刻意延後至此）。

---

## 0. 先讀這些（依序）

1. **本檔**。
2. **設計 spec**：`docs/superpowers/specs/2026-08-04-local-broker-agent-design.md`（「per-user agent registry」段是 Inc1 的既有草圖；通道協定/不變量表仍有效）。
3. **記憶** `project-order-integration`（自動載入；含 Inc0 全歷程、codex 5 輪修了什麼、Inc1 硬門檻）。
4. 選讀：`docs/superpowers/plans/2026-08-04-local-broker-agent-inc0.md`（了解已建成的模組與介面）、`docs/superpowers/reviews/2026-08-06-local-broker-agent-inc0-sim-verification.md`（驗收記錄）。

## 1. 現況（可信事實）

- main = 5282c13（PR #2 merge；樹與驗證版 9f9e23e 逐位一致）；feature branch 已刪。770 pytest 全綠。
- 正式環境**未部署**；使用者部署前提「可信的人用自己永豐帳號 simtrade」＝**Inc1 交付後才成立**。main 上 `ORDER_CHANNEL` 預設 `inprocess`，部署不會誤啟 agent 通道。
- 已建成（Inc0）：`broker/native.py`（唯一 import shioaji）、`agent_protocol.py`（v1，mode/protocol Literal 鎖 sim/1）、`agent_channel.py`（AgentChannel：generation+inbox_lock；AgentNativeGateway）、`web/routers/agent_ws.py`（token WS、commit 後才 ack、拒登關線、換帳號 guard）、`agent/*`（durable buffer、SDK 子程序 rpc_id+poison、runner 泵/補送/respawn/stable_session backoff、CLI `quanquant-agent`）；adapter 三段切（`remote_gateway` 注入）。新依賴 websockets>=14。

## 2. Inc1 範圍（spec 既定草圖 + 硬門檻）

**多人化**（spec「per-user agent registry」段）：
- registry: `user_id → AgentConnection`（取代 Inc0 的單一 `app.state.agent_channel`）。
- `RiskGuard`/`OrderSessionState` per-user 實例化（DB 列已 per-user keyed，runtime 物件跟著拆）；routes/watchdog 由 user_id 查 registry。
- **agent token 簽發**：沿用帳號系統（`project-account-system`），使用者登入後簽發 per-user 短期 token（可 rotation），取代 Inc0 靜態 `AGENT_WS_TOKEN`；WS 握手驗 token→綁 user_id。
- offline 處理 + UI「我的 agent 連線狀態」（per-user badge；Inc0 的全域 badge 要改）。
- lifespan 改啟動 registry；in-process 模式仍要共存（單人自用路徑不能壞）。

**三硬門檻（codex ACCEPT-DEFER 的明文條件，real-mode 前必做；Inc1 應全解或明確再議）**：
1. **command ledger + late-ack 冪等收斂**：server 送單前持久化 cmd_id；agent 端 command ledger 去重＋重連補送 ack；server 冪等接受 late ack、補寫 broker IDs、解除相關 quarantine——修「ack 遺失後 placeholder 委託永不收斂」。
2. **durable 落地 fail-stop**：agent buffer 雙重寫入失敗時 fail-stop＋健康上報（協定要加健康欄位），不得繼續宣稱 healthy。
3. **unknown 配額自動解除**：下行 `query_qty` 指令（op 需新增），恢復 in-process `_reconcile_unknown_quota` 的等價能力。

## 3. Inc1 設計時必須面對的已知縫隙（Inc0 審查中標記、當時刻意留下）

- **RawInbox 無 account/user scope 欄位**：Inc0 用「換帳號 guard 全域擋」規避；多人下**必須 per-user/per-account scope**（可能要加 server 欄位——Inc0 的「零新表」約束到 Inc1 解鎖，schema 演進走 `db/migrate.py` `_MIGRATIONS` nullable ADD COLUMN 慣例＋雙方言可攜）。上行 envelope 帶 immutable account/mode 是 codex 建議的正解。
- 換帳號 guard 現為**全域** unprocessed count——多人下會互相誤擋，要改 per-user scope。
- `adapter.account` 是 mutable 單例欄位、order_report mapper 讀它——per-user 化時要跟著拆。
- supervisor.lock 在 agent 模式=server DB 序列化鎖（per-user 拆分時鎖 scope 要重新設計）；`_reconcile_inner` 在鎖內 await WS（Inc0 接受，多人下要重新評估 head-of-line blocking）。
- protocol=Literal[1]：Inc1 改協定（健康欄位、scope 欄位、query_qty op）時要決定 bump 版本或相容擴充。
- mode 仍鎖 sim（協定/CLI/server/child 四層）；Inc1 不開 real。
- 本機 quanquant.db 有 5 筆 7 月歷史 quarantine 列（processed=0）——同帳號無影響；使用者換帳號測試前需人工清理。

## 4. 必守約束（每 session 都要記得）

- push/PR/merge/deploy 一律需使用者明確指示；繁體中文回覆；GateGuard hook 擋操作時列點補事實重試（非故障）；簡體字 hook 會擋含簡體的寫入。
- graphify 查 repo 優先、勿整樹掃；>3 檔探索派 subagent；指揮官不下場；驗收不自驗。
- 流程（使用者已採納 verified-delivery）：**設計討論/spec → codex 覆核設計（這次要做）→ 拍板 → writing-plans → SDD 實作 → codex 審會跑的代碼 → opus fresh-context 驗收 → 人工實測**。大型子系統計畫走薄計畫（介面+測試清單+棘手演算法），別把整個子系統寫滿代碼進 plan。
- 成本：Inc0 全程約 $86；Inc1 更複雜，維持省用路由（worker sonnet、最終審 opus、外審 codex）。

## 5. 建議的第一步

1. 設計討論：先盤 Inc1 的開放決策清單（registry 生命週期、token 格式/TTL/rotation、command ledger 放哪邊/什麼粒度、RawInbox scope 方案、per-user 鎖拆分、UI 形態），與使用者逐條拍板（比照 Inc0 的 5 決策模式）。
2. 寫 Inc1 設計 spec（`docs/superpowers/specs/`）→ codex 覆核收斂 → 再 writing-plans。
3. 本 handoff 檔已 commit 在 main；Inc1 動工時記得先開 feature branch（勿直接在 main 上實作）。
