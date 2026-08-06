# 本機 Broker Agent Increment 0 — 人工 sim 實測驗收記錄

**日期**：2026-08-06（盤中）
**執行者**：使用者本人（真 Shioaji sim key，F002 已簽）
**環境**：本機 macOS；server `ORDER_CHANNEL=agent ORDER_MODE=sim`（uv run quanquant-web，127.0.0.1:8000）＋ 同機 `quanquant-agent`（getpass/env 憑證，session-only）
**程式版本**：branch `feat/shioaji-order-integration`，HEAD cb078ec（53b3ed9..cb078ec 共 39 commits，770 pytest 全綠）

## 逐項結果（計畫 Task 16 checklist）

| # | 項目 | 結果 |
|---|---|---|
| 1 | agent 登入 → orders 頁 badge 🟢「agent 已連線」、/healthz 200 | ✅ |
| 2 | 盤中下 1 口 Buy MKT/IOC → 委託 filled、成交 1/1、部位表出現 | ✅ |
| 3 | `raw_inbox` 檢查：本次測試回報全部 processed=1、quarantine=0 | ✅（詳下） |
| 4 | kill switch ON → 新單被擋、agent 端無新指令；OFF → 恢復 | ✅ |
| 5 | Ctrl-C 關 agent → badge 🔴、下單被擋；重啟需重輸憑證（session-only 證明）→ badge 恢復、自動 reconcile | ✅ |
| 6 | agent outbox：`SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL` → 0（回報全數送達） | ✅ |

## 過程記錄與佐證

- **憑證排除**：首次啟動輸入的 API key 被永豐回 `StatusCode: 400, key not exist`（agent 正確 fail-safe：錯誤訊息憑證遮蔽生效、backoff 重連如設計）。改用 `.env` 內 7/28 實測過的 `SHIOAJI_TRADE_API_KEY` 組（經 `QQ_AGENT_API_KEY`/`QQ_AGENT_SECRET_KEY` env 餵入）後登入成功。
- **raw_inbox 佐證**（控制方唯讀 SELECT 查證）：本次測試產生兩組 order_report＋deal_report 全部健康消化；表內僅有的 5 筆 quarantine 列全為 2026-07-27/28 歷史殘留（mapper 修正前的退化 payload ×4、Cover 無對應開倉的 fail-closed ×1），非本次產生。

## 已知事項（非缺陷）

1. 5 筆歷史 quarantine 列為 `processed=0` → 未來若**換一個永豐帳號**登入 agent，換帳號防護會拒絕（保守設計）；屆時需人工清理該 5 筆再換帳號。同帳號使用不受影響。
2. cmd_ack 逾時（預設 10s）的委託會停在 unknown 且配額保留、不自動解除——Inc0 刻意保守，Inc1 補自動對帳。
3. Inc1/real-mode 前硬門檻（codex ACCEPT-DEFER 條件）：command ledger＋late-ack 冪等收斂、durable 落地 fail-stop、unknown 配額自動解除。

## 結論

**Increment 0 骨幹驗證通過**：login(sim) → place → report → UI roundtrip ＋ kill switch 擋新單，全鏈在真實 sim 環境成立；憑證 session-only 與斷線重連補送行為符合設計。自動化證據（770 pytest、真 socket 整合測試、codex 5 輪 APPROVE、opus fresh-context 驗收）＋本記錄的人工實測，構成 Inc0 完成定義的全部要件。
