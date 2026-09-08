## 這個 PR 做了什麼

<!-- 一句話說明；若對應 issue 請寫 #編號 -->

## 截圖（改動前 / 改動後）

<!-- 前端改動必附 -->

## 自檢清單

- [ ] 只動了 `src/quanquant/web/templates/`、`src/quanquant/web/static/`、`docs/*.md`（否則已先開 issue，並在下方說明原因）
- [ ] 本機 `uv run pytest` 全綠
- [ ] `uv run python scripts/ci/check_paths.py upstream/main HEAD` 通過
- [ ] `uv run python scripts/ci/check_simplified.py` 通過
- [ ] 沒有引入 CDN、新框架或外部字型
- [ ] 全部繁體中文（台灣用語）
- [ ] 沒有修改 `tests/`

## 敏感檔案說明（若有動到）

<!-- chart.js、chart-guards.js、kill_switch_control、cooldown_control、agent_connection_control、agent_token_control、login、admin_*、任何 hx-confirm 表單：說明改了什麼、為什麼不影響原本的確認／權限語意 -->
