"""quanquant-agent：本機 broker agent CLI（Increment 0，僅 simtrade；Increment 1 起可選
GUI 設定精靈，見 `quanquant.agent.startup` 的七層優先序）。

憑證 session-only：getpass/env 讀入記憶體，不落地、不進 argv、不進 log。

Reviewer 裁決（Important，收斂雙 parser）：`build_parser()` 直接重新匯出
`quanquant.agent.startup.build_parser`（七層優先序的唯一 parser，含
`--gui`/`--reset`/`--no-gui`/`--site`/`--profile`），不再各自維護一份窄範圍
Inc0 parser——先前兩份平行存在（一份實際用於 `main()`、一份僅供
`tests/test_agent_cli.py` 舊斷言相容）在 spec §3「`--site` canonical」明文要求下已無
必要維持分裂，唯一差異只剩 `--server`/`--buffer` 預設值從 env-based URL 改成
`None`（`resolve_startup_plan()` 的 `_resolve_headless_server`/
`_resolve_headless_buffer` 在解析階段補回，headless **行為**逐位不變，見 G5——變的
只是 raw parse 的中繼值，不是最終送進 `AgentRunner` 的值）。`tests/test_agent_cli.py::
test_defaults` 已同步改為斷言 `args.server is None`，其餘既有斷言不動。
"""
import asyncio
import getpass
import os
import sys

from quanquant.agent.startup import build_parser, resolve_startup_plan


def main() -> None:
    args = build_parser().parse_args()
    plan = resolve_startup_plan(args, env=os.environ, is_tty=sys.stdin.isatty())

    if not plan.headless:
        from quanquant.agent.gui.coordinator import run_gui
        asyncio.run(run_gui(site_origin=plan.site, profile=plan.profile, reset=plan.reset))
        return

    token = os.environ.get("QQ_AGENT_TOKEN") or getpass.getpass("Agent token: ")
    api_key = os.environ.get("QQ_AGENT_API_KEY") or getpass.getpass("Shioaji API Key: ")
    secret_key = os.environ.get("QQ_AGENT_SECRET_KEY") or getpass.getpass("Shioaji Secret Key: ")

    from quanquant.agent.buffer import DurableBuffer
    from quanquant.agent.runner import AgentRunner, ChildHandle, FatalAgentError
    from quanquant.agent.ws_client import WebsocketsTransport

    runner = AgentRunner(
        transport=WebsocketsTransport(plan.server, token=token),
        buffer=DurableBuffer(plan.buffer),
        child=ChildHandle(credentials={"api_key": api_key, "secret_key": secret_key},
                          symbol=args.symbol, mode=args.mode, buffer_path=plan.buffer),
        mode=args.mode,
    )
    print(f"agent 啟動（simtrade）→ {plan.server}；Ctrl-C 結束（憑證僅存記憶體）")
    try:
        asyncio.run(runner.run_forever())
    except KeyboardInterrupt:
        print("agent 結束")
    except FatalAgentError as exc:
        # codex round2 fix4(d)：帳號不符等不可重試錯誤——不能讓 run_forever 悶頭重試燒
        # 券商登入配額，這裡接住後印出處置指引並以非零碼結束，讓操作者/監控知道要人工介入。
        print(f"agent 停止（不可重試錯誤，需人工介入）: {exc}")
        sys.exit(2)
