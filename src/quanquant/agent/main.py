"""quanquant-agent：本機 broker agent CLI（Increment 0，僅 simtrade；Increment 1 起可選
GUI 設定精靈，見 `quanquant.agent.startup` 的七層優先序）。

憑證 session-only：getpass/env 讀入記憶體，不落地、不進 argv、不進 log。

`build_parser()`（本檔）是 Inc0 遺留的窄範圍 parser（僅
`--server`/`--mode`/`--symbol`/`--buffer`，`--server`/`--buffer` 預設沿用
env-based 舊行為）——刻意保留、不刪不改，供既有測試/外部呼叫端相容（G5 紅線：這個
函式的輸出逐位不變）。`main()` 實際解析真正 `sys.argv` 改用
`quanquant.agent.startup.build_parser()`（七層優先序的超集合 parser，多了
`--gui`/`--reset`/`--no-gui`/`--site`/`--profile`），兩個 parser 刻意分開、不合併。
"""
import argparse
import asyncio
import getpass
import os
import sys


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="quanquant-agent",
                                description="QuanQuant 本機 broker agent（simtrade）")
    p.add_argument("--server", default=os.environ.get(
        "QQ_AGENT_SERVER", "ws://127.0.0.1:8000/ws/agent"))
    p.add_argument("--mode", choices=["sim"], default="sim")   # Inc0 硬 guard：real 不存在
    p.add_argument("--symbol", default="TXF")
    p.add_argument("--buffer", default=os.environ.get(
        "QQ_AGENT_BUFFER", os.path.expanduser("~/.quanquant-agent/outbox.db")))
    return p


def main() -> None:
    from quanquant.agent.startup import build_parser as build_startup_parser
    from quanquant.agent.startup import resolve_startup_plan

    args = build_startup_parser().parse_args()
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
