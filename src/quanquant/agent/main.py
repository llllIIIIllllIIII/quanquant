"""quanquant-agent：本機 broker agent CLI（Increment 0，僅 simtrade）。

憑證 session-only：getpass/env 讀入記憶體，不落地、不進 argv、不進 log。
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
    args = build_parser().parse_args()
    token = os.environ.get("QQ_AGENT_TOKEN") or getpass.getpass("Agent token: ")
    api_key = os.environ.get("QQ_AGENT_API_KEY") or getpass.getpass("Shioaji API Key: ")
    secret_key = os.environ.get("QQ_AGENT_SECRET_KEY") or getpass.getpass("Shioaji Secret Key: ")

    from quanquant.agent.buffer import DurableBuffer
    from quanquant.agent.runner import AgentRunner, ChildHandle, FatalAgentError
    from quanquant.agent.ws_client import WebsocketsTransport

    runner = AgentRunner(
        transport=WebsocketsTransport(args.server, token=token),
        buffer=DurableBuffer(args.buffer),
        child=ChildHandle(credentials={"api_key": api_key, "secret_key": secret_key},
                          symbol=args.symbol, mode=args.mode, buffer_path=args.buffer),
        mode=args.mode,
    )
    print(f"agent 啟動（simtrade）→ {args.server}；Ctrl-C 結束（憑證僅存記憶體）")
    try:
        asyncio.run(runner.run_forever())
    except KeyboardInterrupt:
        print("agent 結束")
    except FatalAgentError as exc:
        # codex round2 fix4(d)：帳號不符等不可重試錯誤——不能讓 run_forever 悶頭重試燒
        # 券商登入配額，這裡接住後印出處置指引並以非零碼結束，讓操作者/監控知道要人工介入。
        print(f"agent 停止（不可重試錯誤，需人工介入）: {exc}")
        sys.exit(2)
