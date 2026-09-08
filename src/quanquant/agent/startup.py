"""CLI 七層優先序＋`--site` canonical 解析／驗證（spec §5.3）。

- `canonicalize_site()`：`--site` 輸入的唯一驗證/正規化點——格式限定
  `https://host[:port]`（loopback host 例外允許 `http`），禁 path/query/userinfo/
  fragment，預設 port（443/80）正規化省略。
- `ws_url_for()`：canonical site origin → WS URL（scheme 替換＋固定路徑
  `/ws/agent`）。純字串轉換，`setup_routes.py::_derive_ws_url` 是同構的獨立實作
  （消費已算好的 site_origin，職責不同，不合併，見其 docstring）。
- `build_parser()`／`resolve_startup_plan()`：CLI 七層優先序（由上而下先命中先生效，
  見 module 內 `resolve_startup_plan` docstring）。

`agent/main.py` 直接重新匯出這裡的 `build_parser()`／`resolve_startup_plan()`（唯一
parser，不再另外維護一份 Inc0 窄範圍 parser——見 reviewer 裁決，`main.py` 模組
docstring 有完整理由）。
"""
import argparse
import ipaddress
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

_WS_PATH = "/ws/agent"
_DEFAULT_HEADLESS_SERVER = "ws://127.0.0.1:8000/ws/agent"


def _is_loopback_host(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def canonicalize_site(raw: str) -> str:
    """`--site` canonical 定義（spec §3）：格式限 `https://host[:port]`（loopback host
    例外允許 `http`）；禁 path/query/userinfo/fragment；預設 port（443/80）正規化省略。
    不合規 → raise ValueError（訊息供 `argparse.error` 顯示）。"""
    parts = urlsplit(raw)
    if parts.scheme not in ("http", "https"):
        raise ValueError(f"--site 僅接受 http(s) URL：{raw!r}")
    if parts.path not in ("", "/"):
        raise ValueError(f"--site 不得包含 path：{raw!r}")
    if parts.query:
        raise ValueError(f"--site 不得包含 query：{raw!r}")
    if parts.fragment:
        raise ValueError(f"--site 不得包含 fragment：{raw!r}")
    if parts.username or parts.password:
        raise ValueError(f"--site 不得包含 userinfo：{raw!r}")
    host = parts.hostname
    if not host:
        raise ValueError(f"--site 缺少 host：{raw!r}")
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError(f"--site port 不合法：{raw!r}") from exc

    loopback = _is_loopback_host(host)
    if parts.scheme == "http" and not loopback:
        raise ValueError(f"--site 非 loopback host 時僅接受 https：{raw!r}")

    default_port = 443 if parts.scheme == "https" else 80
    if port == default_port:
        port = None

    # `parts.hostname` 回傳裸位址（IPv6 literal 不含中括號，如 "::1"）——reassemble 回
    # origin 字串時若原本是 IPv6（host 含 ":"）必須補回 "[...]"，否則
    # "https://::1:8443" 這種字串在 port 前完全無法分辨位址與 port 的邊界，是無效 URL
    # 且會被後續消費者（`ws_url_for`／WS 連線）靜默帶著錯誤位址繼續跑。
    host_literal = f"[{host}]" if ":" in host else host
    origin = f"{parts.scheme}://{host_literal}"
    if port is not None:
        origin += f":{port}"
    return origin


def ws_url_for(site_origin: str) -> str:
    """https→wss、http→ws，路徑固定 `/ws/agent`。"""
    if site_origin.startswith("https://"):
        return "wss://" + site_origin[len("https://"):] + _WS_PATH
    if site_origin.startswith("http://"):
        return "ws://" + site_origin[len("http://"):] + _WS_PATH
    raise ValueError(f"未知的 site_origin scheme：{site_origin!r}")


@dataclass(frozen=True)
class StartupPlan:
    headless: bool
    site: str | None      # GUI 分支：canonical origin；headless 分支：None
    profile: str | None
    reset: bool
    server: str | None    # headless 專用（--server > env > 預設）
    buffer: str | None    # headless 專用（--buffer > env > 預設）


class _AgentArgumentParser(argparse.ArgumentParser):
    """`parse_args()` 後手動補上互斥/必填檢查（argparse 的
    `add_mutually_exclusive_group()` 只能表達『同時指定就錯』，表達不出『指定 A 就一定
    要有 B』或跨 flag 的條件式禁止），這裡覆寫 `parse_args()` 讓呼叫端仍可直接
    `parser.parse_args([...])` 觸發同一套 `SystemExit` 語意，不必額外呼叫第二個函式。"""

    def parse_args(self, args=None, namespace=None):
        ns = super().parse_args(args, namespace)
        if ns.site is not None:
            try:
                ns.site = canonicalize_site(ns.site)
            except ValueError as exc:
                self.error(str(exc))
        if (ns.gui or ns.reset) and not ns.site:
            self.error("--gui/--reset 需要搭配 --site")
        if ns.site and ns.server:
            self.error("GUI 模式（--site）不可搭配 --server")
        if (ns.gui or ns.reset) and ns.buffer:
            self.error("GUI 模式（--gui/--reset）不可搭配 --buffer")
        return ns


def build_parser() -> argparse.ArgumentParser:
    """七層優先序中「層1 互斥錯誤」由 `mutually_exclusive_group` 在這裡擋（`--no-gui`
    與 `--gui`/`--reset` 同時指定）；GUI 模式必填 `--site` 由 `parser.error` 擋
    （`--gui`/`--reset` 沒帶 `--site`）；GUI 禁 `--server`/`--buffer` 由 `parser.error`
    擋（同時指定 `--site` 與 `--server`，或 `--gui`/`--reset` 與 `--buffer` 同時指定）。

    `--server`/`--buffer` 預設改 `None`（不是拿掉預設值——headless 分支
    `resolve_startup_plan()` 內的 `_resolve_headless_server`/`_resolve_headless_buffer`
    會在解析階段補回與現行 `agent/main.py` 逐位相同的預設，見各自 docstring）；`None`
    才能讓「使用者是否顯式指定」這件事在 GUI 禁用檢查裡可判斷。"""
    p = _AgentArgumentParser(
        prog="quanquant-agent",
        description="QuanQuant 本機 broker agent（simtrade；headless 或 GUI 設定精靈）",
    )
    mode_group = p.add_mutually_exclusive_group()
    mode_group.add_argument("--no-gui", action="store_true")
    mode_group.add_argument("--gui", action="store_true")
    mode_group.add_argument("--reset", action="store_true")

    p.add_argument("--site", default=None)
    p.add_argument("--profile", default=None)
    p.add_argument("--server", default=None)
    p.add_argument("--mode", choices=["sim"], default="sim")   # Inc0 硬 guard：real 不存在
    p.add_argument("--symbol", default="TXF")
    p.add_argument("--buffer", default=None)
    return p


def _resolve_headless_server(args: argparse.Namespace, env: Mapping[str, str]) -> str:
    """headless 解析優先序：`--server` > `QQ_AGENT_SERVER` > 本機預設（與現行
    `agent/main.py` 逐位相同，G5）。"""
    return args.server or env.get("QQ_AGENT_SERVER") or _DEFAULT_HEADLESS_SERVER


def _resolve_headless_buffer(args: argparse.Namespace, env: Mapping[str, str]) -> str:
    """headless 解析優先序：`--buffer` > `QQ_AGENT_BUFFER` > 本機預設（與現行
    `agent/main.py` 逐位相同，G5）。"""
    return args.buffer or env.get("QQ_AGENT_BUFFER") or os.path.expanduser("~/.quanquant-agent/outbox.db")


def _reject_gui_buffer_env_conflict(env: Mapping[str, str]) -> None:
    """spec §5.3：「GUI 模式禁用 `--buffer` 與 `QQ_AGENT_BUFFER` 覆寫（指定即啟動
    錯誤）」——`--buffer` 本身已由 `build_parser()` 的 `parser.error()` 擋下，這裡補上
    env 變數這一半（argparse 看不到 env，只有這裡拿得到 `env` 參數）。"""
    if env.get("QQ_AGENT_BUFFER"):
        print(
            "GUI 模式不支援 QQ_AGENT_BUFFER 環境變數覆寫（同時指定即啟動錯誤）；"
            "請改用 --no-gui headless 模式，或取消這個環境變數後再試一次。",
            file=sys.stderr,
        )
        raise SystemExit(2)


def _build_headless_plan(args: argparse.Namespace, env: Mapping[str, str]) -> StartupPlan:
    return StartupPlan(
        headless=True, site=None, profile=None, reset=False,
        server=_resolve_headless_server(args, env),
        buffer=_resolve_headless_buffer(args, env),
    )


def _build_gui_plan(args: argparse.Namespace, env: Mapping[str, str], *, reset: bool) -> StartupPlan:
    _reject_gui_buffer_env_conflict(env)
    return StartupPlan(
        headless=False, site=args.site, profile=args.profile, reset=reset,
        server=None, buffer=None,
    )


def resolve_startup_plan(
    args: argparse.Namespace, *, env: Mapping[str, str], is_tty: bool,
) -> StartupPlan:
    """層2-7（spec §5.3 表格），由上而下先命中先生效（層1 互斥錯誤已在
    `build_parser()` 的 `parse_args()` 擋下，走不到這裡）：

    2. `--no-gui` → headless（env/getpass 現行為；不探測 keyring/registry）
    3. `--reset`（蘊含 GUI）→ 強制重跑精靈
    4. `--gui` → GUI（不採用 env 三件套，單一來源原則）
    5. env 三件套齊全 → headless 直跑（現行為逐位一致，G5）
    6. tty 且有 `--site` → 等同 `--gui`
    7. 其餘 → headless getpass 現行為
    """
    if args.no_gui:
        return _build_headless_plan(args, env)
    if args.reset:
        return _build_gui_plan(args, env, reset=True)
    if args.gui:
        return _build_gui_plan(args, env, reset=False)

    env_trio_complete = all(
        env.get(key) for key in ("QQ_AGENT_TOKEN", "QQ_AGENT_API_KEY", "QQ_AGENT_SECRET_KEY")
    )
    if env_trio_complete:
        return _build_headless_plan(args, env)

    if is_tty and args.site:
        return _build_gui_plan(args, env, reset=False)

    return _build_headless_plan(args, env)
