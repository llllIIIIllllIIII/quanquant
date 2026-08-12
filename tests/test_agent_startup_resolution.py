import argparse

import pytest

from quanquant.agent.startup import build_parser, canonicalize_site, resolve_startup_plan, ws_url_for


# ---- canonicalize_site ----

@pytest.mark.parametrize("raw,expected", [
    ("https://quant.example", "https://quant.example"),
    ("https://quant.example:443", "https://quant.example"),   # 預設 port 省略
    ("https://quant.example:8443", "https://quant.example:8443"),
    ("http://127.0.0.1:8000", "http://127.0.0.1:8000"),        # loopback 允許 http
    ("http://127.0.0.1:80", "http://127.0.0.1"),
])
def test_canonicalize_site_normalizes(raw, expected):
    assert canonicalize_site(raw) == expected


@pytest.mark.parametrize("raw", [
    "https://quant.example/path",       # 禁 path
    "https://quant.example?x=1",        # 禁 query
    "https://user:pw@quant.example",    # 禁 userinfo
    "https://quant.example#frag",       # 禁 fragment
    "http://quant.example",             # 非 loopback 禁 http
    "ftp://quant.example",              # 非 http(s)
])
def test_canonicalize_site_rejects_invalid(raw):
    with pytest.raises(ValueError):
        canonicalize_site(raw)


def test_ws_url_for_derives_wss_from_https():
    assert ws_url_for("https://quant.example") == "wss://quant.example/ws/agent"


def test_ws_url_for_derives_ws_from_http_loopback():
    assert ws_url_for("http://127.0.0.1:8000") == "ws://127.0.0.1:8000/ws/agent"


# ---- argparse 互斥/必填 ----

def test_no_gui_and_gui_together_is_parser_error():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--no-gui", "--gui", "--site", "https://q.example"])


def test_gui_without_site_is_parser_error():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--gui"])


def test_gui_with_server_is_parser_error():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--gui", "--site", "https://q.example", "--server", "ws://x"])


def test_gui_with_buffer_is_parser_error():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--gui", "--site", "https://q.example", "--buffer", "/x"])


# ---- 七層優先序 ----

def _args(**overrides):
    defaults = dict(no_gui=False, gui=False, reset=False, site=None, profile=None,
                     server=None, buffer=None)
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def test_layer2_no_gui_forces_headless_ignoring_everything_else():
    plan = resolve_startup_plan(_args(no_gui=True), env={}, is_tty=True)
    assert plan.headless is True


def test_layer3_reset_forces_gui():
    plan = resolve_startup_plan(_args(reset=True, site="https://q.example"), env={}, is_tty=False)
    assert plan.headless is False and plan.reset is True and plan.site == "https://q.example"


def test_layer4_gui_flag_forces_gui_even_with_env_trio_present():
    env = {"QQ_AGENT_TOKEN": "t", "QQ_AGENT_API_KEY": "k", "QQ_AGENT_SECRET_KEY": "s"}
    plan = resolve_startup_plan(_args(gui=True, site="https://q.example"), env=env, is_tty=False)
    assert plan.headless is False   # GUI 模式不採用 env 三件套（單一來源原則）


def test_layer5_env_trio_complete_without_gui_flag_goes_headless():
    env = {"QQ_AGENT_TOKEN": "t", "QQ_AGENT_API_KEY": "k", "QQ_AGENT_SECRET_KEY": "s"}
    plan = resolve_startup_plan(_args(), env=env, is_tty=True)
    assert plan.headless is True


def test_layer6_tty_with_site_and_no_env_trio_goes_gui():
    plan = resolve_startup_plan(_args(site="https://q.example"), env={}, is_tty=True)
    assert plan.headless is False and plan.site == "https://q.example"


def test_layer7_fallback_headless_when_no_tty_no_site_no_env():
    plan = resolve_startup_plan(_args(), env={}, is_tty=False)
    assert plan.headless is True


def test_headless_server_priority_flag_beats_env_beats_default():
    plan = resolve_startup_plan(_args(no_gui=True, server="ws://flag"),
                                 env={"QQ_AGENT_SERVER": "ws://env"}, is_tty=True)
    assert plan.server == "ws://flag"
    plan2 = resolve_startup_plan(_args(no_gui=True), env={"QQ_AGENT_SERVER": "ws://env"}, is_tty=True)
    assert plan2.server == "ws://env"
    plan3 = resolve_startup_plan(_args(no_gui=True), env={}, is_tty=True)
    assert plan3.server == "ws://127.0.0.1:8000/ws/agent"   # G5：現行預設值逐位不變
