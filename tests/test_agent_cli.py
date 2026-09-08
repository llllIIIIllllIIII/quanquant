import pytest
from quanquant.agent.main import build_parser, main


def test_mode_real_rejected_by_argparse():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--mode", "real"])


def test_defaults():
    # Reviewer 裁決（Important）：build_parser() 現在是 quanquant.agent.startup 唯一
    # 的 parser，raw parse 的 --server 預設是 None（headless 行為的實際預設值由
    # resolve_startup_plan()/_resolve_headless_server() 在解析階段補回，見
    # test_agent_startup_resolution.py::test_headless_server_priority_flag_beats_env_beats_default
    # 逐位鎖住 "ws://127.0.0.1:8000/ws/agent"，G5 未變）。
    args = build_parser().parse_args([])
    assert args.mode == "sim" and args.server is None


def test_main_reads_credentials_from_env_not_argv(monkeypatch, tmp_path):
    monkeypatch.setenv("QQ_AGENT_TOKEN", "tok")
    monkeypatch.setenv("QQ_AGENT_API_KEY", "AK")
    monkeypatch.setenv("QQ_AGENT_SECRET_KEY", "SK")
    monkeypatch.setenv("QQ_AGENT_BUFFER", str(tmp_path / "o.db"))
    captured = {}

    class _FakeRunner:
        def __init__(self, **kw):
            captured.update(kw)
        async def run_forever(self):
            return None

    monkeypatch.setattr("quanquant.agent.runner.AgentRunner", _FakeRunner)
    monkeypatch.setattr("sys.argv", ["quanquant-agent"])
    main()
    assert captured["mode"] == "sim"
    # 憑證只進 ChildHandle 記憶體，不在 argv
    assert captured["child"]._credentials["api_key"] == "AK"
