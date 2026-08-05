import pytest
from quanquant.agent.main import build_parser, main


def test_mode_real_rejected_by_argparse():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--mode", "real"])


def test_defaults():
    args = build_parser().parse_args([])
    assert args.mode == "sim" and args.server.endswith("/ws/agent")


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
