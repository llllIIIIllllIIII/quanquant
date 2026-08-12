import stat
import sys

import pytest

from quanquant.agent.shortcut_gen import generate_macos_shortcut, generate_windows_shortcut


def test_generate_macos_shortcut_content_and_permission(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "/usr/local/bin/uv")
    out = tmp_path / "QuanQuant Agent.command"
    result = generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                      profile=None, out_path=out)
    content = result.read_text()
    assert 'cd "' in content and str(tmp_path / "repo") in content
    assert '"/usr/local/bin/uv" run quanquant-agent --gui --site "https://q.example"' in content
    mode = stat.S_IMODE(result.stat().st_mode)
    assert mode == 0o700


def test_generate_macos_shortcut_includes_profile_flag_when_given(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "/usr/local/bin/uv")
    out = tmp_path / "QuanQuant Agent.command"
    result = generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                      profile="alice", out_path=out)
    assert '--profile "alice"' in result.read_text()


def test_generate_macos_shortcut_raises_when_uv_not_found(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)
    with pytest.raises(RuntimeError):
        generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                 profile=None, out_path=tmp_path / "x.command")


def test_generate_windows_shortcut_invokes_powershell_with_expected_args(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "C:\\uv\\uv.exe")
    captured = {}
    def _fake_run(cmd, check):
        captured["cmd"] = cmd
        captured["check"] = check
    monkeypatch.setattr("subprocess.run", _fake_run)
    out = tmp_path / "QuanQuant Agent.lnk"
    generate_windows_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                               profile="bob", out_path=out)
    assert captured["check"] is True
    script = captured["cmd"][-1]
    assert "C:\\uv\\uv.exe" in script and "--profile" in script and str(tmp_path / "repo") in script


def test_generate_windows_shortcut_raises_when_uv_not_found(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)
    with pytest.raises(RuntimeError):
        generate_windows_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                   profile=None, out_path=tmp_path / "x.lnk")
