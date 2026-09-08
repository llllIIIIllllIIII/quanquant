import shlex
import stat
import subprocess
from pathlib import Path

import pytest

from quanquant.agent.shortcut_gen import generate_macos_shortcut, generate_windows_shortcut


def test_generate_macos_shortcut_content_and_permission(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "/usr/local/bin/uv")
    out = tmp_path / "QuanQuant Agent.command"
    result = generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                      profile=None, out_path=out)
    content = result.read_text()
    assert "cd " in content and str(tmp_path / "repo") in content
    assert "/usr/local/bin/uv run quanquant-agent --gui --site https://q.example" in content
    mode = stat.S_IMODE(result.stat().st_mode)
    assert mode == 0o700


def test_generate_macos_shortcut_includes_profile_flag_when_given(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "/usr/local/bin/uv")
    out = tmp_path / "QuanQuant Agent.command"
    result = generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                      profile="alice", out_path=out)
    assert "--profile alice" in result.read_text()


def test_generate_macos_shortcut_raises_when_uv_not_found(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: None)
    with pytest.raises(RuntimeError):
        generate_macos_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                                 profile=None, out_path=tmp_path / "x.command")


def test_generate_macos_shortcut_raises_when_repo_path_not_absolute(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "/usr/local/bin/uv")
    with pytest.raises(ValueError):
        generate_macos_shortcut(repo_path=Path("relative/repo"), site="https://q.example",
                                 profile=None, out_path=tmp_path / "x.command")


def test_generate_macos_shortcut_escapes_command_substitution_in_site(tmp_path, monkeypatch):
    """實測重現：舊版用雙引號內插 site，`$(...)` 在雙引號內仍會展開執行。修法是
    `shlex.quote()`——對含 shell metacharacters 的字串整段用單引號包住，POSIX sh
    單引號內完全不做展開（含 `$(...)`／反引號）。本測試除檢查生成內容外，
    也實跑生成的 .command（uv 路徑指到 /bin/echo，避免真的呼叫 uv），驗證注入
    副作用（建立 marker 檔）不會發生。"""
    monkeypatch.setattr("shutil.which", lambda name: "/bin/echo")
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()
    marker = tmp_path / "qq_pwned"
    hostile_site = f"https://x$(touch {marker})"
    out = tmp_path / "QuanQuant Agent.command"

    result = generate_macos_shortcut(repo_path=repo_dir, site=hostile_site, profile=None, out_path=out)
    content = result.read_text()

    # site 必須整段被 shlex.quote 安全包住（單引號），而非裸露在雙引號/無引號情境
    assert shlex.quote(hostile_site) in content

    proc = subprocess.run(["sh", str(result)], capture_output=True, text=True)
    assert not marker.exists(), "$(...) 被展開執行了，命令注入未被擋下"
    assert hostile_site in proc.stdout, "site 應以字面字串傳給 echo，而非被展開"


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


def test_generate_windows_shortcut_raises_when_repo_path_not_absolute(tmp_path, monkeypatch):
    monkeypatch.setattr("shutil.which", lambda name: "C:\\uv\\uv.exe")
    with pytest.raises(ValueError):
        generate_windows_shortcut(repo_path=Path("relative/repo"), site="https://q.example",
                                   profile=None, out_path=tmp_path / "x.lnk")


def test_generate_windows_shortcut_escapes_single_quote_in_profile(tmp_path, monkeypatch):
    """實測重現：舊版用單引號內插 Arguments，profile 含 `'` 會提前終結 PowerShell
    字串字面值、剩餘文字被當程式碼解析。修法是 `_ps_quote()`——把值中的 `'` 雙寫為
    `''`，PowerShell 單引號字面值規則會將 `''` 還原成單一字面 `'`，而不會 breakout。"""
    monkeypatch.setattr("shutil.which", lambda name: "C:\\uv\\uv.exe")
    captured = {}
    def _fake_run(cmd, check):
        captured["cmd"] = cmd
        captured["check"] = check
    monkeypatch.setattr("subprocess.run", _fake_run)
    out = tmp_path / "QuanQuant Agent.lnk"
    hostile_profile = "bob'; Remove-Item C:\\pwned; '"

    generate_windows_shortcut(repo_path=tmp_path / "repo", site="https://q.example",
                               profile=hostile_profile, out_path=out)
    script = captured["cmd"][-1]

    # 原始字串（含裸單引號）不得整段未跳脫地出現在腳本中
    assert hostile_profile not in script
    # 單引號必須雙寫為 '' 才能在 PowerShell 單引號字面值內安全表示
    assert hostile_profile.replace("'", "''") in script
