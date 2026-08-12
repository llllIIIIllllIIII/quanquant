"""桌面捷徑產生器（M4 維運腳本）：由維運者一次性替使用者建立可雙擊啟動的桌面捷徑。

鐵律（spec D5）：捷徑自帶可執行環境——cd 絕對 repo 路徑＋絕對 uv 路徑（跨平台
`shutil.which("uv")` 偵測，不寫死路徑）；macOS `.command` 產生後 `chmod 0700`；
Windows `.lnk` 透過系統內建 PowerShell（WScript.Shell COM）產生，刻意不引入
pywin32（新依賴僅 keyring 已用掉）。本模組對所有值做 shell 跳脫，不依賴上游驗證——
`canonicalize_site` 只檢查 URL 格式合法性，不擋 shell metacharacters，所以 macOS
分支一律用 `shlex.quote()`、Windows 分支一律用 `_ps_quote()` 跳脫所有內插值
（repo_path、uv_path、site、profile）。
"""

import shlex
import shutil
import subprocess
from pathlib import Path


def _uv_or_raise() -> str:
    uv_path = shutil.which("uv")
    if uv_path is None:
        raise RuntimeError("找不到 uv 可執行檔（PATH 未包含 uv），無法產生捷徑")
    return uv_path


def _require_absolute(repo_path: Path) -> None:
    if not repo_path.is_absolute():
        raise ValueError(f"repo_path 必須是絕對路徑：{repo_path!r}")


def _ps_quote(value: str) -> str:
    """PowerShell 單引號字面值跳脫：內含 `'` 需雙寫為 `''`，否則會提前終結字串。"""
    return "'" + value.replace("'", "''") + "'"


def generate_macos_shortcut(*, repo_path: Path, site: str, profile: str | None, out_path: Path) -> Path:
    """cd 絕對 repo 路徑＋exec 絕對 uv 路徑；產生後 chmod 0700。找不到 uv → raise
    RuntimeError（不產生半成品檔案）。所有內插值皆經 `shlex.quote()` 跳脫，防止
    `$(...)`／反引號等 shell metacharacters 注入。"""
    _require_absolute(repo_path)
    uv_path = _uv_or_raise()
    profile_arg = f" --profile {shlex.quote(profile)}" if profile else ""
    content = (
        "#!/bin/sh\n"
        f"cd {shlex.quote(str(repo_path))} && exec {shlex.quote(uv_path)} "
        f"run quanquant-agent --gui --site {shlex.quote(site)}{profile_arg}\n"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    out_path.chmod(0o700)
    return out_path


def generate_windows_shortcut(*, repo_path: Path, site: str, profile: str | None, out_path: Path) -> Path:
    """透過 subprocess 呼叫系統內建 PowerShell（WScript.Shell COM）產生 .lnk——刻意不用
    pywin32，遵守『新依賴僅 keyring』的 Global Constraint。TargetPath=絕對 uv.exe，
    WorkingDirectory=repo_path（等同 Start in）。所有內插值皆經 `_ps_quote()` 跳脫，
    防止內含單引號（如 profile）提前終結 PowerShell 字串字面值。"""
    _require_absolute(repo_path)
    uv_path = _uv_or_raise()
    args = f'run quanquant-agent --gui --site "{site}"'
    if profile:
        args += f' --profile "{profile}"'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ps_script = (
        "$WshShell = New-Object -ComObject WScript.Shell; "
        f"$Shortcut = $WshShell.CreateShortcut({_ps_quote(str(out_path))}); "
        f"$Shortcut.TargetPath = {_ps_quote(uv_path)}; "
        f"$Shortcut.Arguments = {_ps_quote(args)}; "
        f"$Shortcut.WorkingDirectory = {_ps_quote(str(repo_path))}; "
        "$Shortcut.Save()"
    )
    subprocess.run(["powershell", "-NoProfile", "-Command", ps_script], check=True)
    return out_path
