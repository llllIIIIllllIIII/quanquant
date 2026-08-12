"""桌面捷徑產生器（M4 維運腳本）：由維運者一次性替使用者建立可雙擊啟動的桌面捷徑。

鐵律（spec D5）：捷徑自帶可執行環境——cd 絕對 repo 路徑＋絕對 uv 路徑（跨平台
`shutil.which("uv")` 偵測，不寫死路徑）；macOS `.command` 產生後 `chmod 0700`；
Windows `.lnk` 透過系統內建 PowerShell（WScript.Shell COM）產生，刻意不引入
pywin32（新依賴僅 keyring 已用掉）。`site` 參數必須是呼叫端已用 `canonicalize_site`
驗證過的字串，本模組不重複驗證。
"""

import shutil
import subprocess
from pathlib import Path


def _uv_or_raise() -> str:
    uv_path = shutil.which("uv")
    if uv_path is None:
        raise RuntimeError("找不到 uv 可執行檔（PATH 未包含 uv），無法產生捷徑")
    return uv_path


def generate_macos_shortcut(*, repo_path: Path, site: str, profile: str | None, out_path: Path) -> Path:
    """cd 絕對 repo 路徑＋exec 絕對 uv 路徑；產生後 chmod 0700。找不到 uv → raise
    RuntimeError（不產生半成品檔案）。"""
    uv_path = _uv_or_raise()
    profile_arg = f' --profile "{profile}"' if profile else ""
    content = (
        "#!/bin/sh\n"
        f'cd "{repo_path}" && exec "{uv_path}" run quanquant-agent --gui --site "{site}"{profile_arg}\n'
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    out_path.chmod(0o700)
    return out_path


def generate_windows_shortcut(*, repo_path: Path, site: str, profile: str | None, out_path: Path) -> Path:
    """透過 subprocess 呼叫系統內建 PowerShell（WScript.Shell COM）產生 .lnk——刻意不用
    pywin32，遵守『新依賴僅 keyring』的 Global Constraint。TargetPath=絕對 uv.exe，
    WorkingDirectory=repo_path（等同 Start in）。"""
    uv_path = _uv_or_raise()
    args = f'run quanquant-agent --gui --site "{site}"'
    if profile:
        args += f' --profile "{profile}"'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ps_script = (
        "$WshShell = New-Object -ComObject WScript.Shell; "
        f"$Shortcut = $WshShell.CreateShortcut('{out_path}'); "
        f"$Shortcut.TargetPath = '{uv_path}'; "
        f"$Shortcut.Arguments = '{args}'; "
        f"$Shortcut.WorkingDirectory = '{repo_path}'; "
        "$Shortcut.Save()"
    )
    subprocess.run(["powershell", "-NoProfile", "-Command", ps_script], check=True)
    return out_path
