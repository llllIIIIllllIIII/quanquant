# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec：QuanQuant 本機 broker agent 設定精靈 → 獨立 macOS .app（arm64、onedir）。

關鍵收錄（見任務事實 3-6）：
- 樣板：`src/quanquant/agent/gui/templates/*.html` → bundle 內
  `quanquant/agent/gui/templates/`（`Jinja2Templates(directory=Path(__file__).parent/
  "templates")` 執行期會從凍結後的模組旁找這個目錄）。
- keyring：`copy_metadata('keyring')` + `collect_all('keyring')` + hidden-import
  macOS 後端——後端靠 entry-point metadata 動態發現，沒有 metadata 會退化成 fail.Keyring。
- shioaji：`collect_all('shioaji')` 抓原生 `_core.abi3.so` 與所有 submodule；子程序
  （spawn）內才 import。
- uvicorn/websockets/keyring：顯式 hidden-import loop/protocol/backend 自動選擇模組。
"""
import os

from PyInstaller.utils.hooks import collect_all, collect_submodules, copy_metadata

REPO_ROOT = os.path.dirname(os.path.abspath(SPECPATH))
TEMPLATES_SRC = os.path.join(REPO_ROOT, "src", "quanquant", "agent", "gui", "templates")

datas = [(TEMPLATES_SRC, os.path.join("quanquant", "agent", "gui", "templates"))]
binaries = []
hiddenimports = [
    "uvicorn.loops.auto",
    "uvicorn.loops.asyncio",
    "uvicorn.protocols.http.auto",
    "uvicorn.protocols.http.h11_impl",
    "uvicorn.protocols.websockets.auto",
    "uvicorn.protocols.websockets.websockets_impl",
    "uvicorn.lifespan.on",
    "websockets",
    "websockets.legacy",
    "websockets.legacy.client",
    "keyring.backends.macOS",
    "keyring.backends.macOS.api",
]

# keyring 後端靠 entry-point metadata 動態發現（importlib.metadata.entry_points）——
# 沒有 dist-info 就找不到 macOS 後端，check_secure_backend 會看到 fail.Keyring。
datas += copy_metadata("keyring")

# shioaji：原生 binaries + 全 submodule（子程序 spawn 後才 import）。
_sj_datas, _sj_binaries, _sj_hidden = collect_all("shioaji")
datas += _sj_datas
binaries += _sj_binaries
hiddenimports += _sj_hidden

# keyring：全收（含後端 submodule 與其 metadata）。
_kr_datas, _kr_binaries, _kr_hidden = collect_all("keyring")
datas += _kr_datas
binaries += _kr_binaries
hiddenimports += _kr_hidden

# uvicorn 全 submodule（loop/protocol 自動選擇是動態 import）。
hiddenimports += collect_submodules("uvicorn")

block_cipher = None

a = Analysis(
    [os.path.join(REPO_ROOT, "packaging", "agent_app.py")],
    pathex=[os.path.join(REPO_ROOT, "src")],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="QuanQuant Agent",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,  # 保留 console：測試者看得到「agent GUI 已啟動」log；診斷也走這裡。
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch="arm64",
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="QuanQuant Agent",
)

app = BUNDLE(
    coll,
    name="QuanQuant Agent.app",
    icon=None,
    bundle_identifier="tech.orgstar.quanquant.agent",
    info_plist={
        "CFBundleName": "QuanQuant Agent",
        "CFBundleDisplayName": "QuanQuant Agent",
        "CFBundleShortVersionString": "0.1.0",
        "CFBundleVersion": "0.1.0",
        "NSHighResolutionCapable": True,
        # 非 sandbox；允許對外連 staging（App Transport Security 放行）。
        "NSAppTransportSecurity": {"NSAllowsArbitraryLoads": True},
        "LSMinimumSystemVersion": "12.0",
        # console=True 會讓 PyInstaller（osx.py）注入 LSBackgroundOnly=True，使 .app 變背景型：
        # 第一個實例的 uvicorn 永不退出、持續註冊在 LaunchServices，第二次雙擊去 reactivate
        # 背景實例卻無前景可帶 → LS -600（procNotFound）→ Finder「is not open anymore.」。
        # 顯式覆寫回 False（合併順序 spec 在後、必勝），恢復可正常啟動/前景化並出現在 Dock
        # （測試者才能從 Dock 正常結束——關瀏覽器分頁不會停 agent）。
        "LSBackgroundOnly": False,
    },
)
