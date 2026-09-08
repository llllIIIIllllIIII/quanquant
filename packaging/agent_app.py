"""PyInstaller 進入點 wrapper：把 QuanQuant 本機 broker agent 設定精靈打包成獨立 .app。

- `main()` 讀 sys.argv、不接受傳 argv，所以這裡不呼叫 `main()`，改直接呼叫 `run_gui()`，
  把 staging 站網址硬編進來（測試者雙擊即用，免帶任何 CLI 參數）。
- 下單路徑用 `multiprocessing.get_context("spawn")` 開真正的 OS 子程序（見
  `agent.runner.ChildHandle.start()`），凍結後 spawn 會重新執行本執行檔——`__main__`
  第一行必須 `multiprocessing.freeze_support()`，否則子程序會再跑一次 GUI 造成無限分裂。

診斷（僅供凍結環境驗證，靠環境變數開關，正式使用者不會踩到）：
- `QQ_AGENT_DIAG=1`：印出 keyring 後端模組、`check_secure_backend()` 結果、直接 import
  shioaji 結果、並用 spawn 子程序（比照 runner）再驗一次 shioaji import，最後印出
  `webbrowser.open` 會開的 bootstrap URL（含 secret）供 curl。之後照常啟動 GUI。
"""
import multiprocessing
import os

SITE = "https://quant.35-221-233-118.sslip.io"


def _shioaji_child() -> None:
    """spawn 子程序目標：比照 runner 的 child 路徑，在凍結子程序內 import shioaji。
    exitcode 0 代表凍結後子程序能成功載入原生 SDK。"""
    import shioaji  # noqa: F401

    print(f"[child] shioaji {getattr(shioaji, '__version__', '?')} imported OK in spawned child",
          flush=True)


def _spawn_shioaji_smoke() -> None:
    ctx = multiprocessing.get_context("spawn")
    proc = ctx.Process(target=_shioaji_child)
    proc.start()
    proc.join(60)
    print(f"[diag] spawn child exitcode={proc.exitcode}", flush=True)


def _diagnostics() -> None:
    import logging

    logging.basicConfig(level=logging.INFO)

    import keyring

    kr = keyring.get_keyring()
    print(f"[diag] keyring backend module={type(kr).__module__} class={type(kr).__name__}",
          flush=True)
    try:
        from quanquant.agent import keyring_store

        print(f"[diag] check_secure_backend={keyring_store.check_secure_backend()}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[diag] check_secure_backend FAILED: {exc!r}", flush=True)

    try:
        import shioaji

        print(f"[diag] direct shioaji import OK: {getattr(shioaji, '__version__', '?')}",
              flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"[diag] direct shioaji import FAILED: {exc!r}", flush=True)

    _spawn_shioaji_smoke()

    # 攔截 webbrowser.open：不真的開瀏覽器，改印出含 secret 的 bootstrap URL 供 curl。
    import webbrowser

    def _capture(url, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        print(f"[diag] BOOTSTRAP_URL={url}", flush=True)
        return True

    webbrowser.open = _capture


def _run() -> None:
    import asyncio

    from quanquant.agent.gui.coordinator import run_gui
    from quanquant.agent.startup import canonicalize_site

    asyncio.run(run_gui(site_origin=canonicalize_site(SITE), profile=None, reset=False))


if __name__ == "__main__":
    multiprocessing.freeze_support()
    if os.environ.get("QQ_AGENT_DIAG") == "1":
        _diagnostics()
    _run()
