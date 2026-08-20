"""永豐 Shioaji 官方 Python API 測試（期貨版）— 一鍵腳本。

流程照抄官方 sj-trading-demo 的 testing_futures_ordering()：
simulation 登入 → activate_ca → 取 TXFR1 → 平盤價 LMT/ROD/自動新平倉 買 1 口
→ place_order → update_status。全程模擬環境，不會動到真錢。

用法（可測試時段：週一至五 08:00–20:00，18:00 後限台灣 IP）：
    uv run python scripts/shioaji_api_test.py

憑證來源：環境變數，或專案根目錄 .env / .env.local（後者優先），支援兩套命名：
    官方名：API_KEY / SECRET_KEY / CA_CERT_PATH / CA_PASSWORD [/ PERSON_ID]
    本專案名：SHIOAJI_TRADE_API_KEY / SHIOAJI_TRADE_SECRET_KEY /
              SHIOAJI_CA_PATH / SHIOAJI_CA_PASSWD [/ SHIOAJI_PERSON_ID]

通過與否的查法：跑完等約 5 分鐘審核，到永豐簽署中心看測試狀態，或以
正式模式（simulation=False）login 後檢查 accounts 的 signed 欄位是否為 True。
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_env_files() -> None:
    for name in (".env", ".env.local"):
        path = ROOT / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


def _pick(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def main() -> int:
    _load_env_files()

    api_key = _pick("API_KEY", "SHIOAJI_TRADE_API_KEY")
    secret_key = _pick("SECRET_KEY", "SHIOAJI_TRADE_SECRET_KEY")
    ca_path = _pick("CA_CERT_PATH", "SHIOAJI_CA_PATH")
    ca_passwd = _pick("CA_PASSWORD", "SHIOAJI_CA_PASSWD")
    person_id = _pick("PERSON_ID", "SHIOAJI_PERSON_ID")

    if not api_key or not secret_key:
        print("缺少 API_KEY / SECRET_KEY（或 SHIOAJI_TRADE_API_KEY / "
              "SHIOAJI_TRADE_SECRET_KEY），請放進 .env.local 或環境變數。")
        return 1

    import shioaji as sj
    from shioaji.constant import (
        Action,
        FuturesOCType,
        FuturesPriceType,
        OrderType,
    )

    api = sj.Shioaji(simulation=True)
    accounts = api.login(api_key=api_key, secret_key=secret_key)
    print(f"Available accounts: {accounts}")

    if ca_path and ca_passwd:
        kwargs = {"ca_path": ca_path, "ca_passwd": ca_passwd}
        if person_id:
            kwargs["person_id"] = person_id
        api.activate_ca(**kwargs)
        print("CA activated.")
    else:
        print("警告：未提供 CA_CERT_PATH / CA_PASSWORD，跳過 activate_ca。"
              "官方範例在測試中有啟用 CA，建議補上憑證後重跑以免審核不過。")

    if api.futopt_account is None:
        print("登入帳戶中沒有期貨帳戶（futopt_account 為 None）——"
              "請確認期貨戶已開立且期貨 API 已簽署。")
        api.logout()
        return 1

    # 合約檔於 login 後背景下載，取不到就等（最多 30 秒）再取
    contract = None
    for _ in range(30):
        contract = api.Contracts.Futures["TXFR1"]
        if contract is not None:
            break
        time.sleep(1)
    if contract is None:
        print("等 30 秒仍取不到 TXFR1 合約，請稍後重跑。")
        api.logout()
        return 1
    print(f"Contract: {contract}")

    order = sj.order.FuturesOrder(
        action=Action.Buy,
        price=contract.reference,
        quantity=1,
        price_type=FuturesPriceType.LMT,
        order_type=OrderType.ROD,
        octype=FuturesOCType.Auto,
        account=api.futopt_account,
    )
    print(f"Order: {order}")

    trade = api.place_order(contract=contract, order=order)
    print(f"Trade: {trade}")

    api.update_status()
    print(f"Status: {trade.status}")
    print("完成。狀態為 PendingSubmit 或 Submitted 即視為委託成功；"
          "約 5 分鐘後到簽署中心查審核結果。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
