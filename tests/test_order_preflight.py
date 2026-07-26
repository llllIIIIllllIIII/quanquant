"""order_subsystem_preflight：ORDER_MODE 型別拒啟（raise RuntimeError）；缺 key/owner 軟性停用；
real 缺 CA 路徑/密碼/身分證字號或檔案不存在 → 軟性停用（不崩站）；Task 10（round3 F8）加嚴
CA 檔權限檢查（0600 + owner UID），`test_preflight_real_with_full_ca_enabled` 同步補
`os.chmod(ca, 0o600)`，不讓 Task 10 打壞 Task 8 這則既有測試（round3 #19）。"""
import os

import pytest

from quanquant.broker.preflight import order_subsystem_preflight
from quanquant.config import Settings


def _settings(**over):
    base = dict(
        shioaji_trade_api_key="k", shioaji_trade_secret_key="s",
        order_mode="sim", order_owner_user_ids="1",
    )
    base.update(over)
    return Settings(**base)


def test_order_mode_typo_rejected():
    with pytest.raises(RuntimeError):
        order_subsystem_preflight(_settings(order_mode="paper"))


def test_preflight_disabled_when_keys_missing():
    enabled, reason = order_subsystem_preflight(_settings(shioaji_trade_api_key=""))
    assert enabled is False and reason


def test_preflight_disabled_when_no_owners():
    enabled, reason = order_subsystem_preflight(_settings(order_owner_user_ids=""))
    assert enabled is False and reason


def test_preflight_real_without_ca_refuses_with_reason():
    enabled, reason = order_subsystem_preflight(_settings(order_mode="real"))
    assert enabled is False and "CA" in reason


def test_preflight_real_with_full_ca_enabled(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o600)  # Task 10 加嚴權限檢查後，Task 8 的假檔案也要補正確權限
    enabled, reason = order_subsystem_preflight(_settings(
        order_mode="real", shioaji_ca_path=str(ca), shioaji_ca_passwd="pw", shioaji_person_id="A1",
    ))
    assert enabled is True and reason is None


def test_preflight_real_rejects_overly_permissive_ca_file(tmp_path):
    ca = tmp_path / "sinopac.pfx"
    ca.write_bytes(b"fake")
    os.chmod(ca, 0o644)
    enabled, reason = order_subsystem_preflight(_settings(
        order_mode="real", shioaji_ca_path=str(ca), shioaji_ca_passwd="pw", shioaji_person_id="A1",
    ))
    assert enabled is False and "0600" in reason


def test_preflight_real_with_ca_path_pointing_to_missing_file_refused(tmp_path):
    missing = tmp_path / "does-not-exist.pfx"
    enabled, reason = order_subsystem_preflight(_settings(
        order_mode="real", shioaji_ca_path=str(missing), shioaji_ca_passwd="pw", shioaji_person_id="A1",
    ))
    assert enabled is False and "CA" in reason


def test_preflight_sim_enabled_without_ca():
    enabled, reason = order_subsystem_preflight(_settings())
    assert enabled is True and reason is None
