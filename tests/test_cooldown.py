"""冷靜期（self-lockout）repo 層（broker/repository.py 的 create/active/lift/list）：
create→active 找得到；重複 create 回 None（擋自我縮短/重設）；到期後 active 回 None 且可
再 create；lift 清多列回正確 rowcount 且之後 active/list 皆空；list_active_cooldowns 只回
active、依 until 升序、跨 user。

時間一律用顯式 now_ms（不依賴 wall-clock，測得穩定）——契約見 db/models.py Cooldown 與
broker/repository.py 冷靜期段（active＝lifted_ts IS NULL AND until_ts>now）。risk-layer 的
擋單語意（New/Auto 擋、Cover 放行）已由 tests/test_risk_guard.py 覆蓋，本檔不重測。
"""
from sqlmodel import select

from quanquant.broker import repository as brepo
from quanquant.db.models import Cooldown

_NOW = 1_700_000_000_000  # 固定基準 epoch-ms，避免依賴 wall-clock
_HOUR = 3_600_000


def test_create_then_active_found(session):
    row = brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    session.commit()
    assert row is not None and row.id is not None
    found = brepo.active_cooldown(session, user_id=1, now_ms=_NOW)
    assert found is not None and found.id == row.id


def test_active_cooldown_returns_latest_until_when_multiple_rows(session):
    """多筆 active 取 until_ts 最大者（repo 契約：order_by until desc → first）。直接塞兩列
    （繞過 create 的『已 active 則拒』，模擬歷史殘留），驗證挑的是較晚到期那筆。"""
    session.add(Cooldown(user_id=1, until_ts=_NOW + _HOUR, created_ts=_NOW))
    session.add(Cooldown(user_id=1, until_ts=_NOW + 5 * _HOUR, created_ts=_NOW))
    session.commit()
    found = brepo.active_cooldown(session, user_id=1, now_ms=_NOW)
    assert found is not None and found.until_ts == _NOW + 5 * _HOUR


def test_duplicate_create_returns_none_and_keeps_single_row(session):
    """已有 active → 再 create 回 None（擋自我縮短/重設/重複），不新增列。"""
    first = brepo.create_cooldown(session, user_id=1, until_ms=_NOW + 2 * _HOUR, now_ms=_NOW)
    session.commit()
    assert first is not None
    # 嘗試「縮短」到更早到期——必須被拒
    dup = brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    assert dup is None
    rows = session.exec(select(Cooldown).where(Cooldown.user_id == 1)).all()
    assert len(rows) == 1 and rows[0].until_ts == _NOW + 2 * _HOUR  # 原列未被改動


def test_expired_cooldown_not_active_and_recreatable(session):
    """到期後（now 往後推過 until）active 回 None，且可再 create 新的一段。"""
    brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    session.commit()
    later = _NOW + 2 * _HOUR  # 已過原 until
    assert brepo.active_cooldown(session, user_id=1, now_ms=later) is None
    again = brepo.create_cooldown(session, user_id=1, until_ms=later + _HOUR, now_ms=later)
    session.commit()
    assert again is not None
    assert brepo.active_cooldown(session, user_id=1, now_ms=later).id == again.id


def test_lift_clears_all_unlifted_rows_returns_rowcount(session):
    """lift 清該 user 全部 lifted_ts IS NULL 列（含到期未解除殘列），回正確 rowcount，且
    寫入 lifted_by/lifted_ts；之後 active/list 皆空。"""
    brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    session.commit()
    # 另塞一筆「到期但未解除」的殘列——lift 也要清掉它（lifted_ts IS NULL）
    session.add(Cooldown(user_id=1, until_ts=_NOW - _HOUR, created_ts=_NOW - 10 * _HOUR))
    session.commit()

    n = brepo.lift_cooldown(session, user_id=1, admin_user_id=99, now_ms=_NOW)
    session.commit()
    assert n == 2

    assert brepo.active_cooldown(session, user_id=1, now_ms=_NOW) is None
    assert brepo.list_active_cooldowns(session, now_ms=_NOW) == []
    for row in session.exec(select(Cooldown).where(Cooldown.user_id == 1)).all():
        assert row.lifted_ts == _NOW and row.lifted_by == 99


def test_lift_no_unlifted_rows_returns_zero(session):
    assert brepo.lift_cooldown(session, user_id=42, admin_user_id=99, now_ms=_NOW) == 0


def test_lift_scoped_to_target_user_only(session):
    """lift 只清指定 user 的列，不動別人的 active 冷靜期。"""
    brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    brepo.create_cooldown(session, user_id=2, until_ms=_NOW + _HOUR, now_ms=_NOW)
    session.commit()
    assert brepo.lift_cooldown(session, user_id=1, admin_user_id=99, now_ms=_NOW) == 1
    session.commit()
    assert brepo.active_cooldown(session, user_id=1, now_ms=_NOW) is None
    assert brepo.active_cooldown(session, user_id=2, now_ms=_NOW) is not None  # user 2 不受影響


def test_list_active_cooldowns_only_active_ascending_cross_user(session):
    """list_active_cooldowns 只回 active（排除到期／已解除），依 until 升序，跨 user。"""
    brepo.create_cooldown(session, user_id=1, until_ms=_NOW + 3 * _HOUR, now_ms=_NOW)  # 較晚
    brepo.create_cooldown(session, user_id=2, until_ms=_NOW + _HOUR, now_ms=_NOW)      # 較早
    # user 3：已到期（inactive）
    session.add(Cooldown(user_id=3, until_ts=_NOW - _HOUR, created_ts=_NOW - 10 * _HOUR))
    # user 4：未到期但已被解除（inactive）
    session.add(Cooldown(user_id=4, until_ts=_NOW + 9 * _HOUR, created_ts=_NOW,
                         lifted_ts=_NOW, lifted_by=99))
    session.commit()

    active = brepo.list_active_cooldowns(session, now_ms=_NOW)
    assert [c.user_id for c in active] == [2, 1]  # 升序：user2(+1h) 先於 user1(+3h)


def test_list_active_cooldowns_empty_when_none(session):
    assert brepo.list_active_cooldowns(session, now_ms=_NOW) == []


# ---- R2-2（D-2，2026-09-02）：5 分鐘反悔窗——本人可自行取消，逾時後任何人都不行 ----
# `lift_cooldown_by_owner` 取代 admin 提前解除（該路徑已停用，見 web/routers/admin.py）；
# 時間窗判定寫進 SQL WHERE（`created_ts > now_ms-window_ms`），不只信任呼叫端算好的
# 布林值——這裡直接用顯式 now_ms 覆蓋窗內/窗外兩種邊界，不 sleep。

_5MIN = 5 * 60_000


def test_lift_cooldown_by_owner_within_window_succeeds(session):
    """邊界③：啟動後 4:59（未滿 5 分鐘反悔窗）本人可取消。"""
    brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    session.commit()
    later = _NOW + 4 * 60_000 + 59_000
    n = brepo.lift_cooldown_by_owner(session, user_id=1, now_ms=later, window_ms=_5MIN)
    session.commit()
    assert n == 1
    assert brepo.active_cooldown(session, user_id=1, now_ms=later) is None


def test_lift_cooldown_by_owner_after_window_rejected(session):
    """邊界④：超過 5 分鐘反悔窗（5:01）後本人取消被拒——0 rowcount，列完全不受影響。"""
    brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    session.commit()
    later = _NOW + 5 * 60_000 + 1_000
    n = brepo.lift_cooldown_by_owner(session, user_id=1, now_ms=later, window_ms=_5MIN)
    assert n == 0
    assert brepo.active_cooldown(session, user_id=1, now_ms=later) is not None


def test_lift_cooldown_by_owner_records_self_not_admin_as_lifter(session):
    """解除者記自己（本人），不是 admin_user_id——R2-2 是自我取消，不是 admin 代管。"""
    brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    session.commit()
    n = brepo.lift_cooldown_by_owner(session, user_id=1, now_ms=_NOW + 60_000, window_ms=_5MIN)
    session.commit()
    assert n == 1
    row = session.exec(select(Cooldown).where(Cooldown.user_id == 1)).first()
    assert row.lifted_by == 1 and row.lifted_ts == _NOW + 60_000


def test_lift_cooldown_by_owner_no_active_row_returns_zero(session):
    assert brepo.lift_cooldown_by_owner(session, user_id=42, now_ms=_NOW, window_ms=_5MIN) == 0


def test_lift_cooldown_by_owner_scoped_to_target_user_only(session):
    """只清指定 user 的列，不影響同時間窗內其他人的冷靜期。"""
    brepo.create_cooldown(session, user_id=1, until_ms=_NOW + _HOUR, now_ms=_NOW)
    brepo.create_cooldown(session, user_id=2, until_ms=_NOW + _HOUR, now_ms=_NOW)
    session.commit()
    n = brepo.lift_cooldown_by_owner(session, user_id=1, now_ms=_NOW + 60_000, window_ms=_5MIN)
    session.commit()
    assert n == 1
    assert brepo.active_cooldown(session, user_id=1, now_ms=_NOW + 60_000) is None
    assert brepo.active_cooldown(session, user_id=2, now_ms=_NOW + 60_000) is not None
