"""User management + login with an in-memory failure lockout.

Lockout is per-username, in-memory (single-process deployment): 5 consecutive
failures lock the account for 60s. `now` is injectable for tests; production
uses time.monotonic().
"""
import time

from sqlmodel import Session, select

from quanquant.auth.passwords import hash_password, verify_password
from quanquant.db.models import User, _utcnow

_MAX_FAILURES = 5
_LOCK_SECONDS = 60.0
# username -> (consecutive failures, locked_until monotonic timestamp)
_failures: dict[str, tuple[int, float]] = {}


def clear_failures() -> None:
    """Test helper: reset lockout state."""
    _failures.clear()


def _is_locked(username: str, now: float) -> bool:
    _count, until = _failures.get(username, (0, 0.0))
    return now < until


def _record_failure(username: str, now: float) -> None:
    count, until = _failures.get(username, (0, 0.0))
    count += 1
    if count >= _MAX_FAILURES:
        _failures[username] = (0, now + _LOCK_SECONDS)  # lock and reset the counter
    else:
        _failures[username] = (count, until)


def authenticate(
    db: Session, username: str, password: str, *, now: float | None = None
) -> User | None:
    """The user on success; None on unknown user / bad password / inactive / locked."""
    t = time.monotonic() if now is None else now
    if _is_locked(username, t):
        return None
    user = get_by_username(db, username)
    if user is None or not user.is_active or not verify_password(password, user.password_hash):
        _record_failure(username, t)
        return None
    _failures.pop(username, None)
    return user


def get_by_username(db: Session, username: str) -> User | None:
    return db.exec(select(User).where(User.username == username)).first()


def create_user(
    db: Session,
    username: str,
    password: str,
    *,
    display_name: str | None = None,
    role: str = "user",
) -> User:
    if get_by_username(db, username) is not None:
        raise ValueError(f"username {username!r} already exists")
    user = User(
        username=username,
        display_name=display_name or username,
        password_hash=hash_password(password),
        role=role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def reset_password(db: Session, user: User, new_password: str) -> None:
    user.password_hash = hash_password(new_password)
    user.token_version += 1  # revoke every existing cookie
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()
    db.refresh(user)


def change_password(db: Session, user: User, old_password: str, new_password: str) -> bool:
    if not verify_password(old_password, user.password_hash):
        return False
    reset_password(db, user, new_password)
    return True


def set_active(db: Session, user: User, active: bool) -> None:
    user.is_active = active
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()


def set_role(db: Session, user: User, role: str) -> None:
    if role not in ("admin", "user"):
        raise ValueError(f"unknown role {role!r}")
    user.role = role
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()


def list_users(db: Session) -> list[User]:
    return list(db.exec(select(User).order_by(User.username)))


VALID_COLOR_SCHEMES = {"green_up", "red_up"}


def set_color_scheme(db: Session, user: User, scheme: str) -> bool:
    """設定使用者 K 線配色。scheme 非法則回 False 且不寫入。"""
    if scheme not in VALID_COLOR_SCHEMES:
        return False
    user.chart_color_scheme = scheme
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()
    return True


VALID_THEMES = {"dark", "light"}


def set_theme(db: Session, user: User, theme: str) -> bool:
    """設定介面主題。theme 非法則回 False 且不寫入。"""
    if theme not in VALID_THEMES:
        return False
    user.theme = theme
    user.updated_at = _utcnow()
    db.add(user)
    db.commit()
    return True
