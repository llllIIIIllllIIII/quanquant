"""Signed session-cookie payloads (itsdangerous). No server-side session table:
the cookie carries {uid, tv}; every request re-loads the user and checks
is_active + token_version, so deactivation and password changes revoke
immediately."""
import logging
import secrets

from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from quanquant.config import get_settings

SESSION_COOKIE = "qq_session"
MAX_AGE_SECONDS = 30 * 24 * 3600  # 30 days

log = logging.getLogger(__name__)
_fallback_secret: str | None = None  # stable within the process (dev/tests)


def _secret() -> str:
    global _fallback_secret
    configured = get_settings().session_secret
    if configured:
        return configured
    if _fallback_secret is None:
        _fallback_secret = secrets.token_urlsafe(32)
        log.warning("SESSION_SECRET unset — transient signing key (dev only); "
                    "a restart logs everyone out")
    return _fallback_secret


def _serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(_secret(), salt="qq-session")


def sign_session(user_id: int, token_version: int) -> str:
    return _serializer().dumps({"uid": user_id, "tv": token_version})


def load_session(value: str) -> dict | None:
    """{'uid': int, 'tv': int}, or None when invalid/expired/tampered."""
    try:
        data = _serializer().loads(value, max_age=MAX_AGE_SECONDS)
    except (BadSignature, SignatureExpired):
        return None
    if not isinstance(data, dict) or "uid" not in data or "tv" not in data:
        return None
    if not isinstance(data["uid"], int) or not isinstance(data["tv"], int):
        return None
    return data
