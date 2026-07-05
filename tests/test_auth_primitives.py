"""Password hashing + signed session cookies."""
from quanquant.auth.passwords import hash_password, verify_password
from quanquant.auth.tokens import load_session, sign_session


def test_hash_roundtrip():
    h = hash_password("s3cret-pw")
    assert h != "s3cret-pw"
    assert verify_password("s3cret-pw", h)
    assert not verify_password("wrong", h)


def test_verify_garbage_hash_is_false():
    assert not verify_password("pw", "not-a-bcrypt-hash")


def test_session_roundtrip():
    token = sign_session(42, 3)
    data = load_session(token)
    assert data == {"uid": 42, "tv": 3}


def test_tampered_token_rejected():
    token = sign_session(42, 3)
    assert load_session(token[:-2] + "xx") is None
    assert load_session("garbage") is None
