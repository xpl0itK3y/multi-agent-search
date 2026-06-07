"""Password hashing (PBKDF2) and signed session tokens (HMAC) — stdlib only.

Avoids extra dependencies; HMAC-signed compact tokens are equivalent in strength
to JWT HS256 for our session-cookie use.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time

from src.config import settings

_PBKDF2_ITERATIONS = 200_000


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${_PBKDF2_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iterations, salt_hex, digest_hex = stored.split("$")
        if algo != "pbkdf2_sha256":
            return False
        expected = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), bytes.fromhex(salt_hex), int(iterations)
        )
        return hmac.compare_digest(expected.hex(), digest_hex)
    except Exception:
        return False


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _sign(payload_b64: str) -> str:
    signature = hmac.new(
        settings.auth_secret_key.encode(), payload_b64.encode(), hashlib.sha256
    ).digest()
    return _b64encode(signature)


def create_token(user_id: str, *, ttl_seconds: int | None = None) -> str:
    ttl = ttl_seconds if ttl_seconds is not None else settings.auth_token_ttl_seconds
    payload = {"sub": user_id, "exp": int(time.time()) + ttl}
    payload_b64 = _b64encode(json.dumps(payload, separators=(",", ":")).encode())
    return f"{payload_b64}.{_sign(payload_b64)}"


def verify_token(token: str) -> str | None:
    """Return the user id if the token is valid and unexpired, else None."""
    try:
        payload_b64, signature = token.split(".")
        if not hmac.compare_digest(signature, _sign(payload_b64)):
            return None
        payload = json.loads(_b64decode(payload_b64))
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        return payload.get("sub")
    except Exception:
        return None
