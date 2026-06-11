"""Password hashing (PBKDF2) and JWT session tokens (HS256) — stdlib only.

Issues standard JWTs (header.payload.signature, base64url, HMAC-SHA256) without a
third-party library, so tokens are interoperable (decodable on jwt.io) and usable
as ``Authorization: Bearer`` or in an httpOnly cookie.
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


def _sign(signing_input: str) -> str:
    """HMAC-SHA256 over the JWT signing input (``header_b64.payload_b64``)."""
    signature = hmac.new(
        settings.auth_secret_key.encode(), signing_input.encode(), hashlib.sha256
    ).digest()
    return _b64encode(signature)


def _encode_segment(data: dict) -> str:
    return _b64encode(json.dumps(data, separators=(",", ":")).encode())


def create_token(user_id: str, *, email: str | None = None, ttl_seconds: int | None = None) -> str:
    """Issue a signed HS256 JWT for ``user_id`` with iat/exp claims."""
    ttl = ttl_seconds if ttl_seconds is not None else settings.auth_token_ttl_seconds
    now = int(time.time())
    header = {"alg": "HS256", "typ": "JWT"}
    payload: dict = {"sub": user_id, "iat": now, "exp": now + ttl}
    if email:
        payload["email"] = email
    signing_input = f"{_encode_segment(header)}.{_encode_segment(payload)}"
    return f"{signing_input}.{_sign(signing_input)}"


def decode_token(token: str) -> dict | None:
    """Return the JWT claims if the token is well-formed, HS256-signed and unexpired."""
    try:
        header_b64, payload_b64, signature = token.split(".")
        signing_input = f"{header_b64}.{payload_b64}"
        if not hmac.compare_digest(signature, _sign(signing_input)):
            return None
        header = json.loads(_b64decode(header_b64))
        if header.get("alg") != "HS256" or header.get("typ") != "JWT":
            return None
        payload = json.loads(_b64decode(payload_b64))
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        return payload
    except Exception:
        return None


def verify_token(token: str) -> str | None:
    """Return the subject (user id) if the JWT is valid and unexpired, else None."""
    claims = decode_token(token)
    return claims.get("sub") if claims else None
