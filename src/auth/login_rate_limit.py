"""Brute-force protection for the auth endpoints (AUD-027).

Small in-process sliding-window limiters (per API worker), keyed three ways:

- per client IP on login/register (``enforce_auth_rate_limit``);
- per normalized account email on login (``enforce_login_account_rate_limit``), so
  password guesses spread over many addresses still hit one budget per account;
- per user on the routes that verify ``current_password`` (set-password, delete
  account), so a stolen session cannot be used to guess the password.

They are intentionally a no-op when auth is disabled — the app is fully open in that
mode, so throttling the login form would add nothing. For a cross-worker global limit,
back this with Redis (the same pattern as providers/rate_limit.py); per-worker is enough
to blunt a brute force against 2 API workers.
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request

from src.api.dependencies import get_current_user
from src.api.schemas import AuthUser
from src.config import settings


class SlidingWindowLimiter:
    def __init__(self, window_seconds: float = 60.0) -> None:
        self._window = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: str, limit: int) -> bool:
        now = time.monotonic()
        cutoff = now - self._window
        with self._lock:
            hits = self._hits[key]
            while hits and hits[0] < cutoff:
                hits.popleft()
            if len(hits) >= limit:
                return False
            hits.append(now)
            return True

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()


_auth_limiter = SlidingWindowLimiter()
_account_limiter = SlidingWindowLimiter()
_password_check_limiter = SlidingWindowLimiter()


def reset_auth_rate_limiter() -> None:
    """Test hook: drop all recorded hits so suites don't share the auth windows."""
    for limiter in (_auth_limiter, _account_limiter, _password_check_limiter):
        limiter.reset()


def _auth_limit() -> int:
    """Allowed attempts per minute, or 0 when throttling is off."""
    if settings.auth_disabled:
        return 0
    return max(settings.auth_rate_limit_per_minute, 0)


def enforce_auth_rate_limit(request: Request) -> None:
    limit = _auth_limit()
    if not limit:
        return
    client = request.client
    key = client.host if client else "unknown"
    if not _auth_limiter.allow(key, limit):
        raise HTTPException(status_code=429, detail="Too many attempts, please slow down")


def enforce_login_account_rate_limit(email: str) -> None:
    """Per-account login budget. Keyed on the submitted email whether or not it exists,
    so the 429 reveals nothing about which accounts are registered."""
    limit = _auth_limit()
    if not limit:
        return
    if not _account_limiter.allow((email or "").strip().lower(), limit):
        raise HTTPException(status_code=429, detail="Too many attempts, please slow down")


def enforce_password_check_rate_limit(request: Request) -> AuthUser:
    """Require an identity and consume one per-user current-password check allowance."""
    user = get_current_user(request)
    limit = _auth_limit()
    if limit and not _password_check_limiter.allow(user.id, limit):
        raise HTTPException(status_code=429, detail="Too many attempts, please slow down")
    return user
