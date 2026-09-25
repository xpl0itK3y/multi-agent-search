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
from collections import OrderedDict, deque

from fastapi import HTTPException, Request

from src.api.dependencies import get_current_user
from src.api.schemas import AuthUser
from src.config import settings


# Keys one limiter tracks at most. Keys are caller-chosen (an email typed into the login
# form, a client address), so the table must not grow with every distinct value: keys
# whose hits all left the window are dropped as they age out, and past this cap the key
# idle longest goes first. About 1 KB per key, so the cap bounds a limiter near 10 MB.
DEFAULT_MAX_KEYS = 10_000


class SlidingWindowLimiter:
    def __init__(self, window_seconds: float = 60.0, max_keys: int = DEFAULT_MAX_KEYS) -> None:
        self._window = window_seconds
        self._max_keys = max(1, max_keys)
        # Ordered by each key's latest hit, oldest first (a key moves to the end whenever
        # it records a hit), so expired keys, and the one to evict at the cap, sit in front.
        self._hits: OrderedDict[str, deque[float]] = OrderedDict()
        self._lock = threading.Lock()

    def allow(self, key: str, limit: int) -> bool:
        with self._lock:
            # Read the clock under the lock, so hits land in time order across threads.
            now = time.monotonic()
            cutoff = now - self._window
            self._drop_expired(cutoff)
            hits = self._hits.get(key)
            if hits is None:
                if limit <= 0:
                    return False
                if len(self._hits) >= self._max_keys:
                    self._hits.popitem(last=False)
                self._hits[key] = deque((now,))
                return True
            while hits and hits[0] < cutoff:
                hits.popleft()
            if len(hits) >= limit:
                return False
            hits.append(now)
            self._hits.move_to_end(key)
            return True

    def _drop_expired(self, cutoff: float) -> None:
        """Forget every key whose latest hit left the window (they are all at the front)."""
        while self._hits:
            _key, hits = next(iter(self._hits.items()))
            if hits and hits[-1] >= cutoff:
                return
            self._hits.popitem(last=False)

    def __len__(self) -> int:
        with self._lock:
            return len(self._hits)

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
