"""Per-IP brute-force protection for the auth endpoints (AUD-027).

A small in-process sliding-window limiter (per API worker). It is intentionally a
no-op when auth is disabled — the app is fully open in that mode, so throttling the
login form would add nothing. For a cross-worker global limit, back this with Redis
(the same pattern as providers/rate_limit.py); per-worker is enough to blunt a brute
force against 2 API workers.
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request

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


_auth_limiter = SlidingWindowLimiter()


def enforce_auth_rate_limit(request: Request) -> None:
    if settings.auth_disabled:
        return
    limit = settings.auth_rate_limit_per_minute
    if limit <= 0:
        return
    client = request.client
    key = client.host if client else "unknown"
    if not _auth_limiter.allow(key, limit):
        raise HTTPException(status_code=429, detail="Too many attempts, please slow down")
