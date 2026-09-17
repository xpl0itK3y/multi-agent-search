"""Rate limiter for admin mutation endpoints (AUD-027 pattern).

Applies sliding-window rate limiting to admin write/mutation operations
(e.g., requeue, maintenance actions, cache cleanups).
"""

from __future__ import annotations

from fastapi import HTTPException, Request

from src.api.dependencies import require_admin
from src.api.schemas import AuthUser
from src.auth.login_rate_limit import SlidingWindowLimiter
from src.config import settings

_admin_mutation_limiter = SlidingWindowLimiter()


def enforce_admin_rate_limit(request: Request) -> AuthUser:
    """Ensure the caller has admin rights and consume one admin mutation allowance."""
    admin_user = require_admin(request)
    limit = settings.admin_rate_limit_per_minute
    if limit <= 0:
        return admin_user

    key = admin_user.email or admin_user.id
    if not _admin_mutation_limiter.allow(key, limit):
        raise HTTPException(
            status_code=429,
            detail="Too many admin operations, please slow down",
        )
    return admin_user


def reset_admin_rate_limiter() -> None:
    """Reset limiter state (used in tests)."""
    _admin_mutation_limiter.reset()

