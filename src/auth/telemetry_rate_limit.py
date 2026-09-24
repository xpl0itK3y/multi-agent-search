"""Identity and per-user throttle for client telemetry ingestion (ABUSE-ROUTES).

Telemetry is authenticated-only: every stored event belongs to a real account, so the
budget is per user. In-process like the admin limiter — the SPA sends one heartbeat per
30 s per tab, far under the default, so a per-worker window only has to stop floods.
"""

from __future__ import annotations

from fastapi import HTTPException, Request

from src.api.dependencies import get_current_user, resolve_request_user_id
from src.auth.login_rate_limit import SlidingWindowLimiter
from src.config import settings


_telemetry_limiter = SlidingWindowLimiter()


def telemetry_user_id(request: Request) -> str | None:
    """The account a telemetry event is recorded for, after consuming one allowance.

    Auth enabled: the authenticated user (401 when anonymous or the token is revoked).
    Auth disabled: the real account behind a current token, else None — the shared local
    identity has no users row, so its events are dropped instead of stored anonymously."""
    if settings.auth_disabled:
        user_id = resolve_request_user_id(request)
    else:
        user_id = get_current_user(request).id
    limit = settings.telemetry_rate_limit_per_minute
    if user_id and limit > 0 and not _telemetry_limiter.allow(user_id, limit):
        raise HTTPException(status_code=429, detail="Too many telemetry events, please slow down")
    return user_id


def reset_telemetry_rate_limiter() -> None:
    """Reset limiter state (used in tests)."""
    _telemetry_limiter.reset()
