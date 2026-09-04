"""Per-user request throttle for API routes that can trigger LLM spending."""

from __future__ import annotations

from fastapi import HTTPException, Request

from src.api.dependencies import get_current_user, get_research_service
from src.api.schemas import AuthUser
from src.auth.login_rate_limit import SlidingWindowLimiter
from src.config import settings


_llm_route_limiter = SlidingWindowLimiter()


def enforce_llm_rate_limit(request: Request) -> AuthUser:
    """Require an identity and consume one per-user LLM-route allowance."""
    user = get_current_user(request)
    limit = settings.llm_route_rate_limit_per_minute
    if limit <= 0:
        return user

    service = get_research_service(request)
    broker = getattr(service, "broker", None)
    distributed_result = None
    if broker is not None and hasattr(broker, "allow_llm_request"):
        distributed_result = broker.allow_llm_request(user.id, limit)
    allowed = (
        distributed_result
        if distributed_result is not None
        else _llm_route_limiter.allow(user.id, limit)
    )
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Too many AI requests, please slow down",
        )
    return user
