from fastapi import HTTPException, Request

from src.api.schemas import AuthUser
from src.auth.security import verify_token
from src.config import settings
from src.services import ResearchService

# Identity used when auth is disabled (single-tenant / dev mode).
LOCAL_USER = AuthUser(id="local", email="local@local")


def get_research_service(request: Request) -> ResearchService:
    return request.app.state.research_service


def _extract_token(request: Request) -> str | None:
    """JWT from the ``Authorization: Bearer`` header, falling back to the cookie."""
    auth_header = request.headers.get("Authorization") or request.headers.get("authorization")
    if auth_header:
        scheme, _, value = auth_header.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
    return request.cookies.get(settings.auth_cookie_name)


def get_current_user(request: Request) -> AuthUser:
    """Authenticated user from a Bearer JWT or session cookie. Local user when auth is off."""
    if settings.auth_disabled:
        return LOCAL_USER
    token = _extract_token(request)
    user_id = verify_token(token) if token else None
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user = get_research_service(request).get_auth_user(user_id)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def scope_user_id(request: Request) -> str | None:
    """Owner/filter id for research scoping: None when auth is disabled (no scoping)."""
    if settings.auth_disabled:
        return None
    return get_current_user(request).id


def require_admin(request: Request) -> AuthUser:
    """Admin guard for job/queue maintenance routes. No-op identity when auth is disabled
    (trusted single-tenant mode); otherwise requires login and an allow-listed admin email."""
    user = get_current_user(request)
    if settings.auth_disabled:
        return user
    allowed = {e.strip().lower() for e in settings.admin_emails.split(",") if e.strip()}
    if user.email.lower() not in allowed:
        raise HTTPException(status_code=403, detail="Admin privileges required")
    return user


def verify_research_access(research_id: str, request: Request) -> None:
    """Route guard: 404 if the research belongs to another user (no-op when auth is off)."""
    if settings.auth_disabled:
        return
    user_id = get_current_user(request).id
    get_research_service(request)._ensure_research_access(research_id, user_id)
