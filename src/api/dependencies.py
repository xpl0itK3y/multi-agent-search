from fastapi import HTTPException, Request

from src.api.schemas import AuthUser
from src.auth.security import verify_token
from src.config import settings
from src.services import ResearchService

# Identity used when auth is disabled (single-tenant / dev mode).
LOCAL_USER = AuthUser(id="local", email="local@local")


def get_research_service(request: Request) -> ResearchService:
    return request.app.state.research_service


def get_current_user(request: Request) -> AuthUser:
    """Authenticated user from the session cookie. When auth is disabled, a local user."""
    if settings.auth_disabled:
        return LOCAL_USER
    token = request.cookies.get(settings.auth_cookie_name)
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
