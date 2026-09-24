from fastapi import HTTPException, Request

from src.api.schemas import AuthUser
from src.auth.security import decode_token
from src.config import settings
from src.services import ResearchService

# Identity used when auth is disabled (single-tenant / dev mode).
LOCAL_USER = AuthUser(id="local", email="local@local", is_admin=True)


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


def _get_admin_emails() -> set[str]:
    raw = getattr(settings, "admin_emails", "")
    if not raw:
        return set()
    if isinstance(raw, str):
        return {e.strip().lower() for e in raw.split(",") if e.strip()}
    return {str(e).strip().lower() for e in raw if str(e).strip()}


def is_admin_email(email: str | None) -> bool:
    """Whether the address is one of ADMIN_EMAILS (case-insensitive)."""
    return bool(email) and email.lower() in _get_admin_emails()


def _user_from_token(request: Request, token: str | None) -> AuthUser | None:
    """The account a token belongs to, or None when the token is missing, invalid, expired
    or revoked (its 'ver' no longer matches token_version, e.g. after a password change)."""
    claims = decode_token(token) if token else None
    user_id = claims.get("sub") if claims else None
    if not user_id:
        return None
    user = get_research_service(request).get_auth_user(user_id)
    try:
        token_version = int(claims.get("ver", 0))
    except (TypeError, ValueError):
        return None
    if user is None or token_version != user.token_version:
        return None
    return user


def resolve_request_user_id(request: Request) -> str | None:
    """Non-raising identity for attribution (activity, telemetry): the id behind the
    request's current, unrevoked token, else None. Independent of auth_disabled."""
    user = _user_from_token(request, _extract_token(request))
    return user.id if user else None


def request_token_subject(request: Request) -> str | None:
    """The 'sub' of the request's token after the signature and expiry check only: no DB
    read and no revocation check. A cheap per-user key (e.g. to throttle work before a
    lookup); never use it to attribute or authorize anything."""
    token = _extract_token(request)
    claims = decode_token(token) if token else None
    subject = claims.get("sub") if claims else None
    return subject if isinstance(subject, str) and subject else None


def get_current_user(request: Request) -> AuthUser:
    """Authenticated user from a Bearer JWT or session cookie. Local user when auth is off."""
    if settings.auth_disabled:
        return LOCAL_USER
    user = _user_from_token(request, _extract_token(request))
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    allowed = _get_admin_emails()
    if user.email.lower() in allowed:
        user.is_admin = True
    return user


def scope_user_id(request: Request) -> str | None:
    """Owner/filter id for research scoping: None when auth is disabled (no scoping)."""
    if settings.auth_disabled:
        return None
    return get_current_user(request).id


def require_admin(request: Request) -> AuthUser:
    """Admin guard for job/queue maintenance and admin routes.
    
    If auth is disabled:
      - If ADMIN_EMAILS is configured, the request must supply an authorized admin identity.
      - If ADMIN_EMAILS is empty, allow local/dev user.
    If auth is enabled:
      - Requires authenticated user whose email is in ADMIN_EMAILS.
    """
    allowed = _get_admin_emails()
    if settings.auth_disabled and not allowed:
        return LOCAL_USER

    if settings.auth_disabled and allowed:
        token = _extract_token(request)
        if not token:
            raise HTTPException(status_code=401, detail="Admin authentication required")
        # Same revocation rule as get_current_user: a token minted before a password
        # change must not keep admin access just because auth is otherwise off.
        user = _user_from_token(request, token)
        if user is None:
            raise HTTPException(status_code=401, detail="Invalid admin token")
        if user.email.lower() not in allowed:
            raise HTTPException(status_code=403, detail="Admin privileges required")
        user.is_admin = True
        return user

    user = get_current_user(request)
    if not user or user.email.lower() not in allowed:
        raise HTTPException(status_code=403, detail="Admin privileges required")
    user.is_admin = True
    return user


def verify_research_access(research_id: str, request: Request) -> None:
    """Route guard: 404 if the research belongs to another user (no-op when auth is off)."""
    if settings.auth_disabled:
        return
    user_id = get_current_user(request).id
    get_research_service(request)._ensure_research_access(research_id, user_id)
