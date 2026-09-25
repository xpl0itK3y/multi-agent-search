from fastapi import HTTPException, Request

from src.api.schemas import AuthUser
from src.auth.admin_identity import admin_emails
from src.auth.security import decode_token, is_fresh_google_auth
from src.config import settings
from src.services import ResearchService

# Identity used when auth is disabled (single-tenant / dev mode).
LOCAL_USER = AuthUser(id="local", email="local@local", is_admin=True)


def get_research_service(request: Request) -> ResearchService:
    return request.app.state.research_service


def request_bearer_token(request: Request) -> str | None:
    """The token of the ``Authorization: Bearer <token>`` header, or None when there is no
    such header or its token is blank after strip().

    The one rule for "this request authenticates with a bearer token": _extract_token uses
    it, and so does the CSRF exemption for bearer requests (app._is_csrf_violation). If the
    two disagreed, a value that str.strip() empties (e.g. a lone NBSP) would skip the CSRF
    check and still authenticate with the session cookie."""
    auth_header = request.headers.get("Authorization") or request.headers.get("authorization")
    if auth_header:
        scheme, _, value = auth_header.partition(" ")
        if scheme.lower() == "bearer" and value.strip():
            return value.strip()
    return None


def _extract_token(request: Request) -> str | None:
    """JWT from the ``Authorization: Bearer`` header, falling back to the cookie."""
    return request_bearer_token(request) or request.cookies.get(settings.auth_cookie_name)


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


def request_has_fresh_google_auth(request: Request) -> bool:
    """Whether the request's session token was minted by the Google OAuth callback within
    the last FRESH_GOOGLE_AUTH_MAX_AGE_SECONDS: the fresh proof of identity that setting a
    first password, resetting one without the current password, or deleting a passwordless
    account needs. Only the claims are read here; authenticate the same request with
    get_current_user (which also checks revocation) before trusting the answer."""
    token = _extract_token(request)
    return is_fresh_google_auth(decode_token(token) if token else None)


def get_current_user(request: Request) -> AuthUser:
    """Authenticated user from a Bearer JWT or session cookie. Local user when auth is off."""
    if settings.auth_disabled:
        return LOCAL_USER
    user = _user_from_token(request, _extract_token(request))
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    # user.is_admin comes from the service (admin_identity.has_admin_rights): ADMIN_EMAILS
    # plus a verified identity, never the email alone.
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
      - Requires an authenticated admin.
    Either way an admin is an ADMIN_EMAILS account with a verified identity (Google-linked
    or provisioned by scripts/create_admin.py): see src/auth/admin_identity.py.
    """
    if settings.auth_disabled and not admin_emails():
        return LOCAL_USER

    if settings.auth_disabled:
        token = _extract_token(request)
        if not token:
            raise HTTPException(status_code=401, detail="Admin authentication required")
        # Same revocation rule as get_current_user: a token minted before a password
        # change must not keep admin access just because auth is otherwise off.
        user = _user_from_token(request, token)
        if user is None:
            raise HTTPException(status_code=401, detail="Invalid admin token")
    else:
        user = get_current_user(request)
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin privileges required")
    return user


def verify_research_access(research_id: str, request: Request) -> None:
    """Route guard: 404 if the research belongs to another user (no-op when auth is off)."""
    if settings.auth_disabled:
        return
    user_id = get_current_user(request).id
    get_research_service(request)._ensure_research_access(research_id, user_id)
