"""Unit tests for the admin authorization guard (AUD-003), exercised without a DB."""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from src.api.dependencies import LOCAL_USER, require_admin, resolve_request_user_id
from src.api.schemas import AuthUser
from src.auth.security import create_token
from src.config import settings


def _request(headers: dict, user: AuthUser | None) -> Request:
    service = SimpleNamespace(get_auth_user=lambda _uid: user)
    app = SimpleNamespace(state=SimpleNamespace(research_service=service))
    scope = {
        "type": "http",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "app": app,
    }
    return Request(scope)


def test_require_admin_noop_when_auth_disabled(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "", raising=False)
    assert require_admin(_request({}, None)) is LOCAL_USER


def test_require_admin_rejects_anonymous_when_auth_disabled_but_admin_emails_set(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "admin@example.com", raising=False)
    with pytest.raises(HTTPException) as exc:
        require_admin(_request({}, None))
    assert exc.value.status_code == 401


def test_require_admin_rejects_non_admin_when_auth_disabled_but_admin_emails_set(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "x" * 48, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "admin@example.com", raising=False)
    user = AuthUser(id="u1", email="user@example.com")
    token = create_token("u1", email="user@example.com")
    with pytest.raises(HTTPException) as exc:
        require_admin(_request({"Authorization": f"Bearer {token}"}, user))
    assert exc.value.status_code == 403


def test_require_admin_allows_admin_when_auth_disabled_but_admin_emails_set(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "x" * 48, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "admin@example.com", raising=False)
    user = AuthUser(id="a1", email="admin@example.com")
    token = create_token("a1", email="admin@example.com")
    assert require_admin(_request({"Authorization": f"Bearer {token}"}, user)) is user


def test_require_admin_rejects_revoked_token_when_auth_disabled_but_admin_emails_set(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "x" * 48, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "admin@example.com", raising=False)
    # The password changed after this token was minted, so the stored version moved on.
    user = AuthUser(id="a1", email="admin@example.com", token_version=1)
    token = create_token("a1", email="admin@example.com", token_version=0)
    with pytest.raises(HTTPException) as exc:
        require_admin(_request({"Authorization": f"Bearer {token}"}, user))
    assert exc.value.status_code == 401


def test_resolve_request_user_id_only_accepts_current_tokens(monkeypatch):
    monkeypatch.setattr(settings, "auth_secret_key", "x" * 48, raising=False)
    user = AuthUser(id="u1", email="user@example.com", token_version=2)
    current = create_token("u1", token_version=2)
    revoked = create_token("u1", token_version=1)

    assert resolve_request_user_id(_request({"Authorization": f"Bearer {current}"}, user)) == "u1"
    assert resolve_request_user_id(_request({"Authorization": f"Bearer {revoked}"}, user)) is None
    assert resolve_request_user_id(_request({"Authorization": "Bearer not-a-jwt"}, user)) is None
    assert resolve_request_user_id(_request({}, user)) is None
    assert resolve_request_user_id(_request({"Authorization": f"Bearer {current}"}, None)) is None


def test_require_admin_rejects_anonymous(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "admin@example.com", raising=False)
    with pytest.raises(HTTPException) as exc:
        require_admin(_request({}, None))
    assert exc.value.status_code == 401


def test_require_admin_rejects_non_admin(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "x" * 48, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "admin@example.com", raising=False)
    user = AuthUser(id="u1", email="user@example.com")
    token = create_token("u1", email="user@example.com")
    with pytest.raises(HTTPException) as exc:
        require_admin(_request({"Authorization": f"Bearer {token}"}, user))
    assert exc.value.status_code == 403


def test_require_admin_allows_admin(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "x" * 48, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "admin@example.com, ops@example.com", raising=False)
    user = AuthUser(id="a1", email="admin@example.com")
    token = create_token("a1", email="admin@example.com")
    assert require_admin(_request({"Authorization": f"Bearer {token}"}, user)) is user


def _admin_service(monkeypatch, admin_email="owner-admin@example.com"):
    from src.repositories.in_memory_task_store import InMemoryTaskStore
    from src.services import ResearchService

    monkeypatch.setattr(settings, "admin_emails", admin_email, raising=False)
    store = InMemoryTaskStore()
    return store, ResearchService(task_store=store)


def test_admin_initial_password_and_oauth_login(monkeypatch):
    """A Google-created admin account never takes a first password from the login form.

    This used to assert the opposite: any 6+ character password typed for a passwordless
    ADMIN_EMAILS account was saved and returned an admin session (account takeover)."""
    from src.domain.errors import UnauthorizedError

    store, service = _admin_service(monkeypatch)

    # 1. Admin onboards through Google: passwordless account, admin by email.
    user, created = service.get_or_create_oauth_user("owner-admin@example.com", google_subject="sub123")
    assert created is True
    assert user.is_admin is True

    # 2. Subsequent Google sign-in still resolves the same admin account.
    user_again, created_again = service.get_or_create_oauth_user("owner-admin@example.com", google_subject="sub123")
    assert created_again is False
    assert user_again.id == user.id and user_again.is_admin is True

    # 3. Anyone typing the admin email with an arbitrary password is refused, and nothing is stored.
    with pytest.raises(UnauthorizedError):
        service.authenticate_user("owner-admin@example.com", "supersecret123")
    stored = store.get_user_by_id(user.id)
    assert stored.password_hash is None
    assert stored.token_version == user.token_version  # the real admin's sessions survive


@pytest.mark.anyio
async def test_login_with_arbitrary_password_for_passwordless_admin_is_401(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "owner-admin@example.com", raising=False)
    service = client._transport.app.state.research_service
    admin, _ = service.get_or_create_oauth_user("owner-admin@example.com", google_subject="sub-admin")

    response = await client.post(
        "/v1/auth/login", json={"email": "owner-admin@example.com", "password": "hunter22"}
    )

    assert response.status_code == 401
    assert "access_token" not in response.json()
    assert service.task_store.get_user_by_id(admin.id).password_hash is None


def test_register_refuses_admin_email(monkeypatch):
    from src.domain.errors import ForbiddenError

    store, service = _admin_service(monkeypatch)

    with pytest.raises(ForbiddenError) as exc:
        service.register_user("Owner-Admin@Example.com", "secret123")

    assert exc.value.status_code == 403
    assert "scripts/create_admin.py" in exc.value.detail
    assert store.get_user_by_email("owner-admin@example.com") is None


@pytest.mark.anyio
async def test_register_endpoint_refuses_admin_email(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "owner-admin@example.com", raising=False)

    response = await client.post(
        "/v1/auth/register", json={"email": "owner-admin@example.com", "password": "secret123"}
    )

    assert response.status_code == 403
    assert "Google" in response.json()["detail"]
    assert client.cookies.get(settings.auth_cookie_name) is None


def test_google_sign_in_for_existing_local_admin_email_account_conflicts(monkeypatch):
    """A local row for an admin email (e.g. squatted before registration was blocked) is never
    silently handed to the Google identity."""
    from src.domain.errors import ConflictError

    store, service = _admin_service(monkeypatch)
    squatter = store.create_user("squatter", "owner-admin@example.com", "pbkdf2_sha256$1$00$00")

    with pytest.raises(ConflictError) as exc:
        service.get_or_create_oauth_user("owner-admin@example.com", google_subject="sub-real-admin")

    assert exc.value.status_code == 409
    assert store.get_user_by_id(squatter.id).google_subject is None
    assert store.get_user_by_google_subject("sub-real-admin") is None
