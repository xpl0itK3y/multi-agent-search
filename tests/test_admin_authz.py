"""Unit tests for the admin authorization guard (AUD-003), exercised without a DB."""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from src.api.dependencies import LOCAL_USER, require_admin
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


def test_admin_initial_password_and_oauth_login(monkeypatch):
    from src.services import ResearchService
    from src.repositories.in_memory_task_store import InMemoryTaskStore

    store = InMemoryTaskStore()
    service = ResearchService(task_store=store)

    monkeypatch.setattr(settings, "admin_emails", "latundenis55@gmail.com", raising=False)

    # 1. User created via OAuth without password
    user, created = service.get_or_create_oauth_user("latundenis55@gmail.com", google_subject="sub123")
    assert created is True
    assert user.is_admin is True

    # 2. Subsequent OAuth login recognizes is_admin
    user_again, created_again = service.get_or_create_oauth_user("latundenis55@gmail.com", google_subject="sub123")
    assert created_again is False
    assert user_again.is_admin is True

    # 3. Setting initial password for admin on first password login succeeds and grants admin
    auth_user = service.authenticate_user("latundenis55@gmail.com", "supersecret123")
    assert auth_user.is_admin is True

    # 4. Subsequent login with same password succeeds
    auth_user_again = service.authenticate_user("latundenis55@gmail.com", "supersecret123")
    assert auth_user_again.is_admin is True

