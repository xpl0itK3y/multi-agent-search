"""SEC-ADMIN-IDENTITY: ADMIN_EMAILS alone grants nothing. Sign-up verifies no email, so an
account holds admin rights only when its address is listed AND verified: linked to Google,
or provisioned by the operator with scripts/create_admin.py (users.admin_provisioned_at)."""
import importlib.util
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from src.api.app import create_app
from src.auth.admin_identity import has_admin_rights
from src.auth.security import create_token
from src.config import settings
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

ROOT = Path(__file__).resolve().parents[1]
LATE = "late-admin@example.com"
OTHER = "ops-admin@example.com"


@pytest.fixture
async def auth_client(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "admin-identity-" + "x" * 40, raising=False)
    monkeypatch.setattr(settings, "admin_emails", OTHER, raising=False)
    app = create_app()
    service = ResearchService(task_store=InMemoryTaskStore())
    async with app.router.lifespan_context(app):
        app.state.research_service = service
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client, service


def _load_create_admin():
    spec = importlib.util.spec_from_file_location("create_admin", ROOT / "scripts" / "create_admin.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _register(client, email: str, password: str = "squatter-pass1") -> str:
    response = await client.post("/v1/auth/register", json={"email": email, "password": password})
    assert response.status_code == 200
    client.cookies.clear()  # every request below authenticates with its own Bearer token
    return response.json()["access_token"]


def test_admin_rights_need_a_listed_email_and_a_verified_identity(monkeypatch):
    monkeypatch.setattr(settings, "admin_emails", "Ops@Example.com", raising=False)
    stamp = datetime.now(timezone.utc)

    assert has_admin_rights("ops@example.com", "google-sub", None) is True
    assert has_admin_rights(" OPS@example.com", None, stamp) is True
    assert has_admin_rights("ops@example.com", None, None) is False  # self-registered
    assert has_admin_rights("ops@example.com", "", None) is False
    assert has_admin_rights("user@example.com", "google-sub", stamp) is False  # not listed
    assert has_admin_rights(None, "google-sub", stamp) is False


@pytest.mark.anyio
async def test_account_registered_before_its_email_was_listed_is_not_admin(auth_client, monkeypatch):
    client, service = auth_client
    squatter_token = await _register(client, LATE)
    monkeypatch.setattr(settings, "admin_emails", f"{OTHER},{LATE}", raising=False)

    me = await client.get("/v1/auth/me", headers=_bearer(squatter_token))
    admin_list = await client.get("/v1/admin/users", headers=_bearer(squatter_token))

    assert me.status_code == 200 and me.json()["is_admin"] is False
    assert admin_list.status_code == 403
    squatter = service.task_store.get_user_by_email(LATE)
    assert service.task_store.get_admin_users_list(role="admin").users == []
    assert [u.id for u in service.task_store.get_admin_users_list(role="user").users] == [squatter.id]


@pytest.mark.anyio
async def test_create_admin_turns_the_listed_account_admin_and_revokes_the_squatter(auth_client, monkeypatch):
    client, service = auth_client
    squatter_token = await _register(client, LATE)
    monkeypatch.setattr(settings, "admin_emails", f"{OTHER},{LATE}", raising=False)

    user, created = _load_create_admin().create_admin(LATE, "operator-pass1", service=service)

    assert created is False and user.is_admin is True
    assert (await client.get("/v1/auth/me", headers=_bearer(squatter_token))).status_code == 401
    assert (await client.get("/v1/admin/users", headers=_bearer(squatter_token))).status_code == 401
    old_password = await client.post("/v1/auth/login", json={"email": LATE, "password": "squatter-pass1"})
    assert old_password.status_code == 401
    login = await client.post("/v1/auth/login", json={"email": LATE, "password": "operator-pass1"})
    assert login.status_code == 200 and login.json()["user"]["is_admin"] is True
    client.cookies.clear()
    admin_list = await client.get("/v1/admin/users", headers=_bearer(login.json()["access_token"]))
    assert admin_list.status_code == 200
    assert [(u["email"], u["is_admin"]) for u in admin_list.json()["users"]] == [(LATE, True)]


@pytest.mark.anyio
async def test_google_linked_admin_keeps_admin_rights(auth_client):
    client, service = auth_client
    admin, _created = service.get_or_create_oauth_user(OTHER, google_subject="sub-ops")
    token = create_token(admin.id, email=admin.email, token_version=admin.token_version)

    me = await client.get("/v1/auth/me", headers=_bearer(token))

    assert admin.is_admin is True and me.json()["is_admin"] is True
    assert (await client.get("/v1/admin/users", headers=_bearer(token))).status_code == 200


@pytest.mark.anyio
async def test_admin_can_delete_an_unverified_account_squatting_an_admin_email(auth_client, monkeypatch):
    client, service = auth_client
    await _register(client, LATE)
    squatter = service.task_store.get_user_by_email(LATE)
    admin, _created = service.get_or_create_oauth_user(OTHER, google_subject="sub-ops")
    monkeypatch.setattr(settings, "admin_emails", f"{OTHER},{LATE}", raising=False)
    token = create_token(admin.id, email=admin.email, token_version=admin.token_version)

    response = await client.delete(f"/v1/admin/users/{squatter.id}", headers=_bearer(token))

    assert response.status_code == 200
    assert service.task_store.get_user_by_id(squatter.id) is None


@pytest.mark.anyio
async def test_auth_disabled_admin_guard_applies_the_same_rule(auth_client, monkeypatch):
    """require_admin's AUTH_DISABLED + ADMIN_EMAILS branch checks the identity too."""
    client, service = auth_client
    squatter_token = await _register(client, LATE)
    admin, _created = service.get_or_create_oauth_user(OTHER, google_subject="sub-ops")
    monkeypatch.setattr(settings, "admin_emails", f"{OTHER},{LATE}", raising=False)
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    admin_token = create_token(admin.id, email=admin.email, token_version=admin.token_version)

    assert (await client.get("/v1/admin/users", headers=_bearer(squatter_token))).status_code == 403
    assert (await client.get("/v1/admin/users", headers=_bearer(admin_token))).status_code == 200
