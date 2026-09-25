"""SEC2-8: the admin CSV exports are GETs with side effects (an audit row, the shared admin
rate budget), so a cookie session must send the double-submit CSRF header on them, as on
a mutation. Otherwise a cross-site link or redirect, which carries the SameSite=Lax
session cookie, starts an export under the admin's name. Bearer requests stay exempt.

The app tests also run on the Postgres store over one shared database (postgres-smoke),
so ids are unique per test and assertions only look at this test's audit rows."""
import uuid

import pytest

from src.auth.security import create_token
from src.config import settings

EXPORTS = {
    "/v1/admin/users/export": "export_users",
    "/v1/admin/prompts/export": "export_prompts",
    "/v1/admin/tokens/export": "export_tokens",
}


@pytest.fixture
def admin(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    monkeypatch.setattr(settings, "auth_secret_key", "export-csrf-secret-" + "x" * 40)
    user_id = f"export-admin-{uuid.uuid4().hex[:8]}"
    email = f"{user_id}@example.com"
    monkeypatch.setattr(settings, "admin_emails", email)
    store = client._transport.app.state.research_service.task_store
    # Google-linked: a verified ADMIN_EMAILS identity (src/auth/admin_identity.py).
    store.create_user(user_id, email, None, google_subject=f"g-{user_id}")
    yield {"email": email, "token": create_token(user_id, email=email), "store": store}
    store.delete_user(user_id)


def _cookie_session(admin, csrf_header: str | None = "csrf-tok"):
    headers = {"Cookie": f"{settings.auth_cookie_name}={admin['token']}; {settings.csrf_cookie_name}=csrf-tok"}
    if csrf_header is not None:
        headers["X-CSRF-Token"] = csrf_header
    return headers


def _export_audits(admin, action):
    rows = admin["store"].get_admin_audit_logs(limit=200, action=action)
    return [row for row in rows if row.actor_email == admin["email"]]


@pytest.mark.anyio
@pytest.mark.parametrize("path", sorted(EXPORTS))
async def test_cookie_session_without_csrf_header_is_refused_and_not_audited(client, admin, path):
    missing = await client.get(path, headers=_cookie_session(admin, csrf_header=None))
    mismatched = await client.get(path, headers=_cookie_session(admin, csrf_header="forged"))

    assert missing.status_code == 403
    assert mismatched.status_code == 403
    assert missing.json() == {"detail": "CSRF token missing or invalid"}
    assert _export_audits(admin, EXPORTS[path]) == []


@pytest.mark.anyio
@pytest.mark.parametrize("path", sorted(EXPORTS))
async def test_cookie_session_with_matching_csrf_header_exports(client, admin, path):
    response = await client.get(path, headers=_cookie_session(admin))

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/csv")
    assert len(_export_audits(admin, EXPORTS[path])) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("path", sorted(EXPORTS))
async def test_bearer_request_needs_no_csrf_header(client, admin, path):
    response = await client.get(path, headers={"Authorization": f"Bearer {admin['token']}"})

    assert response.status_code == 200, response.text
    assert len(_export_audits(admin, EXPORTS[path])) == 1


@pytest.mark.anyio
async def test_other_admin_gets_and_preflights_stay_unchecked(client, admin):
    listing = await client.get("/v1/admin/users", headers=_cookie_session(admin, csrf_header=None))
    preflight = await client.options(
        "/v1/admin/users/export",
        headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "GET"},
    )

    assert listing.status_code == 200
    assert preflight.status_code != 403
