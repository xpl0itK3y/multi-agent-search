"""SEC2-2: POST /v1/auth/logout signs out everywhere. It revokes every session token of the
account (token_version bump), clears the session cookies, and always answers 200, also
without a valid session. A cookie session still needs the CSRF header, so a cross-site
request cannot sign a user out of every device."""
import logging
import uuid

import httpx
import pytest

from src.api.app import create_app
from src.auth.security import create_token
from src.config import settings
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


@pytest.fixture
async def auth_app(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "logout-test-secret-" + "x" * 40, raising=False)
    app = create_app()
    async with app.router.lifespan_context(app):
        app.state.research_service = ResearchService(task_store=InMemoryTaskStore())

        def client():
            return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver")

        yield app, client


def _email() -> str:
    return f"logout-{uuid.uuid4().hex[:8]}@example.com"


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _register(client, email: str, password: str = "secret123") -> str:
    response = await client.post("/v1/auth/register", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


@pytest.mark.anyio
async def test_logout_revokes_the_bearer_token_it_was_sent_with(auth_app):
    _app, make_client = auth_app
    async with make_client() as client:
        token = await _register(client, _email())
        client.cookies.clear()  # bearer only, like an API client
        assert (await client.get("/v1/auth/me", headers=_bearer(token))).status_code == 200

        out = await client.post("/v1/auth/logout", headers=_bearer(token))

        assert out.status_code == 200
        assert out.json() == {"status": "ok", "revoked": True}
        assert (await client.get("/v1/auth/me", headers=_bearer(token))).status_code == 401


@pytest.mark.anyio
async def test_logout_signs_out_every_device(auth_app):
    _app, make_client = auth_app
    email = _email()
    async with make_client() as laptop, make_client() as phone:
        laptop_token = await _register(laptop, email)
        login = await phone.post("/v1/auth/login", json={"email": email, "password": "secret123"})
        phone_token = login.json()["access_token"]
        assert (await phone.get("/v1/auth/me")).status_code == 200  # phone's cookie session

        await laptop.post("/v1/auth/logout", headers=_bearer(laptop_token))

        assert (await phone.get("/v1/auth/me")).status_code == 401
        assert (await phone.get("/v1/auth/me", headers=_bearer(phone_token))).status_code == 401
        # The password is untouched: signing in again works and gives a live session.
        again = await phone.post("/v1/auth/login", json={"email": email, "password": "secret123"})
        assert again.status_code == 200
        assert (await phone.get("/v1/auth/me", headers=_bearer(again.json()["access_token"]))).status_code == 200


@pytest.mark.anyio
async def test_cookie_logout_clears_cookies_and_revokes_the_cookie_token(auth_app):
    _app, make_client = auth_app
    async with make_client() as client:
        await _register(client, _email())
        cookie_token = client.cookies[settings.auth_cookie_name]

        out = await client.post("/v1/auth/logout", headers={"X-CSRF-Token": client.cookies["csrf_token"]})

        assert out.status_code == 200
        cleared = " ".join(out.headers.get_list("set-cookie"))
        assert f"{settings.auth_cookie_name}=" in cleared and f"{settings.csrf_cookie_name}=" in cleared
        assert settings.auth_cookie_name not in client.cookies
        assert (await client.get("/v1/auth/me", headers=_bearer(cookie_token))).status_code == 401


@pytest.mark.anyio
async def test_cookie_logout_without_csrf_header_is_refused_and_revokes_nothing(auth_app):
    _app, make_client = auth_app
    async with make_client() as client:
        token = await _register(client, _email())

        forced = await client.post("/v1/auth/logout")

        assert forced.status_code == 403
        assert (await client.get("/v1/auth/me")).status_code == 200
        assert (await client.get("/v1/auth/me", headers=_bearer(token))).status_code == 200


@pytest.mark.anyio
@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"Authorization": "Bearer not.a.jwt"},
        {"Authorization": f"Bearer {create_token('no-such-user')}"},
    ],
    ids=["no-session", "garbage-bearer", "unknown-user"],
)
async def test_logout_without_a_valid_session_is_still_200(auth_app, headers):
    _app, make_client = auth_app
    async with make_client() as client:
        out = await client.post("/v1/auth/logout", headers=headers)

    assert out.status_code == 200
    assert out.json() == {"status": "ok", "revoked": False}


@pytest.mark.anyio
async def test_logout_with_an_already_revoked_token_is_200_and_revokes_nothing_more(auth_app):
    app, make_client = auth_app
    store = app.state.research_service.task_store
    async with make_client() as client:
        token = await _register(client, _email())
        client.cookies.clear()
        await client.post("/v1/auth/logout", headers=_bearer(token))
        version = store.get_user_by_id(_subject(token)).token_version

        again = await client.post("/v1/auth/logout", headers=_bearer(token))

    assert again.status_code == 200 and again.json()["revoked"] is False
    assert store.get_user_by_id(_subject(token)).token_version == version


@pytest.mark.anyio
async def test_logout_still_clears_cookies_when_revocation_fails(auth_app, mocker, caplog):
    app, make_client = auth_app
    async with make_client() as client:
        await _register(client, _email())
        mocker.patch.object(
            app.state.research_service, "revoke_user_sessions", side_effect=RuntimeError("db went away")
        )

        with caplog.at_level(logging.ERROR, logger="src.api.app"):
            out = await client.post("/v1/auth/logout", headers={"X-CSRF-Token": client.cookies["csrf_token"]})

        assert out.status_code == 200
        assert out.json() == {"status": "ok", "revoked": False}
        assert settings.auth_cookie_name not in client.cookies
    assert any(record.getMessage() == "logout_revoke_failed" for record in caplog.records)


def _subject(token: str) -> str:
    from src.auth.security import decode_token

    return decode_token(token)["sub"]


def test_revoke_user_sessions_bumps_the_version_and_keeps_the_password():
    service = ResearchService(task_store=InMemoryTaskStore())
    user = service.register_user("keep-pass@example.com", "secret123")
    stored_hash = service.task_store.get_user_by_id(user.id).password_hash

    assert service.revoke_user_sessions(user.id) is True

    after = service.task_store.get_user_by_id(user.id)
    assert after.token_version == user.token_version + 1
    assert after.password_hash == stored_hash
    assert service.authenticate_user("keep-pass@example.com", "secret123").id == user.id


def test_revoke_user_sessions_keeps_a_passwordless_account_passwordless():
    service = ResearchService(task_store=InMemoryTaskStore())
    user, _created = service.get_or_create_oauth_user("google-only@example.com", "g-sub-1")

    assert service.revoke_user_sessions(user.id) is True

    after = service.task_store.get_user_by_id(user.id)
    assert after.password_hash is None
    assert after.token_version == user.token_version + 1
    assert service.revoke_user_sessions("no-such-user") is False


@pytest.mark.postgres
def test_revoke_user_sessions_on_postgres(postgres_session_factory):
    from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore

    service = ResearchService(task_store=SQLAlchemyTaskStore(postgres_session_factory))
    local = service.register_user("pg-logout@example.com", "secret123")
    google, _created = service.get_or_create_oauth_user("pg-google@example.com", "g-pg-1")
    local_hash = service.task_store.get_user_by_id(local.id).password_hash

    assert service.revoke_user_sessions(local.id) is True
    assert service.revoke_user_sessions(google.id) is True
    assert service.revoke_user_sessions("missing") is False

    local_after = service.task_store.get_user_by_id(local.id)
    google_after = service.task_store.get_user_by_id(google.id)
    assert (local_after.token_version, local_after.password_hash) == (local.token_version + 1, local_hash)
    assert (google_after.token_version, google_after.password_hash) == (google.token_version + 1, None)
