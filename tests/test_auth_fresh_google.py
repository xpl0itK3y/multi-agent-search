"""SEC2-3: a stolen session alone cannot add a password login to someone's Google account.

Sessions minted by the Google OAuth callback carry amr=["google"]. Setting the first
password of a passwordless account, resetting the password of a Google-linked account
without the current one (recovery), and deleting a passwordless account need such a token
issued at most 10 minutes earlier; otherwise the API answers 403 with a detail starting
with 'reauth_required'. Password-login, register and set-password tokens never qualify,
and the current_password path is unchanged."""
import time
import urllib.parse
import uuid
from unittest import mock

import httpx
import pytest

from src.api.app import create_app
from src.auth import security
from src.auth.security import (
    AUTH_METHOD_GOOGLE,
    FRESH_GOOGLE_AUTH_MAX_AGE_SECONDS,
    create_token,
    decode_token,
    is_fresh_google_auth,
)
from src.config import settings
from src.domain.errors import BadRequestError, ForbiddenError, UnauthorizedError
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

# ── is_fresh_google_auth ─────────────────────────────────────────────────────


def _claims(age: int, amr=(AUTH_METHOD_GOOGLE,)):
    claims = {"sub": "u", "iat": int(time.time()) - age}
    if amr is not None:
        claims["amr"] = list(amr) if isinstance(amr, tuple) else amr
    return claims


@pytest.mark.parametrize("age", [0, 5, FRESH_GOOGLE_AUTH_MAX_AGE_SECONDS - 5, -30])
def test_recent_google_sign_in_is_fresh(age):
    assert is_fresh_google_auth(_claims(age)) is True


@pytest.mark.parametrize(
    "claims",
    [
        None,
        {},
        _claims(FRESH_GOOGLE_AUTH_MAX_AGE_SECONDS + 5),
        _claims(-3600),  # issued "in the future", beyond clock skew
        _claims(0, amr=None),  # password login, register, set-password
        _claims(0, amr="google"),  # not a list
        _claims(0, amr=("pwd",)),
        {"sub": "u", "amr": ["google"]},  # no iat
        {"sub": "u", "amr": ["google"], "iat": "soon"},
    ],
    ids=["none", "empty", "stale", "future", "no-amr", "amr-str", "pwd", "no-iat", "bad-iat"],
)
def test_other_claims_are_not_fresh(claims):
    assert is_fresh_google_auth(claims) is False


def test_create_token_records_amr_only_when_given():
    assert "amr" not in decode_token(create_token("u"))
    assert decode_token(create_token("u", amr=[AUTH_METHOD_GOOGLE]))["amr"] == ["google"]


# ── service policy ───────────────────────────────────────────────────────────


def _google_user(service, email="g@example.com", subject="g-sub"):
    user, _created = service.get_or_create_oauth_user(email, google_subject=subject)
    return user


@pytest.fixture
def service():
    return ResearchService(task_store=InMemoryTaskStore())


def test_first_password_needs_a_fresh_google_sign_in(service):
    user = _google_user(service)

    with pytest.raises(ForbiddenError) as refused:
        service.set_user_password(user.id, "attacker-pass1")
    assert refused.value.detail.startswith("reauth_required")
    # A current_password is no substitute: there is none to check.
    with pytest.raises(ForbiddenError):
        service.set_user_password(user.id, "attacker-pass1", current_password="anything")
    assert service.task_store.get_user_by_id(user.id).password_hash is None

    updated = service.set_user_password(user.id, "owner-pass1", fresh_google_auth=True)
    assert updated.token_version == user.token_version + 1
    assert service.authenticate_user("g@example.com", "owner-pass1").id == user.id


def test_google_linked_reset_without_current_password_needs_a_fresh_sign_in(service):
    user = _google_user(service)
    service.set_user_password(user.id, "someone-elses1", fresh_google_auth=True)

    with pytest.raises(ForbiddenError) as refused:
        service.set_user_password(user.id, "owner-pass1")
    assert refused.value.detail.startswith("reauth_required")

    # Recovery: the owner signs in with Google again and replaces it without knowing it.
    service.set_user_password(user.id, "owner-pass1", fresh_google_auth=True)
    assert service.authenticate_user("g@example.com", "owner-pass1").id == user.id
    with pytest.raises(UnauthorizedError):
        service.authenticate_user("g@example.com", "someone-elses1")


def test_current_password_path_is_unchanged(service):
    user = _google_user(service)
    service.set_user_password(user.id, "first-pass1", fresh_google_auth=True)

    service.set_user_password(user.id, "second-pass1", current_password="first-pass1")
    # A supplied current password is always checked, fresh sign-in or not.
    with pytest.raises(UnauthorizedError):
        service.set_user_password(user.id, "third-pass1", current_password="wrong", fresh_google_auth=True)
    assert service.authenticate_user("g@example.com", "second-pass1").id == user.id


def test_local_account_still_needs_its_current_password(service):
    user = service.register_user("local@example.com", "local-pass1")

    for fresh in (False, True):  # a local account has no Google identity to re-prove
        with pytest.raises(BadRequestError):
            service.set_user_password(user.id, "new-pass-12", fresh_google_auth=fresh)
    service.set_user_password(user.id, "new-pass-12", current_password="local-pass1")


def test_passwordless_deletion_needs_a_fresh_google_sign_in(service):
    user = _google_user(service)

    with pytest.raises(ForbiddenError) as refused:
        service.delete_user_account(user.id, confirm=True)
    assert refused.value.detail.startswith("reauth_required")
    with pytest.raises(BadRequestError):  # confirm is checked first, as before
        service.delete_user_account(user.id, fresh_google_auth=True)
    assert service.task_store.get_user_by_id(user.id) is not None

    service.delete_user_account(user.id, confirm=True, fresh_google_auth=True)
    assert service.task_store.get_user_by_id(user.id) is None


def test_deletion_of_an_account_with_a_password_still_needs_it(service):
    user = _google_user(service)
    service.set_user_password(user.id, "owner-pass1", fresh_google_auth=True)

    with pytest.raises(BadRequestError):
        service.delete_user_account(user.id, confirm=True, fresh_google_auth=True)
    service.delete_user_account(user.id, confirm=True, current_password="owner-pass1")
    assert service.task_store.get_user_by_id(user.id) is None


# ── API ──────────────────────────────────────────────────────────────────────


@pytest.fixture
async def api(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "fresh-auth-secret-" + "x" * 40, raising=False)
    monkeypatch.setattr(settings, "google_client_id", "cid", raising=False)
    monkeypatch.setattr(settings, "google_client_secret", "sec", raising=False)
    app = create_app()
    async with app.router.lifespan_context(app):
        app.state.research_service = ResearchService(task_store=InMemoryTaskStore())

        def client():
            return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://testserver")

        yield app.state.research_service, client


async def _google_sign_in(client, email, subject):
    login = await client.get("/v1/auth/google/login", follow_redirects=False)
    state = urllib.parse.parse_qs(urllib.parse.urlsplit(login.headers["location"]).query)["state"][0]
    with mock.patch(
        "src.api.app.fetch_userinfo",
        return_value={"email": email, "email_verified": True, "sub": subject},
    ):
        callback = await client.get(f"/v1/auth/google/callback?code=c&state={state}", follow_redirects=False)
    assert callback.status_code == 302, callback.text
    return client.cookies[settings.auth_cookie_name]


def _csrf(client):
    return {"X-CSRF-Token": client.cookies[settings.csrf_cookie_name]}


def _bearer(token):
    return {"Authorization": f"Bearer {token}"}


def _session_token(user, *, age=0, google=True):
    """A session token for ``user`` issued ``age`` seconds ago."""
    with mock.patch.object(security.time, "time", return_value=time.time() - age):
        return create_token(
            user.id,
            email=user.email,
            token_version=user.token_version,
            amr=[AUTH_METHOD_GOOGLE] if google else None,
        )


def _ids():
    tag = uuid.uuid4().hex[:8]
    return f"fresh-{tag}@example.com", f"g-{tag}"


@pytest.mark.anyio
async def test_google_callback_session_carries_the_google_amr(api):
    _service, make_client = api
    email, subject = _ids()
    async with make_client() as client:
        token = await _google_sign_in(client, email, subject)
        registered = await client.post("/v1/auth/register", json={"email": "p" + email, "password": "secret123"})

    claims = decode_token(token)
    assert claims["amr"] == ["google"]
    assert is_fresh_google_auth(claims) is True
    assert "amr" not in decode_token(registered.json()["access_token"])


@pytest.mark.anyio
async def test_new_google_user_sets_a_password_right_after_signing_in(api):
    _service, make_client = api
    email, subject = _ids()
    async with make_client() as client:
        await _google_sign_in(client, email, subject)

        response = await client.post("/v1/auth/set-password", json={"password": "owner-pass1"}, headers=_csrf(client))

        assert response.status_code == 200, response.text
        # The session set-password returns is a plain one: no second reset from it.
        assert "amr" not in decode_token(response.json()["access_token"])
        again = await client.post("/v1/auth/set-password", json={"password": "other-pass1"}, headers=_csrf(client))
    assert again.status_code == 403
    assert again.json()["detail"].startswith("reauth_required")


@pytest.mark.anyio
async def test_stale_or_plain_session_cannot_set_the_first_password(api):
    service, make_client = api
    email, subject = _ids()
    user, _created = service.get_or_create_oauth_user(email, google_subject=subject)
    stale = _session_token(user, age=FRESH_GOOGLE_AUTH_MAX_AGE_SECONDS + 60)
    plain = _session_token(user, google=False)

    async with make_client() as client:
        for token in (stale, plain):
            response = await client.post(
                "/v1/auth/set-password", json={"password": "attacker-pass1"}, headers=_bearer(token)
            )
            assert response.status_code == 403
            assert response.json()["detail"].startswith("reauth_required")
    assert service.task_store.get_user_by_id(user.id).password_hash is None


@pytest.mark.anyio
async def test_owner_recovers_a_google_account_someone_added_a_password_to(api):
    service, make_client = api
    email, subject = _ids()
    user, _created = service.get_or_create_oauth_user(email, google_subject=subject)
    service.set_user_password(user.id, "attacker-pass1", fresh_google_auth=True)

    async with make_client() as attacker:
        attacker_token = (
            await attacker.post("/v1/auth/login", json={"email": email, "password": "attacker-pass1"})
        ).json()["access_token"]
        # A password-login session never counts as a fresh Google sign-in.
        blocked = await attacker.post(
            "/v1/auth/set-password", json={"password": "locked-out1"}, headers=_bearer(attacker_token)
        )
        assert blocked.status_code == 403
        assert blocked.json()["detail"].startswith("reauth_required")

    async with make_client() as owner:
        await _google_sign_in(owner, email, subject)
        recovered = await owner.post("/v1/auth/set-password", json={"password": "owner-pass1"}, headers=_csrf(owner))
        assert recovered.status_code == 200, recovered.text

        # Every earlier session is revoked and only the owner's password works.
        assert (await owner.get("/v1/auth/me", headers=_bearer(attacker_token))).status_code == 401
        login_old = await owner.post("/v1/auth/login", json={"email": email, "password": "attacker-pass1"})
        login_new = await owner.post("/v1/auth/login", json={"email": email, "password": "owner-pass1"})
    assert login_old.status_code == 401
    assert login_new.status_code == 200


@pytest.mark.anyio
async def test_current_password_change_needs_no_google_sign_in(api):
    service, make_client = api
    email, subject = _ids()
    user, _created = service.get_or_create_oauth_user(email, google_subject=subject)
    service.set_user_password(user.id, "first-pass1", fresh_google_auth=True)

    async with make_client() as client:
        token = (await client.post("/v1/auth/login", json={"email": email, "password": "first-pass1"})).json()[
            "access_token"
        ]
        changed = await client.post(
            "/v1/auth/set-password",
            json={"password": "second-pass1", "current_password": "first-pass1"},
            headers=_bearer(token),
        )
    assert changed.status_code == 200, changed.text


@pytest.mark.anyio
async def test_passwordless_account_deletion_needs_a_fresh_google_sign_in(api):
    service, make_client = api
    email, subject = _ids()
    user, _created = service.get_or_create_oauth_user(email, google_subject=subject)

    async with make_client() as client:
        for token in (_session_token(user, age=FRESH_GOOGLE_AUTH_MAX_AGE_SECONDS + 60), _session_token(user, google=False)):
            refused = await client.request(
                "DELETE", "/v1/auth/account", json={"confirm": True}, headers=_bearer(token)
            )
            assert refused.status_code == 403
            assert refused.json()["detail"].startswith("reauth_required")
        assert service.task_store.get_user_by_id(user.id) is not None

        deleted = await client.request(
            "DELETE", "/v1/auth/account", json={"confirm": True}, headers=_bearer(_session_token(user))
        )
    assert deleted.status_code == 200
    assert service.task_store.get_user_by_id(user.id) is None
