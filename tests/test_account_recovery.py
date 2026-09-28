"""AUTH-RECOVERY: password reset by email, email verification, and Google sign-in linking.

Sign-up verified no email, so a stranger's sign-up with someone's address blocked that
person's Google sign-in (409 oauth_conflict), and Google accounts from before migration
000019 (no google_subject, a random password) could not sign in at all. These tests pin
the contract that fixes both: one-time links sent to the address (reset, verification),
and a Google sign-in that links an unlinked local account, removing the password of one
whose address was never verified.
"""
import logging
import re
import secrets
import smtplib
import urllib.parse

import httpx
import pytest

from src.api.app import create_app
from src.auth.security import create_token, hash_password
from src.config import settings
from src.domain import AuthActionPurpose
from src.notifications import AccountEmail
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.services.account_recovery_mixin import (
    RESET_TOKEN_INVALID_DETAIL,
    VERIFICATION_TOKEN_INVALID_DETAIL,
    VERIFICATION_WRONG_ACCOUNT_DETAIL,
    hash_link_token,
)

RESET_DETAIL = "reset_token_invalid: this password reset link is invalid or has expired"
VERIFY_DETAIL = "verification_token_invalid: this verification link is invalid or has expired"
WRONG_ACCOUNT_DETAIL = "verification_wrong_account: sign in to the account this link was sent to"
TOO_MANY = "Too many attempts, please slow down"
_LINK = re.compile(r"(https?://\S+?)(/reset-password|/verify-email)#token=([A-Za-z0-9_-]+)")


class RecordingSender:
    enabled = True

    def __init__(self):
        self.sent = []

    def send(self, message) -> None:
        self.sent.append(message)


class FailingSender(RecordingSender):
    def send(self, message) -> None:
        raise smtplib.SMTPRecipientsRefused({message.to: (550, b"no such user " + message.body.encode())})


@pytest.fixture
async def recovery(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "recovery-test-secret-" + "x" * 40, raising=False)
    monkeypatch.setattr(settings, "email_backend", "console", raising=False)
    monkeypatch.setattr(settings, "public_app_url", "https://veris.example/", raising=False)
    sender = RecordingSender()
    service = ResearchService(task_store=InMemoryTaskStore(), mail_sender=sender)
    app = create_app()
    async with app.router.lifespan_context(app):
        app.state.research_service = service
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client, service, sender


def _email(tag: str = "") -> str:
    return f"owner-{tag or secrets.token_hex(4)}@example.com"


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _links(sender, path: str) -> list[str]:
    """The tokens of the links to ``path`` in the recorded messages, oldest first."""
    return [match.group(3) for message in sender.sent for match in _LINK.finditer(message.body) if match.group(2) == path]


async def _register(client, email: str, password: str = "first-pass1") -> str:
    response = await client.post("/v1/auth/register", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    client.cookies.clear()
    return response.json()["access_token"]


# ── config ────────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_auth_config_offers_recovery_only_when_email_goes_somewhere(client, monkeypatch):
    assert (await client.get("/v1/auth/config")).json() == {
        "google_oauth": False,
        "password_reset": False,
        "email_verification": False,
    }
    monkeypatch.setattr(settings, "email_backend", "smtp", raising=False)
    config = (await client.get("/v1/auth/config")).json()
    assert (config["password_reset"], config["email_verification"]) == (True, True)


# ── forgot password ───────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_forgot_password_answers_alike_for_known_and_unknown_addresses(recovery):
    client, service, sender = recovery
    email = _email()
    await _register(client, email)
    sender.sent.clear()

    known = await client.post("/v1/auth/password/forgot", json={"email": email.upper()})
    unknown = await client.post("/v1/auth/password/forgot", json={"email": _email()})

    assert (known.status_code, known.json()) == (202, {"status": "accepted"})
    assert (unknown.status_code, unknown.json()) == (202, {"status": "accepted"})
    assert known.headers.get("content-length") == unknown.headers.get("content-length")
    assert [message.to for message in sender.sent] == [email]  # only the account's address
    body = sender.sent[0].body
    assert "https://veris.example/reset-password#token=" in body  # no double slash
    assert "60 minutes" in body and email in body
    token = _links(sender, "/reset-password")[0]
    assert len(token) >= 43  # token_urlsafe(32)
    stored = [
        row
        for row in service.task_store.auth_action_tokens.values()
        if row["purpose"] == AuthActionPurpose.PASSWORD_RESET
    ]
    assert len(stored) == 1  # the unknown address stored nothing
    assert stored[0]["token_hash"] == hash_link_token(token) != token
    assert stored[0]["requested_ip"] == "127.0.0.1"


@pytest.mark.anyio
async def test_forgot_password_with_email_disabled_still_answers_202_and_stores_nothing(recovery, monkeypatch):
    client, service, _sender = recovery
    email = _email()
    await _register(client, email)
    service.mail_sender = None  # the EMAIL_BACKEND sender
    monkeypatch.setattr(settings, "email_backend", "disabled", raising=False)

    response = await client.post("/v1/auth/password/forgot", json={"email": email})

    assert (response.status_code, response.json()) == (202, {"status": "accepted"})
    purposes = [row["purpose"] for row in service.task_store.auth_action_tokens.values()]
    assert purposes == [AuthActionPurpose.EMAIL_VERIFICATION]  # the sign-up's, sent while email was on


@pytest.mark.anyio
async def test_forgot_password_is_throttled_per_address_whether_or_not_it_exists(recovery):
    client, _service, sender = recovery
    known = _email()
    await _register(client, known)
    sender.sent.clear()
    for address in (known, _email("nobody")):
        codes = [
            (await client.post("/v1/auth/password/forgot", json={"email": address})).status_code
            for _ in range(6)
        ]
        assert codes == [202] * 5 + [429]
    refused = await client.post("/v1/auth/password/forgot", json={"email": f" {known.upper()} "})
    assert (refused.status_code, refused.json()["detail"]) == (429, TOO_MANY)
    assert len(sender.sent) == 5


@pytest.mark.anyio
async def test_forgot_password_serves_a_new_address_when_the_throttle_is_full_of_lockouts(recovery, monkeypatch):
    """SEC-REC-4: enough addresses at their hourly limit (10,000 per API process) must not
    turn password recovery off for every other address."""
    from src.auth import login_rate_limit

    client, _service, sender = recovery
    monkeypatch.setattr(login_rate_limit._forgot_password_email_limiter, "_max_keys", 2)
    for address in (_email("flood-1"), _email("flood-2")):
        codes = [
            (await client.post("/v1/auth/password/forgot", json={"email": address})).status_code
            for _ in range(6)
        ]
        assert codes == [202] * 5 + [429]
    owner = _email()
    await _register(client, owner)
    sender.sent.clear()

    response = await client.post("/v1/auth/password/forgot", json={"email": owner})

    assert response.status_code == 202
    assert [message.to for message in sender.sent] == [owner]


@pytest.mark.anyio
async def test_forgot_password_is_throttled_per_client(recovery):
    client, _service, _sender = recovery
    codes = [
        (await client.post("/v1/auth/password/forgot", json={"email": _email(str(index))})).status_code
        for index in range(21)
    ]
    assert codes == [202] * 20 + [429]


@pytest.mark.anyio
async def test_the_anonymous_recovery_routes_need_no_csrf_token(recovery):
    client, _service, _sender = recovery
    email = _email()
    response = await client.post("/v1/auth/register", json={"email": email, "password": "first-pass1"})
    assert client.cookies.get(settings.auth_cookie_name)  # a cookie session, and no X-CSRF-Token below

    assert (await client.post("/v1/auth/password/forgot", json={"email": email})).status_code == 202
    assert (await client.post("/v1/auth/password/reset", json={"token": "x", "password": "new-pass1"})).status_code == 400
    # The signed-in ones keep the CSRF check, email verify included: it redeems in the session.
    csrf = {"X-CSRF-Token": client.cookies.get(settings.csrf_cookie_name)}
    for path, body, status in (("/v1/auth/email/verify", {"token": "x"}, 400), ("/v1/auth/email/verification", None, 202)):
        forged = await client.post(path, json=body)
        assert (forged.status_code, forged.json()["detail"]) == (403, "CSRF token missing or invalid"), path
        assert (await client.post(path, json=body, headers=csrf)).status_code == status, path
    assert response.json()["user"]["email_verified"] is False


# ── reset password ────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_a_reset_link_sets_the_password_verifies_the_email_and_signs_out_everywhere(recovery):
    client, service, sender = recovery
    email = _email()
    session = await _register(client, email)
    await client.post("/v1/auth/password/forgot", json={"email": email})
    token = _links(sender, "/reset-password")[-1]
    sender.sent.clear()

    response = await client.post(
        "/v1/auth/password/reset",
        json={"token": token, "password": "second-pass1"},
        headers={"Accept-Language": "es-ES,es;q=0.9"},
    )

    assert (response.status_code, response.json()) == (200, {"status": "ok"})
    assert settings.auth_cookie_name not in response.cookies  # no auto-login
    assert (await client.get("/v1/auth/me", headers=_bearer(session))).status_code == 401
    old = await client.post("/v1/auth/login", json={"email": email, "password": "first-pass1"})
    assert old.status_code == 401
    login = await client.post("/v1/auth/login", json={"email": email, "password": "second-pass1"})
    assert login.status_code == 200 and login.json()["user"]["email_verified"] is True
    # The notice, in the language of the reset request; it carries no link to redeem.
    assert [message.to for message in sender.sent] == [email]
    assert sender.sent[0].subject == "Se ha restablecido tu contraseña de Veris"
    assert "#token=" not in sender.sent[0].body

    again = await client.post("/v1/auth/password/reset", json={"token": token, "password": "third-pass12"})
    assert (again.status_code, again.json()["detail"]) == (400, RESET_DETAIL)
    assert again.json()["detail"].startswith("reset_token_invalid")
    assert service.authenticate_user(email, "second-pass1").email == email


@pytest.mark.anyio
async def test_reset_refuses_a_short_password_and_an_unknown_or_superseded_link(recovery):
    client, _service, sender = recovery
    email = _email()
    await _register(client, email)
    await client.post("/v1/auth/password/forgot", json={"email": email})
    await client.post("/v1/auth/password/forgot", json={"email": email})
    first, second = _links(sender, "/reset-password")

    short = await client.post("/v1/auth/password/reset", json={"token": second, "password": "12345"})
    assert short.status_code == 422
    for token in (first, "", "not-a-token", second.upper()):
        refused = await client.post("/v1/auth/password/reset", json={"token": token, "password": "second-pass1"})
        assert (refused.status_code, refused.json()["detail"]) == (400, RESET_DETAIL), token
    assert (
        await client.post("/v1/auth/password/reset", json={"token": second, "password": "second-pass1"})
    ).status_code == 200


@pytest.mark.anyio
async def test_reset_is_throttled_per_client(recovery, monkeypatch):
    client, _service, _sender = recovery
    monkeypatch.setattr("src.auth.security._PBKDF2_ITERATIONS", 1_000)  # each attempt hashes
    codes = [
        (await client.post("/v1/auth/password/reset", json={"token": f"t{index}", "password": "whatever1"})).status_code
        for index in range(31)
    ]
    assert codes == [400] * 30 + [429]


@pytest.mark.anyio
async def test_a_new_password_retires_the_reset_links_sent_before_it(recovery, monkeypatch):
    """SEC-REC-2: someone copied a reset link during brief access to the mailbox. The owner
    then secures the account (Settings, the operator, or a Google sign-in that removes an
    unverified sign-up's password): the copied link must not take it back."""
    client, service, sender = recovery

    email = _email()
    session = await _register(client, email)
    await client.post("/v1/auth/password/forgot", json={"email": email})
    copied_before_settings = _links(sender, "/reset-password")[-1]
    changed = await client.post(
        "/v1/auth/set-password",
        json={"password": "second-pass1", "current_password": "first-pass1"},
        headers=_bearer(session),
    )
    assert changed.status_code == 200

    monkeypatch.setattr(settings, "admin_emails", "ops@example.com", raising=False)
    service.provision_admin_account("ops@example.com", "operator-pass1")
    _user, link = service.issue_password_reset_link("ops@example.com")
    copied_before_provisioning = link.rpartition("#token=")[2]
    service.provision_admin_account("ops@example.com", "operator-pass2")

    squatted = _email()
    await _register(client, squatted, password="squatter-pass1")
    _user, link = service.issue_password_reset_link(squatted)
    copied_before_google = link.rpartition("#token=")[2]
    await _google_callback(client, monkeypatch, squatted, "g-clears")
    client.cookies.clear()

    for token in (copied_before_settings, copied_before_provisioning, copied_before_google):
        refused = await client.post("/v1/auth/password/reset", json={"token": token, "password": "taken-over1"})
        assert (refused.status_code, refused.json()["detail"]) == (400, RESET_DETAIL)
    assert service.authenticate_user(email, "second-pass1").email == email
    assert service.authenticate_user("ops@example.com", "operator-pass2").is_admin is True
    assert service.task_store.get_user_by_email(squatted).password_hash is None


def test_a_reset_link_sent_before_an_email_change_does_not_redeem():
    """A token works only while the account's email is the address it was sent to."""
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store, mail_sender=RecordingSender())
    user = store.create_user("u-moved", "old@example.com", hash_password("first-pass1"))
    _user, link = service.issue_password_reset_link("old@example.com")
    store.users[user.id] = store.users[user.id].model_copy(update={"email": "new@example.com"})

    with pytest.raises(Exception) as refused:
        service.reset_password_with_token(link.rpartition("#token=")[2], "second-pass1")

    assert refused.value.detail == RESET_TOKEN_INVALID_DETAIL == RESET_DETAIL


# ── email verification ────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_sign_up_sends_a_verification_link_that_verifies_the_address_once(recovery):
    client, _service, sender = recovery
    email = _email()
    response = await client.post(
        "/v1/auth/register",
        json={"email": email, "password": "first-pass1"},
        headers={"Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
    )
    session = response.json()["access_token"]
    client.cookies.clear()

    assert response.json()["user"]["email_verified"] is False
    assert [(message.to, message.subject) for message in sender.sent] == [(email, "Подтвердите адрес почты для Veris")]
    assert "24 часа" in sender.sent[0].body
    token = _links(sender, "/verify-email")[0]

    verified = await client.post("/v1/auth/email/verify", json={"token": token}, headers=_bearer(session))

    assert (verified.status_code, verified.json()) == (200, {"status": "verified"})
    me = await client.get("/v1/auth/me", headers=_bearer(session))
    assert me.status_code == 200 and me.json()["email_verified"] is True  # the session survives
    again = await client.post("/v1/auth/email/verify", json={"token": token}, headers=_bearer(session))
    assert (again.status_code, again.json()["detail"]) == (400, VERIFY_DETAIL)
    assert again.json()["detail"].startswith("verification_token_invalid")
    assert VERIFICATION_TOKEN_INVALID_DETAIL == VERIFY_DETAIL


@pytest.mark.anyio
async def test_resending_a_verification_link(recovery):
    client, _service, sender = recovery
    session = await _register(client, _email())
    sender.sent.clear()

    sent = await client.post("/v1/auth/email/verification", headers=_bearer(session))

    assert (sent.status_code, sent.json()) == (202, {"status": "sent"})
    assert len(_links(sender, "/verify-email")) == 1
    await client.post("/v1/auth/email/verify", json={"token": _links(sender, "/verify-email")[0]}, headers=_bearer(session))
    done = await client.post("/v1/auth/email/verification", headers=_bearer(session))
    assert (done.status_code, done.json()) == (200, {"status": "already_verified"})
    assert len(sender.sent) == 1
    anonymous = await client.post("/v1/auth/email/verification", headers=_bearer("not-a-session"))
    assert anonymous.status_code == 401  # needs a session


@pytest.mark.anyio
async def test_resending_is_throttled_per_user_and_refused_without_email(recovery, monkeypatch):
    client, service, sender = recovery
    session = await _register(client, _email())
    codes = [
        (await client.post("/v1/auth/email/verification", headers=_bearer(session))).status_code for _ in range(6)
    ]
    assert codes == [202] * 5 + [429]
    # Each resend replaced the previous link: only the newest one works.
    tokens = _links(sender, "/verify-email")
    assert len(tokens) == 6  # the sign-up's and five resends
    for token, status in ((tokens[-2], 400), (tokens[-1], 200)):
        redeemed = await client.post("/v1/auth/email/verify", json={"token": token}, headers=_bearer(session))
        assert redeemed.status_code == status

    other = await _register(client, _email())
    service.mail_sender = None
    monkeypatch.setattr(settings, "email_backend", "disabled", raising=False)
    refused = await client.post("/v1/auth/email/verification", headers=_bearer(other))
    assert (refused.status_code, refused.json()["detail"]) == (404, "Email delivery is not configured")


@pytest.mark.anyio
async def test_verify_is_throttled_per_client(recovery):
    client, _service, _sender = recovery
    session = _bearer(await _register(client, _email()))
    codes = [
        (await client.post("/v1/auth/email/verify", json={"token": f"t{index}"}, headers=session)).status_code
        for index in range(31)
    ]
    assert codes == [400] * 30 + [429]


@pytest.mark.anyio
async def test_a_verification_link_redeems_only_in_the_session_of_its_account(recovery):
    """SEC-REC-1: a verification must show that whoever holds the password controls the
    inbox. An anonymous click, or one in another account's session, shows the inbox alone."""
    client, service, sender = recovery
    email = _email()
    session = await _register(client, email)
    token = _links(sender, "/verify-email")[0]
    other = await _register(client, _email())

    anonymous = await client.post("/v1/auth/email/verify", json={"token": token})
    wrong = await client.post("/v1/auth/email/verify", json={"token": token}, headers=_bearer(other))

    assert (anonymous.status_code, anonymous.json()["detail"]) == (401, "Not authenticated")
    assert (wrong.status_code, wrong.json()["detail"]) == (403, VERIFICATION_WRONG_ACCOUNT_DETAIL)
    assert wrong.json()["detail"].startswith("verification_wrong_account")
    assert VERIFICATION_WRONG_ACCOUNT_DETAIL == WRONG_ACCOUNT_DETAIL
    # Neither consumed the link nor verified an address.
    live = service.task_store.get_live_auth_action_token(hash_link_token(token), AuthActionPurpose.EMAIL_VERIFICATION)
    assert live is not None and live.used_at is None
    for bearer in (session, other):
        assert (await client.get("/v1/auth/me", headers=_bearer(bearer))).json()["email_verified"] is False

    verified = await client.post("/v1/auth/email/verify", json={"token": token}, headers=_bearer(session))

    assert (verified.status_code, verified.json()) == (200, {"status": "verified"})
    # Used up now: a dead link, whoever's session.
    used = await client.post("/v1/auth/email/verify", json={"token": token}, headers=_bearer(other))
    assert (used.status_code, used.json()["detail"]) == (400, VERIFY_DETAIL)
    garbage = await client.post("/v1/auth/email/verify", json={"token": "not-a-token"}, headers=_bearer(other))
    assert (garbage.status_code, garbage.json()["detail"]) == (400, VERIFY_DETAIL)


@pytest.mark.anyio
async def test_a_failing_mail_server_never_fails_a_sign_up_nor_leaks_the_link(recovery, caplog):
    client, service, _sender = recovery
    service.mail_sender = FailingSender()
    email = _email()

    with caplog.at_level(logging.INFO):
        response = await client.post("/v1/auth/register", json={"email": email, "password": "first-pass1"})
        forgot = await client.post("/v1/auth/password/forgot", json={"email": email})

    assert response.status_code == 200 and forgot.status_code == 202
    failures = [record for record in caplog.records if record.getMessage().startswith("account_email_failed")]
    assert len(failures) == 2
    assert all("SMTPRecipientsRefused" in record.getMessage() for record in failures)
    tokens = [row["token_hash"] for row in service.task_store.auth_action_tokens.values()]
    assert len(tokens) == 2
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert "#token=" not in logged and email not in logged
    assert all(token_hash not in logged for token_hash in tokens)


@pytest.mark.anyio
async def test_nothing_but_the_console_backend_logs_a_link(recovery, caplog):
    client, _service, sender = recovery
    with caplog.at_level(logging.DEBUG):
        email = _email()
        await _register(client, email)
        await client.post("/v1/auth/password/forgot", json={"email": email})
    tokens = _links(sender, "/verify-email") + _links(sender, "/reset-password")
    assert len(tokens) == 2
    logged = "\n".join(record.getMessage() for record in caplog.records)
    assert all(token not in logged for token in tokens)
    assert "account_email_sent kind=password_reset" in logged


# ── notices ───────────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_changing_the_password_in_settings_tells_the_address(recovery):
    client, _service, sender = recovery
    email = _email()
    session = await _register(client, email)
    sender.sent.clear()

    changed = await client.post(
        "/v1/auth/set-password",
        json={"password": "second-pass1", "current_password": "first-pass1"},
        headers=_bearer(session),
    )

    assert changed.status_code == 200
    assert changed.json()["user"]["email_verified"] is False
    assert [(message.to, message.subject) for message in sender.sent] == [(email, "Your Veris password was changed")]


# ── Google sign-in linking ────────────────────────────────────────────────────


async def _google_callback(client, monkeypatch, email: str, subject: str, language: str = "en"):
    monkeypatch.setattr(settings, "google_client_id", "cid", raising=False)
    monkeypatch.setattr(settings, "google_client_secret", "sec", raising=False)
    login = await client.get("/v1/auth/google/login", follow_redirects=False)
    state = urllib.parse.parse_qs(urllib.parse.urlsplit(login.headers["location"]).query)["state"][0]
    monkeypatch.setattr(
        "src.api.app.fetch_userinfo",
        lambda code: {"email": email, "email_verified": True, "sub": subject, "name": "Owner"},
    )
    return await client.get(
        f"/v1/auth/google/callback?code=abc&state={state}",
        follow_redirects=False,
        headers={"Accept-Language": language},
    )


@pytest.mark.anyio
async def test_google_sign_in_links_a_verified_local_account_and_keeps_its_password(recovery, monkeypatch):
    client, service, sender = recovery
    email = _email()
    session = await _register(client, email)
    # Verified in its own session: the password holder showed control of the inbox.
    verify = await client.post(
        "/v1/auth/email/verify", json={"token": _links(sender, "/verify-email")[0]}, headers=_bearer(session)
    )
    assert verify.status_code == 200
    sender.sent.clear()

    callback = await _google_callback(client, monkeypatch, email, "g-verified")

    assert (callback.status_code, callback.headers["location"]) == (302, "/")
    stored = service.task_store.get_user_by_email(email)
    assert stored.google_subject == "g-verified" and stored.name == "Owner"
    assert (await client.get("/v1/auth/me", headers=_bearer(session))).status_code == 200  # nothing revoked
    assert service.authenticate_user(email, "first-pass1").id == stored.id
    assert [(message.to, message.subject) for message in sender.sent] == [
        (email, "Google sign-in was linked to your Veris account")
    ]
    assert "as well as with your password" in sender.sent[0].body


@pytest.mark.anyio
async def test_google_sign_in_takes_an_unverified_sign_up_from_whoever_made_it(recovery, monkeypatch):
    """Account pre-hijacking: someone signs up with the owner's address first. The owner's
    Google sign-in now gets the account, and the squatter's password and sessions die."""
    client, service, sender = recovery
    email = _email()
    squatter_session = await _register(client, email, password="squatter-pass1")
    sender.sent.clear()

    callback = await _google_callback(client, monkeypatch, email, "g-owner", language="ru")

    assert (callback.status_code, callback.headers["location"]) == (302, "/")
    owner_session = client.cookies.get(settings.auth_cookie_name)
    client.cookies.clear()
    assert (await client.get("/v1/auth/me", headers=_bearer(squatter_session))).status_code == 401
    me = await client.get("/v1/auth/me", headers=_bearer(owner_session))
    assert me.status_code == 200 and me.json()["email_verified"] is True
    squatter_login = await client.post("/v1/auth/login", json={"email": email, "password": "squatter-pass1"})
    assert squatter_login.status_code == 401
    assert service.task_store.get_user_by_email(email).password_hash is None
    assert [(message.to, message.subject) for message in sender.sent] == [
        (email, "К аккаунту Veris привязан вход через Google")
    ]
    assert "пароль, с которым регистрировался аккаунт, удалён" in sender.sent[0].body


@pytest.mark.anyio
async def test_google_sign_in_recovers_a_legacy_google_account(recovery, monkeypatch):
    """Accounts created by Google sign-in before migration 000019 kept no google_subject
    and got a random password: locked out until the linking rule."""
    client, service, sender = recovery
    email = _email()
    legacy = service.task_store.create_user("legacy-user", email, hash_password(secrets.token_urlsafe(24)))

    callback = await _google_callback(client, monkeypatch, email, "g-legacy")

    assert (callback.status_code, callback.headers["location"]) == (302, "/")
    stored = service.task_store.get_user_by_id(legacy.id)
    assert (stored.google_subject, stored.password_hash) == ("g-legacy", None)
    assert stored.email_verified_at is not None
    assert [message.subject for message in sender.sent] == ["Google sign-in was linked to your Veris account"]
    # A second sign-in is an ordinary one: no second link, no second notice.
    again = await _google_callback(client, monkeypatch, email, "g-legacy")
    assert again.headers["location"] == "/" and len(sender.sent) == 1


@pytest.mark.anyio
async def test_google_sign_in_never_relinks_an_account_of_another_google_identity(recovery, monkeypatch):
    client, service, sender = recovery
    email = _email()
    first = await _google_callback(client, monkeypatch, email, "g-first")
    assert first.headers["location"] == "/set-password"  # a new account
    client.cookies.clear()

    second = await _google_callback(client, monkeypatch, email, "g-second")

    assert second.headers["location"] == "/login?error=oauth_conflict"
    assert service.task_store.get_user_by_email(email).google_subject == "g-first"
    assert sender.sent == []


@pytest.mark.anyio
async def test_a_linked_admin_email_gets_admin_rights_once_linked(recovery, monkeypatch):
    client, service, _sender = recovery
    monkeypatch.setattr(settings, "admin_emails", "ops@example.com", raising=False)
    squatted = service.task_store.create_user("ops-squat", "ops@example.com", hash_password("squatter-pass1"))
    token = create_token(squatted.id, token_version=squatted.token_version)
    assert (await client.get("/v1/admin/users", headers=_bearer(token))).status_code == 403

    await _google_callback(client, monkeypatch, "ops@example.com", "g-ops")
    owner = client.cookies.get(settings.auth_cookie_name)
    client.cookies.clear()

    assert (await client.get("/v1/admin/users", headers=_bearer(token))).status_code == 401
    assert (await client.get("/v1/admin/users", headers=_bearer(owner))).status_code == 200


# The lost race of a link (BE-3): link_user_google_subject matches nothing because, between
# the email lookup and the write, something else linked the account or took the subject.


def _lose_the_link_race(monkeypatch, store, meanwhile):
    """Make the store's link write lose: ``meanwhile(user_id, subject)`` runs first (what
    the winner did), then the write reports that it matched nothing."""

    def link(user_id, google_subject, *, clear_password):
        meanwhile(user_id, google_subject)
        return None

    monkeypatch.setattr(store, "link_user_google_subject", link)


def _same_identity_won(store):
    """A concurrent callback of this Google identity linked the account first."""
    return lambda user_id, subject: InMemoryTaskStore.link_user_google_subject(
        store, user_id, subject, clear_password=False
    )


def _another_account_took_the_subject(store):
    return lambda user_id, subject: store.create_user(
        f"taker-{secrets.token_hex(4)}", _email("taker"), None, google_subject=subject
    )


def _the_account_got_another_subject(store):
    return lambda user_id, subject: InMemoryTaskStore.link_user_google_subject(
        store, user_id, f"other-{subject}", clear_password=False
    )


def test_a_link_lost_to_the_same_identity_is_a_plain_sign_in(monkeypatch):
    store = InMemoryTaskStore()
    service = ResearchService(task_store=store, mail_sender=RecordingSender())
    user = store.create_user("u-race", "race@example.com", hash_password("first-pass1"))
    _lose_the_link_race(monkeypatch, store, _same_identity_won(store))

    result = service.sign_in_with_google("Race@Example.com", "g-race", name="Owner")

    # No second notice: the winning callback sent the one for this link.
    assert (result.user.id, result.created, result.linked_notice) == (user.id, False, None)
    assert store.get_user_by_google_subject("g-race").id == user.id


@pytest.mark.parametrize("meanwhile", [_another_account_took_the_subject, _the_account_got_another_subject])
def test_a_link_lost_to_another_identity_or_account_is_a_conflict(monkeypatch, meanwhile):
    from src.domain.errors import ConflictError

    store = InMemoryTaskStore()
    service = ResearchService(task_store=store, mail_sender=RecordingSender())
    store.create_user("u-race", "race@example.com", hash_password("first-pass1"))
    _lose_the_link_race(monkeypatch, store, meanwhile(store))

    with pytest.raises(ConflictError):
        service.sign_in_with_google("race@example.com", "g-race")


@pytest.mark.anyio
async def test_the_callback_that_loses_a_link_race_signs_in_or_reports_a_conflict(recovery, monkeypatch):
    client, service, sender = recovery
    email = _email()
    await _register(client, email)
    sender.sent.clear()
    _lose_the_link_race(monkeypatch, service.task_store, _same_identity_won(service.task_store))

    same = await _google_callback(client, monkeypatch, email, "g-race")

    assert (same.status_code, same.headers["location"]) == (302, "/")
    assert client.cookies.get(settings.auth_cookie_name)  # signed in
    assert sender.sent == []  # the winner's notice is the only one
    client.cookies.clear()

    other = _email()
    await _register(client, other)
    sender.sent.clear()
    _lose_the_link_race(monkeypatch, service.task_store, _another_account_took_the_subject(service.task_store))

    conflict = await _google_callback(client, monkeypatch, other, "g-taken")

    assert conflict.headers["location"] == "/login?error=oauth_conflict"
    assert sender.sent == []


async def _owner_clicks_the_verification_link(client, sender, token_path: str = "/verify-email") -> str:
    """What the address's owner can do with a link a stranger's sign-up sent them: open it
    signed out (or a link scanner that runs the page), or signed in to their own account.
    Neither redeems it. Returns the (still live) token."""
    token = _links(sender, token_path)[-1]
    anonymous = await client.post("/v1/auth/email/verify", json={"token": token})
    assert (anonymous.status_code, anonymous.json()["detail"]) == (401, "Not authenticated")
    own_account = await _register(client, _email("own"))
    elsewhere = await client.post("/v1/auth/email/verify", json={"token": token}, headers=_bearer(own_account))
    assert (elsewhere.status_code, elsewhere.json()["detail"]) == (403, WRONG_ACCOUNT_DETAIL)
    return token


@pytest.mark.anyio
async def test_a_squatted_sign_up_clicked_by_the_owner_stays_unverified_and_google_takes_it(recovery, monkeypatch):
    """SEC-REC-1: someone signs up with the owner's address and password P. The owner opens
    the verification mail. The account must stay unverified, so the owner's Google sign-in
    still clears P and revokes the squatter's sessions."""
    client, service, sender = recovery
    email = _email()
    squatter = await _register(client, email, password="squatter-pass1")

    token = await _owner_clicks_the_verification_link(client, sender)

    me = await client.get("/v1/auth/me", headers=_bearer(squatter))
    assert me.status_code == 200 and me.json()["email_verified"] is False
    assert service.task_store.get_user_by_email(email).email_verified_at is None
    sender.sent.clear()

    callback = await _google_callback(client, monkeypatch, email, "g-owner")

    assert (callback.status_code, callback.headers["location"]) == (302, "/")
    owner = client.cookies.get(settings.auth_cookie_name)
    client.cookies.clear()
    assert (await client.get("/v1/auth/me", headers=_bearer(squatter))).status_code == 401
    login = await client.post("/v1/auth/login", json={"email": email, "password": "squatter-pass1"})
    assert login.status_code == 401
    assert service.task_store.get_user_by_email(email).password_hash is None
    assert [message.subject for message in sender.sent] == ["Google sign-in was linked to your Veris account"]
    assert "the password the account was registered with was removed" in sender.sent[0].body
    assert (await client.get("/v1/auth/me", headers=_bearer(owner))).json()["email_verified"] is True
    # The squatter's link, if they got hold of it, no longer buys a session.
    assert (await client.post("/v1/auth/email/verify", json={"token": token}, headers=_bearer(squatter))).status_code == 401


@pytest.mark.anyio
async def test_a_squatted_admin_address_gives_the_squatter_no_admin_rights(recovery, monkeypatch):
    """SEC-REC-1, ADMIN_EMAILS: the address was registered before the operator listed it.
    The owner's click on the verification mail and Google sign-in must leave the squatter's
    password and sessions without admin rights (they die, in fact)."""
    client, service, sender = recovery
    squatter = await _register(client, "ops@example.com", password="squatter-pass1")
    monkeypatch.setattr(settings, "admin_emails", "ops@example.com", raising=False)
    assert (await client.get("/v1/admin/users", headers=_bearer(squatter))).status_code == 403

    await _owner_clicks_the_verification_link(client, sender)
    await _google_callback(client, monkeypatch, "ops@example.com", "g-ops")
    owner = client.cookies.get(settings.auth_cookie_name)
    client.cookies.clear()

    assert (await client.get("/v1/admin/users", headers=_bearer(squatter))).status_code == 401
    login = await client.post("/v1/auth/login", json={"email": "ops@example.com", "password": "squatter-pass1"})
    assert login.status_code == 401
    assert (await client.get("/v1/admin/users", headers=_bearer(owner))).status_code == 200
    assert service.task_store.get_user_by_email("ops@example.com").password_hash is None


# ── maintenance ───────────────────────────────────────────────────────────────


def test_queue_maintenance_deletes_used_and_expired_links():
    from datetime import datetime, timedelta, timezone

    store = InMemoryTaskStore()
    service = ResearchService(task_store=store, mail_sender=RecordingSender())
    store.create_user("u-links", "links@example.com", hash_password("first-pass1"))
    assert service.send_email_verification("u-links") is True
    for row in store.auth_action_tokens.values():
        row["expires_at"] = datetime.now(timezone.utc) - timedelta(minutes=1)
    _user, used = service.issue_password_reset_link("links@example.com")
    service.reset_password_with_token(used.rpartition("#token=")[2], "second-pass1")
    for row in store.auth_action_tokens.values():  # a clock tick apart even on Windows
        if row["used_at"] is not None:
            row["used_at"] -= timedelta(seconds=1)
    assert service.send_account_notice("u-links", AccountEmail.PASSWORD_CHANGED) is True  # stores nothing
    _user, live = service.issue_password_reset_link("links@example.com")
    assert len(store.auth_action_tokens) == 3

    service.run_queue_maintenance()

    assert [row["token_hash"] for row in store.auth_action_tokens.values()] == [
        hash_link_token(live.rpartition("#token=")[2])
    ]
