"""TOKEN-PURPOSE: every token is signed with AUTH_SECRET_KEY, so each names what it is for.
An OAuth state token (anyone can mint one at /v1/auth/google/login) never passes as a
session token: no identity lookup, no activity-gate slot, no authentication."""
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest
from starlette.requests import Request

from src.api import app as app_module
from src.api.dependencies import request_token_subject, resolve_request_user_id
from src.auth.security import OAUTH_STATE_PURPOSE, create_token, decode_token, verify_token
from src.config import settings


def _request(token: str) -> Request:
    service = SimpleNamespace(get_auth_user=lambda _uid: pytest.fail("no lookup for a state token"))
    app = SimpleNamespace(state=SimpleNamespace(research_service=service))
    return Request({"type": "http", "headers": [(b"authorization", f"Bearer {token}".encode())], "app": app})


def test_state_and_session_tokens_only_decode_for_their_own_purpose(monkeypatch):
    monkeypatch.setattr(settings, "auth_secret_key", "p" * 48, raising=False)
    state = create_token("random-state", ttl_seconds=600, purpose=OAUTH_STATE_PURPOSE)
    session = create_token("user-1")

    assert decode_token(state) is None
    assert verify_token(state) is None
    assert decode_token(state, purpose=OAUTH_STATE_PURPOSE)["sub"] == "random-state"
    assert decode_token(session)["sub"] == "user-1"
    assert decode_token(session, purpose=OAUTH_STATE_PURPOSE) is None


def test_request_identity_helpers_ignore_state_tokens(monkeypatch):
    monkeypatch.setattr(settings, "auth_secret_key", "p" * 48, raising=False)
    state = create_token("random-state", ttl_seconds=600, purpose=OAUTH_STATE_PURPOSE)

    assert request_token_subject(_request(state)) is None
    assert resolve_request_user_id(_request(state)) is None


@pytest.fixture
def oauth_on(monkeypatch):
    monkeypatch.setattr(settings, "auth_secret_key", "p" * 48, raising=False)
    monkeypatch.setattr(settings, "google_client_id", "cid", raising=False)
    monkeypatch.setattr(settings, "google_client_secret", "sec", raising=False)


async def _state(client) -> str:
    login = await client.get("/v1/auth/google/login", follow_redirects=False)
    client.cookies.clear()
    return parse_qs(urlparse(login.headers["location"]).query)["state"][0]


@pytest.mark.anyio
async def test_state_tokens_cost_no_lookup_and_no_gate_slot(client, oauth_on, monkeypatch):
    service = client._transport.app.state.research_service
    lookups = []
    monkeypatch.setattr(service, "get_auth_user", lambda uid: lookups.append(uid))
    before = len(app_module._activity_touch_gate)

    for _ in range(20):
        state = await _state(client)
        assert (await client.get("/v1/models", headers={"Authorization": f"Bearer {state}"})).status_code == 200

    assert len(app_module._activity_touch_gate) == before
    assert lookups == []


@pytest.mark.anyio
async def test_state_token_is_not_a_session(client, oauth_on, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    state = await _state(client)

    response = await client.get("/v1/auth/me", headers={"Authorization": f"Bearer {state}"})

    assert response.status_code == 401


@pytest.mark.anyio
async def test_callback_refuses_a_session_token_as_state(client, oauth_on, mocker):
    fetch = mocker.patch("src.api.app.fetch_userinfo")
    session = create_token("user-1", ttl_seconds=600)
    client.cookies.set("oauth_state", session)

    callback = await client.get(f"/v1/auth/google/callback?code=abc&state={session}", follow_redirects=False)

    assert callback.headers["location"] == "/login?error=oauth_failed"
    fetch.assert_not_called()
