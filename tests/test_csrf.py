"""AUD-029: double-submit CSRF check for cookie-authenticated mutations."""
import uuid

import pytest
from starlette.requests import Request

from src.api.app import _is_csrf_violation
from src.config import settings


def _req(method="POST", path="/v1/research", headers=None, cookies=None):
    hlist = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    if cookies:
        hlist.append((b"cookie", "; ".join(f"{k}={v}" for k, v in cookies.items()).encode()))
    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "headers": hlist,
            "query_string": b"",
            "scheme": "http",
            "server": ("test", 80),
        }
    )


@pytest.fixture
def auth_on(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "csrf_cookie_name", "csrf_token", raising=False)


def test_no_check_when_auth_disabled(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    assert _is_csrf_violation(_req()) is False


def test_safe_methods_pass(auth_on):
    assert _is_csrf_violation(_req(method="GET")) is False


def test_bearer_is_exempt(auth_on):
    assert _is_csrf_violation(_req(headers={"Authorization": "Bearer abc.def.ghi"})) is False


def test_login_register_exempt(auth_on):
    assert _is_csrf_violation(_req(path="/v1/auth/login")) is False
    assert _is_csrf_violation(_req(path="/v1/auth/register")) is False


def test_telemetry_ingest_is_not_exempt(auth_on):
    # Authenticated-only ingestion: a cookie-riding cross-site POST must carry the token.
    assert _is_csrf_violation(_req(path="/v1/telemetry/event", cookies={"csrf_token": "tok123"})) is True


def test_cookie_auth_with_matching_token_passes(auth_on):
    req = _req(headers={"X-CSRF-Token": "tok123"}, cookies={"csrf_token": "tok123"})
    assert _is_csrf_violation(req) is False


def test_cookie_auth_without_header_is_blocked(auth_on):
    assert _is_csrf_violation(_req(cookies={"csrf_token": "tok123"})) is True


def test_cookie_auth_with_mismatched_token_is_blocked(auth_on):
    req = _req(headers={"X-CSRF-Token": "wrong"}, cookies={"csrf_token": "tok123"})
    assert _is_csrf_violation(req) is True


def test_no_cookie_no_header_is_blocked(auth_on):
    assert _is_csrf_violation(_req()) is True


def _raw_req(authorization: bytes, cookies=None, method="POST", path="/v1/research"):
    """Like _req, with the Authorization value as raw header bytes (Starlette decodes them
    as latin-1, so b"\\xa0" arrives as a lone NBSP, which str.strip() removes)."""
    request = _req(method=method, path=path, cookies=cookies)
    request.scope["headers"].append((b"authorization", authorization))
    return request


# SEC2-7: the exemption and _extract_token agree on what counts as a bearer request.
BLANK_BEARERS = [b"Bearer \xa0", b"Bearer    ", b"bearer \t", b"Bearer \x85"]


@pytest.mark.parametrize("value", BLANK_BEARERS)
def test_blank_bearer_is_no_exemption(auth_on, value):
    req = _raw_req(value, cookies={"csrf_token": "tok123", "access_token": "cookie.session.jwt"})
    assert _is_csrf_violation(req) is True


@pytest.mark.parametrize(
    "value",
    [*BLANK_BEARERS, b"Bearer abc.def.ghi", b"bearer  abc", b"Basic dXNlcjpwdw==", b"Bearer", b"Bearerabc"],
)
def test_exemption_matches_what_authentication_uses(auth_on, value):
    from src.api.dependencies import _extract_token

    req = _raw_req(value, cookies={"csrf_token": "tok123", "access_token": "cookie.session.jwt"})
    authenticates_with_header = _extract_token(req) != "cookie.session.jwt"
    assert _is_csrf_violation(req) is (not authenticates_with_header)


@pytest.mark.anyio
async def test_blank_bearer_cannot_skip_csrf_on_a_cookie_session(monkeypatch):
    import httpx

    from src.api.app import create_app

    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "csrf-test-secret-" + "x" * 40, raising=False)
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            email = f"csrf-nbsp-{uuid.uuid4().hex[:8]}@example.com"
            registered = await client.post("/v1/auth/register", json={"email": email, "password": "secret123"})
            assert registered.status_code == 200  # the client now holds the cookie session

            blank = await client.patch(
                "/v1/auth/profile", json={"name": "forged"}, headers={"Authorization": b"Bearer \xa0"}
            )
            with_token = await client.patch(
                "/v1/auth/profile",
                json={"name": "mine"},
                headers={"Authorization": b"Bearer \xa0", "X-CSRF-Token": client.cookies["csrf_token"]},
            )

    assert blank.status_code == 403
    assert with_token.status_code == 200
    assert with_token.json()["name"] == "mine"
