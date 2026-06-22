"""AUD-029: double-submit CSRF check for cookie-authenticated mutations."""
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
