from types import SimpleNamespace
import pytest
from fastapi import HTTPException
from starlette.requests import Request

from src.api.schemas import AuthUser
from src.auth.admin_rate_limit import enforce_admin_rate_limit, reset_admin_rate_limiter
from src.config import settings


def _request_with_user(user: AuthUser | None) -> Request:
    service = SimpleNamespace(get_auth_user=lambda _uid: user)
    app = SimpleNamespace(state=SimpleNamespace(research_service=service))
    headers = [
        (b"host", b"testserver"),
    ]
    scope = {
        "type": "http",
        "headers": headers,
        "app": app,
    }
    return Request(scope)


def test_admin_rate_limit_allows_under_limit(monkeypatch):
    reset_admin_rate_limiter()
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "admin_rate_limit_per_minute", 2, raising=False)

    req = _request_with_user(None)
    user1 = enforce_admin_rate_limit(req)
    assert user1 is not None
    user2 = enforce_admin_rate_limit(req)
    assert user2 is not None

    with pytest.raises(HTTPException) as exc:
        enforce_admin_rate_limit(req)
    assert exc.value.status_code == 429
