"""AUD-027: per-IP brute-force throttle on auth endpoints."""
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi import FastAPI
from starlette.requests import Request

from src.api.schemas import AuthUser
from src.auth import llm_rate_limit
from src.auth.llm_rate_limit import enforce_llm_rate_limit
from src.auth.login_rate_limit import SlidingWindowLimiter, enforce_auth_rate_limit
from src.config import settings


def _req(ip: str = "203.0.113.1") -> Request:
    return Request({"type": "http", "headers": [], "client": (ip, 1111)})


def test_sliding_window_allows_up_to_limit_then_blocks():
    limiter = SlidingWindowLimiter()
    assert [limiter.allow("k", 3) for _ in range(5)] == [True, True, True, False, False]


def test_enforce_is_noop_when_auth_disabled(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "auth_rate_limit_per_minute", 1, raising=False)
    for _ in range(50):
        enforce_auth_rate_limit(_req("198.51.100.9"))  # never raises


def test_enforce_raises_429_over_limit(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_rate_limit_per_minute", 3, raising=False)
    ip = "203.0.113.27"
    for _ in range(3):
        enforce_auth_rate_limit(_req(ip))
    with pytest.raises(HTTPException) as exc:
        enforce_auth_rate_limit(_req(ip))
    assert exc.value.status_code == 429


def _app_req() -> Request:
    app = FastAPI()
    app.state.research_service = SimpleNamespace(broker=None)
    return Request({"type": "http", "headers": [], "client": ("203.0.113.8", 1111), "app": app})


def test_llm_route_limit_is_per_user_and_returns_429(monkeypatch):
    monkeypatch.setattr(settings, "llm_route_rate_limit_per_minute", 1, raising=False)
    monkeypatch.setattr(llm_rate_limit, "_llm_route_limiter", SlidingWindowLimiter())
    current_user = AuthUser(id="limited-user", email="limited@example.com")
    monkeypatch.setattr(llm_rate_limit, "get_current_user", lambda request: current_user)

    assert enforce_llm_rate_limit(_app_req()) == current_user
    with pytest.raises(HTTPException) as exc:
        enforce_llm_rate_limit(_app_req())
    assert exc.value.status_code == 429

    other_user = AuthUser(id="other-user", email="other@example.com")
    monkeypatch.setattr(llm_rate_limit, "get_current_user", lambda request: other_user)
    assert enforce_llm_rate_limit(_app_req()) == other_user


def test_llm_route_limit_uses_distributed_broker(monkeypatch):
    broker = SimpleNamespace(allow_llm_request=lambda user_id, limit: False)
    app = FastAPI()
    app.state.research_service = SimpleNamespace(broker=broker)
    request = Request({"type": "http", "headers": [], "client": None, "app": app})
    monkeypatch.setattr(settings, "llm_route_rate_limit_per_minute", 10, raising=False)
    monkeypatch.setattr(
        llm_rate_limit,
        "get_current_user",
        lambda request: AuthUser(id="redis-user", email="redis@example.com"),
    )

    with pytest.raises(HTTPException) as exc:
        enforce_llm_rate_limit(request)
    assert exc.value.status_code == 429
