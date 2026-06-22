"""AUD-027: per-IP brute-force throttle on auth endpoints."""
import pytest
from fastapi import HTTPException
from starlette.requests import Request

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
