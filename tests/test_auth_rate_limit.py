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


def test_login_account_limit_is_keyed_on_normalized_email(monkeypatch):
    from src.auth.login_rate_limit import enforce_login_account_rate_limit

    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_rate_limit_per_minute", 2, raising=False)

    enforce_login_account_rate_limit("Victim@Example.com")
    enforce_login_account_rate_limit("  victim@example.com ")
    with pytest.raises(HTTPException) as exc:
        enforce_login_account_rate_limit("VICTIM@example.com")
    assert exc.value.status_code == 429
    enforce_login_account_rate_limit("someone-else@example.com")  # separate budget


def test_login_account_limit_is_noop_when_auth_disabled(monkeypatch):
    from src.auth.login_rate_limit import enforce_login_account_rate_limit

    monkeypatch.setattr(settings, "auth_disabled", True, raising=False)
    monkeypatch.setattr(settings, "auth_rate_limit_per_minute", 1, raising=False)
    for _ in range(20):
        enforce_login_account_rate_limit("victim@example.com")  # never raises


@pytest.fixture
async def auth_app(monkeypatch):
    """An auth-enabled app whose requests can come from any client address."""
    import httpx

    from src.api.app import create_app
    from src.repositories import InMemoryTaskStore
    from src.services import ResearchService

    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "ci-test-secret-" + "x" * 40, raising=False)
    monkeypatch.setattr(settings, "auth_rate_limit_per_minute", 3, raising=False)
    app = create_app()
    async with app.router.lifespan_context(app):
        app.state.research_service = ResearchService(task_store=InMemoryTaskStore())

        def client_from(ip: str) -> httpx.AsyncClient:
            transport = httpx.ASGITransport(app=app, client=(ip, 4000))
            return httpx.AsyncClient(transport=transport, base_url="http://testserver")

        yield app, client_from


@pytest.mark.anyio
async def test_login_is_throttled_per_account_across_client_ips(auth_app):
    app, client_from = auth_app
    app.state.research_service.register_user("victim@example.com", "correct-pass1")

    statuses = []
    for n in range(4):  # a fresh IP every attempt, so the per-IP limiter never triggers
        async with client_from(f"198.51.100.{n + 1}") as c:
            response = await c.post(
                "/v1/auth/login", json={"email": "victim@example.com", "password": f"guess-{n}"}
            )
            statuses.append(response.status_code)

    assert statuses == [401, 401, 401, 429]
    async with client_from("198.51.100.50") as c:
        blocked = await c.post(
            "/v1/auth/login", json={"email": "victim@example.com", "password": "correct-pass1"}
        )
        other = await c.post(
            "/v1/auth/login", json={"email": "bystander@example.com", "password": "whatever1"}
        )
    assert blocked.status_code == 429
    assert other.status_code == 401  # other accounts keep their own budget


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("POST", "/v1/auth/set-password", {"password": "new-pass-123"}),
        ("DELETE", "/v1/auth/account", {"confirm": True}),
    ],
)
async def test_current_password_checks_are_throttled_per_user(auth_app, method, path, body):
    app, client_from = auth_app
    user = app.state.research_service.register_user("session-owner@example.com", "correct-pass1")
    from src.auth.security import create_token

    headers = {"Authorization": f"Bearer {create_token(user.id, token_version=user.token_version)}"}
    statuses = []
    for n in range(4):
        async with client_from(f"203.0.113.{n + 1}") as c:
            response = await c.request(
                method, path, json={**body, "current_password": f"guess-{n}"}, headers=headers
            )
            statuses.append(response.status_code)

    assert statuses == [401, 401, 401, 429]
    async with client_from("203.0.113.99") as c:
        response = await c.request(
            method, path, json={**body, "current_password": "correct-pass1"}, headers=headers
        )
    assert response.status_code == 429
    assert app.state.research_service.task_store.get_user_by_id(user.id) is not None
