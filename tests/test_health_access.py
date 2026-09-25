"""HEALTH-ACCESS: /health is the only public health route. Everything else under /health
carries operator detail (worker last_error is raw exception text with DB host, IP and user;
the queue payload lists every user's research ids and admins' notes), so it is admin-only."""
import httpx
import pytest
from fastapi.routing import APIRoute

from src.api.app import create_app
from src.api.dependencies import require_admin
from src.auth.admin_rate_limit import enforce_admin_rate_limit
from src.auth.security import create_token
from src.config import settings
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

PUBLIC_HEALTH_ROUTES = {("GET", "/health")}
DB_ERROR = (
    '(psycopg.OperationalError) connection to server at "db" (172.18.0.3), port 5432 failed: '
    'FATAL: password authentication failed for user "app"'
)


def _depends_on(dependant, calls) -> bool:
    return any(dep.call in calls or _depends_on(dep, calls) for dep in dependant.dependencies)


def test_every_health_route_but_the_probe_requires_an_admin():
    routes = [
        (method, route.path, route)
        for route in create_app().routes
        if isinstance(route, APIRoute) and route.path.startswith("/health")
        for method in route.methods
    ]
    assert ("GET", "/health/workers/{worker_name}") in {(m, p) for m, p, _ in routes}
    unguarded = [
        (method, path)
        for method, path, route in routes
        if (method, path) not in PUBLIC_HEALTH_ROUTES
        # enforce_admin_rate_limit (the admin mutations) runs require_admin itself.
        and not _depends_on(route.dependant, {require_admin, enforce_admin_rate_limit})
    ]
    assert unguarded == []


@pytest.fixture
async def auth_client(monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False, raising=False)
    monkeypatch.setattr(settings, "auth_secret_key", "health-access-" + "x" * 40, raising=False)
    monkeypatch.setattr(settings, "admin_emails", "ops@example.com", raising=False)
    app = create_app()
    service = ResearchService(task_store=InMemoryTaskStore())
    async with app.router.lifespan_context(app):
        app.state.research_service = service
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client, service


def _token(user) -> dict:
    return {"Authorization": f"Bearer {create_token(user.id, token_version=user.token_version)}"}


@pytest.mark.anyio
async def test_worker_errors_and_queue_detail_are_hidden_from_non_admins(auth_client):
    client, service = auth_client
    service.task_store.upsert_worker_heartbeat("job-worker", 0, "error", DB_ERROR)
    member = service.register_user("member@example.com", "member-pass1")
    admin, _created = service.get_or_create_oauth_user("ops@example.com", google_subject="sub-ops")

    for path in ("/health/workers/job-worker", "/health/queues"):
        assert (await client.get(path)).status_code == 401
        refused = await client.get(path, headers=_token(member))
        assert refused.status_code == 403, path
        assert "172.18.0.3" not in refused.text
        assert (await client.get(path, headers=_token(admin))).status_code == 200, path

    worker = await client.get("/health/workers/job-worker", headers=_token(admin))
    assert worker.json()["last_error"] == DB_ERROR
    public = await client.get("/health")
    assert public.status_code == 200 and "last_error" not in public.text
