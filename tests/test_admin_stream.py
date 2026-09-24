"""ADMIN-STREAM: /v1/admin/stream keeps the DB off the event loop, re-authorizes the admin
on every tick, and never sends exception text. The overview's health comes from the same
probes as /health, not a hard-coded "healthy" in the store."""
import json

import pytest

from src.api.schemas import SearchDepth
from src.auth.security import create_token
from src.config import settings
from src.repositories import InMemoryTaskStore
from src.services import ResearchService

ADMIN_EMAIL = "stream-admin@example.com"


def _events(body: str) -> list[tuple[str, dict]]:
    events, name = [], None
    for line in body.splitlines():
        if line.startswith("event: "):
            name = line[len("event: "):]
        elif line.startswith("data: ") and name:
            events.append((name, json.loads(line[len("data: "):])))
            name = None
    return events


@pytest.fixture
def stream_admin(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    monkeypatch.setattr(settings, "auth_secret_key", "s" * 48)
    monkeypatch.setattr(settings, "admin_emails", ADMIN_EMAIL)
    monkeypatch.setattr("src.api.app.ADMIN_STREAM_INTERVAL_SECONDS", 0.01)
    service = client._transport.app.state.research_service
    service.task_store.create_user("stream-admin", ADMIN_EMAIL, None)
    yield service, {"Authorization": f"Bearer {create_token('stream-admin', email=ADMIN_EMAIL)}"}
    service.task_store.delete_user("stream-admin")


@pytest.mark.anyio
async def test_stream_closes_once_the_admin_token_is_revoked(client, stream_admin, mocker):
    service, headers = stream_admin
    real_overview = service.get_admin_overview

    def overview_then_revoke():
        overview = real_overview()
        # A password change bumps token_version: the next tick's re-check must fail.
        service.task_store.update_user_password("stream-admin", "new-hash")
        return overview

    mocker.patch.object(service, "get_admin_overview", side_effect=overview_then_revoke)

    response = await client.get("/v1/admin/stream", headers=headers)

    assert response.status_code == 200
    events = _events(response.text)
    assert [name for name, _ in events] == ["overview", "error"]
    assert events[1][1] == {"error": "unauthorized"}


@pytest.mark.anyio
async def test_stream_closes_when_the_email_leaves_admin_emails(client, stream_admin, mocker, monkeypatch):
    service, headers = stream_admin
    real_overview = service.get_admin_overview

    def overview_then_demote():
        overview = real_overview()
        monkeypatch.setattr(settings, "admin_emails", "someone-else@example.com")
        return overview

    mocker.patch.object(service, "get_admin_overview", side_effect=overview_then_demote)

    response = await client.get("/v1/admin/stream", headers=headers)

    assert [name for name, _ in _events(response.text)] == ["overview", "error"]


@pytest.mark.anyio
async def test_stream_error_event_is_generic_and_logged(client, stream_admin, mocker, caplog):
    service, headers = stream_admin

    def failing_overview():
        service.task_store.update_user_password("stream-admin", "new-hash")  # end after this tick
        raise RuntimeError("could not connect: password=hunter2 host=db.internal")

    mocker.patch.object(service, "get_admin_overview", side_effect=failing_overview)

    with caplog.at_level("ERROR", logger="src.api.app"):
        response = await client.get("/v1/admin/stream", headers=headers)

    assert "hunter2" not in response.text and "db.internal" not in response.text
    assert _events(response.text) == [
        ("error", {"error": "overview_unavailable"}),
        ("error", {"error": "unauthorized"}),
    ]
    assert "admin_stream_overview_failed" in caplog.text


@pytest.mark.anyio
async def test_stream_refuses_a_non_admin_at_connect(client, stream_admin):
    service, _ = stream_admin
    service.task_store.create_user("stream-user", "stream-user@example.com", None)
    token = create_token("stream-user", email="stream-user@example.com")

    response = await client.get("/v1/admin/stream", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 403
    service.task_store.delete_user("stream-user")


@pytest.mark.anyio
async def test_stream_runs_the_db_calls_in_the_threadpool(client, stream_admin, mocker):
    service, headers = stream_admin
    offloaded = []

    async def recording_threadpool(func, *args, **kwargs):
        offloaded.append(getattr(func, "__name__", repr(func)))
        if func == service.get_admin_overview:  # bound methods: equal, not identical
            service.task_store.update_user_password("stream-admin", "new-hash")
        return func(*args, **kwargs)

    mocker.patch("src.api.app.run_in_threadpool", side_effect=recording_threadpool)

    await client.get("/v1/admin/stream", headers=headers)

    stream_calls = [name for name in offloaded if name != "_touch_request_activity"]  # middleware
    assert stream_calls[:3] == ["require_admin", "require_admin", "get_admin_overview"]


def test_overview_health_reflects_the_llm_and_the_failed_jobs():
    store = InMemoryTaskStore()
    assert store.get_admin_overview().system_health == {}  # the store no longer claims health

    down = ResearchService(task_store=store, llm_available=False).get_admin_overview()
    assert down.system_health == {"postgres": "ok", "redis": "disabled", "llm": "down", "overall": "degraded"}

    service = ResearchService(task_store=store, llm_available=True)
    assert service.get_admin_overview().system_health["overall"] == "healthy"

    store.add_task({"id": "t-dead", "description": "d", "queries": ["q"], "status": "failed"})
    job = store.add_search_task_job("t-dead", SearchDepth.EASY.value, max_attempts=1)
    store.claim_next_search_task_job()
    store.record_search_task_job_failure(job.id, "boom")
    degraded = service.get_admin_overview()
    assert degraded.failed_tasks_count == 1
    assert degraded.system_health["overall"] == "degraded"
