"""SEC2-4: PATCH /v1/tasks/{task_id} is admin maintenance, and TaskUpdate is bounded.

A task's results are a finished report's evidence: its sources, verification and
confidence (also on the public share page) read them live. Owners could rewrite them
through this route, so it is admin-only now, audited and on the admin rate budget. The
web UI never calls it; scripts/smoke_postgres_runtime.py does, with AUTH_DISABLED=true.

The app tests also run on the Postgres store over one shared database (postgres-smoke),
so ids are unique per test and assertions only look at this test's rows."""
import math
import uuid

import pytest
from pydantic import ValidationError

from src.api.schemas import (
    TASK_UPDATE_MAX_LOG_CHARS,
    TASK_UPDATE_MAX_METRIC,
    TASK_UPDATE_MAX_RESULT_ITEM_BYTES,
    TASK_UPDATE_MAX_RESULTS,
    SearchTaskMetrics,
    TaskStatus,
    TaskUpdate,
)
from src.auth.security import create_token
from src.config import settings


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _task(store, **fields):
    task_id = _uid("patch-task")
    store.add_task(
        {"id": task_id, "description": "d", "queries": ["q"], "status": TaskStatus.COMPLETED, **fields}
    )
    return task_id


def _source(content="Fetched page text", url="https://real.example/page"):
    return {"url": url, "title": "Real title", "content": content}


# ── TaskUpdate bounds ─────────────────────────────────────────────────────────


def test_a_worker_sized_update_fits():
    # The deepest search depth keeps 24 results with content clipped to 10,000 chars;
    # CJK text with newlines is the most bytes per character a worker writes.
    results = [
        {**_source(content=("数据\n" * 3334)[:10_000]), "snippet": "s" * 2_000, "domain": "real.example"}
        for _ in range(24)
    ]
    update = TaskUpdate(
        status=TaskStatus.COMPLETED,
        result=results,
        search_metrics=SearchTaskMetrics(
            candidate_count=48, extraction_attempts=48, selected_source_count=24, avg_content_chars=10_000.0
        ),
        log="Search completed. Selected 24 sources from 48 collected results.",
    )
    assert len(update.result) == 24


def test_a_long_log_line_is_clipped_not_refused():
    update = TaskUpdate(status=TaskStatus.FAILED, log="Error: " + "x" * 200_000)

    assert len(update.log) == TASK_UPDATE_MAX_LOG_CHARS
    assert update.log.startswith("Error: x") and update.log.endswith("…")
    assert TaskUpdate(log="short").log == "short"


def test_too_many_results_are_refused():
    with pytest.raises(ValidationError):
        TaskUpdate(result=[_source() for _ in range(TASK_UPDATE_MAX_RESULTS + 1)])
    assert len(TaskUpdate(result=[_source() for _ in range(TASK_UPDATE_MAX_RESULTS)]).result) == 100


def test_an_oversized_result_is_refused():
    with pytest.raises(ValidationError, match="bytes of JSON"):
        TaskUpdate(result=[_source(content="x" * TASK_UPDATE_MAX_RESULT_ITEM_BYTES)])


def test_oversized_results_in_total_are_refused():
    near_limit = "x" * (TASK_UPDATE_MAX_RESULT_ITEM_BYTES - 200)
    with pytest.raises(ValidationError, match="in total"):
        TaskUpdate(result=[_source(content=near_limit) for _ in range(40)])


@pytest.mark.parametrize(
    "metrics",
    [
        {"candidate_count": -1},
        {"selected_source_count": TASK_UPDATE_MAX_METRIC + 1},
        {"avg_content_chars": math.nan},
        {"avg_content_chars": math.inf},
    ],
)
def test_out_of_range_metrics_are_refused(metrics):
    with pytest.raises(ValidationError, match="search_metrics"):
        TaskUpdate(search_metrics=SearchTaskMetrics(**metrics))


def test_stored_metrics_are_not_revalidated():
    # SearchTaskMetrics itself stays unbounded: it also parses stored tasks.
    assert SearchTaskMetrics(candidate_count=-1).candidate_count == -1


# ── the route ─────────────────────────────────────────────────────────────────


@pytest.fixture
def auth_on(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    monkeypatch.setattr(settings, "auth_secret_key", "task-patch-secret-" + "x" * 40)
    admin_id = _uid("patch-admin")
    monkeypatch.setattr(settings, "admin_emails", f"{admin_id}@example.com")
    store = client._transport.app.state.research_service.task_store
    store.create_user(admin_id, f"{admin_id}@example.com", None, google_subject=f"g-{admin_id}")
    owner_id = _uid("patch-owner")
    store.create_user(owner_id, f"{owner_id}@example.com", None)
    yield {
        "store": store,
        "admin": {"Authorization": f"Bearer {create_token(admin_id)}"},
        "admin_email": f"{admin_id}@example.com",
        "owner_id": owner_id,
        "owner": {"Authorization": f"Bearer {create_token(owner_id)}"},
    }
    store.delete_user(owner_id)
    store.delete_user(admin_id)


def _owned_task(store, owner_id):
    from src.api.schemas import ResearchRequest, SearchDepth

    research = store.add_research(
        ResearchRequest(prompt="patch topic", depth=SearchDepth.EASY), task_ids=[], user_id=owner_id
    )
    return _task(store, research_id=research.id, result=[_source()])


def _audits(store, task_id):
    return [e for e in store.get_admin_audit_logs(limit=200, action="update_task") if e.target_id == task_id]


@pytest.mark.anyio
async def test_the_owner_cannot_rewrite_their_task(client, auth_on):
    store = auth_on["store"]
    task_id = _owned_task(store, auth_on["owner_id"])
    assert (await client.get(f"/v1/tasks/{task_id}", headers=auth_on["owner"])).status_code == 200

    forged = await client.patch(
        f"/v1/tasks/{task_id}",
        json={"result": [_source(content="Forged evidence")], "log": "x" * 200_000},
        headers=auth_on["owner"],
    )

    assert forged.status_code == 403
    task = store.get_task(task_id)
    assert task.result[0]["content"] == "Fetched page text"
    assert not any("x" * 100 in line for line in task.logs)
    assert _audits(store, task_id) == []


@pytest.mark.anyio
async def test_an_admin_rewrites_any_task_with_an_audit_row(client, auth_on):
    store = auth_on["store"]
    task_id = _owned_task(store, auth_on["owner_id"])

    response = await client.patch(
        f"/v1/tasks/{task_id}",
        json={"status": "failed", "result": [], "log": "admin: " + "y" * 10_000},
        headers=auth_on["admin"],
    )

    assert response.status_code == 200, response.text
    task = store.get_task(task_id)
    assert task.status == TaskStatus.FAILED and task.result == []
    assert len(task.logs[-1]) == TASK_UPDATE_MAX_LOG_CHARS
    [audit] = _audits(store, task_id)
    assert audit.actor_email == auth_on["admin_email"]
    assert audit.details == {"fields": ["log", "result", "status"], "status": "failed", "result_count": 0}


@pytest.mark.anyio
async def test_a_missing_task_is_404_without_an_audit_row(client, auth_on):
    missing = _uid("no-task")

    response = await client.patch(f"/v1/tasks/{missing}", json={"status": "failed"}, headers=auth_on["admin"])

    assert response.status_code == 404
    assert _audits(auth_on["store"], missing) == []


@pytest.mark.anyio
async def test_the_route_spends_the_admin_rate_budget(client, auth_on, monkeypatch):
    monkeypatch.setattr(settings, "admin_rate_limit_per_minute", 1)
    task_id = _owned_task(auth_on["store"], auth_on["owner_id"])

    first = await client.patch(f"/v1/tasks/{task_id}", json={"log": "one"}, headers=auth_on["admin"])
    second = await client.patch(f"/v1/tasks/{task_id}", json={"log": "two"}, headers=auth_on["admin"])

    assert (first.status_code, second.status_code) == (200, 429)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "body",
    [
        {"result": [_source() for _ in range(TASK_UPDATE_MAX_RESULTS + 1)]},
        {"result": [_source(content="x" * TASK_UPDATE_MAX_RESULT_ITEM_BYTES)]},
        {"search_metrics": {"candidate_count": -5}},
    ],
    ids=["too-many", "too-big", "negative-metric"],
)
async def test_out_of_bounds_bodies_are_refused(client, body):
    task_id = _task(client._transport.app.state.research_service.task_store)

    response = await client.patch(f"/v1/tasks/{task_id}", json=body)

    assert response.status_code == 422


@pytest.mark.anyio
async def test_the_postgres_smoke_patch_still_works_with_auth_disabled(client):
    """The exact call scripts/smoke_postgres_runtime.py makes (AUTH_DISABLED=true, no
    ADMIN_EMAILS: the local user is the admin)."""
    store = client._transport.app.state.research_service.task_store
    task_id = _task(store, status=TaskStatus.PENDING)

    response = await client.patch(
        f"/v1/tasks/{task_id}",
        json={
            "status": "completed",
            "result": [{"url": "https://smoke.example", "title": "Smoke", "content": "Patched from smoke script"}],
            "log": "patched via smoke script",
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "completed"
    assert response.json()["result"][0]["url"] == "https://smoke.example"
    assert "patched via smoke script" in store.get_task(task_id).logs
