"""ADMIN-AUDIT: every admin mutation (every non-GET admin route) and every bulk PII export
writes an admin_audit_logs row and goes through enforce_admin_rate_limit. The route list
is enumerated from the app, so a new admin mutation without an entry here fails.

The app tests also run on the Postgres store (postgres-smoke CI job), over one database
for the whole run: ids are unique per test and assertions only look at this test's rows."""
import uuid

import pytest
from fastapi.routing import APIRoute

from src.api.app import create_app
from src.api.dependencies import require_admin
from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, TaskStatus
from src.auth.admin_rate_limit import enforce_admin_rate_limit
from src.auth.security import create_token
from src.config import settings

PEER = "127.0.0.1"  # httpx.ASGITransport's client address

# POSTs that change nothing: the dry-run preview only counts what execute would touch.
READ_ONLY_ADMIN_POSTS = {("POST", "/v1/admin/operations/preview")}


def _dependency_calls(dependant) -> set:
    calls = set()
    for dependency in dependant.dependencies:
        calls.add(dependency.call)
        calls |= _dependency_calls(dependency)
    return calls


def _admin_routes(app) -> dict[tuple[str, str], set]:
    routes = {}
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        calls = _dependency_calls(route.dependant)
        if require_admin in calls or enforce_admin_rate_limit in calls:
            for method in route.methods:
                routes[(method, route.path)] = calls
    return routes


class _StubAnalyzer:
    def run_analysis(self, prompt, tasks, depth=None, **kwargs):
        return "report"


def _recommendation(service, code):
    service.task_store.upsert_worker_heartbeat(
        "maintenance",
        processed_jobs=1,
        status="idle",
        maintenance_summary={
            "recent_operational_recommendations": [
                {"code": code, "message": "m", "shown_count": 1, "active": True, "acknowledged": False},
            ],
        },
    )


def _uid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _dead_search_job(service):
    store = service.task_store
    task_id = _uid("audit-task")
    store.add_task({"id": task_id, "description": "d", "queries": ["q"], "status": TaskStatus.FAILED})
    job = store.add_search_task_job(task_id, SearchDepth.EASY.value, max_attempts=1)
    store.claim_search_task_job_by_id(job.id)
    store.record_search_task_job_failure(job.id, "boom")
    return job.id


def _dead_finalize_job(service):
    store = service.task_store
    service.analyzer = _StubAnalyzer()
    research = store.add_research(ResearchRequest(prompt="audit topic", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(research.id, ResearchStatus.FAILED, "analysis failed")
    job = store.add_research_finalize_job(research.id, max_attempts=1)
    store.claim_research_finalize_job_by_id(job.id)
    store.record_research_finalize_job_failure(job.id, "boom")
    return job.id


def _doomed_user(service):
    user_id = _uid("audit-doomed")
    service.task_store.create_user(user_id, f"{user_id}@example.com", None)
    return f"/v1/admin/users/{user_id}"


# (method, route path) -> (setup(service) -> (url, json body), expected audit action)
AUDITED_ROUTES = {
    ("POST", "/health/queues/maintenance"): (
        lambda service: ("/health/queues/maintenance", None), "run_queue_maintenance"),
    ("POST", "/health/queues/operational-health/recommendations/{code}/ack"): (
        lambda service: (_recommendation(service, "reduce_queue_backlog")
                         or "/health/queues/operational-health/recommendations/reduce_queue_backlog/ack", None),
        "acknowledge_recommendation"),
    ("POST", "/health/queues/operational-health/recommendations/{code}/resolve"): (
        lambda service: (_recommendation(service, "reduce_queue_backlog")
                         or "/health/queues/operational-health/recommendations/reduce_queue_backlog/resolve",
                         {"note": "fixed"}),
        "resolve_recommendation"),
    ("POST", "/v1/search-jobs/{job_id}/requeue"): (
        lambda service: (f"/v1/search-jobs/{_dead_search_job(service)}/requeue", None), "requeue_search_job"),
    ("POST", "/v1/search-jobs/recover-stale"): (
        lambda service: ("/v1/search-jobs/recover-stale", None), "recover_stale_search_jobs"),
    ("POST", "/v1/search-jobs/cleanup"): (
        lambda service: ("/v1/search-jobs/cleanup", None), "cleanup_search_jobs"),
    ("POST", "/v1/research/finalize-jobs/{job_id}/requeue"): (
        lambda service: (f"/v1/research/finalize-jobs/{_dead_finalize_job(service)}/requeue", None),
        "requeue_finalize_job"),
    ("POST", "/v1/research/finalize-jobs/recover-stale"): (
        lambda service: ("/v1/research/finalize-jobs/recover-stale", None), "recover_stale_finalize_jobs"),
    ("POST", "/v1/research/finalize-jobs/cleanup"): (
        lambda service: ("/v1/research/finalize-jobs/cleanup", None), "cleanup_finalize_jobs"),
    ("DELETE", "/v1/admin/users/{user_id}"): (lambda service: (_doomed_user(service), None), "delete_user"),
    ("POST", "/v1/admin/operations/execute"): (
        lambda service: ("/v1/admin/operations/execute", {"action": "cleanup_old_jobs", "params": {}}),
        "cleanup_old_jobs"),
}
AUDITED_EXPORTS = {
    ("GET", "/v1/admin/users/export"): (lambda service: ("/v1/admin/users/export", None), "export_users"),
    ("GET", "/v1/admin/prompts/export"): (lambda service: ("/v1/admin/prompts/export", None), "export_prompts"),
    ("GET", "/v1/admin/tokens/export"): (lambda service: ("/v1/admin/tokens/export", None), "export_tokens"),
}


def test_every_admin_mutation_is_listed_throttled_and_audited_below():
    routes = _admin_routes(create_app())
    mutations = {key for key in routes if key[0] != "GET"} - READ_ONLY_ADMIN_POSTS

    assert mutations == set(AUDITED_ROUTES), "a new admin mutation needs an audit row and an entry here"
    for key in [*AUDITED_ROUTES, *AUDITED_EXPORTS]:
        assert enforce_admin_rate_limit in routes[key], f"{key} is not behind enforce_admin_rate_limit"


def test_every_side_effecting_admin_get_is_csrf_checked():
    """A GET that audits and spends the admin budget is checked for CSRF like a mutation
    (SEC2-8): the csrf middleware keys on the path, so a new one must be listed there."""
    from src.api.app import _CSRF_CHECKED_GET_PATHS

    routes = _admin_routes(create_app())
    side_effecting_gets = {
        path for (method, path), calls in routes.items() if method == "GET" and enforce_admin_rate_limit in calls
    }

    assert side_effecting_gets == {path for _method, path in AUDITED_EXPORTS}
    assert side_effecting_gets == _CSRF_CHECKED_GET_PATHS


@pytest.mark.anyio
@pytest.mark.parametrize("route", sorted(AUDITED_ROUTES) + sorted(AUDITED_EXPORTS))
async def test_admin_route_writes_an_audit_row(client, route):
    service = client._transport.app.state.research_service
    setup, action = {**AUDITED_ROUTES, **AUDITED_EXPORTS}[route]
    url, body = setup(service)

    response = await client.request(route[0], url, json=body)

    assert response.status_code == 200, response.text
    audit = service.task_store.get_admin_audit_logs(limit=1)
    assert [(entry.action, entry.actor_email, entry.ip_address) for entry in audit] == [
        (action, "local@local", PEER)
    ]


@pytest.mark.anyio
async def test_admin_mutations_share_the_admin_rate_limit(client, monkeypatch):
    monkeypatch.setattr(settings, "admin_rate_limit_per_minute", 2)

    statuses = [(await client.post("/v1/search-jobs/cleanup")).status_code for _ in range(2)]
    statuses.append((await client.post("/v1/research/finalize-jobs/cleanup")).status_code)
    statuses.append((await client.get("/v1/admin/users/export")).status_code)

    assert statuses == [200, 200, 429, 429]


def _delete_audits(store, user_id):
    return [e for e in store.get_admin_audit_logs(limit=200, action="delete_user") if e.target_id == user_id]


@pytest.mark.anyio
async def test_user_deletion_audit_names_the_account_and_is_written_first(client, mocker):
    store = client._transport.app.state.research_service.task_store
    victim = _uid("audit-victim")
    store.create_user(victim, f"{victim}@example.com", None)
    mocker.patch.object(store, "delete_user", side_effect=RuntimeError("db went away"))

    with pytest.raises(RuntimeError):
        await client.delete(f"/v1/admin/users/{victim}")

    assert [e.details for e in _delete_audits(store, victim)] == [
        {"deleted_user_id": victim, "deleted_email": f"{victim}@example.com"}
    ]
    mocker.stopall()
    store.delete_user(victim)


@pytest.mark.anyio
async def test_missing_user_is_404_without_an_audit_row(client):
    store = client._transport.app.state.research_service.task_store
    missing = _uid("nobody")

    response = await client.delete(f"/v1/admin/users/{missing}")

    assert response.status_code == 404
    assert _delete_audits(store, missing) == []


@pytest.mark.anyio
async def test_an_admin_cannot_delete_another_admin(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    monkeypatch.setattr(settings, "auth_secret_key", "s" * 48)
    boss, deputy, member = _uid("boss"), _uid("deputy"), _uid("member")
    monkeypatch.setattr(settings, "admin_emails", f"{boss}@example.com, {deputy.upper()}@EXAMPLE.com")
    store = client._transport.app.state.research_service.task_store
    for user_id in (boss, deputy):  # Google-linked: a verified ADMIN_EMAILS identity
        store.create_user(user_id, f"{user_id}@example.com", None, google_subject=f"g-{user_id}")
    store.create_user(member, f"{member}@example.com", None)
    headers = {"Authorization": f"Bearer {create_token(boss, email=f'{boss}@example.com')}"}

    refused = await client.delete(f"/v1/admin/users/{deputy}", headers=headers)
    allowed = await client.delete(f"/v1/admin/users/{member}", headers=headers)

    assert refused.status_code == 403
    assert store.get_user_by_id(deputy) is not None
    assert _delete_audits(store, deputy) == []
    assert allowed.status_code == 200
    assert len(_delete_audits(store, member)) == 1
    store.delete_user(boss)
    store.delete_user(deputy)


@pytest.mark.postgres
def test_acknowledging_a_recommendation_persists_on_postgres(postgres_session_factory):
    """The ack/resolve writes stored RecommendationEvent models in the heartbeat's JSONB,
    which the SQL store could not serialize (TypeError, a 500 on Postgres)."""
    from src.repositories.sqlalchemy_task_store import SQLAlchemyTaskStore
    from src.services import ResearchService

    service = ResearchService(task_store=SQLAlchemyTaskStore(postgres_session_factory))
    _recommendation(service, "reduce_queue_backlog")

    service.acknowledge_operational_recommendation("reduce_queue_backlog")
    resolved = service.resolve_operational_recommendation("reduce_queue_backlog", "scaled workers")

    assert resolved.resolved is True
    summary = service.task_store.get_worker_heartbeat("maintenance").maintenance_summary
    assert [(e.event_type, e.note) for e in summary.recent_operational_recommendation_events] == [
        ("acknowledged", None),
        ("resolved", "scaled workers"),
    ]
