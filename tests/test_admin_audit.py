"""ADMIN-AUDIT: every admin mutation (every non-GET admin route) and every bulk PII export
writes an admin_audit_logs row and goes through enforce_admin_rate_limit. The route list
is enumerated from the app, so a new admin mutation without an entry here fails."""
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


def _dead_search_job(service):
    store = service.task_store
    store.add_task({"id": "audit-task", "description": "d", "queries": ["q"], "status": TaskStatus.FAILED})
    job = store.add_search_task_job("audit-task", SearchDepth.EASY.value, max_attempts=1)
    store.claim_next_search_task_job()
    store.record_search_task_job_failure(job.id, "boom")
    return job.id


def _dead_finalize_job(service):
    store = service.task_store
    service.analyzer = _StubAnalyzer()
    research = store.add_research(ResearchRequest(prompt="audit topic", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(research.id, ResearchStatus.FAILED, "analysis failed")
    job = store.add_research_finalize_job(research.id, max_attempts=1)
    store.claim_next_research_finalize_job()
    store.record_research_finalize_job_failure(job.id, "boom")
    return job.id


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
    ("DELETE", "/v1/admin/users/{user_id}"): (
        lambda service: (service.task_store.create_user("audit-doomed", "doomed@example.com", None)
                         and "/v1/admin/users/audit-doomed", None),
        "delete_user"),
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


@pytest.mark.anyio
async def test_user_deletion_audit_names_the_account_and_is_written_first(client, mocker):
    store = client._transport.app.state.research_service.task_store
    store.create_user("audit-victim", "victim@example.com", None)
    mocker.patch.object(store, "delete_user", side_effect=RuntimeError("db went away"))

    with pytest.raises(RuntimeError):
        await client.delete("/v1/admin/users/audit-victim")

    audit = store.get_admin_audit_logs(action="delete_user")
    assert [(e.target_id, e.details) for e in audit] == [
        ("audit-victim", {"deleted_user_id": "audit-victim", "deleted_email": "victim@example.com"})
    ]


@pytest.mark.anyio
async def test_missing_user_is_404_without_an_audit_row(client):
    store = client._transport.app.state.research_service.task_store

    response = await client.delete("/v1/admin/users/nobody-here")

    assert response.status_code == 404
    assert store.get_admin_audit_logs(action="delete_user") == []


@pytest.mark.anyio
async def test_an_admin_cannot_delete_another_admin(client, monkeypatch):
    monkeypatch.setattr(settings, "auth_disabled", False)
    monkeypatch.setattr(settings, "auth_secret_key", "s" * 48)
    monkeypatch.setattr(settings, "admin_emails", "boss@example.com, Deputy@example.com")
    store = client._transport.app.state.research_service.task_store
    store.create_user("boss", "boss@example.com", None)
    store.create_user("deputy", "deputy@example.com", None)
    store.create_user("member", "member@example.com", None)
    headers = {"Authorization": f"Bearer {create_token('boss', email='boss@example.com')}"}

    refused = await client.delete("/v1/admin/users/deputy", headers=headers)
    allowed = await client.delete("/v1/admin/users/member", headers=headers)

    assert refused.status_code == 403
    assert store.get_user_by_id("deputy") is not None
    assert allowed.status_code == 200
    assert [e.target_id for e in store.get_admin_audit_logs(action="delete_user")] == ["member"]
