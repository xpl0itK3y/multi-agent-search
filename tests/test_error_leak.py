"""SEC-ERROR-LEAK twins: raw exception text never reaches research owners."""
import json

import pytest

from src.api.schemas import ResearchRequest, SearchDepth, TaskStatus
from src.domain.errors import NotFoundError

RAW = "psycopg.OperationalError: connection to postgres://svc:hunter2-SECRET@db:5432 failed"


def _sse_events(body: str) -> list[tuple[str, dict]]:
    events = []
    for block in body.split("\n\n"):
        lines = block.splitlines()
        name = next((line[len("event: "):] for line in lines if line.startswith("event: ")), None)
        data = next((line[len("data: "):] for line in lines if line.startswith("data: ")), None)
        if name and data:
            events.append((name, json.loads(data)))
    return events


@pytest.fixture
def fresh_llm_limiter(monkeypatch):
    from src.auth import llm_rate_limit
    from src.auth.login_rate_limit import SlidingWindowLimiter

    monkeypatch.setattr(llm_rate_limit, "_llm_route_limiter", SlidingWindowLimiter())


@pytest.mark.anyio
async def test_chat_stream_sends_generic_error_and_logs_the_exception(
    client, mocker, caplog, fresh_llm_limiter
):
    service = client._transport.app.state.research_service
    mocker.patch.object(service, "generate_research_answer", side_effect=RuntimeError(RAW))

    with caplog.at_level("ERROR", logger="src.api.app"):
        response = await client.post(
            "/v1/research/r-leak/messages/stream", json={"question": "what happened?"}
        )

    assert response.status_code == 200
    assert "SECRET" not in response.text
    assert _sse_events(response.text)[-1] == (
        "stream_error",
        {"detail": "Answer failed. Please try again."},
    )
    assert any(record.exc_info and "SECRET" in str(record.exc_info[1]) for record in caplog.records)


@pytest.mark.anyio
async def test_chat_stream_keeps_typed_service_error_detail(client, mocker, fresh_llm_limiter):
    service = client._transport.app.state.research_service
    mocker.patch.object(
        service, "generate_research_answer", side_effect=NotFoundError("Research not found")
    )

    response = await client.post(
        "/v1/research/r-missing/messages/stream", json={"question": "anyone there?"}
    )

    assert _sse_events(response.text)[-1] == ("stream_error", {"detail": "Research not found"})


@pytest.mark.anyio
async def test_owner_finalize_job_routes_sanitize_error(client):
    store = client._transport.app.state.research_service.task_store
    research = store.add_research(
        ResearchRequest(prompt="leaky finalize topic", depth=SearchDepth.EASY), task_ids=[]
    )
    job = store.add_research_finalize_job(research.id)
    store.record_research_finalize_job_failure(job.id, RAW)

    by_id = await client.get(f"/v1/research/finalize-jobs/{job.id}")
    latest = await client.get(f"/v1/research/{research.id}/finalize-job")

    for response in (by_id, latest):
        assert response.status_code == 200
        assert "SECRET" not in response.text
        assert response.json()["error"] == "Research failed during analysis. Please try again."
    assert store.get_research_finalize_job(job.id).error == RAW  # admins keep the raw cause


@pytest.mark.anyio
async def test_owner_search_job_routes_sanitize_error(client):
    store = client._transport.app.state.research_service.task_store
    store.add_task(
        {"id": "t-leak", "description": "d", "queries": ["q"], "status": TaskStatus.RUNNING}
    )
    job = store.add_search_task_job("t-leak", SearchDepth.EASY.value)
    store.record_search_task_job_failure(job.id, "search provider timed out: api_key=sk-SECRET")

    by_id = await client.get(f"/v1/search-jobs/{job.id}")
    latest = await client.get("/v1/tasks/t-leak/search-job")

    for response in (by_id, latest):
        assert response.status_code == 200
        assert "SECRET" not in response.text
        assert response.json()["error"] == "Research failed: the analysis timed out. Please try again."
