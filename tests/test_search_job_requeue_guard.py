"""SEARCH-REQUEUE-GUARD: the admin requeue of a dead-letter search job only runs for a
research that is still searching, and a stalled sweep racing it loses.

requeue_search_task_job checked only that the job was DEAD_LETTER. On a completed,
cancelled, failed or finalizing research it set the task PENDING and requeued a job the
worker then drained ('Research no longer active — search skipped'), leaving the task
PENDING on the ended research. On a PROCESSING one neither write touched the research
row, so a sweep that had just listed it still failed it under the requeued search.
"""
from datetime import datetime, timedelta, timezone

import pytest

from src.api.schemas import (
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.domain.errors import ConflictError
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class _Analyzer:
    llm = None

    def run_analysis(self, prompt, tasks, **kwargs):
        return "report over " + ",".join(sorted(t.id for t in tasks if t.result))


class _Broker:
    def __init__(self):
        self.search: list[str] = []
        self.finalize: list[str] = []

    def push_search_job(self, job_id):
        self.search.append(job_id)

    def push_finalize_job(self, job_id):
        self.finalize.append(job_id)


def _service():
    store, broker = InMemoryTaskStore(), _Broker()
    service = ResearchService(task_store=store, analyzer=_Analyzer(), broker=broker)

    def run_search_task(task_id, depth):  # stand-in for SearchAgent: one source per task
        store.update_task(
            task_id,
            TaskUpdate(
                status=TaskStatus.COMPLETED,
                result=[{"url": f"https://example.com/{task_id}", "title": "A", "content": "Body " * 40}],
            ),
        )

    service.run_search_task = run_search_task
    return store, broker, service


def _research_with_a_dead_search(store, status=ResearchStatus.PROCESSING):
    research = store.add_research(ResearchRequest(prompt="requeue topic", depth=SearchDepth.EASY), task_ids=[])
    store.add_task(
        {
            "id": "ok",
            "research_id": research.id,
            "description": "d",
            "queries": ["q"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com/ok", "title": "A", "content": "Body " * 40}],
        }
    )
    store.add_task(
        {
            "id": "bad",
            "research_id": research.id,
            "description": "d",
            "queries": ["q"],
            "status": TaskStatus.FAILED,
            "logs": ["Search job failed after all retries"],
        }
    )
    store.set_research_task_ids(research.id, ["ok", "bad"])
    job = store.add_search_task_job("bad", SearchDepth.EASY.value, max_attempts=1)
    store.claim_search_task_job_by_id(job.id)
    store.record_search_task_job_failure(job.id, "boom")
    if status != ResearchStatus.PROCESSING:
        store.update_research_status(research.id, status, "state after the job died")
    return research.id, job.id


@pytest.mark.parametrize(
    "status",
    [ResearchStatus.COMPLETED, ResearchStatus.CANCELLED, ResearchStatus.FAILED, ResearchStatus.ANALYZING],
)
def test_a_research_that_is_no_longer_searching_refuses_the_requeue(status):
    store, broker, service = _service()
    research_id, job_id = _research_with_a_dead_search(store, status)

    with pytest.raises(ConflictError, match="still searching can be requeued; retry a failed research instead"):
        service.requeue_search_task_job(job_id)

    assert store.get_search_task_job(job_id).status == SearchJobStatus.DEAD_LETTER
    task = store.get_task("bad")
    assert task.status == TaskStatus.FAILED and task.logs == ["Search job failed after all retries"]
    assert store.get_research(research_id).status == status
    assert broker.search == []


def test_a_superseded_dead_letter_job_is_refused():
    store, broker, service = _service()
    _, old_job_id = _research_with_a_dead_search(store)
    store.get_search_task_job(old_job_id).created_at -= timedelta(minutes=5)
    newer = store.add_search_task_job("bad", SearchDepth.EASY.value, max_attempts=1)
    store.claim_search_task_job_by_id(newer.id)
    store.record_search_task_job_failure(newer.id, "boom again")

    with pytest.raises(ConflictError, match="newer search job has superseded"):
        service.requeue_search_task_job(old_job_id)

    assert service.requeue_search_task_job(newer.id).status == SearchJobStatus.PENDING
    assert broker.search == [newer.id]


def test_a_requeue_that_lands_after_the_sweep_listed_the_research_wins():
    store, broker, service = _service()
    research_id, job_id = _research_with_a_dead_search(store)
    store.get_research(research_id).updated_at = datetime.now(timezone.utc) - timedelta(hours=1)
    listed = store.list_stalled_research_ids

    def list_then_requeue(stale_before, limit=50):
        ids = listed(stale_before, limit)
        if research_id in ids:  # the admin requeues between the listing and the CAS
            service.requeue_search_task_job(job_id)
        return ids

    store.list_stalled_research_ids = list_then_requeue

    assert service.sweep_stalled_researches() == []

    assert store.get_research(research_id).status == ResearchStatus.PROCESSING
    assert store.get_task("bad").status == TaskStatus.PENDING
    service.process_search_task_job(job_id)
    for finalize_job_id in broker.finalize:
        service.process_finalize_job(finalize_job_id)
    research = store.get_research(research_id)
    assert research.status == ResearchStatus.COMPLETED and research.final_report == "report over bad,ok"


@pytest.mark.anyio
async def test_the_route_answers_409_for_an_ended_research(client):
    store = client._transport.app.state.research_service.task_store
    research = store.add_research(ResearchRequest(prompt="route topic", depth=SearchDepth.EASY), task_ids=[])
    task_id = f"route-task-{research.id[:8]}"
    store.add_task(
        {"id": task_id, "research_id": research.id, "description": "d", "queries": ["q"], "status": TaskStatus.FAILED}
    )
    job = store.add_search_task_job(task_id, SearchDepth.EASY.value, max_attempts=1)
    store.claim_search_task_job_by_id(job.id)
    store.record_search_task_job_failure(job.id, "boom")
    store.update_research_status(research.id, ResearchStatus.COMPLETED, "done")

    response = await client.post(f"/v1/search-jobs/{job.id}/requeue")

    assert response.status_code == 409
    assert response.json()["detail"] == (
        "Only a search job of a research that is still searching can be requeued; retry a failed research instead"
    )
    assert store.get_task(task_id).status == TaskStatus.FAILED
