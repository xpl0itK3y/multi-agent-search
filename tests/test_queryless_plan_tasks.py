"""QUERYLESS-PLAN: a plan item with no search queries is stored settled, so its research
finalizes on the searches that could run.

Such an item (the orchestrator's _normalize_queries can return [] for one, and an API
client can clear an item's queries before approving) was stored PENDING and got no search
job. _search_settled counts no PENDING task, so the research never finalized after its
real searches completed and the stalled sweep later FAILED it ('stopped making progress').
"""
import uuid
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
from src.domain.models import ResearchPlanItem, ResearchPlanUpdate
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class _MixedPlan:
    def run_decompose(self, prompt, depth, **kwargs):
        return [
            {"id": f"searchable-{uuid.uuid4().hex[:8]}", "description": "a", "queries": ["q1"],
             "status": TaskStatus.PENDING},
            {"id": f"queryless-{uuid.uuid4().hex[:8]}", "description": "b", "queries": [],
             "status": TaskStatus.PENDING},
        ]


class _Analyzer:
    llm = None

    def run_analysis(self, prompt, tasks, **kwargs):
        return "report over " + ",".join(sorted(t.description for t in tasks if t.result))


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
    service = ResearchService(task_store=store, analyzer=_Analyzer(), orchestrator=_MixedPlan(), broker=broker)

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


def _run_jobs(service, broker):
    for job_id in list(broker.search):
        service.process_search_task_job(job_id)
    for job_id in list(broker.finalize):
        service.process_finalize_job(job_id)


def _assert_settled_without_a_job(store, task):
    assert task.status == TaskStatus.COMPLETED and task.result is None
    assert task.logs == [ResearchService.NO_QUERIES_TASK_LOG]
    assert store.get_latest_search_task_job(task.id) is None


def _tasks(store, research_id):
    tasks = {task.description: task for task in store.get_tasks_by_research(research_id)}
    return tasks["a"], tasks["b"]


def test_a_decomposed_plan_with_a_queryless_item_finalizes_on_the_other_searches():
    store, broker, service = _service()
    request = ResearchRequest(prompt="mixed plan", depth=SearchDepth.EASY)
    _, research_id = service.start_research(request)

    service.decompose_and_enqueue(research_id, request)

    searchable, queryless = _tasks(store, research_id)
    _assert_settled_without_a_job(store, queryless)
    assert store.get_latest_search_task_job(searchable.id).status == SearchJobStatus.PENDING
    _run_jobs(service, broker)
    research = store.get_research(research_id)
    assert research.status == ResearchStatus.COMPLETED and research.final_report == "report over a"


def test_an_approved_plan_with_a_cleared_item_finalizes_on_the_other_searches():
    store, broker, service = _service()
    request = ResearchRequest(prompt="mixed plan", depth=SearchDepth.EASY, plan_first=True)
    _, research_id = service.start_research(request)
    service.decompose_and_enqueue(research_id, request)
    first, second = service.get_research_plan(research_id).items
    service.update_research_plan(
        research_id,
        ResearchPlanUpdate(
            items=[
                ResearchPlanItem(id=first.id, description="a", queries=["q1"]),
                ResearchPlanItem(id=second.id, description="b", queries=[""]),  # cleared by the client
            ]
        ),
    )

    service.approve_research_plan(research_id)

    searchable, cleared = _tasks(store, research_id)
    _assert_settled_without_a_job(store, cleared)
    assert cleared.queries == []
    _run_jobs(service, broker)
    research = store.get_research(research_id)
    assert research.status == ResearchStatus.COMPLETED and research.final_report == "report over a"


def test_a_plan_left_with_nothing_to_search_is_refused_before_the_admission():
    store, broker, service = _service()
    request = ResearchRequest(prompt="empty plan", depth=SearchDepth.EASY, plan_first=True)
    _, research_id = service.start_research(request)
    service.decompose_and_enqueue(research_id, request)
    items = service.get_research_plan(research_id).items
    service.update_research_plan(
        research_id,
        ResearchPlanUpdate(items=[ResearchPlanItem(id=i.id, description=i.description, queries=[]) for i in items]),
    )

    with pytest.raises(ConflictError, match="no search queries"):
        service.approve_research_plan(research_id)

    assert store.get_research(research_id).status == ResearchStatus.PLAN_REVIEW
    assert store.get_tasks_by_research(research_id) == [] and broker.search == []


def test_the_stalled_sweep_finalizes_rather_than_fails_a_plan_whose_finalize_enqueue_was_lost():
    store, broker, service = _service()
    request = ResearchRequest(prompt="mixed plan", depth=SearchDepth.EASY)
    _, research_id = service.start_research(request)
    service.decompose_and_enqueue(research_id, request)
    searchable, _ = _tasks(store, research_id)
    # The search finished, but the process died before it enqueued the finalization.
    service.run_search_task(searchable.id, SearchDepth.EASY)
    store.update_search_task_job(store.get_latest_search_task_job(searchable.id).id, SearchJobStatus.COMPLETED)
    store.get_research(research_id).updated_at = datetime.now(timezone.utc) - timedelta(hours=1)

    assert service.sweep_stalled_researches() == [research_id]

    _run_jobs(service, broker)
    research = store.get_research(research_id)
    assert research.status == ResearchStatus.COMPLETED and research.final_report == "report over a"


def test_a_retry_after_a_failed_analysis_finalizes_without_searching_the_queryless_item():
    store, broker, service = _service()
    request = ResearchRequest(prompt="mixed plan", depth=SearchDepth.EASY)
    _, research_id = service.start_research(request)
    service.decompose_and_enqueue(research_id, request)
    for job_id in list(broker.search):
        service.process_search_task_job(job_id)
    store.update_research_status(research_id, ResearchStatus.FAILED, "Research failed during analysis.")
    broker.search.clear()

    service.retry_research(research_id)

    _, queryless = _tasks(store, research_id)
    _assert_settled_without_a_job(store, queryless)
    assert broker.search == []
    _run_jobs(service, broker)
    assert store.get_research(research_id).status == ResearchStatus.COMPLETED
