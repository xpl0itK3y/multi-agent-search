"""OPS-1: mas_worker_jobs_total{status="failure"} counts every failed job attempt.

process_search_task_job and process_finalize_job return the job instead of raising when
an attempt fails (FAILED task, exception, retry scheduled, dead-letter, lost lease), so
the workers used to count those attempts as "success" and WorkerJobFailureRate never
fired. Each attempt is now counted exactly once, from the job it leaves behind.
"""
from datetime import datetime, timedelta, timezone
import uuid

import pytest
from prometheus_client import REGISTRY

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchFinalizeJob,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    SearchTaskJob,
    TaskStatus,
    TaskUpdate,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.workers import FinalizeWorker, SearchWorker
from src.workers.finalize_worker import finalize_attempt_status
from src.workers.search_worker import search_attempt_status


class _ListBroker:
    """Redis-mode broker over two lists: one run_once processes exactly one attempt."""

    def __init__(self):
        self.search: list[str] = []
        self.finalize: list[str] = []

    def push_search_job(self, job_id):
        self.search.append(job_id)

    def push_finalize_job(self, job_id):
        self.finalize.append(job_id)

    def pop_search_job(self):
        return self.search.pop(0) if self.search else None

    def pop_finalize_job(self):
        return self.finalize.pop(0) if self.finalize else None


class _StubAnalyzer:
    llm = None

    def run_analysis(self, prompt, tasks, **kwargs):
        return "stub report"


def _count(worker_name: str, job_type: str, status: str) -> float:
    labels = {"worker_name": worker_name, "job_type": job_type, "status": status}
    return REGISTRY.get_sample_value("mas_worker_jobs_total", labels) or 0.0


def _counts(worker_name: str, job_type: str) -> dict[str, float]:
    return {status: _count(worker_name, job_type, status) for status in ("success", "failure")}


def _worker_name() -> str:
    return f"metrics-{uuid.uuid4().hex[:12]}"


def _search_setup():
    store, broker = InMemoryTaskStore(), _ListBroker()
    service = ResearchService(task_store=store, analyzer=_StubAnalyzer(), broker=broker)
    research = store.add_research(ResearchRequest(prompt="metrics topic", depth=SearchDepth.EASY), task_ids=[])
    store.add_task(
        {"id": "t1", "research_id": research.id, "description": "d", "queries": ["q"], "status": TaskStatus.PENDING}
    )
    store.set_research_task_ids(research.id, ["t1"])
    job = store.add_search_task_job("t1", SearchDepth.EASY.value)
    broker.push_search_job(job.id)
    return store, service, broker, job


def _finalize_setup():
    store, broker = InMemoryTaskStore(), _ListBroker()
    service = ResearchService(task_store=store, analyzer=_StubAnalyzer(), broker=broker)
    research = store.add_research(ResearchRequest(prompt="metrics topic", depth=SearchDepth.EASY), task_ids=["t1"])
    store.add_task(
        {
            "id": "t1",
            "research_id": research.id,
            "description": "d",
            "queries": ["q"],
            "status": TaskStatus.COMPLETED,
            "result": [{"url": "https://example.com", "title": "Example", "content": "Body"}],
        }
    )
    _, job = service.enqueue_research_finalization(research.id)
    assert job is not None and broker.finalize == [job.id]
    return store, service, broker, research, job


def _fail_task(store):
    def run(task_id, depth):
        store.update_task(task_id, TaskUpdate(status=TaskStatus.FAILED, log="Error: provider down"))

    return run


@pytest.mark.parametrize("failure", ["task_failed", "exception"])
def test_every_failed_search_attempt_counts_a_failure(mocker, failure):
    store, service, broker, job = _search_setup()
    if failure == "task_failed":
        mocker.patch.object(service, "run_search_task", side_effect=_fail_task(store))
    else:
        mocker.patch.object(service, "run_search_task", side_effect=RuntimeError("search provider down"))
    worker_name = _worker_name()
    worker = SearchWorker(service, worker_name=worker_name)

    assert worker.run_once() == 1
    assert store.get_search_task_job(job.id).status == SearchJobStatus.PENDING  # retry scheduled
    assert _counts(worker_name, "search") == {"success": 0.0, "failure": 1.0}

    assert worker.run_once() == 1
    assert worker.run_once() == 1
    assert store.get_search_task_job(job.id).status == SearchJobStatus.DEAD_LETTER
    assert _counts(worker_name, "search") == {"success": 0.0, "failure": 3.0}


@pytest.mark.parametrize("status", list(SearchJobStatus))
def test_search_attempt_is_a_success_only_when_it_leaves_the_job_completed(status):
    job = SearchTaskJob(id="j", task_id="t", depth=SearchDepth.EASY, status=status)
    expected = "success" if status == SearchJobStatus.COMPLETED else "failure"
    assert search_attempt_status(job) == expected
    assert search_attempt_status(None) == "failure"  # the job vanished mid-attempt


@pytest.mark.parametrize("status", list(FinalizeJobStatus))
def test_finalize_attempt_is_a_success_only_when_completed_under_its_own_lease(status):
    job = ResearchFinalizeJob(id="j", research_id="r", status=status, lease_epoch=4)
    expected = "success" if status == FinalizeJobStatus.COMPLETED else "failure"
    assert finalize_attempt_status(job, claimed_lease_epoch=4) == expected
    assert finalize_attempt_status(job, claimed_lease_epoch=3) == "failure"
    assert finalize_attempt_status(None, claimed_lease_epoch=4) == "failure"


def test_retried_search_counts_the_failure_then_the_success(mocker):
    store, service, _broker, job = _search_setup()
    attempts = []

    def run(task_id, depth):
        attempts.append(task_id)
        if len(attempts) == 1:
            _fail_task(store)(task_id, depth)
            return
        store.update_task(
            task_id,
            TaskUpdate(status=TaskStatus.COMPLETED, result=[{"url": "https://e.com", "title": "A", "content": "B"}]),
        )

    mocker.patch.object(service, "run_search_task", side_effect=run)
    worker_name = _worker_name()
    worker = SearchWorker(service, worker_name=worker_name)

    assert worker.run_once() == 1
    assert worker.run_once() == 1

    assert store.get_search_task_job(job.id).status == SearchJobStatus.COMPLETED
    assert _counts(worker_name, "search") == {"success": 1.0, "failure": 1.0}


def test_search_attempt_that_raises_out_of_the_service_counts_one_failure(mocker):
    _store, service, _broker, _job = _search_setup()
    mocker.patch.object(service, "process_search_task_job", side_effect=RuntimeError("database down"))
    worker_name = _worker_name()

    with pytest.raises(RuntimeError, match="database down"):
        SearchWorker(service, worker_name=worker_name).run_once()

    assert _counts(worker_name, "search") == {"success": 0.0, "failure": 1.0}


def test_every_failed_finalize_attempt_counts_a_failure(mocker):
    store, service, broker, research, job = _finalize_setup()
    mocker.patch.object(service.finalize_graph_runner, "run", side_effect=RuntimeError("llm provider down"))
    worker_name = _worker_name()
    worker = FinalizeWorker(service, worker_name=worker_name)

    assert worker.run_once() == 1
    assert store.get_research_finalize_job(job.id).status == FinalizeJobStatus.PENDING  # retry scheduled
    assert _counts(worker_name, "finalize") == {"success": 0.0, "failure": 1.0}

    assert worker.run_once() == 1
    assert worker.run_once() == 1
    assert store.get_research_finalize_job(job.id).status == FinalizeJobStatus.DEAD_LETTER
    assert store.get_research(research.id).status == ResearchStatus.FAILED
    assert _counts(worker_name, "finalize") == {"success": 0.0, "failure": 3.0}


def test_successful_finalize_attempt_counts_a_success(mocker):
    store, service, _broker, research, job = _finalize_setup()
    mocker.patch.object(service.finalize_graph_runner, "run", return_value="graph report")
    worker_name = _worker_name()

    assert FinalizeWorker(service, worker_name=worker_name).run_once() == 1

    assert store.get_research_finalize_job(job.id).status == FinalizeJobStatus.COMPLETED
    assert store.get_research(research.id).status == ResearchStatus.COMPLETED
    assert _counts(worker_name, "finalize") == {"success": 1.0, "failure": 0.0}


def test_finalize_attempt_that_lost_its_lease_counts_a_failure_even_if_the_job_completed(mocker):
    # Stale recovery requeues the job while this attempt runs (lease_epoch + 1), and the
    # next runner completes it: the job ends COMPLETED, but not by this attempt.
    store, service, _broker, research, job = _finalize_setup()

    def overrun(research_id, *args, **kwargs):
        (recovered,) = store.recover_stale_research_finalize_jobs(datetime.now(timezone.utc) + timedelta(hours=1))
        next_runner = store.claim_research_finalize_job_by_id(recovered.id)
        assert store.complete_research_finalize_job(next_runner.id, research_id, next_runner.lease_epoch, "next")
        return "late report"

    mocker.patch.object(service.finalize_graph_runner, "run", side_effect=overrun)
    worker_name = _worker_name()

    assert FinalizeWorker(service, worker_name=worker_name).run_once() == 1

    assert store.get_research_finalize_job(job.id).status == FinalizeJobStatus.COMPLETED
    assert store.get_research(research.id).final_report == "next"
    assert _counts(worker_name, "finalize") == {"success": 0.0, "failure": 1.0}


def test_finalize_attempt_that_raises_out_of_the_service_counts_one_failure(mocker):
    _store, service, _broker, _research, _job = _finalize_setup()
    mocker.patch.object(service, "process_finalize_job", side_effect=RuntimeError("database down"))
    worker_name = _worker_name()

    with pytest.raises(RuntimeError, match="database down"):
        FinalizeWorker(service, worker_name=worker_name).run_once()

    assert _counts(worker_name, "finalize") == {"success": 0.0, "failure": 1.0}
