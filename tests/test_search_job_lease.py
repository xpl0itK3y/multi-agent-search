"""SEARCH-LEASE: a search runner that stale recovery or a requeue took its job from stops,
and one that keeps writing progress keeps its job (every leased task write renews the lease).

Search jobs had no lease. A runner that hung past the job timeout lost its job to stale
recovery, and a second runner claimed it; when the first one came back, it still wrote the
task (results, logs, COMPLETED/FAILED), completed or failed the job, scheduled a retry and
triggered finalization, next to the runner that now held the job. Now the claim's lease
epoch fences every job and task write of the run, as for finalize jobs.
"""
from datetime import datetime, timedelta, timezone

import pytest

from src.api.schemas import ResearchRequest, ResearchStatus, SearchDepth, SearchJobStatus, TaskStatus, TaskUpdate
from src.agents.search import SearchAgent
from src.domain import SearchJobLeaseLost
from src.repositories import InMemoryTaskStore
from src.services import ResearchService
from src.services import research_service as research_service_module
from src.services.search_lease import LeasedTaskStore, holding_search_lease, store_for_search
from src.workers.search_worker import SearchWorker, search_attempt_status


class _Analyzer:
    llm = None

    def run_analysis(self, prompt, tasks, **kwargs):
        return "report"


class _Broker:
    def __init__(self):
        self.search: list[str] = []
        self.finalize: list[str] = []

    def push_search_job(self, job_id):
        self.search.append(job_id)

    def push_finalize_job(self, job_id):
        self.finalize.append(job_id)


def _setup(max_attempts=3):
    store, broker = InMemoryTaskStore(), _Broker()
    service = ResearchService(task_store=store, analyzer=_Analyzer(), broker=broker)
    research = store.add_research(ResearchRequest(prompt="lease research", depth=SearchDepth.EASY), task_ids=[])
    store.update_research_status(research.id, ResearchStatus.PROCESSING)
    store.add_task({"id": "t1", "research_id": research.id, "description": "d", "queries": ["q"], "status": TaskStatus.PENDING})
    store.set_research_task_ids(research.id, ["t1"])
    job = store.add_search_task_job("t1", SearchDepth.EASY.value, max_attempts=max_attempts)
    return store, broker, service, research.id, job.id


def _taken_over(store, job_id):
    """Stale recovery hands the job to a second runner (the epoch moves on)."""
    store.recover_stale_search_task_jobs(datetime.now(timezone.utc) + timedelta(hours=1))
    return store.claim_search_task_job_by_id(job_id)


def test_a_runner_whose_job_was_taken_stops_at_its_next_task_write():
    store, broker, service, research_id, job_id = _setup()
    claimed_epoch = store.claim_search_task_job_by_id(job_id).lease_epoch

    def run_search_task(task_id, depth):
        writes = store_for_search(service.task_store)
        writes.update_task(task_id, TaskUpdate(status=TaskStatus.RUNNING, log="started"))
        _taken_over(store, job_id)  # the runner hangs past the timeout meanwhile
        writes.update_task(task_id, TaskUpdate(status=TaskStatus.COMPLETED, log="late results"))

    service.run_search_task = run_search_task
    result = service.process_search_task_job(job_id, lease_epoch=claimed_epoch)

    # The new runner holds the job; the old one wrote nothing after losing it.
    assert (result.status, result.lease_epoch) == (SearchJobStatus.RUNNING, claimed_epoch + 1)
    task = store.get_task("t1")
    assert "started" in task.logs and "late results" not in task.logs
    assert task.status == TaskStatus.RUNNING
    assert broker.search == [] and broker.finalize == []
    assert store.get_research(research_id).status == ResearchStatus.PROCESSING
    assert search_attempt_status(result, claimed_epoch) == "failure"


def test_a_runner_that_finished_after_losing_its_job_neither_completes_it_nor_finalizes():
    store, broker, service, research_id, job_id = _setup()
    claimed_epoch = store.claim_search_task_job_by_id(job_id).lease_epoch

    def run_search_task(task_id, depth):
        store_for_search(service.task_store).update_task(
            task_id, TaskUpdate(status=TaskStatus.COMPLETED, result=[{"url": "https://a.example", "title": "A"}])
        )
        _taken_over(store, job_id)  # lost between the last task write and the job completion

    service.run_search_task = run_search_task
    result = service.process_search_task_job(job_id, lease_epoch=claimed_epoch)

    assert (result.status, result.lease_epoch) == (SearchJobStatus.RUNNING, claimed_epoch + 1)
    assert broker.finalize == []
    assert store.get_research(research_id).status == ResearchStatus.PROCESSING
    assert store.get_latest_research_finalize_job(research_id) is None


def test_a_runner_that_failed_after_losing_its_job_schedules_no_retry():
    store, broker, service, research_id, job_id = _setup()
    claimed_epoch = store.claim_search_task_job_by_id(job_id).lease_epoch

    def run_search_task(task_id, depth):
        _taken_over(store, job_id)
        raise RuntimeError("provider down")

    service.run_search_task = run_search_task
    result = service.process_search_task_job(job_id, lease_epoch=claimed_epoch)

    assert (result.status, result.error) == (SearchJobStatus.RUNNING, None)
    assert result.attempt_count == 2  # the new runner's claim; the failure was not recorded
    assert broker.search == []
    assert store.get_task("t1").status == TaskStatus.PENDING


def test_the_holder_of_the_lease_still_completes_and_finalizes():
    store, broker, service, research_id, job_id = _setup()
    claimed_epoch = store.claim_search_task_job_by_id(job_id).lease_epoch

    def run_search_task(task_id, depth):
        store_for_search(service.task_store).update_task(
            task_id, TaskUpdate(status=TaskStatus.COMPLETED, result=[{"url": "https://a.example", "title": "A"}])
        )

    service.run_search_task = run_search_task
    result = service.process_search_task_job(job_id, lease_epoch=claimed_epoch)

    assert result.status == SearchJobStatus.COMPLETED
    assert search_attempt_status(result, claimed_epoch) == "success"
    assert store.get_research(research_id).status == ResearchStatus.ANALYZING
    assert len(broker.finalize) == 1


def test_the_worker_passes_its_claims_lease(monkeypatch):
    store, broker, service, _, job_id = _setup()
    seen = {}

    def process(job_id_arg, lease_epoch=None):
        seen["args"] = (job_id_arg, lease_epoch)
        return store.get_search_task_job(job_id_arg)

    monkeypatch.setattr(service, "process_search_task_job", process)
    service.broker = None  # polling mode
    SearchWorker(service).run_once()

    assert seen["args"] == (job_id, 0)


def test_run_search_task_writes_through_the_lease_only_inside_one(monkeypatch):
    store, _, service, _, job_id = _setup()
    stores = []

    class _RecordingAgent:
        def __init__(self, task_store, **kwargs):
            stores.append(task_store)

        def run_task(self, task_id):
            pass

    monkeypatch.setattr(research_service_module, "SearchAgent", _RecordingAgent)
    with holding_search_lease(job_id, 0):
        service.run_search_task("t1", SearchDepth.EASY)
    service.run_search_task("t1", SearchDepth.EASY)  # e.g. a chat mini-search: no job, no lease
    with holding_search_lease(job_id, None):  # an unclaimed job: unfenced
        service.run_search_task("t1", SearchDepth.EASY)

    assert isinstance(stores[0], LeasedTaskStore)
    assert stores[1] is store and stores[2] is store


def test_the_search_agent_stops_on_a_lost_lease_instead_of_failing_the_task():
    store, _, _, _, job_id = _setup()
    store.claim_search_task_job_by_id(job_id)
    _taken_over(store, job_id)
    agent = SearchAgent(task_store=LeasedTaskStore(store, job_id, 0))

    with pytest.raises(SearchJobLeaseLost):
        agent.run_task("t1")

    assert store.get_task("t1").status == TaskStatus.PENDING
    assert store.get_task("t1").logs == []


# ── renewal: a search that keeps writing keeps its lease (SEARCH-LEASE-RENEW) ──


def test_a_long_search_that_keeps_writing_is_not_recovered_from_under_its_runner():
    store, broker, service, research_id, job_id = _setup()
    claimed_epoch = store.claim_search_task_job_by_id(job_id).lease_epoch
    maintenance = {}

    def run_search_task(task_id, depth):
        writes = store_for_search(service.task_store)
        # Ten minutes in, past the 300 s job timeout, and still writing progress.
        store.get_search_task_job(job_id).updated_at -= timedelta(minutes=10)
        writes.update_task(task_id, TaskUpdate(log="Searching for: second query"))
        maintenance["recovered"] = service.recover_stale_search_task_jobs().recovered_job_ids
        writes.update_task(
            task_id, TaskUpdate(status=TaskStatus.COMPLETED, result=[{"url": "https://a.example", "title": "A"}])
        )

    service.run_search_task = run_search_task
    result = service.process_search_task_job(job_id, lease_epoch=claimed_epoch)

    assert maintenance["recovered"] == []
    assert (result.status, result.lease_epoch) == (SearchJobStatus.COMPLETED, claimed_epoch)
    assert search_attempt_status(result, claimed_epoch) == "success"
    assert len(broker.finalize) == 1


def test_a_run_that_stopped_writing_is_still_recovered():
    store, broker, service, _, job_id = _setup()
    store.claim_search_task_job_by_id(job_id)
    store.get_search_task_job(job_id).updated_at -= timedelta(minutes=10)  # hung: no write since

    assert service.recover_stale_search_task_jobs().recovered_job_ids == [job_id]
    job = store.get_search_task_job(job_id)
    assert (job.status, job.lease_epoch) == (SearchJobStatus.PENDING, 1)
    assert broker.search == [job_id]
