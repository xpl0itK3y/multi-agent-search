"""STALLED-SWEEP: a research nothing will move again is finalized or failed (so it can be
retried), and a retry redispatches a task a dead finalize worker left RUNNING.

Replan and tie-break tasks run inline in the finalize worker with no search job; a worker
killed mid-search left them RUNNING for good, and a retry then flipped the research to
PROCESSING with nothing queued. A crash between a status change and the job it comes with
(retry, finalize enqueue) left the research PROCESSING/ANALYZING with no job at all, and
neither stale-job recovery nor retry (FAILED only) could reach it.
"""
from datetime import datetime, timedelta, timezone

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


class _Analyzer:
    llm = None

    def run_analysis(self, prompt, tasks, **kwargs):
        return "report over " + ",".join(sorted(t.id for t in tasks if t.status == TaskStatus.COMPLETED))


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


def _research(store, *tasks, status=ResearchStatus.PROCESSING):
    research = store.add_research(ResearchRequest(prompt="stalled research", depth=SearchDepth.EASY), task_ids=[])
    for task in tasks:
        store.add_task({"research_id": research.id, "description": "d", "queries": ["q"], **task})
    store.set_research_task_ids(research.id, [task["id"] for task in tasks])
    if status != ResearchStatus.PROCESSING:
        store.update_research_status(research.id, status, "Research failed during analysis.")
    return research


def _completed(task_id):
    return {
        "id": task_id,
        "status": TaskStatus.COMPLETED,
        "result": [{"url": f"https://example.com/{task_id}", "title": "A", "content": "Body " * 40}],
    }


def _idle_for(store, research_id, hours=1):
    store.get_research(research_id).updated_at = datetime.now(timezone.utc) - timedelta(hours=hours)


def _run_jobs(store, service, broker):
    for job_id in list(broker.search):
        if store.get_search_task_job(job_id).status == SearchJobStatus.PENDING:
            service.process_search_task_job(job_id)
    for job_id in list(broker.finalize):
        service.process_finalize_job(job_id)


# ── retry ────────────────────────────────────────────────────────────────────


def test_retry_redispatches_a_replan_task_a_dead_finalize_worker_left_running():
    store, broker, service = _service()
    research = _research(
        store, _completed("t1"), {"id": "replan-1", "status": TaskStatus.RUNNING}, status=ResearchStatus.FAILED
    )

    retried = service.retry_research(research.id)

    assert retried.status == ResearchStatus.PROCESSING
    assert store.get_task("replan-1").status == TaskStatus.PENDING
    job = store.get_latest_search_task_job("replan-1")
    assert job is not None and job.status == SearchJobStatus.PENDING and broker.search == [job.id]
    _run_jobs(store, service, broker)
    final = store.get_research(research.id)
    assert final.status == ResearchStatus.COMPLETED and final.final_report == "report over replan-1,t1"


def test_retry_leaves_a_running_task_whose_job_a_worker_still_holds():
    store, broker, service = _service()
    research = _research(
        store, _completed("t1"), {"id": "t2", "status": TaskStatus.RUNNING}, status=ResearchStatus.FAILED
    )
    held = store.add_search_task_job("t2", SearchDepth.EASY.value)
    store.claim_search_task_job_by_id(held.id)

    service.retry_research(research.id)

    assert store.get_task("t2").status == TaskStatus.RUNNING
    assert store.get_latest_search_task_job("t2").id == held.id and broker.search == []


# ── maintenance sweep ────────────────────────────────────────────────────────


def test_sweep_fails_a_processing_research_left_with_an_orphaned_task_so_it_can_be_retried():
    store, broker, service = _service()
    research = _research(store, _completed("t1"), {"id": "replan-1", "status": TaskStatus.RUNNING})
    _idle_for(store, research.id)

    result = service.run_queue_maintenance()

    assert result.stalled_research_ids == [research.id] and result.total_count >= 1
    failed = store.get_research(research.id)
    assert failed.status == ResearchStatus.FAILED
    assert failed.final_report == service.STALLED_RESEARCH_REPORT
    service.retry_research(research.id)
    _run_jobs(store, service, broker)
    assert store.get_research(research.id).status == ResearchStatus.COMPLETED


def test_sweep_finalizes_a_research_whose_searches_all_settled():
    store, broker, service = _service()
    research = _research(store, _completed("t1"), {"id": "t2", "status": TaskStatus.FAILED})
    for task_id, status in (("t1", SearchJobStatus.COMPLETED), ("t2", SearchJobStatus.DEAD_LETTER)):
        job = store.add_search_task_job(task_id, SearchDepth.EASY.value)
        store.update_search_task_job(job.id, status)
    _idle_for(store, research.id)  # the worker died before it enqueued the finalization

    assert service.sweep_stalled_researches() == [research.id]

    current = store.get_research(research.id)
    assert current.status == ResearchStatus.ANALYZING
    job = store.get_latest_research_finalize_job(research.id)
    assert job.status == FinalizeJobStatus.PENDING and broker.finalize == [job.id]
    service.process_finalize_job(job.id)
    assert store.get_research(research.id).final_report == "report over t1"


def test_sweep_finalizes_past_a_chat_follow_up_task_left_running():
    """A chat mini-search runs inline in the API process with no search job; one that
    process died in stays RUNNING. It neither gates nor feeds the report, so the sweep
    finalizes over the report tasks instead of failing the research."""
    store, broker, service = _service()
    chat_id = f"{service._CHAT_TASK_PREFIX}orphaned"
    research = _research(store, _completed("t1"), {"id": chat_id, "status": TaskStatus.RUNNING})
    job = store.add_search_task_job("t1", SearchDepth.EASY.value)
    store.update_search_task_job(job.id, SearchJobStatus.COMPLETED)
    _idle_for(store, research.id)

    assert service.sweep_stalled_researches() == [research.id]

    assert store.get_research(research.id).status == ResearchStatus.ANALYZING
    finalize = store.get_latest_research_finalize_job(research.id)
    assert finalize.status == FinalizeJobStatus.PENDING and broker.finalize == [finalize.id]
    service.process_finalize_job(finalize.id)
    assert store.get_research(research.id).final_report == "report over t1"


def test_sweep_does_not_finalize_a_research_with_only_chat_tasks():
    store, broker, service = _service()
    research = _research(store, _completed(f"{service._CHAT_TASK_PREFIX}only"))
    _idle_for(store, research.id)

    assert service.sweep_stalled_researches() == [research.id]

    assert store.get_research(research.id).status == ResearchStatus.FAILED
    assert broker.finalize == [] and store.get_latest_research_finalize_job(research.id) is None


def test_sweep_fails_an_analyzing_research_left_without_a_live_finalize_job():
    store, broker, service = _service()
    research = _research(store, _completed("t1"), status=ResearchStatus.ANALYZING)
    dead = store.add_research_finalize_job(research.id, max_attempts=1)
    claimed = store.claim_research_finalize_job_by_id(dead.id)
    store.record_research_finalize_job_failure(dead.id, "boom", lease_epoch=claimed.lease_epoch)
    _idle_for(store, research.id)

    assert service.sweep_stalled_researches() == [research.id]

    assert store.get_research(research.id).status == ResearchStatus.FAILED
    service.retry_research(research.id)  # finalize path: reuses the stopped job
    assert broker.finalize == [dead.id]
    _run_jobs(store, service, broker)
    assert store.get_research(research.id).status == ResearchStatus.COMPLETED


def test_sweep_leaves_busy_decomposing_fresh_and_parked_researches_alone():
    store, broker, service = _service()
    searching = _research(store, {"id": "s1", "status": TaskStatus.PENDING})
    store.add_search_task_job("s1", SearchDepth.EASY.value)
    finalizing = _research(store, _completed("f1"), status=ResearchStatus.ANALYZING)
    running = store.add_research_finalize_job(finalizing.id)
    store.claim_research_finalize_job_by_id(running.id)
    decomposing = _research(store)
    store.merge_research_graph_state(decomposing.id, {"decompose_pending": True})
    in_review = _research(store, status=ResearchStatus.PLAN_REVIEW)
    for research in (searching, finalizing, decomposing, in_review):
        _idle_for(store, research.id)
    fresh = _research(store, {"id": "o1", "status": TaskStatus.RUNNING})

    assert service.sweep_stalled_researches() == []

    assert [store.get_research(r.id).status for r in (searching, finalizing, decomposing, in_review, fresh)] == [
        ResearchStatus.PROCESSING,
        ResearchStatus.ANALYZING,
        ResearchStatus.PROCESSING,
        ResearchStatus.PLAN_REVIEW,
        ResearchStatus.PROCESSING,
    ]


def test_sweep_loses_to_a_research_that_moved_after_it_was_listed(monkeypatch):
    store, broker, service = _service()
    research = _research(store, {"id": "o1", "status": TaskStatus.RUNNING})
    _idle_for(store, research.id)
    listed = store.list_stalled_research_ids

    def list_then_touch(stale_before, limit=50):
        ids = listed(stale_before, limit)
        store.append_research_graph_event(research.id, {"step": "search", "detail": "still going"})
        return ids

    monkeypatch.setattr(store, "list_stalled_research_ids", list_then_touch)

    assert service.sweep_stalled_researches() == []
    assert store.get_research(research.id).status == ResearchStatus.PROCESSING


def test_sweep_is_bounded_per_pass(monkeypatch):
    store, broker, service = _service()
    monkeypatch.setattr(service, "STALLED_RESEARCH_SWEEP_LIMIT", 2)
    stalled = []
    for index in range(3):
        research = _research(store, {"id": f"o{index}", "status": TaskStatus.RUNNING})
        _idle_for(store, research.id, hours=3 - index)
        stalled.append(research.id)

    assert service.sweep_stalled_researches() == stalled[:2]
    assert service.sweep_stalled_researches() == stalled[2:]
