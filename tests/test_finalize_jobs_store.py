from datetime import datetime, timedelta, timezone

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    ResearchStatus,
    SearchDepth,
    TaskStatus,
)
from src.repositories import InMemoryTaskStore
from src.services import ResearchService


def test_in_memory_store_tracks_finalize_jobs():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )

    job = store.add_research_finalize_job(research.id)
    assert job.research_id == research.id
    assert job.status == FinalizeJobStatus.PENDING

    pending = store.get_pending_research_finalize_jobs()
    assert [item.id for item in pending] == [job.id]

    updated = store.update_research_finalize_job(
        job.id,
        FinalizeJobStatus.RUNNING,
    )
    assert updated is not None
    assert updated.status == FinalizeJobStatus.RUNNING
    assert store.get_pending_research_finalize_jobs() == []


def test_in_memory_store_claims_next_finalize_job():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    first = store.add_research_finalize_job(research.id)
    store.add_research_finalize_job(research.id)

    claimed = store.claim_next_research_finalize_job()

    assert claimed is not None
    assert claimed.id == first.id
    assert claimed.status == FinalizeJobStatus.RUNNING


def test_in_memory_finalize_lease_renews_and_fences_stale_runner():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    store.update_research_status(research.id, ResearchStatus.ANALYZING)
    job = store.add_research_finalize_job(research.id)
    claimed = store.claim_research_finalize_job_by_id(job.id)
    assert claimed is not None
    stale_epoch = claimed.lease_epoch

    claimed.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    assert store.renew_research_finalize_job_lease(job.id, stale_epoch) is True
    stale_before = datetime.now(timezone.utc) - timedelta(minutes=5)
    assert store.recover_stale_research_finalize_jobs(stale_before) == []

    claimed.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    recovered = store.recover_stale_research_finalize_jobs(stale_before)
    assert recovered[0].lease_epoch == stale_epoch + 1
    assert store.renew_research_finalize_job_lease(job.id, stale_epoch) is False
    assert (
        store.complete_research_finalize_job(
            job.id,
            research.id,
            stale_epoch,
            "stale report",
        )
        is None
    )
    assert store.get_research(research.id).status == ResearchStatus.ANALYZING
    assert store.get_research(research.id).final_report is None

    reclaimed = store.claim_research_finalize_job_by_id(job.id)
    assert reclaimed is not None
    completed = store.complete_research_finalize_job(
        job.id,
        research.id,
        reclaimed.lease_epoch,
        "fresh report",
    )
    assert completed is not None
    assert completed.status == FinalizeJobStatus.COMPLETED
    assert store.get_research(research.id).final_report == "fresh report"


def test_finalize_service_discards_result_after_lease_recovery(monkeypatch):
    store = InMemoryTaskStore()

    class RecoveringAnalyzer:
        job_id: str

        def run_analysis(self, *args, **kwargs):
            active_job = store.get_research_finalize_job(self.job_id)
            active_job.updated_at = datetime.now(timezone.utc) - timedelta(minutes=10)
            recovered = store.recover_stale_research_finalize_jobs(
                datetime.now(timezone.utc) - timedelta(minutes=5)
            )
            assert [item.id for item in recovered] == [self.job_id]
            return "stale report"

    analyzer = RecoveringAnalyzer()
    service = ResearchService(task_store=store, analyzer=analyzer)
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=["task-1"],
    )
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "completed task",
            "queries": ["query"],
            "status": TaskStatus.COMPLETED,
            "result": [
                {
                    "url": "https://example.com",
                    "title": "Example",
                    "content": "Evidence",
                }
            ],
        }
    )
    _, job = service.enqueue_research_finalization(research.id)
    assert job is not None
    analyzer.job_id = job.id
    monkeypatch.setattr(
        "src.services.research_service.settings.use_langgraph_finalize_graph",
        False,
    )

    processed = service.process_finalize_job(job.id)

    assert processed is not None
    assert processed.status == FinalizeJobStatus.PENDING
    assert processed.lease_epoch == 1
    current = store.get_research(research.id)
    assert current.status == ResearchStatus.ANALYZING
    assert current.final_report is None


def test_in_memory_store_requeues_then_dead_letters_finalize_job():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    job = store.add_research_finalize_job(research.id, max_attempts=2)

    store.claim_next_research_finalize_job()
    retried = store.record_research_finalize_job_failure(job.id, "boom-1")
    assert retried is not None
    assert retried.status == FinalizeJobStatus.PENDING
    assert retried.attempt_count == 1

    store.claim_next_research_finalize_job()
    dead_lettered = store.record_research_finalize_job_failure(job.id, "boom-2")
    assert dead_lettered is not None
    assert dead_lettered.status == FinalizeJobStatus.DEAD_LETTER
    assert dead_lettered.error == "boom-2"


def test_in_memory_store_manually_requeues_dead_letter_finalize_job():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    job = store.add_research_finalize_job(research.id, max_attempts=1)

    store.claim_next_research_finalize_job()
    store.record_research_finalize_job_failure(job.id, "boom")

    requeued = store.requeue_research_finalize_job(job.id)

    assert requeued is not None
    assert requeued.status == FinalizeJobStatus.PENDING
    assert requeued.attempt_count == 0
    assert requeued.error is None


def test_in_memory_store_lists_running_and_dead_letter_finalize_jobs():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    running = store.add_research_finalize_job(research.id)
    dead = store.add_research_finalize_job(research.id)
    running.status = FinalizeJobStatus.RUNNING
    dead.status = FinalizeJobStatus.DEAD_LETTER

    assert [job.id for job in store.get_running_research_finalize_jobs()] == [running.id]
    assert [job.id for job in store.get_dead_letter_research_finalize_jobs()] == [dead.id]
