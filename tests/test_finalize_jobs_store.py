from src.api.schemas import FinalizeJobStatus, ResearchRequest, SearchDepth
from src.repositories import InMemoryTaskStore


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
