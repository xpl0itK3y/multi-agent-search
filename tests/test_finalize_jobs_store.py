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
