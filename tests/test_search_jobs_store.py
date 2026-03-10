from src.api.schemas import ResearchRequest, SearchDepth, SearchJobStatus
from src.repositories import InMemoryTaskStore


def test_in_memory_store_tracks_search_jobs():
    store = InMemoryTaskStore()
    research = store.add_research(
        ResearchRequest(prompt="topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    store.add_task(
        {
            "id": "task-1",
            "research_id": research.id,
            "description": "task",
            "queries": ["query"],
            "status": "pending",
        }
    )

    job = store.add_search_task_job("task-1")
    assert job.task_id == "task-1"
    assert job.status == SearchJobStatus.PENDING

    pending = store.get_pending_search_task_jobs()
    assert [item.id for item in pending] == [job.id]

    updated = store.update_search_task_job(job.id, SearchJobStatus.RUNNING)
    assert updated is not None
    assert updated.status == SearchJobStatus.RUNNING
    assert store.get_latest_search_task_job("task-1").id == job.id
