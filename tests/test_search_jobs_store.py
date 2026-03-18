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

    job = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    assert job.task_id == "task-1"
    assert job.depth == SearchDepth.EASY
    assert job.status == SearchJobStatus.PENDING

    pending = store.get_pending_search_task_jobs()
    assert [item.id for item in pending] == [job.id]

    updated = store.update_search_task_job(job.id, SearchJobStatus.RUNNING)
    assert updated is not None
    assert updated.status == SearchJobStatus.RUNNING
    assert store.get_latest_search_task_job("task-1").id == job.id


def test_in_memory_store_claims_next_search_job():
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": "pending",
        }
    )
    first = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    store.add_search_task_job("task-1", SearchDepth.EASY.value)

    claimed = store.claim_next_search_task_job()

    assert claimed is not None
    assert claimed.id == first.id
    assert claimed.status == SearchJobStatus.RUNNING


def test_in_memory_store_requeues_then_dead_letters_search_job():
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": "pending",
        }
    )
    job = store.add_search_task_job("task-1", SearchDepth.EASY.value, max_attempts=2)

    store.claim_next_search_task_job()
    retried = store.record_search_task_job_failure(job.id, "boom-1")
    assert retried is not None
    assert retried.status == SearchJobStatus.PENDING
    assert retried.attempt_count == 1

    store.claim_next_search_task_job()
    dead_lettered = store.record_search_task_job_failure(job.id, "boom-2")
    assert dead_lettered is not None
    assert dead_lettered.status == SearchJobStatus.DEAD_LETTER
    assert dead_lettered.error == "boom-2"


def test_in_memory_store_manually_requeues_dead_letter_search_job():
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": "pending",
        }
    )
    job = store.add_search_task_job("task-1", SearchDepth.EASY.value, max_attempts=1)

    store.claim_next_search_task_job()
    store.record_search_task_job_failure(job.id, "boom")

    requeued = store.requeue_search_task_job(job.id)

    assert requeued is not None
    assert requeued.status == SearchJobStatus.PENDING
    assert requeued.attempt_count == 0
    assert requeued.error is None


def test_in_memory_store_lists_running_and_dead_letter_search_jobs():
    store = InMemoryTaskStore()
    store.add_task(
        {
            "id": "task-1",
            "description": "task",
            "queries": ["query"],
            "status": "pending",
        }
    )
    running = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    dead = store.add_search_task_job("task-1", SearchDepth.EASY.value)
    running.status = SearchJobStatus.RUNNING
    dead.status = SearchJobStatus.DEAD_LETTER

    assert [job.id for job in store.get_running_search_task_jobs()] == [running.id]
    assert [job.id for job in store.get_dead_letter_search_task_jobs()] == [dead.id]
