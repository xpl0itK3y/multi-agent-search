import uuid
from datetime import datetime, timedelta, timezone

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    SearchDepth,
    ResearchStatus,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.repositories import SQLAlchemyTaskStore


import pytest


@pytest.mark.postgres
def test_sqlalchemy_task_store_persists_research_and_tasks(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    research = store.add_research(
        ResearchRequest(prompt="research topic", depth=SearchDepth.MEDIUM),
        task_ids=[],
    )
    task_id = str(uuid.uuid4())
    task = store.add_task(
        {
            "id": task_id,
            "research_id": research.id,
            "description": "desc",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    synced_research = store.set_research_task_ids(research.id, [task_id])
    updated = store.update_task(
        task.id,
        TaskUpdate(
            status=TaskStatus.COMPLETED,
            result=[{"url": "https://example.com", "title": "Example", "content": "Body"}],
            log="done",
        ),
    )
    research_status = store.update_research_status(
        research.id,
        ResearchStatus.COMPLETED,
        "final report",
    )

    fetched_research = store.get_research(research.id)
    fetched_tasks = store.get_tasks_by_research(research.id)

    assert fetched_research is not None
    assert fetched_research.status == ResearchStatus.COMPLETED
    assert fetched_research.final_report == "final report"
    assert fetched_research.task_ids == [task_id]
    assert research_status is not None
    assert synced_research is not None
    assert synced_research.task_ids == [task_id]
    assert task.status == TaskStatus.PENDING
    assert updated is not None
    assert updated.status == TaskStatus.COMPLETED
    assert updated.logs[-1] == "done"
    assert updated.result[0]["url"] == "https://example.com"
    assert len(fetched_tasks) == 1


@pytest.mark.postgres
def test_sqlalchemy_task_store_persists_finalize_jobs(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    research = store.add_research(
        ResearchRequest(prompt="research topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    job = store.add_research_finalize_job(research.id)

    assert job.status == FinalizeJobStatus.PENDING

    pending = store.get_pending_research_finalize_jobs()
    assert [item.id for item in pending] == [job.id]

    updated = store.update_research_finalize_job(
        job.id,
        FinalizeJobStatus.COMPLETED,
    )
    assert updated is not None
    assert updated.status == FinalizeJobStatus.COMPLETED
    assert store.get_pending_research_finalize_jobs() == []


@pytest.mark.postgres
def test_sqlalchemy_task_store_persists_search_jobs(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    research = store.add_research(
        ResearchRequest(prompt="research topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    store.add_task(
        {
            "id": str(uuid.uuid4()),
            "research_id": research.id,
            "description": "desc",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )
    task = store.get_tasks_by_research(research.id)[0]

    job = store.add_search_task_job(task.id, SearchDepth.HARD.value)
    assert job.status == SearchJobStatus.PENDING
    assert job.depth == SearchDepth.HARD

    pending = store.get_pending_search_task_jobs()
    assert [item.id for item in pending] == [job.id]

    updated = store.update_search_task_job(job.id, SearchJobStatus.COMPLETED)
    assert updated is not None
    assert updated.status == SearchJobStatus.COMPLETED
    assert store.get_latest_search_task_job(task.id).id == job.id


@pytest.mark.postgres
def test_sqlalchemy_task_store_lists_running_and_dead_letter_jobs(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    research = store.add_research(
        ResearchRequest(prompt="research topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_id = str(uuid.uuid4())
    store.add_task(
        {
            "id": task_id,
            "research_id": research.id,
            "description": "desc",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )

    search_running = store.add_search_task_job(task_id, SearchDepth.EASY.value)
    search_dead = store.add_search_task_job(task_id, SearchDepth.EASY.value)
    store.update_search_task_job(search_running.id, SearchJobStatus.RUNNING)
    store.update_search_task_job(search_dead.id, SearchJobStatus.DEAD_LETTER, "boom")

    finalize_running = store.add_research_finalize_job(research.id)
    finalize_dead = store.add_research_finalize_job(research.id)
    store.update_research_finalize_job(finalize_running.id, FinalizeJobStatus.RUNNING)
    store.update_research_finalize_job(finalize_dead.id, FinalizeJobStatus.DEAD_LETTER, "boom")

    assert [job.id for job in store.get_running_search_task_jobs()] == [search_running.id]
    assert [job.id for job in store.get_dead_letter_search_task_jobs()] == [search_dead.id]
    assert [job.id for job in store.get_running_research_finalize_jobs()] == [finalize_running.id]
    assert [job.id for job in store.get_dead_letter_research_finalize_jobs()] == [finalize_dead.id]


@pytest.mark.postgres
def test_sqlalchemy_task_store_requeues_dead_letter_jobs(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    research = store.add_research(
        ResearchRequest(prompt="research topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_id = str(uuid.uuid4())
    store.add_task(
        {
            "id": task_id,
            "research_id": research.id,
            "description": "desc",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )

    search_job = store.add_search_task_job(task_id, SearchDepth.EASY.value, max_attempts=1)
    claimed_search = store.claim_next_search_task_job()
    assert claimed_search is not None
    dead_search = store.record_search_task_job_failure(search_job.id, "boom")
    assert dead_search is not None
    assert dead_search.status == SearchJobStatus.DEAD_LETTER

    requeued_search = store.requeue_search_task_job(search_job.id)
    assert requeued_search is not None
    assert requeued_search.status == SearchJobStatus.PENDING
    assert requeued_search.attempt_count == 0
    assert requeued_search.error is None

    finalize_job = store.add_research_finalize_job(research.id, max_attempts=1)
    claimed_finalize = store.claim_next_research_finalize_job()
    assert claimed_finalize is not None
    dead_finalize = store.record_research_finalize_job_failure(finalize_job.id, "boom")
    assert dead_finalize is not None
    assert dead_finalize.status == FinalizeJobStatus.DEAD_LETTER

    requeued_finalize = store.requeue_research_finalize_job(finalize_job.id)
    assert requeued_finalize is not None
    assert requeued_finalize.status == FinalizeJobStatus.PENDING
    assert requeued_finalize.attempt_count == 0
    assert requeued_finalize.error is None


@pytest.mark.postgres
def test_sqlalchemy_task_store_recovers_stale_running_jobs(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    research = store.add_research(
        ResearchRequest(prompt="research topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_id = str(uuid.uuid4())
    store.add_task(
        {
            "id": task_id,
            "research_id": research.id,
            "description": "desc",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )

    search_job = store.add_search_task_job(task_id, SearchDepth.EASY.value)
    store.update_search_task_job(search_job.id, SearchJobStatus.RUNNING)

    finalize_job = store.add_research_finalize_job(research.id)
    store.update_research_finalize_job(finalize_job.id, FinalizeJobStatus.RUNNING)

    stale_before = datetime.now(timezone.utc) + timedelta(minutes=1)
    recovered_search = store.recover_stale_search_task_jobs(stale_before)
    recovered_finalize = store.recover_stale_research_finalize_jobs(stale_before)

    assert [job.id for job in recovered_search] == [search_job.id]
    assert [job.id for job in recovered_finalize] == [finalize_job.id]
    assert store.get_search_task_job(search_job.id).status == SearchJobStatus.PENDING
    assert store.get_research_finalize_job(finalize_job.id).status == FinalizeJobStatus.PENDING


@pytest.mark.postgres
def test_sqlalchemy_task_store_cleans_up_old_terminal_jobs(postgres_session_factory):
    store = SQLAlchemyTaskStore(postgres_session_factory)

    research = store.add_research(
        ResearchRequest(prompt="research topic", depth=SearchDepth.EASY),
        task_ids=[],
    )
    task_id = str(uuid.uuid4())
    store.add_task(
        {
            "id": task_id,
            "research_id": research.id,
            "description": "desc",
            "queries": ["query"],
            "status": TaskStatus.PENDING,
        }
    )

    old_search = store.add_search_task_job(task_id, SearchDepth.EASY.value)
    old_finalize = store.add_research_finalize_job(research.id)
    store.update_search_task_job(old_search.id, SearchJobStatus.COMPLETED)
    store.update_research_finalize_job(old_finalize.id, FinalizeJobStatus.DEAD_LETTER, "boom")

    older_than = datetime.now(timezone.utc) + timedelta(minutes=1)
    deleted_search = store.cleanup_old_search_task_jobs(older_than)
    deleted_finalize = store.cleanup_old_research_finalize_jobs(older_than)

    assert deleted_search == [old_search.id]
    assert deleted_finalize == [old_finalize.id]
    assert store.get_search_task_job(old_search.id) is None
    assert store.get_research_finalize_job(old_finalize.id) is None
