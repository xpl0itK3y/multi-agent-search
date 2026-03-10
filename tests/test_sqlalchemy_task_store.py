import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchRequest,
    SearchDepth,
    ResearchStatus,
    SearchJobStatus,
    TaskStatus,
    TaskUpdate,
)
from src.config import settings
from src.repositories import SQLAlchemyTaskStore


def _truncate_tables(session_factory):
    with session_factory() as session:
        session.execute(
            text(
                "TRUNCATE TABLE search_task_jobs, research_finalize_jobs, search_results, search_tasks, researches "
                "RESTART IDENTITY CASCADE"
            )
        )
        session.commit()


def test_sqlalchemy_task_store_persists_research_and_tasks():
    engine = create_engine(settings.resolved_database_url, pool_pre_ping=True)
    session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    store = SQLAlchemyTaskStore(session_factory)
    _truncate_tables(session_factory)

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


def test_sqlalchemy_task_store_persists_finalize_jobs():
    engine = create_engine(settings.resolved_database_url, pool_pre_ping=True)
    session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    store = SQLAlchemyTaskStore(session_factory)
    _truncate_tables(session_factory)

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


def test_sqlalchemy_task_store_persists_search_jobs():
    engine = create_engine(settings.resolved_database_url, pool_pre_ping=True)
    session_factory = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    store = SQLAlchemyTaskStore(session_factory)
    _truncate_tables(session_factory)

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

    job = store.add_search_task_job(task.id)
    assert job.status == SearchJobStatus.PENDING

    pending = store.get_pending_search_task_jobs()
    assert [item.id for item in pending] == [job.id]

    updated = store.update_search_task_job(job.id, SearchJobStatus.COMPLETED)
    assert updated is not None
    assert updated.status == SearchJobStatus.COMPLETED
    assert store.get_latest_search_task_job(task.id).id == job.id
