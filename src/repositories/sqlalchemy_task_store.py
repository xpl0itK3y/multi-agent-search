import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from src.api.schemas import (
    FinalizeJobStatus,
    ResearchFinalizeJob,
    SearchJobStatus,
    SearchTaskJob,
    ResearchRecord,
    ResearchRequest,
    ResearchStatus,
    SearchTask,
    TaskUpdate,
)
from src.db.models import ResearchFinalizeJobORM, ResearchORM, SearchTaskJobORM, SearchTaskORM
from src.repositories.mappers import (
    research_finalize_job_orm_to_schema,
    research_orm_to_record,
    search_task_job_orm_to_schema,
    search_result_dicts_to_orm,
    search_task_orm_to_schema,
)


class SQLAlchemyTaskStore:
    def __init__(self, session_factory: Callable[[], Session]):
        self.session_factory = session_factory

    @contextmanager
    def session_scope(self):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def add_research(self, request: ResearchRequest, task_ids: list[str]) -> ResearchRecord:
        research = ResearchORM(
            id=str(uuid.uuid4()),
            prompt=request.prompt,
            depth=request.depth.value,
            status=ResearchStatus.PROCESSING.value,
            task_ids=task_ids,
        )
        with self.session_scope() as session:
            session.add(research)
            session.flush()
            session.refresh(research)
            return research_orm_to_record(research)

    def get_research(self, research_id: str) -> ResearchRecord | None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return None
            return research_orm_to_record(research)

    def update_research_status(
        self,
        research_id: str,
        status: ResearchStatus,
        report: str | None = None,
    ) -> ResearchRecord | None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return None

            research.status = status.value
            if report is not None:
                research.final_report = report
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            return research_orm_to_record(research)

    def add_task(self, task_data: dict) -> SearchTask:
        task = SearchTaskORM(
            id=task_data["id"],
            research_id=task_data.get("research_id"),
            description=task_data["description"],
            queries=task_data.get("queries", []),
            status=getattr(task_data.get("status"), "value", task_data.get("status", "pending")),
            logs=task_data.get("logs", []),
        )
        with self.session_scope() as session:
            session.add(task)
            session.flush()
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.id == task.id)
            )
            persisted = session.execute(statement).scalar_one()
            return search_task_orm_to_schema(persisted)

    def set_research_task_ids(
        self,
        research_id: str,
        task_ids: list[str],
    ) -> ResearchRecord | None:
        with self.session_scope() as session:
            research = session.get(ResearchORM, research_id)
            if research is None:
                return None

            research.task_ids = task_ids
            research.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(research)
            return research_orm_to_record(research)

    def add_research_finalize_job(self, research_id: str) -> ResearchFinalizeJob:
        job = ResearchFinalizeJobORM(
            id=str(uuid.uuid4()),
            research_id=research_id,
            status=FinalizeJobStatus.PENDING.value,
        )
        with self.session_scope() as session:
            session.add(job)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def get_research_finalize_job(self, job_id: str) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            job = session.get(ResearchFinalizeJobORM, job_id)
            if job is None:
                return None
            return research_finalize_job_orm_to_schema(job)

    def get_latest_research_finalize_job(
        self,
        research_id: str,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.research_id == research_id)
                .order_by(ResearchFinalizeJobORM.created_at.desc())
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None
            return research_finalize_job_orm_to_schema(job)

    def get_pending_research_finalize_jobs(self) -> list[ResearchFinalizeJob]:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.status == FinalizeJobStatus.PENDING.value)
                .order_by(ResearchFinalizeJobORM.created_at.asc())
            )
            jobs = session.execute(statement).scalars().all()
            return [research_finalize_job_orm_to_schema(job) for job in jobs]

    def claim_next_research_finalize_job(self) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            statement = (
                select(ResearchFinalizeJobORM)
                .where(ResearchFinalizeJobORM.status == FinalizeJobStatus.PENDING.value)
                .order_by(ResearchFinalizeJobORM.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None

            job.status = FinalizeJobStatus.RUNNING.value
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def update_research_finalize_job(
        self,
        job_id: str,
        status: FinalizeJobStatus,
        error: str | None = None,
    ) -> ResearchFinalizeJob | None:
        with self.session_scope() as session:
            job = session.get(ResearchFinalizeJobORM, job_id)
            if job is None:
                return None

            job.status = status.value
            job.error = error
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return research_finalize_job_orm_to_schema(job)

    def add_search_task_job(self, task_id: str, depth: str) -> SearchTaskJob:
        job = SearchTaskJobORM(
            id=str(uuid.uuid4()),
            task_id=task_id,
            depth=depth,
            status=SearchJobStatus.PENDING.value,
        )
        with self.session_scope() as session:
            session.add(job)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def get_search_task_job(self, job_id: str) -> SearchTaskJob | None:
        with self.session_scope() as session:
            job = session.get(SearchTaskJobORM, job_id)
            if job is None:
                return None
            return search_task_job_orm_to_schema(job)

    def get_latest_search_task_job(self, task_id: str) -> SearchTaskJob | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.task_id == task_id)
                .order_by(SearchTaskJobORM.created_at.desc())
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None
            return search_task_job_orm_to_schema(job)

    def get_pending_search_task_jobs(self) -> list[SearchTaskJob]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.status == SearchJobStatus.PENDING.value)
                .order_by(SearchTaskJobORM.created_at.asc())
            )
            jobs = session.execute(statement).scalars().all()
            return [search_task_job_orm_to_schema(job) for job in jobs]

    def claim_next_search_task_job(self) -> SearchTaskJob | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskJobORM)
                .where(SearchTaskJobORM.status == SearchJobStatus.PENDING.value)
                .order_by(SearchTaskJobORM.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job = session.execute(statement).scalars().first()
            if job is None:
                return None

            job.status = SearchJobStatus.RUNNING.value
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def update_search_task_job(
        self,
        job_id: str,
        status: SearchJobStatus,
        error: str | None = None,
    ) -> SearchTaskJob | None:
        with self.session_scope() as session:
            job = session.get(SearchTaskJobORM, job_id)
            if job is None:
                return None

            job.status = status.value
            job.error = error
            job.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(job)
            return search_task_job_orm_to_schema(job)

    def get_task(self, task_id: str) -> SearchTask | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.id == task_id)
            )
            task = session.execute(statement).scalar_one_or_none()
            if task is None:
                return None
            return search_task_orm_to_schema(task)

    def get_all_tasks(self) -> list[SearchTask]:
        with self.session_scope() as session:
            statement = select(SearchTaskORM).options(selectinload(SearchTaskORM.results))
            tasks = session.execute(statement).scalars().all()
            return [search_task_orm_to_schema(task) for task in tasks]

    def get_tasks_by_research(self, research_id: str) -> list[SearchTask]:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.research_id == research_id)
            )
            tasks = session.execute(statement).scalars().all()
            return [search_task_orm_to_schema(task) for task in tasks]

    def update_task(self, task_id: str, update: TaskUpdate) -> SearchTask | None:
        with self.session_scope() as session:
            statement = (
                select(SearchTaskORM)
                .options(selectinload(SearchTaskORM.results))
                .where(SearchTaskORM.id == task_id)
            )
            task = session.execute(statement).scalar_one_or_none()
            if task is None:
                return None

            if update.status is not None:
                task.status = update.status.value
            if update.result is not None:
                task.results = search_result_dicts_to_orm(task_id, update.result)
            if update.log:
                task.logs = [*task.logs, update.log]

            task.updated_at = datetime.now(timezone.utc)
            session.flush()
            session.refresh(task)
            refreshed = session.execute(statement).scalar_one()
            return search_task_orm_to_schema(refreshed)
